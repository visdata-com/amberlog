//! `/api/` 下 POST 端点共用的请求体读取（REQ-AMB-ING-JSON-ENDPOINT-V2「请求头与请求体大小」、REQ-AMB-ING-BODY-ENCODING，
//! `_json`、`_search`、`_sent` 共用）。
//!
//! 按 REQ-AMB-ERR-ENVELOPE-V3 校验链的「`Content-Encoding`」与「请求体大小与解码」两步：先只凭请求头判编码（415），
//! 再边读边计数判收到的字节（413），最后对 gzip 与 zstd 解压、解压所得越限即停（413）、解码失败 400 `invalid_encoding`。
//! 收到的字节越限时不读完请求体：这种 413 应答携带 `Connection: close`，发出后关闭连接；读完之后的 413 与 400 不带。
//! 不看 `Content-Type`；不用 axum 的 `Bytes`/`Json` 提取器——前者受 `DefaultBodyLimit` 约束，后者检查 `Content-Type`。
//! 请求体是否合法 JSON（下一步）由调用方以 `amberlog_common::json` 判定，解析代码不感知请求是否压缩。

use std::future::poll_fn;
use std::io::Read;
use std::pin::Pin;

use amberlog_common::error::Reason;
use axum::body::{Body, Bytes, HttpBody};
use axum::http::{HeaderMap, HeaderValue, header};
use axum::response::{IntoResponse, Response};

use crate::gateway::ApiError;

/// zstd 帧窗口的上限：8 MiB（REQ-AMB-ING-BODY-ENCODING；RFC 9659 的建议值）。单段帧的窗口即帧内容大小，由 libzstd 按此判定。
const ZSTD_WINDOW_LOG_MAX: u32 = 23;

/// 读出请求体并按 `Content-Encoding` 解码：编码不可接受时 415 `unsupported_encoding`（不读请求体）；
/// 收到的字节数超过 `max_bytes` 时 413 `payload_too_large`（`Content-Length` 已超出则不读，否则读到超出即停）；
/// 解压所得超过 `max_bytes` 时 413（解压到超出即停）；解码失败时 400 `invalid_encoding`。
pub async fn read_body(
    headers: &HeaderMap,
    body: Body,
    max_bytes: u64,
) -> Result<Bytes, BodyError> {
    let coding = content_coding(headers)?;
    let raw = read_raw(headers, body, max_bytes).await?;
    match coding {
        Coding::Identity => Ok(raw),
        Coding::Gzip | Coding::Zstd => decode(coding, &raw, max_bytes).map(Bytes::from),
    }
}

/// 读出收到的原始字节，超过 `max_bytes` 即停。
async fn read_raw(headers: &HeaderMap, body: Body, max_bytes: u64) -> Result<Bytes, BodyError> {
    let too_large = || BodyError::TooLarge {
        max_bytes,
        unread: true,
    };
    let declared = headers
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok());
    if declared.is_some_and(|n| n > max_bytes) {
        return Err(too_large());
    }
    let mut body = body;
    let mut out = Vec::with_capacity(declared.map_or(0, |n| n as usize));
    while let Some(frame) = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
        // 读请求体出错即客户端中途断开或传输层错误：连接已不可用，按请求体不完整处理
        let frame = frame.map_err(|_| BodyError::Incomplete)?;
        if let Ok(data) = frame.into_data() {
            if out.len() as u64 + data.len() as u64 > max_bytes {
                return Err(too_large());
            }
            out.extend_from_slice(&data);
        }
    }
    Ok(Bytes::from(out))
}

/// 解压收到的字节，解压所得至多读到 `max_bytes + 1` 字节即停。空请求体、流不完整、尾随杂字节都是解码失败。
fn decode(coding: Coding, raw: &[u8], max_bytes: u64) -> Result<Vec<u8>, BodyError> {
    if raw.is_empty() {
        return Err(BodyError::InvalidEncoding);
    }
    let decoder: Box<dyn Read + '_> = match coding {
        Coding::Gzip => Box::new(flate2::read::MultiGzDecoder::new(raw)),
        Coding::Zstd => {
            let mut d = zstd::stream::read::Decoder::with_buffer(raw)
                .map_err(|_| BodyError::InvalidEncoding)?;
            d.window_log_max(ZSTD_WINDOW_LOG_MAX)
                .map_err(|_| BodyError::InvalidEncoding)?;
            Box::new(d)
        }
        Coding::Identity => return Ok(raw.to_vec()),
    };
    let mut out = Vec::new();
    let result = decoder
        .take(max_bytes.saturating_add(1))
        .read_to_end(&mut out);
    // 解压所得已越限时回 413，即便其后解码器才会发现损坏（块允许两者皆可）
    if out.len() as u64 > max_bytes {
        return Err(BodyError::TooLarge {
            max_bytes,
            unread: false,
        });
    }
    result.map_err(|_| BodyError::InvalidEncoding)?;
    Ok(out)
}

/// 读取请求体被拒的原因；转成错误信封即对应的应答。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyError {
    /// 415 `unsupported_encoding`。
    UnsupportedEncoding,
    /// 413 `payload_too_large`（收到的或解压所得的字节越限）；`unread` 为真即请求体没有读完。
    TooLarge { max_bytes: u64, unread: bool },
    /// 400 `invalid_encoding`：gzip 或 zstd 解码失败。
    InvalidEncoding,
    /// 读请求体中途出错（客户端断开或传输层错误）：400 `invalid_json`，连接多半已不可用。
    Incomplete,
}

impl From<BodyError> for ApiError {
    fn from(e: BodyError) -> ApiError {
        match e {
            BodyError::UnsupportedEncoding => ApiError::new(
                Reason::UnsupportedEncoding,
                "unsupported Content-Encoding; send the body uncompressed or as gzip or zstd",
            ),
            BodyError::TooLarge { max_bytes, .. } => ApiError::new(
                Reason::PayloadTooLarge,
                format!(
                    "the request body, as received or decompressed, exceeds ingest.max_body_bytes ({max_bytes} bytes)"
                ),
            ),
            BodyError::InvalidEncoding => ApiError::new(
                Reason::InvalidEncoding,
                "the request body could not be decoded per its Content-Encoding",
            ),
            BodyError::Incomplete => ApiError::new(
                Reason::InvalidJson,
                "the request body could not be read completely",
            ),
        }
    }
}

impl IntoResponse for BodyError {
    /// 请求体没有读完时应答携带 `Connection: close`，HTTP 库发出应答后关闭连接（REQ-AMB-ING-BODY-ENCODING）。
    fn into_response(self) -> Response {
        let mut res = ApiError::from(self).into_response();
        if matches!(self, BodyError::TooLarge { unread: true, .. }) {
            res.headers_mut()
                .insert(header::CONNECTION, HeaderValue::from_static("close"));
        }
        res
    }
}

/// 可接受的内容编码。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Coding {
    Identity,
    Gzip,
    Zstd,
}

/// 判定 `Content-Encoding`：请求头至多出现一次；取值按 RFC 9110 的列表写法以逗号切分、去掉首尾空白与空元素后，
/// 恰剩零个（等同 `identity`）或一个可接受的编码（不区分大小写）。其余一律 415。
fn content_coding(headers: &HeaderMap) -> Result<Coding, BodyError> {
    let mut values = headers.get_all(header::CONTENT_ENCODING).iter();
    let Some(value) = values.next() else {
        return Ok(Coding::Identity);
    };
    if values.next().is_some() {
        return Err(BodyError::UnsupportedEncoding);
    }
    let value = value.to_str().map_err(|_| BodyError::UnsupportedEncoding)?;
    let mut codings = value.split(',').map(str::trim).filter(|c| !c.is_empty());
    let coding = match (codings.next(), codings.next()) {
        (None, _) => return Ok(Coding::Identity),
        (Some(c), None) => c,
        (Some(_), Some(_)) => return Err(BodyError::UnsupportedEncoding),
    };
    [
        ("identity", Coding::Identity),
        ("gzip", Coding::Gzip),
        ("x-gzip", Coding::Gzip),
        ("zstd", Coding::Zstd),
    ]
    .into_iter()
    .find(|(name, _)| coding.eq_ignore_ascii_case(name))
    .map(|(_, c)| c)
    .ok_or(BodyError::UnsupportedEncoding)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.append(
                header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    async fn read(pairs: &[(&str, &str)], body: &'static [u8], max: u64) -> Result<Bytes, Reason> {
        read_body(&headers(pairs), Body::from(body), max)
            .await
            .map_err(|e| ApiError::from(e).reason())
    }

    fn gzip(data: &[u8]) -> Vec<u8> {
        use std::io::Write;
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        e.write_all(data).unwrap();
        e.finish().unwrap()
    }

    fn zstd(data: &[u8]) -> Vec<u8> {
        zstd::bulk::compress(data, 3).unwrap()
    }

    async fn read_vec(pairs: &[(&str, &str)], body: Vec<u8>, max: u64) -> Result<Bytes, Reason> {
        read_body(&headers(pairs), Body::from(body), max)
            .await
            .map_err(|e| ApiError::from(e).reason())
    }

    #[tokio::test]
    async fn content_encoding_values() {
        for ok in [
            &[][..],
            &[("content-encoding", "")],
            &[("content-encoding", "identity")],
            &[("content-encoding", "IDENTITY")],
            &[("content-encoding", " , identity , ")],
        ] {
            assert!(read(ok, b"[]", 10).await.is_ok(), "{ok:?}");
        }
        for bad in [
            "br",
            "identity, gzip",
            "gzip, gzip",
            "gzip, zstd",
            "deflate",
            "gzip;q=1",
            "compress",
        ] {
            assert_eq!(
                read(&[("content-encoding", bad)], b"[]", 10).await,
                Err(Reason::UnsupportedEncoding),
                "{bad}"
            );
        }
        // 请求头出现多于一次
        assert_eq!(
            read(
                &[
                    ("content-encoding", "identity"),
                    ("content-encoding", "identity")
                ],
                b"[]",
                10
            )
            .await,
            Err(Reason::UnsupportedEncoding)
        );
    }

    #[tokio::test]
    async fn gzip_and_zstd_are_decoded() {
        let data = br#"[{"a":1},{"a":2}]"#;
        for name in ["gzip", "GZIP", "x-gzip", " , gzip , "] {
            assert_eq!(
                read_vec(&[("content-encoding", name)], gzip(data), 100)
                    .await
                    .unwrap(),
                &data[..],
                "{name}"
            );
        }
        assert_eq!(
            read_vec(&[("content-encoding", "Zstd")], zstd(data), 100)
                .await
                .unwrap(),
            &data[..]
        );
        // 多个 gzip 成员、多个 zstd 帧依次相接
        let two = [gzip(b"[{\"a\":1},"), gzip(b"{\"a\":2}]")].concat();
        assert_eq!(
            read_vec(&[("content-encoding", "gzip")], two, 100)
                .await
                .unwrap(),
            &data[..]
        );
        let two = [zstd(b"[{\"a\":1},"), zstd(b"{\"a\":2}]")].concat();
        assert_eq!(
            read_vec(&[("content-encoding", "zstd")], two, 100)
                .await
                .unwrap(),
            &data[..]
        );
    }

    #[tokio::test]
    async fn decoded_size_limit_and_decode_failures() {
        let data = vec![b' '; 1000];
        // 解压所得恰为上限可接受，超出 413
        assert!(
            read_vec(&[("content-encoding", "gzip")], gzip(&data), 1000)
                .await
                .is_ok()
        );
        assert_eq!(
            read_vec(&[("content-encoding", "gzip")], gzip(&data), 999).await,
            Err(Reason::PayloadTooLarge)
        );
        assert_eq!(
            read_vec(&[("content-encoding", "zstd")], zstd(&data), 999).await,
            Err(Reason::PayloadTooLarge)
        );
        let ok = gzip(b"[]");
        let mut crc = ok.clone();
        let n = crc.len();
        crc[n - 8] ^= 0xFF;
        let truncated = ok[..ok.len() - 3].to_vec();
        let trailing = [ok.clone(), b"xx".to_vec()].concat();
        for bad in [Vec::new(), crc, truncated, trailing, b"not gzip".to_vec()] {
            assert_eq!(
                read_vec(&[("content-encoding", "gzip")], bad.clone(), 100).await,
                Err(Reason::InvalidEncoding),
                "{bad:?}"
            );
        }
        let ok = zstd(b"[]");
        let truncated = ok[..ok.len() - 1].to_vec();
        let trailing = [ok.clone(), b"xx".to_vec()].concat();
        for bad in [Vec::new(), truncated, trailing, b"not zstd".to_vec()] {
            assert_eq!(
                read_vec(&[("content-encoding", "zstd")], bad.clone(), 100).await,
                Err(Reason::InvalidEncoding),
                "{bad:?}"
            );
        }
        // 收到的字节越限：413，不论能否解码
        assert_eq!(
            read_vec(&[("content-encoding", "gzip")], vec![0; 101], 100).await,
            Err(Reason::PayloadTooLarge)
        );
    }

    #[tokio::test]
    async fn size_limit_is_inclusive_and_checked_before_parsing() {
        assert_eq!(read(&[], b"12345", 5).await.unwrap(), &b"12345"[..]);
        assert_eq!(read(&[], b"123456", 5).await, Err(Reason::PayloadTooLarge));
        // 声明的长度已超出：不读即拒
        assert_eq!(
            read(&[("content-length", "6")], b"", 5).await,
            Err(Reason::PayloadTooLarge)
        );
        // 415 先于 413
        assert_eq!(
            read(&[("content-encoding", "br")], b"123456", 5).await,
            Err(Reason::UnsupportedEncoding)
        );
    }

    #[tokio::test]
    async fn content_type_is_not_looked_at() {
        for ct in [
            "text/plain",
            "application/x-www-form-urlencoded",
            "nonsense",
        ] {
            assert!(read(&[("content-type", ct)], b"[]", 10).await.is_ok());
        }
    }
}

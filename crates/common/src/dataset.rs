//! 数据集标识 `<org>/<stream>`（REQ-AMB-DM-NAMESPACE、ADR-0010）。
//!
//! [`Dataset`] 只承载标识，不做校验：进到这里的都是已通过校验的取值（或从对象存储列举、WAL 重放中恢复的取值）。
//! `{org}` 与 `{stream}` 路径段的解码与校验由 [`Dataset::from_path_segments`] 完成，接入面取原始（未解码）路径段交给它；
//! 存在性由存储侧的登记表判定，不在这里。

use std::fmt;

use crate::error::Reason;

/// 当前版本唯一接受的 `{org}`。
pub const DEFAULT_ORG: &str = "default";

/// 一个数据集：治理面的抽象单位，全局唯一。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Dataset {
    pub org: String,
    pub stream: String,
}

impl Dataset {
    pub fn new(org: impl Into<String>, stream: impl Into<String>) -> Self {
        Dataset {
            org: org.into(),
            stream: stream.into(),
        }
    }

    /// 由原始（未解码）的 `{org}`、`{stream}` 路径段得到数据集，按校验链先 `{org}` 后 `{stream}`
    /// （REQ-AMB-ERR-ENVELOPE-V2）：`{org}` 不是 `default` 为 [`Reason::OrgNotFound`]，
    /// `{stream}` 不合命名规则为 [`Reason::InvalidStreamName`]。
    pub fn from_path_segments(raw_org: &str, raw_stream: &str) -> Result<Dataset, Reason> {
        let org = org_from_path_segment(raw_org)?;
        let stream = stream_from_path_segment(raw_stream)?;
        Ok(Dataset { org, stream })
    }
}

/// `<org>/<stream>`：快照 `summary.dataset` 与日志字段 `dataset` 的写法（REQ-AMB-STO-HOT-LAYOUT）。
impl fmt::Display for Dataset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.org, self.stream)
    }
}

/// 由原始 `{org}` 路径段得到 org：先百分号解码，当前版本只接受 `default`，其余（含解码失败）为 [`Reason::OrgNotFound`]。
pub fn org_from_path_segment(raw: &str) -> Result<String, Reason> {
    match percent_decode_segment(raw) {
        Some(org) if org == DEFAULT_ORG => Ok(org),
        _ => Err(Reason::OrgNotFound),
    }
}

/// 由原始 `{stream}` 路径段得到 stream 名：先百分号解码，再按 [`is_valid_stream_name`] 校验；
/// 不匹配（含解码失败）为 [`Reason::InvalidStreamName`]。
pub fn stream_from_path_segment(raw: &str) -> Result<String, Reason> {
    match percent_decode_segment(raw) {
        Some(stream) if is_valid_stream_name(&stream) => Ok(stream),
        _ => Err(Reason::InvalidStreamName),
    }
}

/// stream 名规则 `^[a-z0-9][a-z0-9_]{0,63}$`。
pub fn is_valid_stream_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    (1..=64).contains(&bytes.len())
        && matches!(bytes[0], b'a'..=b'z' | b'0'..=b'9')
        && bytes[1..]
            .iter()
            .all(|b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'_'))
}

/// 按 RFC 3986 §2.1 对一个路径段作百分号解码。`%` 后不是两位十六进制数、或解码所得不是合法 UTF-8 时为 `None`
/// （调用方按名称不匹配处理）。
pub fn percent_decode_segment(raw: &str) -> Option<String> {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hi = hex_value(*bytes.get(i + 1)?)?;
            let lo = hex_value(*bytes.get(i + 2)?)?;
            out.push(hi << 4 | lo);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

fn hex_value(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dataset_display_is_org_slash_stream() {
        assert_eq!(Dataset::new("default", "app").to_string(), "default/app");
    }

    /// 场景「stream 名按正则校验」的表，在路径段一级核对（端到端由 `_json` 的系统测试核对）。
    #[test]
    fn stream_segment_table() {
        let a64 = "a".repeat(64);
        let a65 = "a".repeat(65);
        let ok = ["a", "0abc", a64.as_str(), "%61pp", "a_b", "nginx_access"];
        for raw in ok {
            assert!(stream_from_path_segment(raw).is_ok(), "{raw} 应被接受");
        }
        assert_eq!(stream_from_path_segment("%61pp").unwrap(), "app");
        let bad = [
            a65.as_str(),
            "App",
            "%41pp",
            "a-b",
            "a.b",
            "%C3%A9",
            "%FF",
            "a%2Fb",
            "_system",
            "",
            "%",
            "%6",
            "%zz",
        ];
        for raw in bad {
            assert_eq!(
                stream_from_path_segment(raw),
                Err(Reason::InvalidStreamName),
                "{raw} 应被拒绝"
            );
        }
    }

    #[test]
    fn org_segment_decoded_then_only_default() {
        assert_eq!(org_from_path_segment("default").unwrap(), "default");
        assert_eq!(org_from_path_segment("%64efault").unwrap(), "default");
        assert_eq!(org_from_path_segment("%64%65fault").unwrap(), "default");
        for raw in ["Default", "acme", "", "%FF", "default%2F", "%"] {
            assert_eq!(
                org_from_path_segment(raw),
                Err(Reason::OrgNotFound),
                "{raw}"
            );
        }
    }

    /// 校验链先 `{org}` 后 `{stream}`：两者都不合法时报 `org_not_found`。
    #[test]
    fn org_checked_before_stream() {
        assert_eq!(
            Dataset::from_path_segments("acme", "App"),
            Err(Reason::OrgNotFound)
        );
        assert_eq!(
            Dataset::from_path_segments("default", "App"),
            Err(Reason::InvalidStreamName)
        );
        assert_eq!(
            Dataset::from_path_segments("%64efault", "%61pp"),
            Ok(Dataset::new("default", "app"))
        );
    }
}

//! 身份平台令牌的解析与验证（REQ-AMB-AUTH-OIDC B 组）：JWS 紧凑序列、八种非对称算法、按 `kid` 选公钥、
//! `iss`、`aud`、`exp`、`nbf` 与声明的读取。只做计算，不访问网络；公钥集由 [`super::Idp`] 取得与缓存。
//!
//! 签名验证直接用 ring（rustls 的后端，已在依赖树中）：RS 系列为 RSASSA-PKCS1-v1_5，PS 系列为 RSASSA-PSS，ES 系列为
//! 定长 `r‖s` 的 ECDSA（RFC 7518 §3.3～§3.5）。

use ring::signature::{self, RsaPublicKeyComponents, UnparsedPublicKey};
use serde_json::{Map, Value};

/// 块认的八种签名算法（REQ-AMB-AUTH-OIDC-VERIFY-ALG）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Alg {
    Rs256,
    Rs384,
    Rs512,
    Ps256,
    Ps384,
    Ps512,
    Es256,
    Es384,
}

impl Alg {
    fn parse(s: &str) -> Option<Alg> {
        Some(match s {
            "RS256" => Alg::Rs256,
            "RS384" => Alg::Rs384,
            "RS512" => Alg::Rs512,
            "PS256" => Alg::Ps256,
            "PS384" => Alg::Ps384,
            "PS512" => Alg::Ps512,
            "ES256" => Alg::Es256,
            "ES384" => Alg::Es384,
            _ => return None,
        })
    }

    fn as_str(self) -> &'static str {
        match self {
            Alg::Rs256 => "RS256",
            Alg::Rs384 => "RS384",
            Alg::Rs512 => "RS512",
            Alg::Ps256 => "PS256",
            Alg::Ps384 => "PS384",
            Alg::Ps512 => "PS512",
            Alg::Es256 => "ES256",
            Alg::Es384 => "ES384",
        }
    }

    fn rsa(self) -> Option<&'static signature::RsaParameters> {
        Some(match self {
            Alg::Rs256 => &signature::RSA_PKCS1_2048_8192_SHA256,
            Alg::Rs384 => &signature::RSA_PKCS1_2048_8192_SHA384,
            Alg::Rs512 => &signature::RSA_PKCS1_2048_8192_SHA512,
            Alg::Ps256 => &signature::RSA_PSS_2048_8192_SHA256,
            Alg::Ps384 => &signature::RSA_PSS_2048_8192_SHA384,
            Alg::Ps512 => &signature::RSA_PSS_2048_8192_SHA512,
            Alg::Es256 | Alg::Es384 => return None,
        })
    }

    fn curve(self) -> Option<Curve> {
        match self {
            Alg::Es256 => Some(Curve::P256),
            Alg::Es384 => Some(Curve::P384),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Curve {
    P256,
    P384,
}

impl Curve {
    /// 坐标的字节数。
    fn size(self) -> usize {
        match self {
            Curve::P256 => 32,
            Curve::P384 => 48,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Material {
    Rsa {
        n: Vec<u8>,
        e: Vec<u8>,
    },
    /// 未压缩的点 `0x04‖x‖y`。
    Ec {
        curve: Curve,
        point: Vec<u8>,
    },
}

/// 公钥集中的一把可用公钥（RFC 7517）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Jwk {
    kid: Option<String>,
    /// JWK 自带的 `alg`；带了就只用于该算法。
    alg: Option<String>,
    material: Material,
}

impl Jwk {
    /// 公钥与算法 `alg` 相容：类型（RSA 或椭圆曲线及其曲线）对得上；JWK 带 `alg` 时须与之相同。
    fn compatible(&self, alg: Alg) -> bool {
        if self.alg.as_deref().is_some_and(|a| a != alg.as_str()) {
            return false;
        }
        match &self.material {
            Material::Rsa { .. } => alg.rsa().is_some(),
            Material::Ec { curve, .. } => alg.curve() == Some(*curve),
        }
    }
}

/// 解析公钥集：须是含数组 `keys` 的 JSON 对象，否则为 `None`（按取得失败处理）。数组中不能用于验证签名的元素
/// （不是 RSA 或 P-256、P-384 的公钥，`use` 不是 `sig`，字段缺失或编码有误）被略过。
pub fn parse_jwks(v: &Value) -> Option<Vec<Jwk>> {
    let keys = v.as_object()?.get("keys")?.as_array()?;
    Some(keys.iter().filter_map(jwk).collect())
}

fn jwk(v: &Value) -> Option<Jwk> {
    let o = v.as_object()?;
    let text = |k: &str| o.get(k).and_then(Value::as_str);
    if text("use").is_some_and(|u| u != "sig") {
        return None;
    }
    let material = match text("kty")? {
        "RSA" => Material::Rsa {
            n: strip_leading_zeros(base64url(text("n")?.as_bytes())?),
            e: strip_leading_zeros(base64url(text("e")?.as_bytes())?),
        },
        "EC" => {
            let curve = match text("crv")? {
                "P-256" => Curve::P256,
                "P-384" => Curve::P384,
                _ => return None,
            };
            let x = base64url(text("x")?.as_bytes())?;
            let y = base64url(text("y")?.as_bytes())?;
            if x.len() != curve.size() || y.len() != curve.size() {
                return None;
            }
            let mut point = Vec::with_capacity(1 + 2 * curve.size());
            point.push(0x04);
            point.extend_from_slice(&x);
            point.extend_from_slice(&y);
            Material::Ec { curve, point }
        }
        _ => return None,
    };
    Some(Jwk {
        kid: text("kid").map(str::to_owned),
        alg: text("alg").map(str::to_owned),
        material,
    })
}

fn strip_leading_zeros(mut v: Vec<u8>) -> Vec<u8> {
    let zeros = v.iter().take_while(|b| **b == 0).count();
    v.drain(..zeros.min(v.len().saturating_sub(1)));
    v
}

/// 无填充 base64url（RFC 4648 §5）解码；含该字母表以外的字符或长度不可能时为 `None`。
pub fn base64url(s: &[u8]) -> Option<Vec<u8>> {
    if s.len() % 4 == 1 {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let mut acc = 0u32;
    let mut bits = 0;
    for &c in s {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            _ => return None,
        };
        acc = acc << 6 | u32::from(v);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Some(out)
}

fn is_base64url(seg: &[u8]) -> bool {
    seg.iter()
        .all(|c| c.is_ascii_alphanumeric() || *c == b'-' || *c == b'_')
}

/// 形如 JWS 紧凑序列：三段 base64url 字符串以 `.` 连接（REQ-AMB-AUTH-CREDENTIAL-V3 凭据判定的最后一步）。
pub fn looks_like_jws(token: &[u8]) -> bool {
    let mut n = 0;
    for seg in token.split(|b| *b == b'.') {
        n += 1;
        if n > 3 || !is_base64url(seg) {
            return false;
        }
    }
    n == 3
}

/// 解出头部与载荷、通过算法检查的令牌（REQ-AMB-AUTH-OIDC-VERIFY-FORM、-VERIFY-ALG）。
#[derive(Debug)]
pub struct Parsed {
    alg: Alg,
    /// 头部的 `kid`；头部没有 `kid` 时为 `None`。
    kid: Option<String>,
    payload: Map<String, Value>,
    signing_input: Vec<u8>,
    signature: Vec<u8>,
}

/// 按 RFC 7515 解出头部与载荷（都须是 JSON 对象）并检查 `alg`；任一不合为 `None`（验证不通过）。
pub fn parse(token: &[u8]) -> Option<Parsed> {
    if !looks_like_jws(token) {
        return None;
    }
    let mut parts = token.split(|b| *b == b'.');
    let (h, p, s) = (parts.next()?, parts.next()?, parts.next()?);
    let header: Map<String, Value> = serde_json::from_slice(&base64url(h)?).ok()?;
    let payload: Map<String, Value> = serde_json::from_slice(&base64url(p)?).ok()?;
    let alg = Alg::parse(header.get("alg")?.as_str()?)?;
    let kid = match header.get("kid") {
        None => None,
        Some(Value::String(k)) => Some(k.clone()),
        Some(_) => return None,
    };
    Some(Parsed {
        alg,
        kid,
        payload,
        signing_input: token[..h.len() + 1 + p.len()].to_vec(),
        signature: base64url(s)?,
    })
}

/// 按头部为令牌选公钥的结果（REQ-AMB-AUTH-OIDC-VERIFY-KEY）。
#[derive(Debug, PartialEq, Eq)]
pub enum Choice<'k> {
    Key(&'k Jwk),
    /// 头部的 `kid` 不在公钥集中：先重取公钥集再判定（REQ-AMB-AUTH-OIDC-JWKS-REFETCH）。
    UnknownKid,
    /// 没有可用的公钥：`kid` 在公钥集中而与 `alg` 不相容，或头部没有 `kid` 而与 `alg` 相容的公钥不恰为一把。
    None,
}

impl Parsed {
    /// 选公钥：头部有 `kid` 时取 `kid` 相等且与 `alg` 相容的一把；没有 `kid` 时公钥集中恰有一把与 `alg` 相容才用它。
    pub fn choose<'k>(&self, keys: &'k [Jwk]) -> Choice<'k> {
        match &self.kid {
            Some(kid) => {
                let mut same = keys
                    .iter()
                    .filter(|k| k.kid.as_deref() == Some(kid))
                    .peekable();
                if same.peek().is_none() {
                    return Choice::UnknownKid;
                }
                same.find(|k| k.compatible(self.alg))
                    .map_or(Choice::None, Choice::Key)
            }
            None => {
                let mut fit = keys.iter().filter(|k| k.compatible(self.alg));
                match (fit.next(), fit.next()) {
                    (Some(k), None) => Choice::Key(k),
                    _ => Choice::None,
                }
            }
        }
    }

    /// 以公钥 `key` 验证签名。
    pub fn signature_valid(&self, key: &Jwk) -> bool {
        let (msg, sig) = (&self.signing_input[..], &self.signature[..]);
        match (&key.material, self.alg.rsa(), self.alg.curve()) {
            (Material::Rsa { n, e }, Some(params), None) => RsaPublicKeyComponents { n, e }
                .verify(params, msg, sig)
                .is_ok(),
            (Material::Ec { curve, point }, None, Some(c)) if *curve == c => {
                let alg = match c {
                    Curve::P256 => &signature::ECDSA_P256_SHA256_FIXED,
                    Curve::P384 => &signature::ECDSA_P384_SHA384_FIXED,
                };
                UnparsedPublicKey::new(alg, point).verify(msg, sig).is_ok()
            }
            _ => false,
        }
    }

    /// 核对载荷并读出声明（REQ-AMB-AUTH-OIDC-VERIFY-ISS 至 -PREFERRED-USERNAME）；任一不合为 `None`。
    /// 签名另由 [`Parsed::signature_valid`] 验证。
    pub fn claims(&self, expect: &Expect<'_>) -> Option<Claims> {
        let p = &self.payload;
        if p.get("iss").and_then(Value::as_str) != Some(expect.issuer) {
            return None;
        }
        let aud_ok = match p.get("aud") {
            Some(Value::String(a)) => a == expect.audience,
            Some(Value::Array(list)) => list.iter().any(|a| a.as_str() == Some(expect.audience)),
            _ => false,
        };
        if !aud_ok {
            return None;
        }
        let exp = p.get("exp")?.as_f64()?;
        if expect.now >= exp + CLOCK_SKEW_SECS {
            return None;
        }
        if let Some(nbf) = p.get("nbf")
            && expect.now < nbf.as_f64()? - CLOCK_SKEW_SECS
        {
            return None;
        }
        let subject = p
            .get(expect.subject_claim)?
            .as_str()
            .filter(|s| !s.is_empty())?
            .to_owned();
        let groups = match p.get(expect.groups_claim) {
            Some(Value::Array(list)) => list
                .iter()
                .filter_map(|g| g.as_str().map(str::to_owned))
                .collect(),
            _ => Vec::new(),
        };
        let text = |k: &str| p.get(k).and_then(Value::as_str).map(str::to_owned);
        Some(Claims {
            subject,
            groups,
            client_id: text("client_id").or_else(|| text("azp")),
            preferred_username: text("preferred_username"),
        })
    }
}

/// 时钟偏差的容许量（REQ-AMB-AUTH-OIDC-VERIFY-EXP、-VERIFY-NBF）。
const CLOCK_SKEW_SECS: f64 = 60.0;

/// 载荷要对上的配置与认证时刻。
#[derive(Debug, Clone, Copy)]
pub struct Expect<'a> {
    pub issuer: &'a str,
    pub audience: &'a str,
    pub subject_claim: &'a str,
    pub groups_claim: &'a str,
    /// 认证时刻（Unix 秒）。
    pub now: f64,
}

/// 验证通过的身份平台令牌的声明：主体声明的值、组、客户端编号与可读名（REQ-AMB-AUTH-OIDC-SUBJECT 至 -PREFERRED-USERNAME）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claims {
    pub subject: String,
    pub groups: Vec<String>,
    pub client_id: Option<String>,
    pub preferred_username: Option<String>,
}

#[cfg(test)]
mod tests;

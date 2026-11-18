//! 服务令牌的格式与哈希行（REQ-AMB-AUTH-AUTHZ-TOKEN-FORMAT、-TOKEN-MATCH；`amberlog token new` 按 REQ-AMB-CFG-STARTUP-V10）。

use sha2::{Digest, Sha256};

/// 服务令牌的前缀。
const PREFIX: &str = "amb_";

/// 前缀之后的 base64url 字符数：32 字节随机量的无填充编码。
const BODY_LEN: usize = 43;

/// 哈希行的前缀。
pub const HASH_PREFIX: &str = "sha256:";

/// RFC 4648 §5 的 base64url 字母表。
const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// `credential` 符合服务令牌的格式：`amb_` 后接恰 43 个 base64url 字母表字符。
pub fn is_service_token(credential: &[u8]) -> bool {
    credential.len() == PREFIX.len() + BODY_LEN
        && credential.starts_with(PREFIX.as_bytes())
        && credential[PREFIX.len()..]
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || *b == b'-' || *b == b'_')
}

/// 凭据 UTF-8 字节的 SHA-256 摘要。
pub fn digest(credential: &[u8]) -> [u8; 32] {
    Sha256::digest(credential).into()
}

/// 摘要的哈希行写法：`sha256:` 加 64 个小写十六进制字符。
pub fn hash_line(digest: &[u8; 32]) -> String {
    let mut s = String::with_capacity(HASH_PREFIX.len() + 64);
    s.push_str(HASH_PREFIX);
    for b in digest {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// 解析哈希行；不是 `sha256:` 加 64 个小写十六进制字符为 `None`。
pub fn parse_hash_line(line: &str) -> Option<[u8; 32]> {
    let hex = line.strip_prefix(HASH_PREFIX)?.as_bytes();
    if hex.len() != 64 {
        return None;
    }
    let nibble = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    };
    let mut out = [0u8; 32];
    for (o, [hi, lo]) in out.iter_mut().zip(hex.as_chunks::<2>().0) {
        *o = nibble(*hi)? << 4 | nibble(*lo)?;
    }
    Some(out)
}

/// 以 32 字节随机量 `random` 构成服务令牌：`amb_` 加其无填充 base64url 编码（43 个字符）。
pub fn from_random(random: &[u8; 32]) -> String {
    let mut s = String::with_capacity(PREFIX.len() + BODY_LEN);
    s.push_str(PREFIX);
    let mut bits = 0u32;
    let mut n = 0;
    for &b in random {
        bits = bits << 8 | u32::from(b);
        n += 8;
        while n >= 6 {
            n -= 6;
            s.push(ALPHABET[(bits >> n & 0x3f) as usize] as char);
        }
    }
    if n > 0 {
        s.push(ALPHABET[(bits << (6 - n) & 0x3f) as usize] as char);
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_format() {
        let t = from_random(&[0u8; 32]);
        assert_eq!(t, format!("amb_{}", "A".repeat(43)));
        let t = from_random(&[0xffu8; 32]);
        assert_eq!(t.len(), 47);
        assert!(is_service_token(t.as_bytes()));
        assert!(t.ends_with('8'), "末字符只用高 4 位、低 2 位补 0：{t}");
        for bad in [
            "amb_".to_owned() + &"A".repeat(42),
            "amb_".to_owned() + &"A".repeat(44),
            "AMB_".to_owned() + &"A".repeat(43),
            "amb_".to_owned() + &"A".repeat(42) + "=",
            "amb_".to_owned() + &"A".repeat(42) + "+",
            "s3cret".to_owned(),
        ] {
            assert!(!is_service_token(bad.as_bytes()), "{bad}");
        }
    }

    #[test]
    fn hash_lines() {
        let d = digest(b"abc");
        let line = hash_line(&d);
        assert_eq!(
            line,
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(parse_hash_line(&line), Some(d));
        for bad in [
            "sha256:BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD",
            "sha256:ba7816bf",
            "sha512:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        ] {
            assert_eq!(parse_hash_line(bad), None, "{bad}");
        }
    }
}

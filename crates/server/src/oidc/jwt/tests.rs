//! 令牌验证的单元测试。签名用的密钥都在测试里现场生成：ECDSA 由 ring 生成，RSA 由 `openssl` 命令行生成（ring 不生成 RSA 密钥）。

use ring::rand::SystemRandom;
use ring::signature::{self as sig, EcdsaKeyPair, KeyPair, RsaKeyPair};
use serde_json::{Value, json};

use super::*;

enum Key {
    Ec(EcdsaKeyPair, &'static str),
    Rsa(RsaKeyPair),
}

fn b64(bytes: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let n = chunk.iter().fold(0u32, |n, b| n << 8 | u32::from(*b)) << (8 * (3 - chunk.len()));
        for i in 0..=chunk.len() {
            out.push(A[(n >> (18 - 6 * i) & 0x3f) as usize] as char);
        }
    }
    out
}

fn ec(alg: &'static str) -> Key {
    let rng = SystemRandom::new();
    let a = match alg {
        "ES256" => &sig::ECDSA_P256_SHA256_FIXED_SIGNING,
        _ => &sig::ECDSA_P384_SHA384_FIXED_SIGNING,
    };
    let pkcs8 = EcdsaKeyPair::generate_pkcs8(a, &rng).unwrap();
    Key::Ec(
        EcdsaKeyPair::from_pkcs8(a, pkcs8.as_ref(), &rng).unwrap(),
        alg,
    )
}

fn rsa() -> Key {
    let dir = std::env::temp_dir().join(format!(
        "amberlog-jwt-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let (pem, path) = (dir.join("k.pem"), dir.join("k.der"));
    let (pem_s, der_s) = (pem.to_str().unwrap(), path.to_str().unwrap());
    let openssl = |args: &[&str]| {
        let status = std::process::Command::new("openssl")
            .args(args)
            .stderr(std::process::Stdio::null())
            .status();
        assert!(status.unwrap().success(), "openssl {args:?}");
    };
    openssl(&[
        "genpkey",
        "-algorithm",
        "RSA",
        "-pkeyopt",
        "rsa_keygen_bits:2048",
        "-out",
        pem_s,
    ]);
    // ring 读 PKCS#1 的 DER（RSAPrivateKey）
    openssl(&[
        "pkey",
        "-traditional",
        "-outform",
        "DER",
        "-in",
        pem_s,
        "-out",
        der_s,
    ]);
    let der = std::fs::read(&path).unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
    Key::Rsa(RsaKeyPair::from_der(&der).unwrap())
}

impl Key {
    fn jwk(&self, kid: &str) -> Value {
        match self {
            Key::Ec(k, alg) => {
                let p = k.public_key().as_ref();
                let n = (p.len() - 1) / 2;
                let crv = if *alg == "ES256" { "P-256" } else { "P-384" };
                json!({"kty": "EC", "crv": crv, "kid": kid, "x": b64(&p[1..1 + n]), "y": b64(&p[1 + n..])})
            }
            Key::Rsa(k) => {
                let c: ring::rsa::PublicKeyComponents<Vec<u8>> = k.public().into();
                json!({"kty": "RSA", "kid": kid, "n": b64(&c.n), "e": b64(&c.e)})
            }
        }
    }

    fn sign(&self, alg: &str, header: &Value, payload: &Value) -> String {
        let input = format!(
            "{}.{}",
            b64(header.to_string().as_bytes()),
            b64(payload.to_string().as_bytes())
        );
        let rng = SystemRandom::new();
        let s = match self {
            Key::Ec(k, _) => k.sign(&rng, input.as_bytes()).unwrap().as_ref().to_vec(),
            Key::Rsa(k) => {
                let pad: &dyn sig::RsaEncoding = match alg {
                    "RS256" => &sig::RSA_PKCS1_SHA256,
                    "RS384" => &sig::RSA_PKCS1_SHA384,
                    "RS512" => &sig::RSA_PKCS1_SHA512,
                    "PS256" => &sig::RSA_PSS_SHA256,
                    "PS384" => &sig::RSA_PSS_SHA384,
                    _ => &sig::RSA_PSS_SHA512,
                };
                let mut out = vec![0; k.public().modulus_len()];
                k.sign(pad, &rng, input.as_bytes(), &mut out).unwrap();
                out
            }
        };
        format!("{input}.{}", b64(&s))
    }
}

const NOW: f64 = 1_800_000_000.0;

fn expect() -> Expect<'static> {
    Expect {
        issuer: "https://idp.test",
        audience: "amberlog",
        subject_claim: "sub",
        groups_claim: "groups",
        now: NOW,
    }
}

fn payload() -> Value {
    json!({"iss": "https://idp.test", "aud": "amberlog", "sub": "u-1", "exp": NOW + 600.0, "groups": ["sec"]})
}

fn keys(list: &[Value]) -> Vec<Jwk> {
    parse_jwks(&json!({ "keys": list })).unwrap()
}

/// 完整验证：解析、选公钥、验签名、核对载荷。
fn verify(token: &str, set: &[Jwk]) -> Option<Claims> {
    let p = parse(token.as_bytes())?;
    let Choice::Key(k) = p.choose(set) else {
        return None;
    };
    p.signature_valid(k).then(|| p.claims(&expect())).flatten()
}

#[test]
fn eight_algorithms_verify() {
    let r = rsa();
    let set = keys(&[r.jwk("r")]);
    for alg in ["RS256", "RS384", "RS512", "PS256", "PS384", "PS512"] {
        let t = r.sign(alg, &json!({"alg": alg, "kid": "r"}), &payload());
        assert_eq!(verify(&t, &set).unwrap().subject, "u-1", "{alg}");
    }
    for alg in ["ES256", "ES384"] {
        let k = ec(alg);
        let set = keys(&[k.jwk("e")]);
        let t = k.sign(alg, &json!({"alg": alg, "kid": "e"}), &payload());
        assert_eq!(verify(&t, &set).unwrap().subject, "u-1", "{alg}");
    }
}

#[test]
fn rejected_algorithms_and_forms() {
    let k = ec("ES256");
    let set = keys(&[k.jwk("e")]);
    let none = format!(
        "{}.{}.",
        b64(br#"{"alg":"none"}"#),
        b64(payload().to_string().as_bytes())
    );
    assert!(parse(none.as_bytes()).is_none());
    let hs = k.sign("ES256", &json!({"alg": "HS256", "kid": "e"}), &payload());
    assert!(parse(hs.as_bytes()).is_none());
    // RS 签名冒充 ES：头部写 ES256、公钥是 EC 的，签名长度对不上
    let r = rsa();
    let forged = r.sign("RS256", &json!({"alg": "ES256", "kid": "e"}), &payload());
    assert!(verify(&forged, &set).is_none());
    // 头部或载荷不是 JSON 对象
    let arr = format!("{}.{}.AA", b64(b"[1]"), b64(b"{}"));
    assert!(parse(arr.as_bytes()).is_none());
    // 改动签名一个字节
    let good = k.sign("ES256", &json!({"alg": "ES256", "kid": "e"}), &payload());
    let mut bad = good.clone().into_bytes();
    let last = bad.len() - 2;
    bad[last] = if bad[last] == b'A' { b'B' } else { b'A' };
    assert!(verify(std::str::from_utf8(&bad).unwrap(), &set).is_none());
    assert!(verify(&good, &set).is_some());
}

#[test]
fn key_choice() {
    let a = ec("ES256");
    let b = ec("ES256");
    let p384 = ec("ES384");
    let t = a.sign("ES256", &json!({"alg": "ES256", "kid": "a"}), &payload());
    let parsed = parse(t.as_bytes()).unwrap();
    assert_eq!(parsed.choose(&keys(&[b.jwk("b")])), Choice::UnknownKid);
    // kid 在集中而曲线不符：不重取
    assert_eq!(parsed.choose(&keys(&[p384.jwk("a")])), Choice::None);
    // 另一把同 kid 的公钥签不出它
    let set = keys(&[b.jwk("a")]);
    assert!(verify(&t, &set).is_none());
    // JWK 自带的 alg 与头部不同
    let mut j = a.jwk("a");
    j["alg"] = json!("ES384");
    assert_eq!(parsed.choose(&keys(&[j])), Choice::None);

    let no_kid = a.sign("ES256", &json!({"alg": "ES256"}), &payload());
    assert!(verify(&no_kid, &keys(&[a.jwk("a"), p384.jwk("p")])).is_some());
    let p = parse(no_kid.as_bytes()).unwrap();
    assert_eq!(p.choose(&keys(&[a.jwk("a"), b.jwk("b")])), Choice::None);
}

#[test]
fn payload_checks() {
    let k = ec("ES256");
    let set = keys(&[k.jwk("e")]);
    let with = |f: &dyn Fn(&mut Value)| {
        let mut p = payload();
        f(&mut p);
        verify(
            &k.sign("ES256", &json!({"alg": "ES256", "kid": "e"}), &p),
            &set,
        )
    };
    assert!(with(&|p| p["iss"] = json!("https://idp.test/")).is_none());
    assert!(with(&|p| p["aud"] = json!("other")).is_none());
    assert!(with(&|p| p["aud"] = json!(["x", "amberlog"])).is_some());
    assert!(with(&|p| p["aud"] = json!(["x"])).is_none());
    assert!(with(&|p| p["exp"] = json!(NOW - 120.0)).is_none());
    assert!(with(&|p| p["exp"] = json!(NOW - 30.0)).is_some());
    assert!(with(&|p| p["exp"] = json!(NOW - 60.0)).is_none());
    assert!(with(&|p| p["exp"] = json!("soon")).is_none());
    assert!(
        with(&|p| {
            p.as_object_mut().unwrap().remove("exp");
        })
        .is_none()
    );
    assert!(with(&|p| p["nbf"] = json!(NOW + 120.0)).is_none());
    assert!(with(&|p| p["nbf"] = json!(NOW + 30.0)).is_some());
    assert!(
        with(&|p| {
            p.as_object_mut().unwrap().remove("sub");
        })
        .is_none()
    );
    assert!(with(&|p| p["sub"] = json!("")).is_none());
    assert!(with(&|p| p["sub"] = json!(7)).is_none());
}

#[test]
fn claims_read() {
    let k = ec("ES256");
    let set = keys(&[k.jwk("e")]);
    let read = |p: Value| {
        verify(
            &k.sign("ES256", &json!({"alg": "ES256", "kid": "e"}), &p),
            &set,
        )
        .unwrap()
    };
    let mut p = payload();
    p["groups"] = json!(["a", 1, "b", null]);
    p["client_id"] = json!("c");
    p["azp"] = json!("z");
    p["preferred_username"] = json!("alice");
    let c = read(p);
    assert_eq!(c.groups, vec!["a", "b"]);
    assert_eq!(c.client_id.as_deref(), Some("c"));
    assert_eq!(c.preferred_username.as_deref(), Some("alice"));
    let mut p = payload();
    p["groups"] = json!("sec");
    p["azp"] = json!("z");
    let c = read(p);
    assert!(c.groups.is_empty());
    assert_eq!(c.client_id.as_deref(), Some("z"));
    assert_eq!(c.preferred_username, None);
    let mut p = payload();
    p.as_object_mut().unwrap().remove("groups");
    assert_eq!(read(p).client_id, None);
}

#[test]
fn shapes() {
    assert!(looks_like_jws(b"a.b.c"));
    assert!(looks_like_jws(b"eyJ.eyJ."));
    assert!(!looks_like_jws(b"a.b"));
    assert!(!looks_like_jws(b"a.b.c.d"));
    assert!(!looks_like_jws(b"a.b+.c"));
    assert!(!looks_like_jws(b"amb_abc"));
    assert_eq!(base64url(b"aGVsbG8").unwrap(), b"hello");
    assert_eq!(base64url(b"_-8"), Some(vec![0xff, 0xef]));
    assert!(base64url(b"a").is_none());
    assert!(base64url(b"aa=").is_none());
    // 不能用于签名的公钥被略过，不使整个公钥集无效
    let set = parse_jwks(&json!({"keys": [{"kty": "oct", "k": "AA"}, {"kty": "EC", "crv": "P-521"}, {"use": "enc", "kty": "RSA", "n": "AQAB", "e": "AQAB"}]})).unwrap();
    assert!(set.is_empty());
    assert!(parse_jwks(&json!([])).is_none());
    assert!(parse_jwks(&json!({"keys": {}})).is_none());
}

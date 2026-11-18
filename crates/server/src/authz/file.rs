//! 授权文件的解析与校验（REQ-AMB-AUTH-AUTHZ A 组「授权文件」「服务令牌」）。
//!
//! 授权文件是 TOML：顶层只有 `format_version`（必填，只能为 1）、`tokens`（可选，令牌条目的表数组）与
//! `idp_grants`（可选，身份平台映射的表数组）。任何一处不合都使整个文件无效：不认识的键、类型不符、不认识的角色名、不合法的数据集名模式、重复的条目名或哈希行。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use toml::Value;

use super::token;
use super::{Grant, IdpGrant, IdpMatch, Pattern, Role, Table, TokenEntry};

/// 读到的授权文件内容为什么无效：每条说明一处问题，指出所在的键。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Invalid(pub Vec<String>);

impl std::fmt::Display for Invalid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.join("; "))
    }
}

/// 解析授权文件的文本 `text`。
pub fn parse(text: &str) -> Result<Table, Invalid> {
    let root: toml::Table = text.parse().map_err(|e: toml::de::Error| {
        Invalid(vec![format!("not a valid TOML document: {}", e.message())])
    })?;
    let mut why = Vec::new();
    let mut version = None;
    let mut tokens = Vec::new();
    let mut idp_grants = Vec::new();
    for (key, value) in &root {
        match key.as_str() {
            "format_version" => version = Some(value),
            "tokens" => match value.as_array() {
                Some(items) => {
                    for (i, item) in items.iter().enumerate() {
                        let at = format!("tokens[{i}]");
                        match item.as_table() {
                            Some(t) => {
                                if let Some(entry) = token_entry(t, &at, &mut why) {
                                    tokens.push(entry);
                                }
                            }
                            None => why.push(format!("{at}: must be a table")),
                        }
                    }
                }
                None => why.push("tokens: must be an array of tables".to_owned()),
            },
            "idp_grants" => match value.as_array() {
                Some(items) => {
                    for (i, item) in items.iter().enumerate() {
                        let at = format!("idp_grants[{i}]");
                        match item.as_table() {
                            Some(t) => {
                                if let Some(g) = idp_grant(t, &at, &mut why) {
                                    idp_grants.push(g);
                                }
                            }
                            None => why.push(format!("{at}: must be a table")),
                        }
                    }
                }
                None => why.push("idp_grants: must be an array of tables".to_owned()),
            },
            other => why.push(format!("{other}: unknown top-level key")),
        }
    }
    match version {
        None => why.push("format_version: required key is missing".to_owned()),
        Some(Value::Integer(1)) => {}
        Some(Value::Integer(_)) => why.push("format_version: must be 1".to_owned()),
        Some(_) => why.push("format_version: must be an integer".to_owned()),
    }

    let mut names = HashSet::new();
    let mut by_digest = HashMap::new();
    for (i, entry) in tokens.iter().enumerate() {
        if !names.insert(entry.name.as_str()) {
            why.push(format!(
                "tokens: name {:?} appears more than once",
                entry.name
            ));
        }
        for d in &entry.digests {
            // 只指出条目，不写出哈希行（REQ-AMB-AUTH-AUTHZ：哈希行不进任何日志）
            if let Some(first) = by_digest.insert(*d, i) {
                why.push(format!(
                    "tokens[{first}] and tokens[{i}]: the same hash appears more than once"
                ));
            }
        }
    }
    if !why.is_empty() {
        return Err(Invalid(why));
    }
    Ok(Table {
        tokens,
        by_digest,
        idp_grants,
    })
}

/// `idp_grants` 的一个元素：恰含 `group`、`subject`、`client_id` 之一（非空字符串）与 `grants`
/// （REQ-AMB-AUTH-AUTHZ-FILE-IDP-GRANTS）。
fn idp_grant(t: &toml::Table, at: &str, why: &mut Vec<String>) -> Option<IdpGrant> {
    let before = why.len();
    let mut matchers = Vec::new();
    for (key, value) in t {
        let make = match key.as_str() {
            "group" => IdpMatch::Group,
            "subject" => IdpMatch::Subject,
            "client_id" => IdpMatch::ClientId,
            "grants" => continue,
            _ => {
                why.push(format!("{at}.{key}: unknown key"));
                continue;
            }
        };
        match value.as_str().filter(|s| !s.is_empty()) {
            Some(s) => matchers.push(make(s.to_owned())),
            None => why.push(format!("{at}.{key}: must be a non-empty string")),
        }
    }
    let n = matchers.len();
    if n != 1 && why.len() == before {
        why.push(format!(
            "{at}: must contain exactly one of group, subject, client_id"
        ));
    }
    let grants = match t.get("grants") {
        None => {
            why.push(format!("{at}.grants: required key is missing"));
            None
        }
        Some(v) => grants(v, &format!("{at}.grants"), why),
    };
    if why.len() > before {
        return None;
    }
    Some(IdpGrant {
        matcher: matchers.pop()?,
        grants: Arc::from(grants?),
    })
}

const ENTRY_KEYS: [&str; 5] = ["name", "hashes", "grants", "expires_at", "sources"];

fn token_entry(t: &toml::Table, at: &str, why: &mut Vec<String>) -> Option<TokenEntry> {
    let before = why.len();
    for key in t.keys() {
        if !ENTRY_KEYS.contains(&key.as_str()) {
            why.push(format!("{at}.{key}: unknown key"));
        }
    }
    let name = match t.get("name") {
        None => {
            why.push(format!("{at}.name: required key is missing"));
            None
        }
        Some(Value::String(s)) if valid_entry_name(s) => Some(s.clone()),
        Some(_) => {
            why.push(format!(
                "{at}.name: must be a string matching ^[a-z0-9][a-z0-9_-]{{0,63}}$"
            ));
            None
        }
    };
    let digests = match t.get("hashes") {
        None => {
            why.push(format!("{at}.hashes: required key is missing"));
            None
        }
        Some(v) => match nonempty_strings(v) {
            Some(lines) => {
                let parsed: Option<Vec<_>> =
                    lines.iter().map(|l| token::parse_hash_line(l)).collect();
                if parsed.is_none() {
                    why.push(format!(
                        "{at}.hashes: each item must be sha256: followed by 64 lowercase hex digits"
                    ));
                }
                parsed
            }
            None => {
                why.push(format!("{at}.hashes: must be a non-empty array of strings"));
                None
            }
        },
    };
    let grants = match t.get("grants") {
        None => {
            why.push(format!("{at}.grants: required key is missing"));
            None
        }
        Some(v) => grants(v, &format!("{at}.grants"), why),
    };
    let expires_at = match t.get("expires_at") {
        None => Some(None),
        Some(Value::String(s)) => match parse_rfc3339(s) {
            Some(ts) => Some(Some(ts)),
            None => {
                why.push(format!(
                    "{at}.expires_at: must be an RFC 3339 timestamp with a time zone offset"
                ));
                None
            }
        },
        Some(_) => {
            why.push(format!("{at}.expires_at: must be a string"));
            None
        }
    };
    let sources = match t.get("sources") {
        None => Some(None),
        Some(v) => match nonempty_strings(v) {
            Some(list) if list.iter().all(|s| valid_source(s)) => Some(Some(Arc::from(list))),
            _ => {
                why.push(format!(
                    "{at}.sources: must be a non-empty array of strings matching ^[A-Za-z0-9][A-Za-z0-9_.-]{{0,127}}$"
                ));
                None
            }
        },
    };
    if why.len() > before {
        return None;
    }
    Some(TokenEntry {
        name: name?,
        digests: digests?,
        grants: Arc::from(grants?),
        expires_at: expires_at?,
        sources: sources?,
    })
}

/// `grants`：非空表数组，每个元素只含 `role` 与（带范围的角色必填、其余不得出现的）`streams`（REQ-AMB-AUTH-AUTHZ-FILE-GRANTS）。
/// 身份平台的映射（`idp_grants`）也用这一写法。
pub(super) fn grants(v: &Value, at: &str, why: &mut Vec<String>) -> Option<Vec<Grant>> {
    let Some(items) = v.as_array().filter(|a| !a.is_empty()) else {
        why.push(format!("{at}: must be a non-empty array of tables"));
        return None;
    };
    let before = why.len();
    let mut out = Vec::with_capacity(items.len());
    for (i, item) in items.iter().enumerate() {
        let at = format!("{at}[{i}]");
        let Some(t) = item.as_table() else {
            why.push(format!("{at}: must be a table"));
            continue;
        };
        for key in t.keys() {
            if key != "role" && key != "streams" {
                why.push(format!("{at}.{key}: unknown key"));
            }
        }
        let role = match t.get("role") {
            None => {
                why.push(format!("{at}.role: required key is missing"));
                continue;
            }
            Some(Value::String(s)) => match Role::parse(s) {
                Some(r) => r,
                None => {
                    why.push(format!(
                        "{at}.role: {s:?} is not one of writer, querier, catalog_reader, governance_viewer, metrics_reader, admin"
                    ));
                    continue;
                }
            },
            Some(_) => {
                why.push(format!("{at}.role: must be a string"));
                continue;
            }
        };
        let streams = match (role.scoped(), t.get("streams")) {
            (true, None) => {
                why.push(format!("{at}.streams: required for role {}", role.as_str()));
                continue;
            }
            (false, Some(_)) => {
                why.push(format!(
                    "{at}.streams: not allowed for role {}",
                    role.as_str()
                ));
                continue;
            }
            (false, None) => Vec::new(),
            (true, Some(v)) => {
                let patterns: Option<Vec<Pattern>> = nonempty_strings(v)
                    .and_then(|list| list.iter().map(|s| Pattern::parse(s)).collect());
                match patterns {
                    Some(p) => p,
                    None => {
                        why.push(format!(
                            "{at}.streams: must be a non-empty array of stream names, prefix patterns ending in * or *"
                        ));
                        continue;
                    }
                }
            }
        };
        out.push(Grant { role, streams });
    }
    (why.len() == before).then_some(out)
}

fn nonempty_strings(v: &Value) -> Option<Vec<String>> {
    let items = v.as_array().filter(|a| !a.is_empty())?;
    items
        .iter()
        .map(|i| i.as_str().map(str::to_owned))
        .collect()
}

/// 令牌条目名：`^[a-z0-9][a-z0-9_-]{0,63}$`。
fn valid_entry_name(s: &str) -> bool {
    let b = s.as_bytes();
    (1..=64).contains(&b.len())
        && (b[0].is_ascii_lowercase() || b[0].is_ascii_digit())
        && b[1..]
            .iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'_' || *c == b'-')
}

/// 来源标识：`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`（与 `X-Amberlog-Source` 的取值规则相同）。
fn valid_source(s: &str) -> bool {
    amberlog_ingest::counters::valid_source(s)
}

/// 带时区偏移的 RFC 3339 时刻（RFC 3339 §5.6 `date-time`：`T` 与 `Z` 不区分大小写，秒的小数部分可选）。
fn parse_rfc3339(s: &str) -> Option<jiff::Timestamp> {
    let b = s.as_bytes();
    let digits =
        |r: std::ops::Range<usize>| b.get(r).is_some_and(|d| d.iter().all(u8::is_ascii_digit));
    let shape = b.len() >= 20
        && digits(0..4)
        && b[4] == b'-'
        && digits(5..7)
        && b[7] == b'-'
        && digits(8..10)
        && matches!(b[10], b'T' | b't')
        && digits(11..13)
        && b[13] == b':'
        && digits(14..16)
        && b[16] == b':'
        && digits(17..19);
    if !shape {
        return None;
    }
    let mut i = 19;
    if b[i] == b'.' {
        let start = i + 1;
        i = start;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == start {
            return None;
        }
    }
    let offset_ok = match &b[i..] {
        [b'Z' | b'z'] => true,
        [b'+' | b'-', h1, h2, b':', m1, m2] => [h1, h2, m1, m2].iter().all(|d| d.is_ascii_digit()),
        _ => false,
    };
    if !offset_ok {
        return None;
    }
    // 形状已按 RFC 3339 判定；日期与时刻的取值范围（月、日、闰年、偏移量）交给 jiff。jiff 不认小写的 `t`、`z`，先改写
    let normalized: String = s
        .chars()
        .map(|c| match c {
            't' => 'T',
            'z' => 'Z',
            c => c,
        })
        .collect();
    normalized.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    const H1: &str = "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
    const H2: &str = "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";

    fn ok(text: &str) -> Table {
        parse(text).unwrap_or_else(|e| panic!("应为有效：{e}\n{text}"))
    }

    fn bad(text: &str) -> String {
        match parse(text) {
            Ok(_) => panic!("应为无效：\n{text}"),
            Err(e) => e.to_string(),
        }
    }

    #[test]
    fn valid_files() {
        let t = ok("format_version = 1\n");
        assert!(t.tokens.is_empty());
        let t = ok(&format!(
            r#"format_version = 1
[[tokens]]
name = "vector-a"
hashes = ["{H1}", "{H2}"]
expires_at = "2030-01-01T00:00:00+08:00"
sources = ["vector-a", "host.1"]
grants = [{{ role = "writer", streams = ["app", "app_*"] }}, {{ role = "metrics_reader" }}]
[[tokens]]
name = "root"
hashes = ["sha256:{}"]
grants = [{{ role = "admin" }}, {{ role = "querier", streams = ["*"] }}]
"#,
            "0".repeat(64)
        ));
        assert_eq!(t.tokens.len(), 2);
        assert_eq!(t.tokens[0].digests.len(), 2);
        assert_eq!(t.tokens[0].grants.len(), 2);
        assert!(t.tokens[0].expires_at.is_some());
        assert_eq!(t.tokens[0].sources.as_deref().map(<[String]>::len), Some(2));
        assert_eq!(t.by_digest.len(), 3);
    }

    #[test]
    fn invalid_files() {
        let entry = |grants: &str| {
            format!(
                "format_version = 1\n[[tokens]]\nname = \"q\"\nhashes = [\"{H1}\"]\ngrants = {grants}\n"
            )
        };
        for (text, mention) in [
            (
                entry(r#"[{ role = "superuser", streams = ["app"] }]"#),
                "superuser",
            ),
            ("format_version = 2\n".to_owned(), "format_version"),
            ("tokens = []\n".to_owned(), "format_version"),
            ("format_version = \"1\"\n".to_owned(), "format_version"),
            (
                "format_version = 1\n[[tokens]]\nname = \"q\"\ngrants = [{ role = \"admin\" }]\n"
                    .to_owned(),
                "hashes",
            ),
            (
                entry(r#"[{ role = "admin", streams = ["app"] }]"#),
                "streams",
            ),
            (entry(r#"[{ role = "querier" }]"#), "streams"),
            (
                entry(r#"[{ role = "querier", streams = ["App*"] }]"#),
                "streams",
            ),
            (entry(r#"[{ role = "querier", streams = [] }]"#), "streams"),
            (
                entry(r#"[{ role = "querier", streams = ["a*b"] }]"#),
                "streams",
            ),
            (
                entry(r#"[{ role = "querier", streams = ["app"], extra = 1 }]"#),
                "extra",
            ),
            (entry("[]"), "grants"),
            (
                format!(
                    "format_version = 1\n[[tokens]]\nname = \"q\"\nhashes = [\"{H1}\"]\ngrants = [{{ role = \"admin\" }}]\n[[tokens]]\nname = \"q\"\nhashes = [\"{H2}\"]\ngrants = [{{ role = \"admin\" }}]\n"
                ),
                "more than once",
            ),
            (
                format!(
                    "format_version = 1\n[[tokens]]\nname = \"a\"\nhashes = [\"{H1}\"]\ngrants = [{{ role = \"admin\" }}]\n[[tokens]]\nname = \"b\"\nhashes = [\"{H1}\"]\ngrants = [{{ role = \"admin\" }}]\n"
                ),
                "more than once",
            ),
            ("format_version = 1\nusers = []\n".to_owned(), "users"),
            (
                "format_version = 1\n[[tokens]]\nname = \"Q\"\nhashes = [\"x\"]\n".to_owned(),
                "name",
            ),
            (
                format!(
                    "format_version = 1\n[[tokens]]\nname = \"q\"\nhashes = [\"{H1}\"]\ngrants = [{{ role = \"admin\" }}]\nexpires_at = 2030-01-01T00:00:00Z\n"
                ),
                "expires_at",
            ),
            (
                format!(
                    "format_version = 1\n[[tokens]]\nname = \"q\"\nhashes = [\"{H1}\"]\ngrants = [{{ role = \"admin\" }}]\nexpires_at = \"2030-01-01T00:00:00\"\n"
                ),
                "expires_at",
            ),
            (
                format!(
                    "format_version = 1\n[[tokens]]\nname = \"q\"\nhashes = [\"{H1}\"]\ngrants = [{{ role = \"admin\" }}]\nsources = [\"-x\"]\n"
                ),
                "sources",
            ),
            ("format_version = 1\n[[tokens\n".to_owned(), "TOML"),
        ] {
            let why = bad(&text);
            assert!(why.contains(mention), "{why:?} 应提到 {mention}\n{text}");
        }
    }

    #[test]
    fn rfc3339_timestamps() {
        for ok in [
            "2030-01-01T00:00:00Z",
            "2030-01-01t00:00:00z",
            "2030-01-01T00:00:00.123+08:00",
            "2024-02-29T23:59:59-05:30",
        ] {
            assert!(parse_rfc3339(ok).is_some(), "{ok}");
        }
        for bad in [
            "2030-01-01T00:00:00",
            "2030-01-01 00:00:00Z",
            "2030-01-01T00:00Z",
            "2030-13-01T00:00:00Z",
            "2023-02-29T00:00:00Z",
            "2030-01-01T00:00:00.Z",
            "2030-01-01T00:00:00+0800",
            "2030-01-01T00:00:00Z[UTC]",
        ] {
            assert!(parse_rfc3339(bad).is_none(), "{bad}");
        }
    }
}

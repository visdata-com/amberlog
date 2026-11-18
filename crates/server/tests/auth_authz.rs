//! REQ-AMB-AUTH-AUTHZ 的系统测试：以子进程启动已构建的 `amberlog`，经 HTTP 响应与 stderr 的运行日志断言。
//! 授权文件与服务令牌由测试现场生成（[`common::authz`]），断言里不写令牌原文。

mod common;

use std::process::Output;
use std::time::Duration;

use common::api::{FLUSH_1H, ingest_ok, minute_of, now_us, sent_element};
use common::authz::{self, ServiceToken, events, get_as, search_as, token_entry, wait_event};
use common::{BEARER, Fixture};

const QUERIER_ALL: &str = r#"[{ role = "querier", streams = ["*"] }]"#;

fn run(cmd: &mut std::process::Command) -> (Option<i32>, String) {
    let Output { status, stderr, .. } = cmd.output().expect("启动 amberlog");
    (status.code(), String::from_utf8_lossy(&stderr).into_owned())
}

// ---------- 授权文件 ----------

#[test]
fn authz_invalid_file_refuses_start() {
    let f = Fixture::new();
    let h = ServiceToken::generate().hash;
    let entry = |grants: &str| authz::entry("q", &[&h], grants, "");
    let files = [
        authz::file_text(&[entry(r#"[{ role = "superuser", streams = ["app"] }]"#)]),
        "format_version = 2\n".to_owned(),
        "format_version = 1\n[[tokens]]\nname = \"q\"\ngrants = [{ role = \"admin\" }]\n"
            .to_owned(),
        authz::file_text(&[entry(r#"[{ role = "admin", streams = ["app"] }]"#)]),
        authz::file_text(&[entry(r#"[{ role = "querier", streams = ["App*"] }]"#)]),
        authz::file_text(&[
            entry(r#"[{ role = "admin" }]"#),
            authz::entry(
                "q",
                &[&ServiceToken::generate().hash],
                r#"[{ role = "admin" }]"#,
                "",
            ),
        ]),
        "format_version = 1\nusers = []\n".to_owned(),
    ];
    let mut paths: Vec<_> = files
        .iter()
        .enumerate()
        .map(|(i, text)| {
            let p = f.root().join(format!("authz-{i}.toml"));
            std::fs::write(&p, text).unwrap();
            p
        })
        .collect();
    paths.push(f.root().join("no-such-authz.toml"));
    for path in &paths {
        let value = authz::authz_file_value(path);
        let (code, stderr) = run(&mut f.serve(&[("auth.authz_file", Some(&value))]));
        assert_eq!(code, Some(2), "{}: {stderr}", path.display());
        assert!(stderr.contains(&path.display().to_string()), "{stderr}");
        assert!(!stderr.contains("\"event\":\"starting\""), "{stderr}");
    }
}

#[test]
fn authz_disabled_file_checked() {
    let f = Fixture::new();
    let w = ServiceToken::generate();
    let s = authz::start(
        &f,
        &[token_entry(
            "w",
            &w,
            r#"[{ role = "writer", streams = ["app"] }]"#,
        )],
        &[("auth.enabled", Some("false")), ("auth.token", None)],
    );
    ingest_ok(&s, "other", r#"[{"a":1}]"#);
    let r = s.request(
        "POST",
        "/api/default/_search",
        &[("Content-Type", "application/json")],
        common::api::search_body("SELECT count(*) AS n FROM other").as_bytes(),
    );
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(s.get("/metrics").status, 200);
    assert!(events(&s, "access").is_empty() && events(&s, "access_denied").is_empty());
    drop(s);

    let path = authz::write_text(
        &f,
        &authz::file_text(&[token_entry(
            "w",
            &w,
            r#"[{ role = "superuser", streams = ["app"] }]"#,
        )]),
    );
    let value = authz::authz_file_value(&path);
    let (code, stderr) = run(&mut f.serve(&[
        ("auth.enabled", Some("false")),
        ("auth.token", None),
        ("auth.authz_file", Some(&value)),
    ]));
    assert_eq!(code, Some(2), "{stderr}");
}

// ---------- 服务令牌 ----------

#[test]
fn authz_token_format_and_hash() {
    let f = Fixture::new();
    let q = ServiceToken::generate();
    // 不符合服务令牌格式的字符串 X，其哈希行也登记在同一条目里
    let x = "not-a-service-token-but-registered";
    let s = authz::start(
        &f,
        &[authz::entry(
            "q",
            &[&q.hash, &authz::hash_line(x)],
            QUERIER_ALL,
            "",
        )],
        &[],
    );
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
    let sql = "SELECT count(*) AS n FROM app";
    assert_eq!(search_as(&s, &q.bearer(), sql).status, 200);
    for bearer in [
        format!("Bearer {x}"),
        format!("Bearer {}", &q.token[..q.token.len() - 1]),
    ] {
        search_as(&s, &bearer, sql).assert_envelope(401, "unauthorized");
    }
}

#[test]
fn authz_token_expired_401() {
    let f = Fixture::new();
    let e = ServiceToken::generate();
    let expires = rfc3339_after(Duration::from_secs(5));
    let s = authz::start(
        &f,
        &[authz::entry(
            "e",
            &[&e.hash],
            QUERIER_ALL,
            &format!("expires_at = \"{expires}\""),
        )],
        &[],
    );
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
    let sql = "SELECT count(*) AS n FROM app";
    assert_eq!(search_as(&s, &e.bearer(), sql).status, 200);
    std::thread::sleep(Duration::from_secs(10));
    let r = search_as(&s, &e.bearer(), sql);
    r.assert_envelope(401, "unauthorized");
    let challenge = r
        .header("www-authenticate")
        .expect("401 应带 WWW-Authenticate");
    assert!(!challenge.contains("insufficient_scope"), "{challenge}");
}

/// 此刻之后 `after` 的 UTC 时刻，RFC 3339 写法（精确到秒）。
fn rfc3339_after(after: Duration) -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + after.as_secs();
    let (days, sod) = (secs / 86_400, secs % 86_400);
    // 自 1970-01-01 起的天数换算为公历年月日（H. Hinnant 的 civil_from_days）
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        sod / 3600,
        sod % 3600 / 60,
        sod % 60
    )
}

// ---------------------------------------------------------------------------
// 主体
// ---------------------------------------------------------------------------

const QUERIER_APP: &str = r#"[{ role = "querier", streams = ["app"] }]"#;
const WRITER_ALL: &str = r#"[{ role = "writer", streams = ["*"] }]"#;
const WRITER_APP: &str = r#"[{ role = "writer", streams = ["app"] }]"#;

/// 以 `auth.token` 查 `SELECT count(*) AS n FROM <stream>` 得到的 `n`。
fn count(s: &common::Server, stream: &str) -> serde_json::Value {
    let r = search_as(s, BEARER, &format!("SELECT count(*) AS n FROM {stream}"));
    assert_eq!(r.status, 200, "{}", r.text());
    r.json()["hits"][0]["n"].clone()
}

#[test]
fn authz_auth_token_is_admin() {
    let f = Fixture::new();
    let q = ServiceToken::generate();
    let s = authz::start(&f, &[token_entry("q", &q, QUERIER_APP)], &[]);
    let rs = [
        authz::ingest_as(&s, BEARER, "hr", &[], r#"[{"a":1}]"#),
        search_as(&s, BEARER, "SELECT count(*) AS n FROM hr"),
        get_as(&s, BEARER, "/metrics"),
    ];
    let statuses: Vec<u16> = rs.iter().map(|r| r.status).collect();
    assert_eq!(statuses, [200, 200, 200], "{rs:?}");
    let id = rs[1].header("x-request-id").unwrap().to_owned();
    let e = wait_event(&s, "access", |e| e["request_id"] == id.as_str());
    assert_eq!(e["endpoint"], "search");
    assert_eq!(e["principal"], "token:auth.token");
}

// ---------------------------------------------------------------------------
// 授权文件的重读：块定的周期，真实等待 70 秒与 140 秒
// ---------------------------------------------------------------------------

#[test]
fn authz_reload_revokes_token() {
    let f = Fixture::new();
    let (q, other) = (ServiceToken::generate(), ServiceToken::generate());
    let s = authz::start(&f, &[token_entry("q", &q, QUERIER_ALL)], &[]);
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
    let sql = "SELECT count(*) AS n FROM app";
    assert_eq!(search_as(&s, &q.bearer(), sql).status, 200);
    authz::write(&f, &[token_entry("other", &other, QUERIER_ALL)]);
    std::thread::sleep(Duration::from_secs(70));
    search_as(&s, &q.bearer(), sql).assert_envelope(401, "unauthorized");
}

#[test]
fn authz_reload_invalid_keeps_old() {
    let f = Fixture::new();
    let q = ServiceToken::generate();
    let s = authz::start(&f, &[token_entry("q", &q, QUERIER_ALL)], &[]);
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
    let path = authz::write(
        &f,
        &[token_entry(
            "q",
            &q,
            r#"[{ role = "superuser", streams = ["*"] }]"#,
        )],
    );
    std::thread::sleep(Duration::from_secs(70));
    let sql = "SELECT count(*) AS n FROM app";
    assert_eq!(search_as(&s, &q.bearer(), sql).status, 200);
    assert_eq!(s.get("/readyz").status, 200);
    let failed = events(&s, "authz_reload_failed");
    assert_eq!(failed.len(), 1, "{failed:#?}");
    assert_eq!(failed[0]["level"], "error");
    assert_eq!(failed[0]["authz_file"], path.display().to_string());
    std::thread::sleep(Duration::from_secs(70));
    assert_eq!(events(&s, "authz_reload_failed").len(), 1);
}

// ---------------------------------------------------------------------------
// 端点权限与写入越权
// ---------------------------------------------------------------------------

#[test]
fn authz_writer_cannot_query() {
    let f = Fixture::new();
    let w = ServiceToken::generate();
    let s = authz::start(&f, &[token_entry("w", &w, WRITER_ALL)], &[]);
    let bearer = w.bearer();
    let r = s.request(
        "POST",
        "/api/default/_search",
        &[
            ("Authorization", bearer.as_str()),
            ("Content-Type", "application/json"),
        ],
        b"not json",
    );
    r.assert_envelope(403, "forbidden");
    assert_eq!(
        r.header("www-authenticate"),
        Some(r#"Bearer error="insufficient_scope""#)
    );
}

#[test]
fn authz_querier_cannot_write_or_metrics() {
    let f = Fixture::new();
    let q = ServiceToken::generate();
    let s = authz::start(&f, &[token_entry("q", &q, QUERIER_ALL)], &[]);
    let bearer = q.bearer();
    let element = format!("[{}]", sent_element("src", minute_of(now_us()), 1));
    let rs = [
        authz::ingest_as(&s, &bearer, "app", &[], r#"[{"a":1}]"#),
        s.request(
            "POST",
            "/api/default/app/_sent",
            &[
                ("Authorization", bearer.as_str()),
                ("Content-Type", "application/json"),
            ],
            element.as_bytes(),
        ),
        get_as(&s, &bearer, "/metrics"),
    ];
    for r in &rs {
        r.assert_envelope(403, "forbidden");
    }
    search_as(&s, BEARER, "SELECT count(*) AS n FROM app").assert_envelope(400, "stream_not_found");
}

#[test]
fn authz_write_out_of_scope_403() {
    let f = Fixture::new();
    let w = ServiceToken::generate();
    let s = authz::start(
        &f,
        &[token_entry(
            "w",
            &w,
            r#"[{ role = "writer", streams = ["app_*"] }]"#,
        )],
        &[],
    );
    ingest_ok(&s, "hr", r#"[{"a":1}]"#);
    let hr = authz::ingest_as(&s, &w.bearer(), "hr", &[], r#"[{"a":1}]"#);
    let new_one = authz::ingest_as(&s, &w.bearer(), "new_one", &[], r#"[{"a":1}]"#);
    hr.assert_envelope(403, "forbidden");
    new_one.assert_envelope(403, "forbidden");
    assert_eq!(hr.body, new_one.body);
    search_as(&s, BEARER, "SELECT count(*) AS n FROM new_one")
        .assert_envelope(400, "stream_not_found");
    assert_eq!(count(&s, "hr"), 1);
}

/// `/metrics` 中摄取与查询两类指标的行（REQ-AMB-OBS-HEALTH-METRICS-V7 的 `amberlog_ingest_records_total` 与
/// `amberlog_search_rejected_total`）。
fn ingest_and_search_metrics(s: &common::Server) -> Vec<String> {
    let r = get_as(s, BEARER, "/metrics");
    assert_eq!(r.status, 200, "{}", r.text());
    r.text()
        .lines()
        .filter(|l| {
            l.starts_with("amberlog_ingest_records_total")
                || l.starts_with("amberlog_search_rejected_total")
        })
        .map(str::to_owned)
        .collect()
}

#[test]
fn authz_denied_no_schema_or_metrics_change() {
    let f = Fixture::new();
    let (w, q) = (ServiceToken::generate(), ServiceToken::generate());
    let s = authz::start(
        &f,
        &[
            token_entry("w", &w, r#"[{ role = "writer", streams = ["app_*"] }]"#),
            token_entry("q", &q, QUERIER_APP),
        ],
        &[],
    );
    ingest_ok(&s, "hr", r#"[{"a":1}]"#);
    let fields_before = get_as(&s, BEARER, "/api/default/hr/_fields");
    assert_eq!(fields_before.status, 200, "{}", fields_before.text());
    let metrics_before = ingest_and_search_metrics(&s);
    assert!(
        metrics_before
            .iter()
            .any(|l| l.contains(r#"stream="hr""#) && l.contains(r#"outcome="accepted""#)),
        "{metrics_before:?}"
    );

    authz::ingest_as(&s, &w.bearer(), "hr", &[], r#"[{"a":1,"b":"x"}]"#)
        .assert_envelope(403, "forbidden");
    search_as(&s, &q.bearer(), "SELECT count(*) AS n FROM hr")
        .assert_envelope(400, "stream_not_found");

    assert_eq!(
        get_as(&s, BEARER, "/api/default/hr/_fields").body,
        fields_before.body
    );
    assert_eq!(ingest_and_search_metrics(&s), metrics_before);
    assert_eq!(count(&s, "hr"), 1);
}

#[test]
fn authz_name_check_before_authz() {
    let f = Fixture::new();
    let w = ServiceToken::generate();
    let s = authz::start(&f, &[token_entry("w", &w, WRITER_APP)], &[]);
    authz::ingest_as(&s, &w.bearer(), "Bad-Name", &[], r#"[{"a":1}]"#)
        .assert_envelope(400, "invalid_stream_name");
    let bearer = w.bearer();
    s.request(
        "POST",
        "/api/acme/app/_json",
        &[
            ("Authorization", bearer.as_str()),
            ("Content-Type", "application/json"),
        ],
        br#"[{"a":1}]"#,
    )
    .assert_envelope(404, "org_not_found");
}

// ---------------------------------------------------------------------------
// 来源限定
// ---------------------------------------------------------------------------

#[test]
fn authz_source_restricted_json() {
    let f = Fixture::new();
    let st = ServiceToken::generate();
    let s = authz::start(
        &f,
        &[authz::entry(
            "s",
            &[&st.hash],
            WRITER_APP,
            r#"sources = ["vector-a"]"#,
        )],
        &[],
    );
    let send = |headers: &[(&str, &str)]| {
        authz::ingest_as(&s, &st.bearer(), "app", headers, r#"[{"a":1}]"#)
    };
    for headers in [
        &[][..],
        &[("X-Amberlog-Source", "vector-b")],
        &[
            ("X-Amberlog-Source", "vector-a"),
            ("X-Amberlog-Source", "vector-a"),
        ],
    ] {
        send(headers).assert_envelope(403, "forbidden");
    }
    let ok = send(&[("X-Amberlog-Source", "vector-a")]);
    assert_eq!(ok.status, 200, "{}", ok.text());
    assert_eq!(count(&s, "app"), 1);
}

#[test]
fn authz_source_unrestricted() {
    let f = Fixture::new();
    let w = ServiceToken::generate();
    let s = authz::start(&f, &[token_entry("w", &w, WRITER_APP)], &[]);
    for headers in [&[][..], &[("X-Amberlog-Source", "any.src")]] {
        let r = authz::ingest_as(&s, &w.bearer(), "app", headers, r#"[{"a":1}]"#);
        assert_eq!(r.status, 200, "{headers:?}：{}", r.text());
    }
}

// ---------------------------------------------------------------------------
// 认证关闭
// ---------------------------------------------------------------------------

#[test]
fn authz_disabled_allows_all() {
    let f = Fixture::new();
    let s = f.start(&[("auth.enabled", Some("false")), ("auth.token", None)]);
    ingest_ok(&s, "hr", r#"[{"a":1}]"#);
    let search = |sql: &str| {
        s.request(
            "POST",
            "/api/default/_search",
            &[("Content-Type", "application/json")],
            common::api::search_body(sql).as_bytes(),
        )
    };
    let ledger = search("SELECT dataset FROM _ledger");
    let rs = [
        search("SELECT count(*) AS n FROM hr"),
        s.get("/api/default/hr/_fields"),
        s.get("/metrics"),
        s.get("/iceberg/v1/config"),
        s.request(
            "POST",
            "/api/default/hr/_json",
            &[("Content-Type", "application/json")],
            br#"[{"a":1}]"#,
        ),
    ];
    assert_eq!(ledger.status, 200, "{}", ledger.text());
    for r in &rs {
        assert_eq!(r.status, 200, "{r:?}");
    }
    let rows = ledger.json()["hits"].as_array().unwrap().clone();
    assert!(
        rows.iter().any(|h| h["dataset"] == "default/hr"),
        "{rows:?}"
    );
}

// ---------------------------------------------------------------------------
// 访问事件
// ---------------------------------------------------------------------------

#[test]
fn authz_log_access_values() {
    for (value, want) in [
        ("none", &[][..]),
        ("reads", &["search"][..]),
        ("all", &["json", "search", "metrics"][..]),
    ] {
        let f = Fixture::new();
        let a = ServiceToken::generate();
        let quoted = format!("\"{value}\"");
        let s = authz::start(
            &f,
            &[token_entry("a", &a, r#"[{ role = "admin" }]"#)],
            &[("log.access", Some(&quoted))],
        );
        let bearer = a.bearer();
        let rs = [
            authz::ingest_as(&s, &bearer, "app", &[], r#"[{"a":1}]"#),
            search_as(&s, &bearer, "SELECT count(*) AS n FROM app"),
            get_as(&s, &bearer, "/metrics"),
        ];
        for r in &rs {
            assert_eq!(r.status, 200, "{value}：{r:?}");
        }
        // 每个请求作答之前已写出它的事件；以最后一个请求的一行（或 `none` 时的一个 401 之后的片刻）为准
        let last = rs[2].header("x-request-id").unwrap().to_owned();
        if want.contains(&"metrics") {
            wait_event(&s, "access", |e| e["request_id"] == last.as_str());
        } else {
            std::thread::sleep(Duration::from_millis(500));
        }
        let got: Vec<String> = events(&s, "access")
            .iter()
            .map(|e| e["endpoint"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(got, want, "log.access = {value}");
    }
}

#[test]
fn authz_denied_event_and_no_401() {
    let f = Fixture::new();
    let w = ServiceToken::generate();
    let s = authz::start(
        &f,
        &[token_entry("w", &w, WRITER_APP)],
        &[("log.access", Some("\"none\""))],
    );
    let sql = "SELECT count(*) AS n FROM app";
    search_as(&s, &w.bearer(), sql).assert_envelope(403, "forbidden");
    let wrong = ServiceToken::generate();
    search_as(&s, &wrong.bearer(), sql).assert_envelope(401, "unauthorized");
    let e = wait_event(&s, "access_denied", |_| true);
    // 401 若逐条记，它的一行此时也已写出：再等片刻收齐
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(events(&s, "access_denied").len(), 1);
    assert_eq!(e["level"], "warn");
    assert_eq!(e["status"], 403);
    assert_eq!(e["endpoint"], "search");
    assert_eq!(e["dataset"], serde_json::Value::Null);
    assert_eq!(e["principal"], "token:w");
    for event in ["access", "access_denied"] {
        assert!(
            events(&s, event).iter().all(|e| e["status"] != 401),
            "{event}"
        );
    }
}

#[test]
fn authz_access_event_no_secrets() {
    let f = Fixture::new();
    let (q, w) = (ServiceToken::generate(), ServiceToken::generate());
    let entries = [
        token_entry("q", &q, QUERIER_APP),
        token_entry("w", &w, WRITER_APP),
    ];
    // 先以不记访问的配置写入 `app` 并停机提交，前提里的这次写入不进本次的运行日志
    let mut s = authz::start(&f, &entries, &[("log.access", Some("\"none\"")), FLUSH_1H]);
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
    s.signal("TERM");
    assert_eq!(s.wait_exit(Duration::from_secs(30)), Some(Some(0)));
    drop(s);
    let s = authz::start(&f, &entries, &[("log.access", Some("\"all\""))]);
    let fields = get_as(&s, &q.bearer(), "/api/default/app/_fields?probe=qs-marker");
    assert_eq!(fields.status, 200, "{}", fields.text());
    let json = authz::ingest_as(&s, &w.bearer(), "app", &[], r#"[{"note":"body-marker"}]"#);
    assert_eq!(json.status, 200, "{}", json.text());
    let denied = search_as(&s, &w.bearer(), "SELECT count(*) AS n FROM app");
    denied.assert_envelope(403, "forbidden");
    let id = denied.header("x-request-id").unwrap().to_owned();
    wait_event(&s, "access_denied", |e| e["request_id"] == id.as_str());
    let access = events(&s, "access");
    let endpoints: Vec<&str> = access
        .iter()
        .map(|e| e["endpoint"].as_str().unwrap())
        .collect();
    assert_eq!(endpoints, ["fields", "json"]);
    let denied = events(&s, "access_denied");
    assert_eq!(denied.len(), 1);
    assert_eq!(denied[0]["endpoint"], "search");
    let secrets: Vec<String> = [&q, &w]
        .iter()
        .flat_map(|t| {
            [
                t.token.clone(),
                t.hash.trim_start_matches("sha256:").to_owned(),
            ]
        })
        .collect();
    for line in access.iter().chain(&denied) {
        let text = line.to_string();
        // 以变量比较，令牌原文不进断言消息
        assert!(
            !secrets.iter().any(|x| text.contains(x.as_str())),
            "访问事件含令牌原文或摘要（endpoint {}）",
            line["endpoint"]
        );
        for marker in ["qs-marker", "body-marker", "SELECT"] {
            assert!(!text.contains(marker), "{marker}：{text}");
        }
    }
}

// ---------------------------------------------------------------------------
// 依赖 `_search` 请求体一步（REQ-AMB-QRY-SEARCH-ENDPOINT-V10 的报告顺序）的场景
// ---------------------------------------------------------------------------

#[test]
fn authz_principal_not_from_headers() {
    let f = Fixture::new();
    let q = ServiceToken::generate();
    let s = authz::start(&f, &[token_entry("q", &q, QUERIER_APP)], &[]);
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
    ingest_ok(&s, "hr", r#"[{"a":1}]"#);
    let bearer = q.bearer();
    let r = s.request(
        "POST",
        "/api/default/_search",
        &[
            ("Authorization", bearer.as_str()),
            ("Content-Type", "application/json"),
            ("X-Amberlog-User", "admin"),
            ("X-Forwarded-User", "admin"),
            ("X-Amberlog-Stream", "app"),
        ],
        common::api::search_body("SELECT count(*) AS n FROM hr").as_bytes(),
    );
    r.assert_envelope(400, "stream_not_found");
    let id = r.header("x-request-id").unwrap().to_owned();
    let e = wait_event(&s, "access", |e| e["request_id"] == id.as_str());
    assert_eq!(e["principal"], "token:q");
}

#[test]
fn authz_write_target_from_path_only() {
    let f = Fixture::new();
    let w = ServiceToken::generate();
    let s = authz::start(
        &f,
        &[token_entry(
            "w",
            &w,
            r#"[{ role = "writer", streams = ["app_*"] }]"#,
        )],
        &[],
    );
    ingest_ok(&s, "app_a", r#"[{"a":1}]"#);
    ingest_ok(&s, "hr", r#"[{"a":1}]"#);
    let bearer = w.bearer();
    for other in ["hr", "ops"] {
        let r = authz::ingest_as(
            &s,
            &bearer,
            "app_a",
            &[("X-Amberlog-Stream", other), ("X-Amberlog-Source", other)],
            r#"[{"a":2}]"#,
        );
        assert_eq!(r.status, 200, "{other}：{}", r.text());
    }
    assert_eq!(count(&s, "app_a"), 3);
    assert_eq!(count(&s, "hr"), 1);
    search_as(&s, BEARER, "SELECT count(*) AS n FROM ops").assert_envelope(400, "stream_not_found");
}

#[test]
fn authz_scope_patterns() {
    let f = Fixture::new();
    let (q1, q2, q3) = (
        ServiceToken::generate(),
        ServiceToken::generate(),
        ServiceToken::generate(),
    );
    let s = authz::start(
        &f,
        &[
            token_entry("q1", &q1, r#"[{ role = "querier", streams = ["app_*"] }]"#),
            token_entry("q2", &q2, QUERIER_APP),
            token_entry("q3", &q3, QUERIER_ALL),
        ],
        &[],
    );
    for stream in ["app", "app_web", "apple"] {
        ingest_ok(&s, stream, r#"[{"a":1}]"#);
    }
    for (token, visible) in [
        (&q1, &["app_web"][..]),
        (&q2, &["app"][..]),
        (&q3, &["app", "app_web", "apple"][..]),
    ] {
        for stream in ["app", "app_web", "apple"] {
            let r = search_as(
                &s,
                &token.bearer(),
                &format!("SELECT count(*) AS n FROM {stream}"),
            );
            if visible.contains(&stream) {
                assert_eq!(r.status, 200, "{token:?} {stream}：{}", r.text());
            } else {
                r.assert_envelope(400, "stream_not_found");
            }
        }
    }
}

#[test]
fn authz_grants_union() {
    let f = Fixture::new();
    let m = ServiceToken::generate();
    let s = authz::start(
        &f,
        &[token_entry(
            "m",
            &m,
            r#"[{ role = "writer", streams = ["app"] }, { role = "querier", streams = ["web"] }]"#,
        )],
        &[],
    );
    ingest_ok(&s, "web", r#"[{"a":1}]"#);
    let bearer = m.bearer();
    let rs = [
        authz::ingest_as(&s, &bearer, "app", &[], r#"[{"a":1}]"#),
        search_as(&s, &bearer, "SELECT count(*) AS n FROM web"),
        authz::ingest_as(&s, &bearer, "web", &[], r#"[{"a":1}]"#),
        search_as(&s, &bearer, "SELECT count(*) AS n FROM app"),
    ];
    let statuses: Vec<u16> = rs.iter().map(|r| r.status).collect();
    assert_eq!(statuses, [200, 200, 403, 400], "{rs:?}");
    rs[3].assert_envelope(400, "stream_not_found");
}

#[test]
fn authz_out_of_scope_same_as_absent() {
    let f = Fixture::new();
    let q = ServiceToken::generate();
    let s = authz::start(&f, &[token_entry("q", &q, QUERIER_APP)], &[]);
    let bearer = q.bearer();
    let ask = || {
        (
            search_as(&s, &bearer, "SELECT count(*) AS n FROM hr"),
            get_as(&s, &bearer, "/api/default/hr/_fields"),
        )
    };
    let (search_absent, fields_absent) = ask();
    search_absent.assert_envelope(400, "stream_not_found");
    fields_absent.assert_envelope(400, "stream_not_found");
    ingest_ok(&s, "hr", r#"[{"a":1}]"#);
    let (search_hidden, fields_hidden) = ask();
    authz::assert_same_as_absent(&search_hidden, &search_absent);
    authz::assert_same_as_absent(&fields_hidden, &fields_absent);
}

#[test]
fn authz_access_event_reads_default() {
    let f = Fixture::new();
    let (w, q) = (ServiceToken::generate(), ServiceToken::generate());
    let s = authz::start(
        &f,
        &[
            token_entry("w", &w, WRITER_APP),
            token_entry("q", &q, QUERIER_APP),
        ],
        &[],
    );
    let json = authz::ingest_as(&s, &w.bearer(), "app", &[], r#"[{"a":1}]"#);
    assert_eq!(json.status, 200, "{}", json.text());
    let search = search_as(&s, &q.bearer(), "SELECT count(*) AS n FROM app");
    assert_eq!(search.status, 200, "{}", search.text());
    let fields = get_as(&s, &q.bearer(), "/api/default/app/_fields");
    assert_eq!(fields.status, 200, "{}", fields.text());
    let id = |r: &common::Resp| r.header("x-request-id").unwrap().to_owned();
    let (search_id, fields_id) = (id(&search), id(&fields));
    wait_event(&s, "access", |e| e["request_id"] == fields_id.as_str());
    let access = events(&s, "access");
    assert_eq!(access.len(), 2, "{access:#?}");
    let e = access
        .iter()
        .find(|e| e["endpoint"] == "search")
        .expect("search 的 access 记录");
    assert_eq!(e["dataset"], "default/app");
    assert_eq!(e["status"], 200);
    assert_eq!(e["principal"], "token:q");
    assert_eq!(e["roles"], serde_json::json!(["querier"]));
    assert_eq!(e["request_id"], search_id.as_str());
    let e = access
        .iter()
        .find(|e| e["endpoint"] == "fields")
        .expect("fields 的 access 记录");
    assert_eq!(e["request_id"], fields_id.as_str());
    assert!(access.iter().all(|e| e["endpoint"] != "json"));
    for e in &access {
        let text = e.to_string();
        assert!(!text.contains("SELECT"), "{text}");
        assert!(!text.contains(q.token.as_str()), "access 记录含令牌原文");
    }
}

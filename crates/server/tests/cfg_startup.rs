//! REQ-AMB-CFG-STARTUP-V2 的系统测试：以子进程启动已构建的 `amberlog`，经退出码、stdout 与 stderr 观察。
//!
//! 文件权限类场景须以非 root 身份运行。场景「object_store.url 的相邻斜杠、点号桶名与大写方案名属格式错误」要核对
//! 存储桶里没有新增对象，用 MinIO 台架（`tools/testbed.sh`）。

mod common;

use std::io::{BufRead, BufReader};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use common::api::{FLUSH_1H, ingest_ok};
use common::store::{Ds, FLUSH_1S, kill_at_marker, snapshot_rel};
use common::{Fixture, HeldPort, Kv, Server, TempDir, amberlog, set_mode};

struct Run {
    code: i32,
    stdout: String,
    stderr: String,
}

fn run(cmd: &mut Command) -> Run {
    let Output {
        status,
        stdout,
        stderr,
    } = cmd.output().expect("启动 amberlog");
    Run {
        code: status.code().expect("进程应正常退出而不是被信号终止"),
        stdout: String::from_utf8_lossy(&stdout).into_owned(),
        stderr: String::from_utf8_lossy(&stderr).into_owned(),
    }
}

const S3_UNREACHABLE: &[(&str, Option<&str>)] = &[
    ("object_store.url", Some("\"s3://bucket\"")),
    ("object_store.s3.conditional_put", Some("\"etag\"")),
    ("object_store.s3.endpoint", Some("\"http://127.0.0.1:1\"")),
    ("object_store.s3.force_path_style", Some("true")),
    ("object_store.s3.access_key_id", Some("\"k\"")),
    ("object_store.s3.secret_access_key", Some("\"s\"")),
];

fn assert_exit(r: &Run, code: i32, stderr_has: &[&str]) {
    assert_eq!(r.code, code, "stderr:\n{}", r.stderr);
    for s in stderr_has {
        assert!(r.stderr.contains(s), "stderr 应含 {s:?}:\n{}", r.stderr);
    }
}

// ---------- 命令行 ----------

#[test]
fn cfg_version_output() {
    let dir = TempDir::new();
    let r = run(amberlog(dir.path()).arg("--version"));
    assert_eq!(r.code, 0);
    let line = r.stdout.strip_suffix('\n').expect("一行输出");
    assert!(!line.contains('\n'));
    // ^amberlog \d+\.\d+\.\d+.* \([0-9a-f]{7,}\)$
    let rest = line.strip_prefix("amberlog ").expect("以 `amberlog ` 开头");
    let (ver, commit) = rest.rsplit_once(" (").expect("` (<commit>)` 结尾");
    let commit = commit.strip_suffix(')').unwrap();
    assert!(
        commit.len() >= 7
            && commit
                .bytes()
                .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    );
    let nums: Vec<&str> = ver.splitn(3, '.').collect();
    assert_eq!(nums.len(), 3);
    assert!(nums[0].bytes().all(|b| b.is_ascii_digit()) && !nums[0].is_empty());
    assert!(nums[1].bytes().all(|b| b.is_ascii_digit()) && !nums[1].is_empty());
    assert!(nums[2].bytes().next().is_some_and(|b| b.is_ascii_digit()));
}

#[test]
fn cfg_usage_errors_exit2() {
    let f = Fixture::new();
    f.write_config(&[]);
    for args in [
        &[][..],
        &["frobnicate"],
        &["serve", "--bogus"],
        &[
            "serve",
            "--config",
            "amberlog.toml",
            "--config",
            "amberlog.toml",
        ],
        &["serve", "--config"],
    ] {
        let r = run(amberlog(f.root()).args(args));
        assert_eq!(r.code, 2, "{args:?}");
        assert!(!r.stderr.is_empty(), "{args:?}");
    }
}

/// REQ-AMB-CFG-STARTUP-V6 起只读目录模式的子命令是 `catalog`；写错的 `catalogue` 与不带子命令同样是用法错误。
/// 配置的 `server.http_addr` 是测试占住的端口（[`HeldPort`]），以此断言进程没有走到监听。
#[test]
fn cfg_unknown_subcommand_exit2() {
    let f = Fixture::new();
    let held = HeldPort::new();
    f.write_config(&[("server.http_addr", Some(&held.addr()))]);
    for args in [
        &["catalogue", "--config", "amberlog.toml"][..],
        &["--config", "amberlog.toml"],
    ] {
        let mut c = amberlog(f.root());
        c.args(args);
        let mut p = Server::spawn(c);
        let code = p
            .wait_exit(Duration::from_secs(30))
            .unwrap_or_else(|| panic!("{args:?} 没有退出"));
        assert!(p.wait_line(|_| true).is_some(), "{args:?} 的 stderr 为空");
        assert_eq!(code, Some(2), "{args:?}：{:#?}", p.stderr_lines());
        held.assert_never_listened(code, &p);
    }
}

#[test]
fn cfg_evidence_subcommand_usage_exit2() {
    let f = Fixture::new();
    f.write_config(&[]);
    for args in [
        &["evidence", "--config", "amberlog.toml"][..],
        &["evidence", "check", "--config", "amberlog.toml"],
    ] {
        let r = run(amberlog(f.root()).args(args));
        assert_eq!(r.code, 2, "{args:?}");
        assert!(!r.stderr.is_empty(), "{args:?}");
        assert!(!r.stderr.contains("\"event\":\"starting\""), "{args:?}");
    }
}

#[test]
fn cfg_config_file_unusable_exit2() {
    let f = Fixture::new();
    let unreadable = f.root().join("unreadable.toml");
    std::fs::write(&unreadable, f.toml(&[])).unwrap();
    set_mode(&unreadable, 0o000);
    std::fs::write(f.root().join("broken.toml"), "a = [").unwrap();
    for name in ["unreadable.toml", "broken.toml"] {
        let r = run(amberlog(f.root()).args(["serve", "--config", name]));
        assert_exit(&r, 2, &[name]);
    }
}

// ---------- 配置来源 ----------

fn env_only(f: &Fixture) -> Command {
    let mut c = amberlog(f.root());
    c.arg("serve")
        .env("AMBERLOG_OBJECT_STORE__URL", f.store_url())
        .env("AMBERLOG_WAL__DIR", f.root().join("wal"))
        .env("AMBERLOG_AUTH__TOKEN", "secret-token")
        .env("AMBERLOG_SERVER__HTTP_ADDR", "127.0.0.1:0");
    c
}

#[test]
fn cfg_env_name_mapping() {
    let f = Fixture::new();
    for name in [
        "AMBERLOG_SERVER_HTTP_ADDR",
        "AMBERLOG_server__http_addr",
        "AMBERLOG_SERVER__HTTP_ADDRS",
    ] {
        let r = run(env_only(&f).env(name, "127.0.0.1:18802"));
        assert_exit(&r, 2, &[name]);
    }
}

#[test]
fn cfg_index_hints_env_rejected() {
    let f = Fixture::new();
    let r = run(env_only(&f).env("AMBERLOG_INDEX_HINTS", "x"));
    assert_eq!(r.code, 2, "stderr:\n{}", r.stderr);
    assert!(
        r.stderr.contains("AMBERLOG_INDEX_HINTS") || r.stderr.contains("index_hints"),
        "{}",
        r.stderr
    );
}

#[test]
fn cfg_missed_collection_keys_invalid_exit2() {
    let f = Fixture::new();
    for (kv, key) in [
        (
            ("reconciliation.grace", Some("\"30s\"")),
            "reconciliation.grace",
        ),
        (
            ("reconciliation.grace", Some("\"25h\"")),
            "reconciliation.grace",
        ),
        (
            ("alerts.missed_collection.interval", Some("\"90s\"")),
            "alerts.missed_collection.interval",
        ),
        (
            ("alerts.missed_collection.enabled", Some("\"true\"")),
            "alerts.missed_collection.enabled",
        ),
        (
            ("alerts.missed_collection.severity", Some("\"high\"")),
            "alerts.missed_collection.severity",
        ),
    ] {
        let r = run(&mut f.serve(&[kv]));
        assert_exit(&r, 2, &[key]);
    }
    let s = f.start(&[("reconciliation.grace", Some("\"10m\""))]);
    s.wait_readyz(200, Duration::from_secs(60));
}

#[test]
fn cfg_missed_collection_headers_env_rejected() {
    let f = Fixture::new();
    let var = "AMBERLOG_ALERTS__MISSED_COLLECTION__WEBHOOK_HEADERS";
    let r = run(env_only(&f).env(var, "x"));
    assert_eq!(r.code, 2, "stderr:\n{}", r.stderr);
    assert!(
        r.stderr.contains(var)
            || r.stderr
                .contains("alerts.missed_collection.webhook_headers"),
        "{}",
        r.stderr
    );
    assert!(!r.stderr.contains("\"event\":\"starting\""), "{}", r.stderr);
}

#[test]
fn cfg_empty_env_is_set() {
    let f = Fixture::new();
    for (var, key) in [
        ("AMBERLOG_AUTH__TOKEN", "auth.token"),
        ("AMBERLOG_WAL__DIR", "wal.dir"),
    ] {
        let r = run(f.serve(&[]).env(var, ""));
        assert_exit(&r, 2, &[key]);
    }
}

// ---------- 配置值校验 ----------

/// 读子进程 stderr 直到出现含 `needle` 的一行（或超时），随后终止进程；返回已读到的全部行。
fn stderr_until(cmd: &mut Command, needle: &str) -> (Vec<String>, Option<i32>) {
    let mut child = cmd
        .stderr(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    let stderr = child.stderr.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines() {
            if tx.send(line.unwrap_or_default()).is_err() {
                break;
            }
        }
    });
    let mut lines = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(20);
    while let Some(left) = deadline.checked_duration_since(Instant::now()) {
        match rx.recv_timeout(left) {
            Ok(line) => {
                let hit = line.contains(needle);
                lines.push(line);
                if hit {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    let exited = child.try_wait().unwrap().and_then(|s| s.code());
    let _ = child.kill();
    let _ = child.wait();
    (lines, exited)
}

#[test]
fn cfg_auth_disabled_without_token() {
    let f = Fixture::new();
    let (lines, exited) = stderr_until(
        &mut f.serve(&[("auth.token", None), ("auth.enabled", Some("false"))]),
        "\"event\":\"auth_disabled\"",
    );
    assert_ne!(exited, Some(2), "{lines:#?}");
    let ev: serde_json::Value = lines
        .iter()
        .filter_map(|l| serde_json::from_str(l).ok())
        .find(|v: &serde_json::Value| v["event"] == "auth_disabled")
        .unwrap_or_else(|| panic!("应有 auth_disabled 事件：{lines:#?}"));
    assert_eq!(ev["level"], "warn");
}

#[test]
fn cfg_missing_required_exit2() {
    let f = Fixture::new();
    let r = run(&mut f.serve(&[("auth.token", None)]));
    assert_exit(&r, 2, &["auth.token"]);
    assert!(!r.stderr.contains("\"event\":\"starting\""));
}

#[test]
fn cfg_other_required_missing_exit2() {
    let f = Fixture::new();
    let s3 = &[
        ("object_store.url", Some("\"s3://bucket\"")),
        ("object_store.s3.conditional_put", None),
    ][..];
    for (overrides, key) in [
        (&[("object_store.url", None)][..], "object_store.url"),
        (&[("wal.dir", None)][..], "wal.dir"),
        (s3, "object_store.s3.conditional_put"),
    ] {
        let r = run(&mut f.serve(overrides));
        assert_exit(&r, 2, &[key]);
    }
}

#[test]
fn cfg_unknown_key_exit2() {
    let f = Fixture::new();
    let r = run(&mut f.serve(&[("ingest.max_body_byte", Some("1024"))]));
    assert_exit(&r, 2, &["ingest.max_body_byte"]);
}

#[test]
fn cfg_missing_config_file_exit2() {
    let dir = TempDir::new();
    let r = run(amberlog(dir.path()).args(["serve", "--config", "/nonexistent.toml"]));
    assert_exit(&r, 2, &["/nonexistent.toml"]);
}

#[test]
fn cfg_value_ranges_exit2() {
    let f = Fixture::new();
    // 各行共用同一个配置文件，所以逐行现写现跑；最后一行的取值经环境变量给出
    let rows: [(Kv, &str); 4] = [
        (("flush.interval", Some("\"0s\"")), "flush.interval"),
        (("storage.prefix", Some("\"..\"")), "storage.prefix"),
        (
            ("server.http_addr", Some("\"localhost:8800\"")),
            "server.http_addr",
        ),
        (("auth.enabled", None), "auth.enabled"),
    ];
    for (kv, key) in rows {
        let mut cmd = f.serve(&[kv]);
        if kv.1.is_none() {
            cmd.env("AMBERLOG_AUTH__ENABLED", "0");
        }
        let r = run(&mut cmd);
        assert_exit(&r, 2, &[key]);
        assert!(!r.stderr.contains("\"event\":\"starting\""));
    }
}

#[test]
fn cfg_round1a_keys_range_exit2() {
    let f = Fixture::new();
    let r = run(&mut f.serve(&[("ingest.max_columns", Some("0"))]));
    assert_exit(&r, 2, &["ingest.max_columns"]);
}

#[test]
fn cfg_query_limits_range_exit2() {
    let f = Fixture::new();
    for (kv, key) in [
        (
            ("query.max_concurrency", Some("0")),
            "query.max_concurrency",
        ),
        (
            ("query.max_memory_bytes", Some("0")),
            "query.max_memory_bytes",
        ),
        (
            ("query.max_memory_bytes", Some("\"512MiB\"")),
            "query.max_memory_bytes",
        ),
    ] {
        let r = run(&mut f.serve(&[kv]));
        assert_exit(&r, 2, &[key]);
    }
}

#[test]
fn cfg_lake_watermark_not_above_seal_grace() {
    let f = Fixture::new();
    let r = run(&mut f.serve(&[
        ("lifecycle.seal_grace", Some("\"2h\"")),
        ("lifecycle.lake_watermark", Some("\"1h\"")),
    ]));
    assert_exit(&r, 2, &["lifecycle.seal_grace", "lifecycle.lake_watermark"]);
}

#[test]
fn cfg_new_keys_value_format_invalid_exit2() {
    let f = Fixture::new();
    let rows: [Kv; 14] = [
        ("ingest.max_columns", Some("\"1000\"")),
        ("ingest.max_backlog_memory_bytes", Some("0")),
        ("wal.alert_bytes", Some("0")),
        ("compaction.target_file_bytes", Some("0")),
        ("compaction.target_file_bytes", Some("\"134217728\"")),
        ("lifecycle.seal_grace", Some("\"0s\"")),
        ("hot_fsck.interval", Some("\"0s\"")),
        ("hot_fsck.min_orphan_age", Some("\"1.5h\"")),
        ("retention.period", Some("\"0s\"")),
        ("retention.period", Some("\"36501d\"")),
        ("retention.period", Some("30")),
        ("hot_retention.snapshots", Some("\"1000\"")),
        ("cold_retention.snapshots", Some("\"1\"")),
        ("query.max_concurrency", Some("\"4\"")),
    ];
    for kv in rows {
        let r = run(&mut f.serve(&[kv]));
        assert_exit(&r, 2, &[kv.0]);
        assert!(!r.stderr.contains("\"event\":\"starting\""), "{kv:?}");
    }
}

#[test]
fn cfg_value_format_invalid_exit2() {
    let f = Fixture::new();
    let s3 = |extra: (&'static str, &'static str)| -> Vec<(&'static str, Option<&'static str>)> {
        let mut v: Vec<(&str, Option<&str>)> = vec![
            ("object_store.url", Some("\"s3://bucket\"")),
            ("object_store.s3.conditional_put", Some("\"etag\"")),
        ];
        v.retain(|(k, _)| *k != extra.0);
        v.push((extra.0, Some(extra.1)));
        v
    };
    let a65 = format!("\"{}\"", "a".repeat(65));
    // (来源为文件时的键与 TOML 取值) 或 (环境变量名与取值)，期望 stderr 所含键名
    enum Src {
        File(Vec<(&'static str, Option<&'static str>)>),
        Env(&'static str, &'static str),
    }
    let file = |k: &'static str, v: &'static str| Src::File(vec![(k, Some(v))]);
    let rows: Vec<(Src, &str)> = vec![
        (file("auth.enabled", "\"true\""), "auth.enabled"),
        (
            file("ingest.max_body_bytes", "\"1024\""),
            "ingest.max_body_bytes",
        ),
        (file("flush.max_records", "1.5"), "flush.max_records"),
        (file("server.http_addr", "8800"), "server.http_addr"),
        (Src::Env("AMBERLOG_AUTH__ENABLED", "True"), "auth.enabled"),
        (Src::Env("AMBERLOG_AUTH__ENABLED", "yes"), "auth.enabled"),
        (
            Src::Env("AMBERLOG_INGEST__MAX_BODY_BYTES", "1_000"),
            "ingest.max_body_bytes",
        ),
        (
            Src::Env("AMBERLOG_INGEST__MAX_BODY_BYTES", "+5"),
            "ingest.max_body_bytes",
        ),
        (
            Src::Env("AMBERLOG_INGEST__MAX_BODY_BYTES", "1e3"),
            "ingest.max_body_bytes",
        ),
        (
            Src::Env("AMBERLOG_INGEST__MAX_BODY_BYTES", "9223372036854775808"),
            "ingest.max_body_bytes",
        ),
        (file("flush.interval", "\"5\""), "flush.interval"),
        (file("flush.interval", "\"-5s\""), "flush.interval"),
        (file("flush.interval", "\"1.5s\""), "flush.interval"),
        (file("flush.interval", "\"5 s\""), "flush.interval"),
        (file("flush.interval", "\"1_0s\""), "flush.interval"),
        (file("flush.interval", "\"5sec\""), "flush.interval"),
        (file("flush.interval", "\"106751992d\""), "flush.interval"),
        (file("flush.max_records", "0"), "flush.max_records"),
        (file("ingest.max_body_bytes", "0"), "ingest.max_body_bytes"),
        (file("query.max_size", "0"), "query.max_size"),
        (
            file("storage.commit_max_retries", "-1"),
            "storage.commit_max_retries",
        ),
        (file("query.default_size", "-1"), "query.default_size"),
        (file("server.http_addr", "\"0.0.0.0\""), "server.http_addr"),
        (
            file("server.http_addr", "\"0.0.0.0:65536\""),
            "server.http_addr",
        ),
        (
            file("server.http_addr", "\"[fe80::1%eth0]:8800\""),
            "server.http_addr",
        ),
        (file("storage.prefix", "\"_x\""), "storage.prefix"),
        (file("storage.prefix", "\"a/b\""), "storage.prefix"),
        (
            Src::File(vec![("storage.prefix", Some(a65.clone().leak()))]),
            "storage.prefix",
        ),
        (
            file("object_store.url", "\"file://host/data\""),
            "object_store.url",
        ),
        (
            file("object_store.url", "\"file:///data?x=1\""),
            "object_store.url",
        ),
        (
            file("object_store.url", "\"file:///data#f\""),
            "object_store.url",
        ),
        (
            file("object_store.url", "\"file:///a/../b\""),
            "object_store.url",
        ),
        (
            file("object_store.url", "\"file:///a/%2E%2E/b\""),
            "object_store.url",
        ),
        (
            file("object_store.url", "\"file:///a%2Fb\""),
            "object_store.url",
        ),
        (
            file("object_store.url", "\"file:///a%00b\""),
            "object_store.url",
        ),
        (
            file("object_store.url", "\"file:///%FF\""),
            "object_store.url",
        ),
        (
            Src::File(s3(("object_store.url", "\"s3://user@bucket\""))),
            "object_store.url",
        ),
        (
            Src::File(s3(("object_store.url", "\"s3://my$bucket\""))),
            "object_store.url",
        ),
        (
            Src::File(s3(("object_store.s3.endpoint", "\"minio:9000\""))),
            "object_store.s3.endpoint",
        ),
        (
            Src::File(s3(("object_store.s3.region", "\"\""))),
            "object_store.s3.region",
        ),
        (
            Src::File(s3(("object_store.s3.conditional_put", "\"none\""))),
            "object_store.s3.conditional_put",
        ),
        (file("auth.token", "\"abc def\""), "auth.token"),
        (file("auth.token", "\"abc=def\""), "auth.token"),
        (file("log.level", "\"warning\""), "log.level"),
        (file("log.format", "\"yaml\""), "log.format"),
    ];
    for (src, key) in rows {
        let (mut cmd, shown) = match src {
            Src::File(o) => {
                let shown = format!("{o:?}");
                (f.serve(&o), shown)
            }
            Src::Env(k, v) => {
                let mut c = f.serve(&[]);
                c.env(k, v);
                (c, format!("{k}={v}"))
            }
        };
        let r = run(&mut cmd);
        assert_eq!(r.code, 2, "{shown}\n{}", r.stderr);
        assert!(
            r.stderr.contains(key),
            "{shown}: stderr 应含 {key}\n{}",
            r.stderr
        );
        assert!(!r.stderr.contains("\"event\":\"starting\""), "{shown}");
    }
}

#[test]
fn cfg_unsupported_scheme_exit2() {
    let f = Fixture::new();
    let r = run(&mut f.serve(&[("object_store.url", Some("\"gs://bucket/p\""))]));
    assert_exit(&r, 2, &["object_store.url"]);
}

#[test]
fn cfg_encryption_keys_exit2() {
    let f = Fixture::new();
    let pki = common::tls::Pki::new(f.root());
    let ca = pki.ca("cfg-ca");
    let pair = pki.leaf(&ca, "cfg-leaf", common::tls::KeyKind::P256);
    let (c, k) = (
        format!("{:?}", pair.cert.display().to_string()),
        format!("{:?}", pair.key.display().to_string()),
    );
    fn with<'a>(extra: &[Kv<'a>]) -> Vec<Kv<'a>> {
        let mut o = S3_UNREACHABLE.to_vec();
        o.extend_from_slice(extra);
        o
    }
    let rows: [(Vec<Kv>, &[&str]); 4] = [
        (
            with(&[("server.tls.cert_file", Some(&c))]),
            &["server.tls.key_file"],
        ),
        (
            with(&[
                ("server.tls.cert_file", Some(&c)),
                ("server.tls.key_file", Some(&k)),
                ("server.behind_tls_proxy", Some("true")),
            ]),
            &["server.tls.cert_file", "server.behind_tls_proxy"],
        ),
        (
            with(&[("object_store.s3.server_side_encryption", Some("\"SSE_S3\""))]),
            &["object_store.s3.server_side_encryption"],
        ),
        (
            with(&[
                ("object_store.s3.server_side_encryption", Some("\"sse_s3\"")),
                ("object_store.s3.sse_kms_key_id", Some("\"k1\"")),
            ]),
            &[
                "object_store.s3.server_side_encryption",
                "object_store.s3.sse_kms_key_id",
            ],
        ),
    ];
    for (o, keys) in rows {
        let r = run(&mut f.serve(&o));
        assert_exit(&r, 2, keys);
        assert!(!r.stderr.contains(r#""event":"starting""#), "{}", r.stderr);
    }
    let r = run(f
        .serve(S3_UNREACHABLE)
        .env("AMBERLOG_SERVER__BEHIND_TLS_PROXY", "yes"));
    assert_exit(&r, 2, &["server.behind_tls_proxy"]);
    assert!(!r.stderr.contains(r#""event":"starting""#), "{}", r.stderr);
}

#[test]
fn cfg_s3_key_pair_incomplete_exit2() {
    let f = Fixture::new();
    let base = [
        ("object_store.url", Some("\"s3://bucket\"")),
        ("object_store.s3.conditional_put", Some("\"etag\"")),
    ];
    for (set, missing) in [
        (
            "object_store.s3.access_key_id",
            "object_store.s3.secret_access_key",
        ),
        (
            "object_store.s3.secret_access_key",
            "object_store.s3.access_key_id",
        ),
    ] {
        let mut o = base.to_vec();
        o.push((set, Some("\"x\"")));
        let r = run(&mut f.serve(&o));
        assert_exit(&r, 2, &[missing]);
    }
}

#[test]
fn cfg_default_size_exceeds_max_exit2() {
    let f = Fixture::new();
    let r = run(&mut f.serve(&[
        ("query.default_size", Some("200")),
        ("query.max_size", Some("100")),
    ]));
    assert_exit(&r, 2, &["query.default_size", "query.max_size"]);
}

// ---------- 启动顺序 ----------

#[test]
fn cfg_wal_dir_unusable_exit3() {
    let f = Fixture::new();
    let file = f.root().join("F");
    std::fs::write(&file, b"").unwrap();
    let ro = f.root().join("R");
    std::fs::create_dir(&ro).unwrap();
    set_mode(&ro, 0o555);
    for dir in [file.join("sub"), ro] {
        let v = format!("\"{}\"", dir.display());
        let r = run(&mut f.serve(&[("wal.dir", Some(&v))]));
        assert_exit(&r, 3, &[]);
    }
}

#[test]
fn cfg_startup_order_first_failure() {
    let f = Fixture::new();
    let held = HeldPort::new();
    let file = f.root().join("F");
    std::fs::write(&file, b"").unwrap();
    let wal_under_file = format!("\"{}\"", file.join("sub").display());
    let busy_addr = held.addr();
    let busy = Some(busy_addr.as_str());
    let mut s3_then_busy = S3_UNREACHABLE.to_vec();
    s3_then_busy.push(("server.http_addr", busy));
    let rows: [(Vec<Kv>, i32); 3] = [
        (
            vec![
                ("log.level", Some("\"warning\"")),
                ("wal.dir", Some(&wal_under_file)),
            ],
            2,
        ),
        (
            vec![
                ("log.level", Some("\"warning\"")),
                ("server.http_addr", busy),
            ],
            2,
        ),
        (s3_then_busy, 3),
    ];
    for (o, code) in rows {
        let r = run(&mut f.serve(&o));
        assert_exit(&r, code, &[]);
        assert!(!r.stderr.contains("\"event\":\"ready\""));
    }
}

#[test]
fn cfg_listen_failure_exit4() {
    let f = Fixture::new();
    let held = HeldPort::new();
    let r = run(&mut f.serve(&[("server.http_addr", Some(&held.addr()))]));
    assert_exit(&r, 4, &[]);
}

#[test]
fn cfg_object_store_unreachable_exit3() {
    let f = Fixture::new();
    let r = run(&mut f.serve(S3_UNREACHABLE));
    assert_exit(&r, 3, &[]);
}

#[test]
fn cfg_file_url_dir_missing_exit3() {
    let f = Fixture::new();
    let missing = format!("\"file://{}\"", f.root().join("no-such-dir").display());
    let r = run(&mut f.serve(&[("object_store.url", Some(&missing))]));
    assert_exit(&r, 3, &[]);
}

// ---------- 依赖 /readyz 与 ready 事件的场景 ----------

const READY_WAIT: Duration = Duration::from_secs(5);

#[test]
fn cfg_config_equals_form() {
    let f = Fixture::new();
    f.write_config(&[]);
    let mut c = amberlog(f.root());
    c.args(["serve", "--config=amberlog.toml"]);
    let s = Server::start(c);
    assert_eq!(s.wait_readyz(200, READY_WAIT).status, 200);
}

#[test]
fn cfg_env_overrides_file() {
    let f = Fixture::new();
    let addr = format!("127.0.0.1:{}", common::free_port());
    let mut c = f.serve(&[("server.http_addr", Some("\"127.0.0.1:8800\""))]);
    c.env("AMBERLOG_SERVER__HTTP_ADDR", &addr);
    let s = Server::start(c);
    assert_eq!(s.addr, addr.parse().unwrap());
    assert_eq!(s.get("/healthz").status, 200);
}

#[test]
fn cfg_file_overrides_default() {
    let f = Fixture::new();
    let addr = format!("127.0.0.1:{}", common::free_port());
    let s = f.start(&[("server.http_addr", Some(&format!("\"{addr}\"")))]);
    assert_eq!(s.addr, addr.parse().unwrap());
    assert_eq!(s.get("/healthz").status, 200);
}

#[test]
fn cfg_no_config_file_without_flag() {
    let f = Fixture::new();
    std::fs::write(f.root().join("amberlog.toml"), "bogus_key = 1\n").unwrap();
    let mut c = amberlog(f.root());
    c.arg("serve")
        .env("AMBERLOG_OBJECT_STORE__URL", f.store_url())
        .env("AMBERLOG_WAL__DIR", f.wal_dir())
        .env("AMBERLOG_AUTH__TOKEN", common::TOKEN)
        .env("AMBERLOG_SERVER__HTTP_ADDR", "127.0.0.1:0");
    let s = Server::start(c);
    assert_eq!(s.wait_readyz(200, READY_WAIT).status, 200);
}

/// 用默认端口 8800 的两个场景串行执行（两者都要绑定 0.0.0.0:8800：一个由被测进程监听，一个由测试自己占住）。
/// 同一台机器上同时跑两份 cfg_startup 时，这两个用默认端口 8800 的测试可能偶发失败，按环境问题处理。
static PORT_8800: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// 前提带明文声明（默认地址 `0.0.0.0:8800` 不是回环地址，REQ-AMB-ARCH-ENCRYPTION PLAIN-DECLARED）。
#[test]
fn cfg_defaults_listen_and_log_format() {
    let _port = PORT_8800.lock().unwrap_or_else(|e| e.into_inner());
    let f = Fixture::new();
    let s = f.start(&[
        ("server.http_addr", None),
        ("server.behind_tls_proxy", Some("true")),
    ]);
    let r = common::http(
        "127.0.0.1:8800".parse().unwrap(),
        "GET",
        "/readyz",
        &[],
        b"",
    );
    assert_eq!(r.status, 200);
    let ready = s
        .json_lines()
        .into_iter()
        .find(|v| v["event"] == "ready")
        .unwrap();
    assert_eq!(ready["http_addr"], "0.0.0.0:8800");
}

/// 同一默认配置去掉明文声明（REQ-AMB-ARCH-ENCRYPTION PLAIN-REJECT、CONFIG-STEP）。
#[test]
fn cfg_defaults_listen_undeclared_exit2() {
    let _port = PORT_8800.lock().unwrap_or_else(|e| e.into_inner());
    // 先占住默认地址：被测进程走到监听这一步必然绑定失败（退出码 4、stderr 含 `cannot listen on`）
    let _held = std::net::TcpListener::bind("0.0.0.0:8800").expect("0.0.0.0:8800 已被占用");
    let f = Fixture::new();
    let mut child = f
        .serve(&[("server.http_addr", None)])
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(s) = child.try_wait().unwrap() {
            break s;
        }
        assert!(Instant::now() < deadline, "进程没有退出");
        std::thread::sleep(Duration::from_millis(20));
    };
    let mut stderr = String::new();
    std::io::Read::read_to_string(child.stderr.as_mut().unwrap(), &mut stderr).unwrap();
    assert_eq!(status.code(), Some(2), "{stderr}");
    // 退出之前没有试过监听
    assert!(!stderr.contains("cannot listen on"), "{stderr}");
    assert!(stderr.contains("server.http_addr"), "{stderr}");
    assert!(stderr.contains("server.behind_tls_proxy"), "{stderr}");
    assert!(!stderr.contains("\"event\":\"starting\""), "{stderr}");
}

#[test]
fn cfg_wal_dir_relative_to_cwd() {
    let f = Fixture::new();
    let s = f.start(&[("wal.dir", Some("\"w\""))]);
    assert_eq!(s.wait_readyz(200, READY_WAIT).status, 200);
    assert!(f.root().join("w").is_dir());
}

#[test]
fn cfg_object_store_url_percent_decoding() {
    let f = Fixture::new();
    let dx = f.root().join("D x");
    std::fs::create_dir(&dx).unwrap();
    let encoded = format!("\"file://{}/D%20x\"", f.root().display());
    let s = f.start(&[("object_store.url", Some(&encoded))]);
    assert_eq!(s.wait_readyz(200, READY_WAIT).status, 200);
    assert!(dx.join("amberlog").is_dir());
    drop(s);

    let raw = format!("\"file://{}/D x\"", f.root().display());
    let r = run(&mut f.serve(&[("object_store.url", Some(&raw))]));
    assert_exit(&r, 2, &["object_store.url"]);
}

#[test]
fn cfg_file_root_prefix_autocreate() {
    let f = Fixture::new();
    let s = f.start(&[]);
    assert_eq!(s.wait_readyz(200, READY_WAIT).status, 200);
}

#[test]
fn cfg_object_store_probe_under_system() {
    let f = Fixture::new();
    let s = f.start(&[]);
    assert_eq!(s.wait_readyz(200, READY_WAIT).status, 200);
    let system = f.store_dir().join("amberlog/_system");
    // 只数文件：子目录不算对象
    let files = std::fs::read_dir(&system)
        .unwrap()
        .filter(|e| e.as_ref().unwrap().file_type().unwrap().is_file())
        .count();
    assert!(files >= 1);
}

#[test]
fn cfg_startup_log_events() {
    let f = Fixture::new();
    let s = f.start(&[]);
    let lines = s.json_lines();
    assert_eq!(lines[0]["event"], "starting");
    assert!(lines[0]["version"].is_string());
    let ready = lines.iter().find(|v| v["event"] == "ready").unwrap();
    assert!(ready["http_addr"].is_string() && ready["object_store"].is_string());
    assert_eq!(ready["mode"], "serve");
    // 夹具以回环地址明文监听（REQ-AMB-CFG-STARTUP-V7）
    assert_eq!(ready["tls"], false);
}

/// `YYYY-MM-DDTHH:MM:SS[.frac]Z`
fn is_rfc3339_utc(ts: &str) -> bool {
    let b = ts.as_bytes();
    let digits = |r: std::ops::Range<usize>| b[r].iter().all(u8::is_ascii_digit);
    b.len() >= 20
        && digits(0..4)
        && b[4] == b'-'
        && digits(5..7)
        && b[7] == b'-'
        && digits(8..10)
        && b[10] == b'T'
        && digits(11..13)
        && b[13] == b':'
        && digits(14..16)
        && b[16] == b':'
        && digits(17..19)
        && b[b.len() - 1] == b'Z'
        && (b.len() == 20 || (b[19] == b'.' && digits(20..b.len() - 1) && b.len() > 21))
}

#[test]
fn cfg_log_json_lines_on_stderr() {
    let f = Fixture::new();
    let s = f.start(&[]);
    assert_eq!(s.wait_readyz(200, READY_WAIT).status, 200);
    for line in s.stderr_lines() {
        let v: serde_json::Value =
            serde_json::from_str(&line).unwrap_or_else(|_| panic!("不是 JSON：{line}"));
        assert!(v["level"].is_string() && v["msg"].is_string(), "{line}");
        assert!(is_rfc3339_utc(v["ts"].as_str().unwrap()), "{line}");
    }
    assert!(
        s.stdout_lines()
            .iter()
            .all(|l| serde_json::from_str::<serde_json::Value>(l).is_err())
    );
}

#[test]
fn cfg_version_matches_log_events() {
    let f = Fixture::new();
    let v = run(amberlog(f.root()).arg("--version"));
    let semver = v.stdout.split_whitespace().nth(1).unwrap().to_owned();
    let s = f.start(&[]);
    assert_eq!(s.wait_readyz(200, READY_WAIT).status, 200);
    for event in ["starting", "ready"] {
        let e = s
            .json_lines()
            .into_iter()
            .find(|l| l["event"] == event)
            .unwrap();
        assert_eq!(e["version"], semver.as_str(), "{event}");
        assert_eq!(e["level"], "info", "{event}");
    }
}

#[test]
fn cfg_stable_events_not_filtered() {
    let f = Fixture::new();
    let s = f.start(&[
        ("log.level", Some("\"error\"")),
        ("log.format", Some("\"json\"")),
        ("auth.enabled", Some("false")),
        ("auth.token", None),
    ]);
    let events: Vec<(String, String)> = s
        .json_lines()
        .iter()
        .filter(|v| v["event"].is_string())
        .map(|v| {
            (
                v["event"].as_str().unwrap().to_owned(),
                v["level"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    let want = [
        ("starting", "info"),
        ("auth_disabled", "warn"),
        ("ready", "info"),
    ];
    assert_eq!(
        events,
        want.map(|(e, l)| (e.to_owned(), l.to_owned())).to_vec()
    );
}

#[test]
fn cfg_text_format_emits_stable_events() {
    let f = Fixture::new();
    let s = f.start(&[
        ("log.format", Some("\"text\"")),
        ("auth.enabled", Some("false")),
        ("auth.token", None),
    ]);
    let text = s.stderr_lines().join("\n");
    let pos = |name: &str| {
        text.find(&format!("event={name}"))
            .unwrap_or_else(|| panic!("缺 {name}：{text}"))
    };
    assert!(pos("starting") < pos("auth_disabled") && pos("auth_disabled") < pos("ready"));
}

#[test]
fn cfg_ready_http_addr_actual() {
    let f = Fixture::new();
    let s = f.start(&[("server.http_addr", Some("\"127.0.0.1:0\""))]);
    let ready = s
        .json_lines()
        .into_iter()
        .find(|v| v["event"] == "ready")
        .unwrap();
    let addr = ready["http_addr"].as_str().unwrap();
    let port: u16 = addr.strip_prefix("127.0.0.1:").unwrap().parse().unwrap();
    assert_ne!(port, 0);
    assert_eq!(s.get("/healthz").status, 200);
    assert_eq!(ready["object_store"], f.store_url().as_str());
}

#[test]
fn cfg_ready_logged_once() {
    let f = Fixture::new();
    let s = f.start(&[]);
    let system = f.store_dir().join("amberlog/_system");
    set_mode(&system, 0o555);
    assert_eq!(s.wait_readyz(503, Duration::from_secs(40)).status, 503);
    set_mode(&system, 0o755);
    assert_eq!(s.wait_readyz(200, Duration::from_secs(40)).status, 200);
    let n = s
        .stderr_lines()
        .iter()
        .filter(|l| l.contains("\"event\":\"ready\""))
        .count();
    assert_eq!(n, 1);
}

// ---------- 已有数据集的启动 ----------

/// 以 `flush.interval` 为 `1h` 启动，依次摄取各批（须得 200），SIGTERM 停机（停机时 flush 并提交）。
fn committed(f: &Fixture, overrides: &[Kv], batches: &[(&str, &str)]) {
    let mut all = vec![FLUSH_1H];
    all.extend_from_slice(overrides);
    let mut s = f.start(&all);
    for (stream, body) in batches {
        ingest_ok(&s, stream, body);
    }
    stop(&mut s);
    for (stream, _) in batches {
        assert!(f.dataset(stream).current().is_some(), "{stream} 应已提交");
    }
}

fn stop(s: &mut Server) {
    s.signal("TERM");
    assert_eq!(
        s.wait_exit(Duration::from_secs(30)),
        Some(Some(0)),
        "{:#?}",
        s.stderr_lines()
    );
}

/// 把数据集的当前快照文件中的 `format_version` 就地改为 `version`（REQ-AMB-CFG-STARTUP-V5 场景所写的构造）。
fn set_current_format_version(ds: &Ds, version: u64) {
    let n = ds.current().expect("当前快照存在");
    let mut snap = ds.snapshot(n);
    snap["format_version"] = serde_json::json!(version);
    ds.write_json(&snapshot_rel(n), &snap);
}

/// 启动后 `/readyz` 为 200。
fn start_ready(f: &Fixture, overrides: &[Kv]) -> Server {
    let s = f.start(overrides);
    assert_eq!(
        s.wait_readyz(200, Duration::from_secs(10)).status,
        200,
        "{:#?}",
        s.stderr_lines()
    );
    s
}

#[test]
fn cfg_object_store_url_edge_forms() {
    let bucket = testbed("TESTBED_S3_BUCKET");
    let endpoint = testbed("TESTBED_S3_ENDPOINT");
    let listing = BucketListing::new(&bucket, &endpoint);
    let prefixes = ["a/", "amberlog/", &format!("{bucket}/")];
    let before = listing.keys(&prefixes);

    let f = Fixture::new();
    let d = f.store_dir();
    let ep = format!("\"{endpoint}\"");
    let id = format!("\"{}\"", testbed("TESTBED_S3_ACCESS_KEY_ID"));
    let secret = format!("\"{}\"", testbed("TESTBED_S3_SECRET_ACCESS_KEY"));
    for url in [
        format!("s3://../{bucket}"),
        format!("s3://./{bucket}"),
        format!("s3://{bucket}/a//b"),
        format!("file://{}//x", d.display()),
        format!("S3://{bucket}"),
    ] {
        let url = format!("\"{url}\"");
        let r = run(&mut f.serve(&[
            ("object_store.url", Some(&url)),
            ("object_store.s3.endpoint", Some(&ep)),
            ("object_store.s3.force_path_style", Some("true")),
            ("object_store.s3.conditional_put", Some("\"etag\"")),
            ("object_store.s3.access_key_id", Some(&id)),
            ("object_store.s3.secret_access_key", Some(&secret)),
        ]));
        assert_exit(&r, 2, &["object_store.url"]);
    }
    assert_eq!(listing.keys(&prefixes), before, "存储桶中不应新增对象");
    assert_eq!(
        std::fs::read_dir(&d).unwrap().count(),
        0,
        "{d:?} 下不应新增对象"
    );
}

#[test]
fn cfg_baseline_read_denied_exit3() {
    let f = Fixture::new();
    committed(&f, &[], &[("app", r#"[{"a":1}]"#)]);
    let manifest = f.dataset("app").path("manifest");
    set_mode(&manifest, 0o000);
    // `server.http_addr` 指向一个已被占用的端口：先监听、后读快照的实现会以监听失败的 4 退出，
    // 只有在监听之前就因读取失败退出的实现得到 3
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = format!("\"{}\"", occupied.local_addr().unwrap());
    let r = run(&mut f.serve(&[("server.http_addr", Some(&addr))]));
    set_mode(&manifest, 0o755);
    assert_exit(&r, 3, &[]);
    assert!(
        !r.stderr.contains("\"event\":\"ready\""),
        "不应开始监听：\n{}",
        r.stderr
    );
    drop(occupied);
}

#[test]
fn cfg_external_tamper_no_startup_failure() {
    let f = Fixture::new();
    committed(&f, &[], &[("app", r#"[{"a":1}]"#), ("web", r#"[{"a":1}]"#)]);
    let app = f.dataset("app");
    let n = app.current().unwrap();
    app.write(&snapshot_rel(n + 1), b"{}");
    let snap = app.snapshot(n);
    let file = app.file_entries(&snap)[0]["path"]
        .as_str()
        .unwrap()
        .to_owned();
    app.remove(&file);
    let bad = f.store_dir().join("amberlog/default/Bad-Name");
    std::fs::create_dir_all(&bad).unwrap();
    std::fs::write(bad.join("x"), b"x").unwrap();

    let s = start_ready(&f, &[]);
    ingest_ok(&s, "web", r#"[{"a":2}]"#);
}

#[test]
fn cfg_suspended_dataset_no_startup_failure() {
    let f = Fixture::new();
    committed(
        &f,
        &[],
        &[
            ("app", r#"[{"a":1}]"#),
            ("web", r#"[{"a":1}]"#),
            ("ok", r#"[{"a":1}]"#),
        ],
    );
    set_current_format_version(&f.dataset("app"), 3);
    let web = f.dataset("web");
    web.remove(&snapshot_rel(web.current().unwrap()));
    let s = start_ready(&f, &[]);
    ingest_ok(&s, "ok", r#"[{"a":2}]"#);
}

#[test]
fn cfg_startup_dataset_write_failure_not_fatal() {
    let f = Fixture::new();
    committed(&f, &[], &[("app", r#"[{"a":1}]"#)]);
    let app = f.dataset("app");
    let n = app.current().unwrap();
    // 崩溃残留：快照 N+1 已创建、提示尚未写出
    let (mut x, marker) = f.start_paused(&[FLUSH_1S], "commit.after_snapshot_create");
    ingest_ok(&x, "app", r#"[{"a":2}]"#);
    kill_at_marker(&mut x, &marker);
    assert_eq!(app.hint(), Some(n));
    assert!(app.exists(&snapshot_rel(n + 1)), "应已创建快照 N+1");

    let manifest = app.path("manifest");
    set_mode(&manifest, 0o555);
    let s = f.start(&[]);
    let ready = s.wait_readyz(200, Duration::from_secs(10)).status;
    set_mode(&manifest, 0o755);
    assert_eq!(ready, 200, "{:#?}", s.stderr_lines());
}

/// REQ-AMB-LIFE-DURABILITY 场景「旧 `wal.dir` 挂回时相容的滞留记录被收回、冲突的被丢弃并报告」的实例 X、Y 与
/// W1、W2：X 在 W1 上提交 `seq` 0、重启后确认两条（`host` 为整数）后被强杀；Y 以空的 W2 把 `host` 定为字符串并提交。
/// 返回以 W1 启动所用的配置项。
/// 按 REQ-AMB-LIFE-DURABILITY-V2 两个过时 `wal.dir` 场景的步骤构造实例 X、Y 与 W1、W2，返回 W1 的配置取值。
/// `extra` 是 X、Y（以及调用方启动 Z 时）共用的其余配置；`batch` 与 `other` 是 X 重启后与 Y 各自摄取的内容。
fn stale_w1(f: &Fixture, extra: &[Kv], batch: &str, other: &str) -> String {
    let w1 = format!("\"{}\"", f.root().join("w1").display());
    let w2 = format!("\"{}\"", f.root().join("w2").display());
    let start = |w: &str| {
        let mut kvs = vec![FLUSH_1H, ("wal.dir", Some(w))];
        kvs.extend_from_slice(extra);
        f.start(&kvs)
    };
    let mut x = start(&w1);
    ingest_ok(&x, "app", r#"{"seq":0}"#);
    stop(&mut x);
    let mut x = start(&w1);
    ingest_ok(&x, "app", batch);
    x.sigkill();
    let mut y = start(&w2);
    ingest_ok(&y, "app", other);
    stop(&mut y);
    w1
}

/// 场景「类型不同的滞留记录落入影子列收回」的构造。
fn stale_w1_shadow(f: &Fixture) -> String {
    stale_w1(
        f,
        &[],
        r#"[{"seq":1,"host":123},{"seq":2}]"#,
        r#"{"seq":3,"host":"web1"}"#,
    )
}

const MAX_COLUMNS_3: Kv = ("ingest.max_columns", Some("3"));

#[test]
fn cfg_wal_stale_event_fields() {
    let f = Fixture::new();
    // 场景「会使列数超限的滞留记录被丢弃并报告」的构造
    let w1 = stale_w1(
        &f,
        &[MAX_COLUMNS_3],
        r#"[{"seq":1,"a":1},{"seq":2}]"#,
        r#"{"seq":3,"b":"x"}"#,
    );
    let z = start_ready(
        &f,
        &[FLUSH_1H, ("wal.dir", Some(w1.as_str())), MAX_COLUMNS_3],
    );
    let lines = z.json_lines();
    let stale: Vec<(usize, &serde_json::Value)> = lines
        .iter()
        .enumerate()
        .filter(|(_, l)| l["event"] == "wal_stale")
        .collect();
    assert_eq!(stale.len(), 1, "{lines:#?}");
    let (at, e) = stale[0];
    assert_eq!(e["level"], "warn");
    assert_eq!(e["dataset"], "default/app");
    let (recovered, discarded) = (
        e["recovered"].as_u64().expect("recovered 是整数"),
        e["discarded"].as_u64().expect("discarded 是整数"),
    );
    assert_eq!(recovered + discarded, 2, "{e}");
    let ready = lines
        .iter()
        .position(|l| l["event"] == "ready")
        .expect("有 ready 事件");
    assert!(at < ready, "wal_stale 应在 ready 之前");
}

#[test]
fn cfg_no_wal_stale_for_suspended_dataset() {
    let f = Fixture::new();
    let w1 = stale_w1_shadow(&f);
    set_current_format_version(&f.dataset("app"), 3);
    let z = start_ready(&f, &[FLUSH_1H, ("wal.dir", Some(w1.as_str()))]);
    assert!(
        !z.stderr_lines()
            .iter()
            .any(|l| l.contains("\"event\":\"wal_stale\"")),
        "{:#?}",
        z.stderr_lines()
    );
}

/// 台架环境变量；缺失时 panic 并提示怎样起台架。
fn testbed(k: &str) -> String {
    common::s3::testbed(k)
}

/// 直连台架的存储桶列举（path-style，root 凭据）。
struct BucketListing {
    store: object_store::aws::AmazonS3,
    rt: tokio::runtime::Runtime,
}

impl BucketListing {
    fn new(bucket: &str, endpoint: &str) -> BucketListing {
        let store = object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region("us-east-1")
            .with_endpoint(endpoint)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .with_access_key_id(testbed("TESTBED_S3_ACCESS_KEY_ID"))
            .with_secret_access_key(testbed("TESTBED_S3_SECRET_ACCESS_KEY"))
            .build()
            .unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        BucketListing { store, rt }
    }

    /// 以 `prefixes` 之一开头的全部键。
    fn keys(&self, prefixes: &[&str]) -> std::collections::BTreeSet<String> {
        use futures::TryStreamExt;
        use object_store::ObjectStore;
        let all: Vec<object_store::ObjectMeta> = self
            .rt
            .block_on(self.store.list(None).try_collect())
            .unwrap();
        all.into_iter()
            .map(|m| m.location.to_string())
            .filter(|k| prefixes.iter().any(|p| k.starts_with(p)))
            .collect()
    }
}

/// stderr 中 `event` 为 `name` 的 JSON 日志。
fn events(s: &Server, name: &str) -> Vec<serde_json::Value> {
    s.json_lines()
        .into_iter()
        .filter(|v| v["event"] == name)
        .collect()
}

#[test]
fn cfg_dataset_disabled_event() {
    let f = Fixture::new();
    committed(&f, &[], &[("app", r#"[{"a":1}]"#)]);
    let app = f.dataset("app");
    let head = app.head();
    let bucket = &head["buckets"][0];
    app.remove(bucket["manifest"].as_str().unwrap());
    let (min, max) = (
        bucket["time_min"].as_i64().unwrap(),
        bucket["time_max"].as_i64().unwrap(),
    );

    let s = start_ready(&f, &[("log.level", Some("\"error\""))]);
    let body = serde_json::json!({"query": {"sql": "SELECT * FROM app", "start_time": min, "end_time": max + 1}})
        .to_string();
    for _ in 0..2 {
        let r = s.request(
            "POST",
            "/api/default/_search",
            &[("Authorization", common::BEARER)],
            body.as_bytes(),
        );
        assert_eq!(r.status, 500, "{}", r.text());
    }
    let got = events(&s, "dataset_disabled");
    assert_eq!(got.len(), 1, "{:#?}", s.stderr_lines());
    assert_eq!(got[0]["level"], "error");
    assert_eq!(got[0]["dataset"], "default/app");
    assert_eq!(got[0]["state"], "manifest_unreadable");
}

#[test]
fn cfg_dataset_disabled_format_version() {
    let f = Fixture::new();
    committed(&f, &[], &[("app", r#"[{"a":1}]"#), ("ok", r#"[{"a":1}]"#)]);
    set_current_format_version(&f.dataset("app"), 3);

    let mut s = start_ready(&f, &[]);
    let got = events(&s, "dataset_disabled");
    assert_eq!(got.len(), 1, "{:#?}", s.stderr_lines());
    assert_eq!(got[0]["level"], "error");
    assert_eq!(got[0]["dataset"], "default/app");
    assert_eq!(got[0]["state"], "format_version");
    stop(&mut s);

    let s = start_ready(&f, &[("log.format", Some("\"text\""))]);
    assert!(
        s.stderr_lines()
            .iter()
            .any(|l| l.contains("dataset_disabled")),
        "{:#?}",
        s.stderr_lines()
    );
}

#[test]
fn cfg_digest_mismatch_not_filtered() {
    // 构造同 REQ-AMB-STO-SEAL-COMPACTION 场景「合并输入被改动时放弃合并并发出信号」
    let f = Fixture::new();
    let hour = 3_600_000_000;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;
    let start = now.div_euclid(hour) * hour - 2 * hour;
    common::store::seed_small_files(&f, "app", start, 3, 10);
    let app = f.dataset("app");
    let label = common::store::bucket_label(start);
    let m = app.bucket_manifest(&app.head(), &label).unwrap();
    let file = m["files"][1]["path"].as_str().unwrap().to_owned();
    common::store::rewrite_parquet_uncompressed(&app.path(&file));

    let s = f.start(&[
        ("lifecycle.seal_grace", Some("\"1s\"")),
        ("log.level", Some("\"error\"")),
    ]);
    std::thread::sleep(Duration::from_secs(60));
    let got = events(&s, "digest_mismatch");
    assert!(!got.is_empty(), "{:#?}", s.stderr_lines());
    assert_eq!(got[0]["level"], "error");
    assert_eq!(got[0]["dataset"], "default/app");
    assert_eq!(got[0]["path"], file.as_str());
}

#[test]
fn cfg_digest_mismatch_tier_hot() {
    // 构造同 REQ-AMB-STO-SEAL-COMPACTION-V3 场景「合并输入被改动时放弃合并并发出信号」
    let f = Fixture::new();
    let hour = 3_600_000_000;
    let start = common::api::now_us().div_euclid(hour) * hour - 2 * hour;
    common::store::seed_small_files(&f, "app", start, 3, 10);
    let app = f.dataset("app");
    let label = common::store::bucket_label(start);
    let m = app.bucket_manifest(&app.head(), &label).unwrap();
    let file = m["files"][1]["path"].as_str().unwrap().to_owned();
    common::lake::alter_parquet_rows(&app.path(&file));

    let s = f.start(&[("lifecycle.seal_grace", Some("\"1s\""))]);
    std::thread::sleep(Duration::from_secs(60));
    let got = events(&s, "digest_mismatch");
    assert_eq!(got.len(), 1, "{:#?}", s.stderr_lines());
    assert_eq!(got[0]["level"], "error");
    assert_eq!(got[0]["dataset"], "default/app");
    assert_eq!(got[0]["tier"], "hot");
    assert_eq!(got[0]["path"], file.as_str());
}

/// 冷层写入失败为 `fault-injection` 下的对象存储故障注入（`TESTBED_FAULT_STORE`）。
#[test]
fn cfg_lake_backlog_high_not_filtered() {
    let f = Fixture::new();
    let (s, faults) = f.start_with_store_faults(&[
        FLUSH_1S,
        ("lifecycle.lake_watermark", Some("\"1h\"")),
        ("lifecycle.lake_alert_after", Some("\"1h\"")),
        ("log.level", Some("\"error\"")),
    ]);
    faults.set("write _iceberg/ 403\n");
    let hour = 3_600_000_000;
    let start = common::api::now_us().div_euclid(hour) * hour - 4 * hour;
    let recs: Vec<String> = (0..10)
        .map(|i| format!(r#"{{"_timestamp":{},"seq":{i}}}"#, start + 1 + i * 1000))
        .collect();
    ingest_ok(&s, "app", &format!("[{}]", recs.join(",")));
    let (got, _) = common::api::poll(
        Duration::from_secs(130),
        || events(&s, "lake_backlog_high"),
        |v| !v.is_empty(),
    );
    assert!(!got.is_empty(), "{:#?}", s.stderr_lines());
    let e = &got[0];
    assert_eq!(e["level"], "warn");
    assert_eq!(e["dataset"], "default/app");
    assert!(e["bucket"].is_string(), "{e}");
    assert!(e["lag_seconds"].is_i64(), "{e}");
    assert_eq!(e["alert_after_seconds"], 3600);
}

#[test]
fn cfg_wal_backlog_high_not_filtered() {
    let f = Fixture::new();
    let s = f.start(&[
        ("wal.alert_bytes", Some("1048576")),
        FLUSH_1H,
        ("log.level", Some("\"error\"")),
    ]);
    // 32 条、每条带一个 65536 字节的随机字符串字段（xorshift，不可压缩）
    let mut x: u64 = 0x2545_F491_4F6C_DD1D;
    for batch in 0..2 {
        let records: Vec<serde_json::Value> = (0..16)
            .map(|i| {
                let blob: String = (0..65536)
                    .map(|_| {
                        x ^= x << 13;
                        x ^= x >> 7;
                        x ^= x << 17;
                        char::from(b'a' + (x % 26) as u8)
                    })
                    .collect();
                serde_json::json!({"seq": batch * 16 + i, "blob": blob})
            })
            .collect();
        ingest_ok(&s, "app", &serde_json::Value::Array(records).to_string());
    }
    std::thread::sleep(Duration::from_secs(15));
    let lines = s.json_lines();
    let events: Vec<&serde_json::Value> = lines
        .iter()
        .filter(|l| l["event"] == "wal_backlog_high")
        .collect();
    assert_eq!(events.len(), 1, "{lines:#?}");
    let e = events[0];
    assert_eq!(e["level"], "warn");
    assert!(e["wal_bytes"].is_u64(), "{e}");
    assert_eq!(e["alert_bytes"], 1_048_576);
}

// ---------- 不对应块场景的加强测试 ----------

/// 优雅停机并等到退出码 0。
fn stop_ok(mut s: Server) {
    s.signal("TERM");
    assert_eq!(
        s.wait_exit(Duration::from_secs(30)),
        Some(Some(0)),
        "{:#?}",
        s.stderr_lines()
    );
}

/// `_system/` 下各名字的计数：（就绪探测对象数，条件写探测对象数，其余对象数）。
fn probe_names(names: &[String]) -> (usize, usize, usize) {
    let ready = names.iter().filter(|n| *n == "readyz-probe").count();
    let create = names
        .iter()
        .filter(|n| n.starts_with("create-only-probe-"))
        .count();
    (ready, create, names.len() - ready - create)
}

/// 启动探测不发删除：`file://` 后端连续启动三次，就绪探测对象固定为一个，条件写探测每次取一个新键、此前的都还在
/// （REQ-AMB-CFG-STARTUP-V5 条件写探测「对同一个此前不存在的键」；写出的键是否清理不作承诺）。`object_store` 本地后端
/// 写入时的暂存文件（名字带 `#`，写完改名）不是对象：停机赶上一次进行中的就绪探测写入时它会留下，不计入。
#[test]
fn cfg_probe_objects_no_delete_across_restarts_file() {
    let f = Fixture::new();
    let system = f.store_dir().join("amberlog/_system");
    for round in 1..=3 {
        stop_ok(f.start(&[]));
        let names: Vec<String> = std::fs::read_dir(&system)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .filter(|n| !n.contains('#'))
            .collect();
        assert_eq!(probe_names(&names), (1, round, 0), "{names:?}");
    }
}

/// 同上，S3 后端（MinIO 台架）。
#[test]
fn cfg_probe_objects_no_delete_across_restarts_s3() {
    let inst = common::s3::S3Instance::new();
    let store = inst.store();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let prefix = store.root.clone().join("_system");
    for round in 1..=3 {
        stop_ok(inst.start(&[]));
        let names: Vec<String> = rt
            .block_on(store.list(&prefix))
            .unwrap()
            .into_iter()
            .map(|m| m.location.filename().unwrap().to_owned())
            .collect();
        assert_eq!(probe_names(&names), (1, round, 0), "{names:?}");
    }
}

/// 工作线程数不受 `TOKIO_WORKER_THREADS` 影响：取值非法时照常启动、就绪。
#[test]
fn cfg_tokio_worker_threads_env_ignored() {
    let f = Fixture::new();
    let mut cmd = f.serve(&[]);
    cmd.env("TOKIO_WORKER_THREADS", "abc");
    let s = Server::start(cmd);
    assert_eq!(s.wait_readyz(200, READY_WAIT).status, 200);
}

/// 以 `inst` 的对象存储配置外加 `extra` 启动、另设环境变量 `env`，等到 `ready`。
fn start_s3_with_env(inst: &common::s3::S3Instance, extra: &[Kv], env: &[(&str, &str)]) -> Server {
    let owned = inst.config(extra);
    let kv: Vec<Kv> = owned.iter().map(|(k, v)| (*k, v.as_deref())).collect();
    let mut cmd = inst.fixture.serve(&kv);
    cmd.envs(env.iter().copied());
    Server::start(cmd)
}

/// S3 客户端只读凭据类 `AWS_*` 环境变量：端点、服务端加密、校验和、签名等其余 `AWS_*` 不生效，取值非法也照常启动。
#[test]
fn cfg_s3_non_credential_aws_env_ignored() {
    let inst = common::s3::S3Instance::new();
    let s = start_s3_with_env(
        &inst,
        &[],
        &[
            ("AWS_ENDPOINT", "not a url"),
            ("AWS_ENDPOINT_URL", "not a url"),
            ("AWS_SERVER_SIDE_ENCRYPTION", "no-such-mode"),
            ("AWS_SSE_KMS_KEY_ID", "no-such-key"),
            ("AWS_CHECKSUM_ALGORITHM", "no-such-algorithm"),
            ("AWS_SKIP_SIGNATURE", "maybe"),
            ("AWS_UNSIGNED_PAYLOAD", "maybe"),
            ("AWS_COPY_IF_NOT_EXISTS", "no-such-mode"),
            ("AWS_REQUEST_PAYER", "maybe"),
        ],
    );
    assert_eq!(s.wait_readyz(200, READY_WAIT).status, 200);
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
}

/// 凭据类 `AWS_*` 环境变量仍生效：不配 `object_store.s3.access_key_id`／`secret_access_key` 时，
/// 以 `AWS_ACCESS_KEY_ID`、`AWS_SECRET_ACCESS_KEY` 给出的凭据启动、就绪。
#[test]
fn cfg_s3_credential_aws_env_effective() {
    let inst = common::s3::S3Instance::new();
    let id = common::s3::testbed("TESTBED_S3_ACCESS_KEY_ID");
    let secret = common::s3::testbed("TESTBED_S3_SECRET_ACCESS_KEY");
    let s = start_s3_with_env(
        &inst,
        &[
            ("object_store.s3.access_key_id", None),
            ("object_store.s3.secret_access_key", None),
        ],
        &[
            ("AWS_ACCESS_KEY_ID", &id),
            ("AWS_SECRET_ACCESS_KEY", &secret),
        ],
    );
    assert_eq!(s.wait_readyz(200, READY_WAIT).status, 200);
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
}

// ---------- REQ-AMB-CFG-STARTUP-V5 新增的配置项与条件写探测 ----------

#[test]
fn cfg_conditional_put_values() {
    let inst = common::s3::S3Instance::new();
    for v in ["\"create_only\"", "\"etag\""] {
        let s = inst.start(&[("object_store.s3.conditional_put", Some(v))]);
        assert_eq!(s.wait_readyz(200, READY_WAIT).status, 200, "{v}");
        stop_ok(s);
    }
    let owned = inst.config(&[("object_store.s3.conditional_put", Some("\"none\""))]);
    let kv: Vec<Kv> = owned.iter().map(|(k, v)| (*k, v.as_deref())).collect();
    let r = run(&mut inst.fixture.serve(&kv));
    assert_exit(&r, 2, &["object_store.s3.conditional_put"]);
}

#[test]
fn cfg_non_credential_aws_env_ignored() {
    let inst = common::s3::S3Instance::new();
    let s = start_s3_with_env(
        &inst,
        &[("object_store.s3.conditional_put", Some("\"create_only\""))],
        &[
            ("AWS_SKIP_SIGNATURE", "true"),
            ("AWS_ENDPOINT_URL", "http://127.0.0.1:1"),
        ],
    );
    assert_eq!(s.wait_readyz(200, READY_WAIT).status, 200);
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
}

#[test]
fn cfg_m0a_durations_range_exit2() {
    let f = Fixture::new();
    for key in ["storage.delete_delay", "lifecycle.lake_alert_after"] {
        let r = run(&mut f.serve(&[(key, Some("\"0s\""))]));
        assert_exit(&r, 2, &[key]);
    }
}

#[test]
fn cfg_retention_check_interval_range_exit2() {
    let f = Fixture::new();
    for v in ["\"0s\"", "\"-1h\""] {
        let r = run(&mut f.serve(&[("retention.check_interval", Some(v))]));
        assert_exit(&r, 2, &["retention.check_interval"]);
        assert!(!r.stderr.contains(r#""event":"starting""#), "{}", r.stderr);
    }
}

#[test]
fn cfg_cold_compaction_interval_range_exit2() {
    let f = Fixture::new();
    let r = run(&mut f.serve(&[("cold_compaction.check_interval", Some("\"0s\""))]));
    assert_exit(&r, 2, &["cold_compaction.check_interval"]);
}

#[test]
fn cfg_cold_retention_range_exit2() {
    let f = Fixture::new();
    for (key, v) in [
        ("cold_retention.snapshots", "0"),
        ("cold_retention.age", "\"0s\""),
        ("cold_retention.check_interval", "\"0s\""),
    ] {
        let r = run(&mut f.serve(&[(key, Some(v))]));
        assert_exit(&r, 2, &[key]);
    }
}

#[test]
fn cfg_retention_not_above_lake_watermark() {
    let f = Fixture::new();
    let r = run(&mut f.serve(&[("retention.period", Some("\"72h\""))]));
    assert_exit(&r, 2, &["retention.period", "lifecycle.lake_watermark"]);
}

/// 以 `inst` 的对象存储配置（`conditional_put` 为 `create_only`）运行，被测进程对对象存储的请求按 `rules` 注入故障
/// （`TESTBED_FAULT_STORE`，见 `amberlog_storage::fault::store`），直到退出。
fn run_s3_with_store_faults(inst: &common::s3::S3Instance, name: &str, rules: &str) -> Run {
    let control = inst.fixture.root().join(name);
    std::fs::write(&control, rules).unwrap();
    let owned = inst.config(&[("object_store.s3.conditional_put", Some("\"create_only\""))]);
    let kv: Vec<Kv> = owned.iter().map(|(k, v)| (*k, v.as_deref())).collect();
    let mut cmd = inst.fixture.serve(&kv);
    cmd.env("TESTBED_FAULT_STORE", &control);
    run(&mut cmd)
}

#[test]
fn cfg_create_only_probe_exit2() {
    let inst = common::s3::S3Instance::new();
    // 第一次运行：存储忽略 `If-None-Match`，带该头的写入一律按普通写入成功
    let r = run_s3_with_store_faults(&inst, "faults-1", "create _system/ ignore-condition\n");
    assert_exit(&r, 2, &[]);
    assert!(!r.stderr.contains(r#""event":"ready""#), "{}", r.stderr);
    // 第二次运行：带该头的写入回 HTTP 501
    let r = run_s3_with_store_faults(&inst, "faults-2", "create _system/ 501\n");
    assert_exit(&r, 2, &[]);
    assert!(!r.stderr.contains(r#""event":"ready""#), "{}", r.stderr);
}

#[test]
fn cfg_create_only_probe_unreachable_exit3() {
    let inst = common::s3::S3Instance::new();
    // 第一次「仅当不存在时创建」照常执行，第二次（同一键）回 HTTP 503
    let r = run_s3_with_store_faults(&inst, "faults", "create _system/ 503 skip=1\n");
    assert_exit(&r, 3, &[]);
    assert!(!r.stderr.contains(r#""event":"ready""#), "{}", r.stderr);
}

// ---------------------------------------------------------------------------
// 只读目录模式（REQ-AMB-CFG-STARTUP-V6 ⑤）
// ---------------------------------------------------------------------------

/// 一组只读目录模式的合法配置：`object_store.url`、`auth.token` 与回环地址的 `server.http_addr`（不写 `wal.dir`）。
const CATALOG_MINIMAL: &[Kv] = &[("wal.dir", None)];

#[test]
fn cfg_catalog_minimal_required() {
    let f = Fixture::new();
    let c = Server::start(f.catalog(CATALOG_MINIMAL));
    assert_eq!(c.wait_readyz(200, Duration::from_secs(60)).status, 200);
    assert!(
        c.json_lines()
            .iter()
            .any(|v| v["event"] == "ready" && v["mode"] == "catalog")
    );

    let out = amberlog(f.root())
        .arg("serve")
        .arg("--config")
        .arg(f.root().join("catalog.toml"))
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("wal.dir"));
}

#[test]
fn cfg_catalog_writer_keys_validated_unused() {
    let f = Fixture::new();
    for (key, kv) in [
        ("flush.interval", ("flush.interval", Some("\"0s\""))),
        ("ingest.max_body_byte", ("ingest.max_body_byte", Some("1"))),
    ] {
        let mut overrides = CATALOG_MINIMAL.to_vec();
        overrides.push(kv);
        let out = f.catalog(&overrides).output().unwrap();
        assert_eq!(out.status.code(), Some(2), "{key}");
        assert!(String::from_utf8_lossy(&out.stderr).contains(key), "{key}");
    }

    let w = f.root().join("no-such-wal");
    let wal = format!("\"{}\"", w.display());
    let c = Server::start(f.catalog(&[("wal.dir", Some(&wal))]));
    assert_eq!(c.wait_readyz(200, Duration::from_secs(60)).status, 200);
    assert!(!w.exists());
}

#[test]
fn cfg_hot_retention_range_exit2() {
    let f = Fixture::new();
    for (kv, key) in [
        (
            ("hot_retention.snapshots", Some("1")),
            "hot_retention.snapshots",
        ),
        (("hot_retention.age", Some("\"0s\"")), "hot_retention.age"),
    ] {
        let r = run(&mut f.serve(&[kv]));
        assert_exit(&r, 2, &[key]);
    }
}

#[test]
fn cfg_serve_refuses_retired_prefix_before_replay() {
    let f = Fixture::new();
    let mut a = f.start(&[FLUSH_1H]);
    ingest_ok(&a, "app", r#"[{"k":"x"}]"#);
    a.sigkill();
    let system = f.store_dir().join("amberlog/_system");
    std::fs::create_dir_all(&system).unwrap();
    std::fs::write(system.join("retirement.json"), b"{}").unwrap();
    let ds = f.dataset("app");
    let before = ds.keys();
    let held = HeldPort::new();
    let addr = held.addr();
    let mut s = Server::spawn(f.serve(&[FLUSH_1H, ("server.http_addr", Some(addr.as_str()))]));
    let code = s.wait_exit(Duration::from_secs(30));
    assert_eq!(code, Some(Some(2)), "{:#?}", s.stderr_lines());
    held.assert_never_listened(Some(2), &s);
    let stderr = s.stderr_lines().join("\n");
    assert!(stderr.contains("_system/retirement.json"), "{stderr}");
    assert_eq!(ds.keys(), before);
}

// ---------------------------------------------------------------------------
// 离线子命令（REQ-AMB-CFG-STARTUP-V7：retire、evidence export、evidence verify）
// ---------------------------------------------------------------------------

/// 「目录 `D` 中有一个已停止的写入实例提交过的 stream `app`（100 条记录）」：`D` 为夹具的 `file://` 目录。
fn app_committed(f: &Fixture) {
    let records: Vec<String> = (0..100).map(|i| format!(r#"{{"seq":{i}}}"#)).collect();
    committed(f, &[], &[("app", &format!("[{}]", records.join(",")))]);
}

/// 夹具根目录下新建的空目录。
fn empty_dir(f: &Fixture, name: &str) -> std::path::PathBuf {
    let d = f.root().join(name);
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// 只含给定键的配置文件（TOML 取值原样写入）。
fn raw_config(f: &Fixture, name: &str, kv: &[(&str, String)]) -> std::path::PathBuf {
    let p = f.root().join(name);
    let body: String = kv.iter().map(|(k, v)| format!("{k} = {v}\n")).collect();
    std::fs::write(&p, body).unwrap();
    p
}

fn quoted(p: &std::path::Path) -> String {
    format!("\"{}\"", p.display())
}

/// `amberlog evidence export --config <cfg> --dataset default/app --output <out>`。
fn export_cmd(f: &Fixture, cfg: &std::path::Path, out: &str) -> Command {
    let mut c = amberlog(f.root());
    c.arg("evidence")
        .arg("export")
        .arg("--config")
        .arg(cfg)
        .args(["--dataset", "default/app", "--output", out]);
    c
}

fn retire_cmd(f: &Fixture, cfg: &std::path::Path) -> Command {
    let mut c = amberlog(f.root());
    c.arg("retire").arg("--config").arg(cfg);
    c
}

#[test]
fn cfg_offline_no_auth_token() {
    let f = Fixture::new();
    app_committed(&f);
    let w = empty_dir(&f, "w-offline");
    let cfg = raw_config(
        &f,
        "offline.toml",
        &[
            ("object_store.url", format!("\"{}\"", f.store_url())),
            ("wal.dir", quoted(&w)),
            ("storage.delete_delay", "\"1s\"".into()),
        ],
    );
    let mut verify = amberlog(f.root());
    verify
        .arg("evidence")
        .arg("verify")
        .arg("--config")
        .arg(&cfg)
        .args(["--evidence", "e.tar"]);
    for (what, mut cmd) in [
        ("export", export_cmd(&f, &cfg, "e.tar")),
        ("verify", verify),
        ("retire", retire_cmd(&f, &cfg)),
    ] {
        let r = run(&mut cmd);
        assert_eq!(r.code, 0, "{what}: {}", r.stderr);
        assert!(!r.stderr.contains("auth.token"), "{what}: {}", r.stderr);
        assert_eq!(
            r.stderr.matches("\"event\":\"starting\"").count(),
            1,
            "{what}: {}",
            r.stderr
        );
        assert!(
            !r.stderr.contains("\"event\":\"ready\""),
            "{what}: {}",
            r.stderr
        );
    }
}

#[test]
fn cfg_offline_missing_required_exit2() {
    let f = Fixture::new();
    let retire = raw_config(
        &f,
        "retire.toml",
        &[("object_store.url", format!("\"{}\"", f.store_url()))],
    );
    let export = f.root().join("export.toml");
    std::fs::write(&export, f.toml(&[("object_store.url", None)])).unwrap();
    for (key, mut cmd) in [
        ("wal.dir", retire_cmd(&f, &retire)),
        ("object_store.url", export_cmd(&f, &export, "e.tar")),
    ] {
        let r = run(&mut cmd);
        assert_exit(&r, 2, &[key]);
        assert!(!r.stderr.contains("\"event\":\"starting\""), "{}", r.stderr);
    }
    assert!(!f.root().join("e.tar").exists());
}

#[test]
fn cfg_offline_object_store_unreachable_exit3() {
    let f = Fixture::new();
    let w = empty_dir(&f, "w-offline");
    let wal = quoted(&w);
    let mut overrides = S3_UNREACHABLE.to_vec();
    overrides.push(("wal.dir", Some(&wal)));
    let cfg = f.root().join("offline.toml");
    std::fs::write(&cfg, f.toml(&overrides)).unwrap();
    for mut cmd in [retire_cmd(&f, &cfg), export_cmd(&f, &cfg, "e.tar")] {
        let r = run(&mut cmd);
        assert_exit(&r, 3, &[]);
    }
    assert!(!f.root().join("e.tar").exists());
}

#[cfg(feature = "fault-injection")]
#[test]
fn cfg_offline_signal_during_startup_exit6() {
    let f = Fixture::new();
    app_committed(&f);
    let w = empty_dir(&f, "w-offline");
    let wal = quoted(&w);
    let cfg = f.root().join("offline.toml");
    std::fs::write(
        &cfg,
        f.toml(&[
            ("wal.dir", Some(&wal)),
            ("storage.delete_delay", Some("\"1s\"")),
        ]),
    )
    .unwrap();
    let ctl = common::retire::Ctl::new(&f);
    const STEP: &str = "offline.before_probe";
    for cmd in [retire_cmd(&f, &cfg), export_cmd(&f, &cfg, "e.tar")] {
        ctl.hold(STEP);
        let mut p = Server::spawn(ctl.apply(cmd, &[STEP], None));
        ctl.wait_held(STEP, &p, Duration::from_secs(60));
        p.signal("TERM");
        assert_eq!(
            p.wait_exit(Duration::from_secs(30)),
            Some(Some(6)),
            "{:#?}",
            p.stderr_lines()
        );
        assert!(p.stdout_lines().is_empty(), "{:#?}", p.stdout_lines());
    }
    assert!(!f.root().join("e.tar").exists());
    assert!(
        !f.store_dir()
            .join("amberlog/_system/retirement.json")
            .exists()
    );
}

#[test]
fn cfg_offline_keys_validated_unused() {
    let f = Fixture::new();
    app_committed(&f);
    let cfg = f.root().join("evidence.toml");
    for (n, (kv, key)) in [
        (("flush.interval", Some("\"0s\"")), "flush.interval"),
        (("ingest.max_body_byte", Some("1")), "ingest.max_body_byte"),
    ]
    .into_iter()
    .enumerate()
    {
        std::fs::write(&cfg, f.toml(&[kv])).unwrap();
        let r = run(&mut export_cmd(&f, &cfg, &format!("e{}.tar", n + 1)));
        assert_exit(&r, 2, &[key]);
    }

    let w = f.root().join("W");
    let (p1, p2) = (f.root().join("P1"), f.root().join("P2"));
    let (wal, cert, key) = (quoted(&w), quoted(&p1), quoted(&p2));
    std::fs::write(
        &cfg,
        f.toml(&[
            ("wal.dir", Some(&wal)),
            ("server.tls.cert_file", Some(&cert)),
            ("server.tls.key_file", Some(&key)),
        ]),
    )
    .unwrap();
    let r = run(&mut export_cmd(&f, &cfg, "e3.tar"));
    assert_eq!(r.code, 0, "{}", r.stderr);
    assert!(!w.exists());
}

// ---------- 告警配置项（REQ-AMB-CFG-STARTUP-V8 替代说明 ①） ----------

#[test]
fn cfg_alerts_keys_invalid_exit2() {
    let f = Fixture::new();
    for (kv, key) in [
        (
            ("alerts.max_concurrency", Some("0")),
            "alerts.max_concurrency",
        ),
        (
            ("alerts.history_retention", Some("\"30m\"")),
            "alerts.history_retention",
        ),
        (
            ("alerts.history_retention", Some("\"36501d\"")),
            "alerts.history_retention",
        ),
        (
            ("alerts.max_concurrency", Some("\"2\"")),
            "alerts.max_concurrency",
        ),
    ] {
        let r = run(&mut f.serve(&[kv]));
        assert_exit(&r, 2, &[key]);
        assert!(!r.stderr.contains("\"event\":\"starting\""), "{}", r.stderr);
    }
    let r = run(f.serve(&[]).env("AMBERLOG_ALERTS__RULES", "[]"));
    assert_eq!(r.code, 2, "{}", r.stderr);
    assert!(
        r.stderr.contains("AMBERLOG_ALERTS__RULES") || r.stderr.contains("alerts.rules"),
        "{}",
        r.stderr
    );
    assert!(!r.stderr.contains("\"event\":\"starting\""), "{}", r.stderr);
}

#[test]
fn cfg_alerts_keys_from_env() {
    let f = Fixture::new();
    let rule = r#"[{ name = "r1", interval = "1m", sql = "SELECT * FROM app", condition = { rows = "> 0" }, webhook_url = "https://127.0.0.1:9/hook" }]"#;
    let mut cmd = f.serve(&[("alerts.rules", Some(rule))]);
    cmd.env("AMBERLOG_ALERTS__MAX_CONCURRENCY", "2")
        .env("AMBERLOG_ALERTS__HISTORY_RETENTION", "2h")
        .env("AMBERLOG_ALERTS__CA_FILE", "");
    let s = Server::start(cmd);
    s.wait_readyz(200, Duration::from_secs(60));
    let stderr = s.stderr_lines().join("\n");
    assert!(!stderr.contains("alerts.ca_file"), "{stderr}");
}

// ---------------------------------------------------------------------------
// 漏采判定的两条稳定事件（REQ-AMB-CFG-STARTUP-V8 B 半；输出条件归 REQ-AMB-GOV-MISSED-COLLECTION）
// ---------------------------------------------------------------------------

/// 以 `reconciliation.grace` 为 `1m`、`log.level` 为 `error` 启动，`app` 已存在，等到开始受理请求超过 `warm`。
fn error_level_instance(warm: Duration) -> common::missed::Instance {
    let i = common::missed::Instance::start(&[
        common::missed::GRACE_1M,
        ("log.level", Some("\"error\"")),
    ]);
    common::api::create_stream(&i.server, "app");
    i.wait_accepting(warm);
    i
}

/// 长时场景：没有已有结论的窗口只在它结束加宽限期晚于基线时刻时才算转为缺失，场景的 W 是
/// 此前第 20 分钟，实例要先运行 22 分钟。设了 `TESTBED_SOAK_HOURS` 时照场景跑满；缺省跑缩短版：实例运行 2 分钟、
/// W 取此前第 1 分钟（同样在基线之后到期）。
#[test]
fn cfg_reconciliation_missing_not_filtered() {
    use common::missed::{events, in_window, ingest_at, minute_ago, report};
    let (warm, k) = if std::env::var_os("TESTBED_SOAK_HOURS").is_some() {
        (Duration::from_secs(22 * 60), 20)
    } else {
        (Duration::from_secs(120), 1)
    };
    let i = error_level_instance(warm);
    let w = minute_ago(k);
    report(&i.server, "app", "fw-1", w, 5);
    ingest_at(&i.server, "app", Some("fw-1"), &in_window(w, 3));
    std::thread::sleep(Duration::from_secs(300));
    let got = events(&i.server, "reconciliation_missing");
    let e = got.first().unwrap_or_else(|| {
        panic!(
            "没有 reconciliation_missing：{:#?}",
            i.server.stderr_lines()
        )
    });
    assert_eq!(e["level"], "warn");
    assert!(e["dataset"].is_string() && e["sources"].is_string(), "{e}");
    assert!(e["window_start"].is_i64() && e["missing"].is_u64(), "{e}");
}

#[test]
fn cfg_source_silent_not_filtered() {
    use common::missed::{events, minute_ago, report};
    let i = error_level_instance(Duration::from_secs(120));
    let (w1, w2) = (minute_ago(5), minute_ago(4));
    report(&i.server, "app", "fw-1", w1, 0);
    report(&i.server, "app", "fw-1", w2, 0);
    std::thread::sleep(Duration::from_secs(360));
    let got = events(&i.server, "source_silent");
    let e = got
        .first()
        .unwrap_or_else(|| panic!("没有 source_silent：{:#?}", i.server.stderr_lines()));
    assert_eq!(e["level"], "warn");
    assert!(e["dataset"].is_string() && e["source"].is_string(), "{e}");
    assert!(e["last_window_start"].is_i64(), "{e}");
}

/// 启动时把可同时打开的文件数的软上限调到硬上限，并在日志里报出生效值；生效值低于 65536 时另有一行 warn
/// （REQ-AMB-CFG-STARTUP-V9）。
#[test]
fn cfg_open_file_limit_raised_to_hard() {
    let host_hard = common::open_file_limits(std::process::id()).1;
    assert!(host_hard >= 65536, "测试机的硬上限 {host_hard} 低于 65536");
    for (hard, low) in [(4096, true), (65536, false)] {
        let f = Fixture::new();
        let mut cmd = f.serve(&[]);
        common::limit_open_files(&mut cmd, 1024, hard);
        let s = Server::start(cmd);
        assert_eq!(common::open_file_limits(s.pid()), (hard, hard));
        let lines = s.json_lines();
        let info = lines
            .iter()
            .find(|l| {
                l["level"] == "info"
                    && l["msg"]
                        .as_str()
                        .is_some_and(|m| m.starts_with("open file limit is"))
            })
            .unwrap_or_else(|| panic!("{lines:#?}"));
        assert_eq!(info["soft"], hard, "{info}");
        assert_eq!(info["hard"], hard, "{info}");
        let warned = lines.iter().any(|l| {
            l["level"] == "warn" && l["msg"].as_str().is_some_and(|m| m.contains("LimitNOFILE"))
        });
        assert_eq!(warned, low, "{lines:#?}");
    }
}

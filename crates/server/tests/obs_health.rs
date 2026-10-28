//! REQ-AMB-OBS-HEALTH-METRICS 的系统测试。
//!
//! 「对象存储不可达」用 `file://` 临时目录上的目录权限制造：把 `<prefix>/_system/` 改为 555，
//! 就绪探测对 `readyz-probe` 的写入即失败；场景「readyz 为 503 时不拒绝 api 请求」照替身字段用 MinIO 台架加进程内
//! TCP 转发（`common::s3`），按节拍计提交失败的两个对象存储不可达场景同样如此。

mod common;

use std::collections::BTreeSet;
use std::time::{Duration, Instant, SystemTime};

use common::alerting::{RuleToml, rules};
use common::api::{
    FLUSH_1H, SlowRequest, WalFault, ingest, ingest_ok, ingest_with, minute_of, now_us, sent,
    sent_element,
};
use common::s3::S3Instance;
use common::store::snapshot_rel;
use common::{Fixture, Server, amberlog, set_mode};

#[test]
fn healthz_independent_of_storage() {
    let f = Fixture::new();
    let s = f.start(&[]);
    set_mode(&f.store_dir().join("amberlog/_system"), 0o555);
    assert_eq!(s.wait_readyz(503, Duration::from_secs(40)).status, 503);
    let r = s.get("/healthz");
    assert_eq!(r.status, 200);
    assert_eq!(r.json()["status"], "ok");
}

#[test]
fn health_ready_bodies() {
    let f = Fixture::new();
    let s = f.start(&[]);
    let h = s.get("/healthz");
    let r = s.get("/readyz");
    assert_eq!((h.status, r.status), (200, 200), "{}", r.text());
    assert_eq!(h.json(), serde_json::json!({"status": "ok"}));
    assert_eq!(r.json(), serde_json::json!({"status": "ready"}));
}

#[test]
fn readyz_reflects_object_store() {
    let f = Fixture::new();
    let s = f.start(&[]);
    set_mode(&f.store_dir().join("amberlog/_system"), 0o555);
    std::thread::sleep(Duration::from_secs(31));
    let r = s.get("/readyz");
    let v = r.assert_envelope(503, "not_ready");
    assert_eq!(v["details"]["checks"]["object_store"], false);
}

#[test]
fn readyz_reflects_wal_dir() {
    let f = Fixture::new();
    let s = f.start(&[]);
    set_mode(&f.wal_dir(), 0o555);
    let r = s.get("/readyz");
    let v = r.assert_envelope(503, "not_ready");
    let checks = &v["details"]["checks"];
    assert_eq!(
        (
            &checks["wal"],
            &checks["object_store"],
            &checks["shutting_down"]
        ),
        (
            &serde_json::json!(false),
            &serde_json::json!(true),
            &serde_json::json!(false)
        )
    );
    set_mode(&f.wal_dir(), 0o755);
    assert_eq!(s.get("/readyz").status, 200);
}

#[test]
fn readyz_probe_fixed_object() {
    let f = Fixture::new();
    let s = f.start(&[("storage.prefix", Some("\"p1\""))]);
    assert_eq!(s.wait_readyz(200, Duration::from_secs(5)).status, 200);
    let system = f.store_dir().join("p1/_system");
    // 只数对象：对象存储本地后端写入时先写同目录下的临时文件（名字带 `#`）再改名，这类临时文件不算
    let count = || {
        std::fs::read_dir(&system)
            .unwrap()
            .filter(|e| {
                !e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .contains('#')
            })
            .count()
    };
    let before = count();
    let start = SystemTime::now();
    std::thread::sleep(Duration::from_secs(70));
    let probe = system.join("readyz-probe");
    let modified = std::fs::metadata(&probe).unwrap().modified().unwrap();
    assert!(modified > start);
    assert_eq!(count(), before);
}

#[test]
fn metrics_build_info_matches_version() {
    let f = Fixture::new();
    let out = amberlog(f.root()).arg("--version").output().unwrap();
    let line = String::from_utf8(out.stdout).unwrap();
    let rest = line.trim().strip_prefix("amberlog ").unwrap();
    let (semver, commit) = rest.rsplit_once(" (").unwrap();
    let commit = commit.strip_suffix(')').unwrap();
    let s = f.start(&[]);
    let text = s.get_auth("/metrics").text();
    let want = format!("amberlog_build_info{{version=\"{semver}\",commit=\"{commit}\"}} 1");
    assert!(text.lines().any(|l| l == want), "{text}");
}

#[test]
fn metrics_requires_auth() {
    let f = Fixture::new();
    let s = f.start(&[]);
    s.get("/metrics").assert_envelope(401, "unauthorized");
}

// ---------------------------------------------------------------------------
// 停机、摄取计数与指标格式
// ---------------------------------------------------------------------------

/// 发 SIGTERM（保持一个在途慢请求，停机停在排空阶段），等到停机闸门生效（`/readyz` 不再是 200）。
fn draining(s: &Server) -> SlowRequest {
    let slow = SlowRequest::start(s, "app", r#"[{"seq":1}]"#);
    s.signal("TERM");
    let deadline = Instant::now() + Duration::from_secs(5);
    while s.get("/readyz").status == 200 {
        assert!(
            Instant::now() < deadline,
            "SIGTERM 后 5 秒内 readyz 仍为 200"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    slow
}

/// `GET /metrics` 的响应体。
fn metrics(s: &Server) -> String {
    let r = s.get_auth("/metrics");
    assert_eq!(r.status, 200, "{}", r.text());
    r.text()
}

/// 样本 `series`（指标名连同标签，写法与渲染一致）的值；没有该样本为 `None`。
fn sample(text: &str, series: &str) -> Option<f64> {
    text.lines()
        .find_map(|l| l.strip_prefix(series)?.strip_prefix(' '))
        .map(|v| v.trim().parse().expect("样本值是数"))
}

fn ingest_total(text: &str, stream: &str, outcome: &str) -> f64 {
    sample(
        text,
        &format!(
            "amberlog_ingest_records_total{{org=\"default\",stream=\"{stream}\",outcome=\"{outcome}\"}}"
        ),
    )
    .unwrap_or(0.0)
}

#[test]
fn readyz_shutting_down() {
    let f = Fixture::new();
    let s = f.start(&[]);
    let _slow = draining(&s);
    let v = s.get("/readyz").assert_envelope(503, "not_ready");
    assert_eq!(v["details"]["checks"]["shutting_down"], true);
}

#[test]
fn readyz_not_ready_does_not_gate_api() {
    let inst = S3Instance::new();
    let s = inst.start(&[]);
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
    inst.relay.stop();
    std::thread::sleep(Duration::from_secs(31));
    assert_eq!(s.wait_readyz(503, Duration::from_secs(10)).status, 503);
    let r = ingest(&s, "app", r#"[{"a":2}]"#);
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["status"][0]["successful"], 1);
}

/// 场景前提「配置文件含一条启用的告警规则 `r1`」：`alerts.rules` 的取值（`webhook_url` 不会真去连接）。
fn rule_r1() -> String {
    rules(&[RuleToml::new("r1", "https://127.0.0.1:9/hook")])
}

#[test]
fn metrics_stable_set() {
    let f = Fixture::new();
    let r1 = rule_r1();
    let s = f.start(&[("alerts.rules", Some(&r1))]);
    ingest_ok(&s, "app", r#"[{"a":1},{"a":2}]"#);
    let text = metrics(&s);
    assert!(
        text.lines().any(|l| l.starts_with("amberlog_build_info")),
        "{text}"
    );
    assert!(
        text.lines()
            .any(|l| l.starts_with("amberlog_ingest_records_total")
                && l.contains("outcome=\"accepted\"")),
        "{text}"
    );
    // 含 `amberlog_backlog_memory_bytes` 与两个告警指标（REQ-AMB-OBS-HEALTH-METRICS-V6）
    assert!(
        text.lines()
            .any(|l| l.starts_with("amberlog_backlog_memory_bytes")),
        "{text}"
    );
    for family in [
        "amberlog_alert_evaluations_total",
        "amberlog_alert_deliveries_total",
    ] {
        assert!(
            text.lines()
                .any(|l| l.starts_with(family) && l.contains("rule=\"r1\"")),
            "{text}"
        );
    }
}

#[test]
fn metrics_format_types_labels() {
    let f = Fixture::new();
    let r1 = rule_r1();
    let s = f.start(&[
        ("flush.interval", Some("\"1s\"")),
        ("alerts.rules", Some(&r1)),
    ]);
    let r = ingest(&s, "app", r#"[{"a":1},5]"#);
    assert_eq!(r.status, 200, "{}", r.text());
    std::thread::sleep(Duration::from_secs(5));
    let r = s.get_auth("/metrics");
    assert_eq!(
        r.header("content-type"),
        Some("text/plain; version=0.0.4; charset=utf-8")
    );
    let text = r.text();
    let stable = [
        ("amberlog_build_info", "gauge"),
        ("amberlog_ingest_records_total", "counter"),
        ("amberlog_manifest_commits_total", "counter"),
        ("amberlog_ready", "gauge"),
        ("amberlog_dataset_disabled", "gauge"),
        ("amberlog_wal_bytes", "gauge"),
        ("amberlog_lake_lag_seconds", "gauge"),
        ("amberlog_search_rejected_total", "counter"),
        // REQ-AMB-OBS-HEALTH-METRICS-V6 的稳定集合
        ("amberlog_backlog_memory_bytes", "gauge"),
        ("amberlog_alert_evaluations_total", "counter"),
        ("amberlog_alert_deliveries_total", "counter"),
        ("amberlog_reconciliation_windows", "gauge"),
    ];
    let known = |name: &str| stable.iter().any(|(n, _)| *n == name) || NON_STABLE.contains(&name);
    // 指标族从样本行的指标名收集：只输出样本、不输出 `# TYPE` 行的指标族同样要属于稳定集合
    let sampled: BTreeSet<&str> = text
        .lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
        .map(|l| &l[..l.find(['{', ' ']).unwrap_or(l.len())])
        .collect();
    for name in &sampled {
        assert!(known(name), "{name} 不属稳定集合：{text}");
    }
    // `# TYPE` 行只用来核对类型；各块规定的非稳定指标（桶状态、迟到记录、合并、巡检）不在本场景的断言之内
    let types: BTreeSet<(&str, &str)> = text
        .lines()
        .filter_map(|l| l.strip_prefix("# TYPE "))
        .map(|l| l.split_once(' ').unwrap())
        .filter(|(name, _)| !NON_STABLE.contains(name))
        .collect();
    for (name, t) in &types {
        assert!(
            stable.iter().any(|(n, st)| n == name && st == t),
            "{name} {t} 不属稳定集合：{text}"
        );
    }
    // 除 `amberlog_dataset_disabled` 外有样本的指标族逐族要求 `# TYPE` 行存在且类型正确
    for (name, t) in stable
        .iter()
        .filter(|(n, _)| *n != "amberlog_dataset_disabled")
    {
        assert!(types.contains(&(name, t)), "缺少 # TYPE {name} {t}：{text}");
    }
    assert!(
        !text
            .lines()
            .any(|l| l.starts_with("amberlog_dataset_disabled")),
        "{text}"
    );
    for (family, labels) in [
        ("amberlog_build_info", vec!["version", "commit"]),
        (
            "amberlog_ingest_records_total",
            vec!["org", "stream", "outcome"],
        ),
        (
            "amberlog_manifest_commits_total",
            vec!["org", "stream", "result"],
        ),
        ("amberlog_ready", vec![]),
        ("amberlog_wal_bytes", vec![]),
        ("amberlog_lake_lag_seconds", vec!["org", "stream"]),
        ("amberlog_search_rejected_total", vec!["reason"]),
        ("amberlog_backlog_memory_bytes", vec![]),
        ("amberlog_alert_evaluations_total", vec!["rule", "outcome"]),
        ("amberlog_alert_deliveries_total", vec!["rule", "result"]),
        (
            "amberlog_reconciliation_windows",
            vec!["org", "stream", "verdict"],
        ),
    ] {
        let want: BTreeSet<&str> = labels.into_iter().collect();
        let samples: Vec<&str> = text
            .lines()
            .filter(|l| !l.starts_with('#'))
            .filter(|l| {
                l.strip_prefix(family)
                    .is_some_and(|r| r.starts_with(['{', ' ']))
            })
            .collect();
        assert!(!samples.is_empty(), "{family} 无样本：{text}");
        for line in samples {
            assert_eq!(label_names(line), want, "{line}");
        }
    }
    assert!(
        text.lines()
            .any(|l| l.starts_with("amberlog_build_info{") && l.ends_with(" 1")),
        "{text}"
    );
}

/// 各块规定的非稳定指标族：桶状态、迟到记录、合并、巡检四个，快照回收、查询资源上限与只读目录端点各一个（REQ-AMB-OBS-HEALTH-METRICS-V5），
/// 告警评估记录的一个（REQ-AMB-ALR-EVALUATIONS，REQ-AMB-OBS-HEALTH-METRICS-V6）。
const NON_STABLE: [&str; 8] = [
    "amberlog_hot_buckets",
    "amberlog_late_records_total",
    "amberlog_compactions_total",
    "amberlog_hot_fsck_deleted_total",
    "amberlog_snapshot_gc_deleted_total",
    "amberlog_search_running",
    "amberlog_catalog_requests_total",
    "amberlog_alert_history_unwritten",
];

#[test]
fn metrics_search_rejected_zero_at_start() {
    let f = Fixture::new();
    let s = f.start(&[]);
    assert_eq!(s.wait_readyz(200, Duration::from_secs(30)).status, 200);
    let text = s.get_auth("/metrics").text();
    let lines: Vec<&str> = text.lines().collect();
    assert!(
        lines.contains(&"amberlog_search_rejected_total{reason=\"too_many_queries\"} 0"),
        "{text}"
    );
    assert!(
        lines.contains(&"amberlog_search_rejected_total{reason=\"query_memory_exceeded\"} 0"),
        "{text}"
    );
}

/// 样本行的标签名集合。
fn label_names(line: &str) -> BTreeSet<&str> {
    let Some(start) = line.find('{') else {
        return BTreeSet::new();
    };
    let end = line.rfind('}').unwrap();
    let mut names = BTreeSet::new();
    let mut rest = &line[start + 1..end];
    while let Some((name, after)) = rest.split_once("=\"") {
        names.insert(name.trim_start_matches(','));
        // 跳过带转义的取值
        let mut chars = after.char_indices();
        let mut close = after.len();
        while let Some((i, c)) = chars.next() {
            match c {
                '\\' => {
                    chars.next();
                }
                '"' => {
                    close = i;
                    break;
                }
                _ => {}
            }
        }
        rest = &after[close + 1..];
    }
    names
}

#[test]
fn metrics_ready_mirrors_readyz() {
    let f = Fixture::new();
    let s = f.start(&[]);
    assert_eq!(s.wait_readyz(200, Duration::from_secs(10)).status, 200);
    assert_eq!(sample(&metrics(&s), "amberlog_ready"), Some(1.0));
    let _slow = draining(&s);
    assert_eq!(sample(&metrics(&s), "amberlog_ready"), Some(0.0));
    assert_eq!(s.get("/readyz").status, 503);
}

#[test]
fn metrics_ingest_per_record() {
    let f = Fixture::new();
    let s = f.start(&[]);
    let before = metrics(&s);
    let r = ingest(&s, "m", r#"[{"a":1},"x"]"#);
    assert_eq!(r.status, 200, "{}", r.text());
    let after = metrics(&s);
    for outcome in ["accepted", "rejected"] {
        assert_eq!(
            ingest_total(&after, "m", outcome) - ingest_total(&before, "m", outcome),
            1.0,
            "{outcome}：{after}"
        );
    }
}

#[test]
fn metrics_ingest_not_counted_on_replay() {
    let f = Fixture::new();
    let mut s = f.start(&[FLUSH_1H]);
    ingest_ok(&s, "rp", r#"[{"a":1},{"a":2}]"#);
    s.sigkill();
    let s = f.start(&[FLUSH_1H]);
    assert_eq!(s.wait_readyz(200, Duration::from_secs(10)).status, 200);
    assert_eq!(ingest_total(&metrics(&s), "rp", "accepted"), 0.0);
    // 核实确实重放过：两条记录都查得回（重放失效时计数同样为 0）
    assert_eq!(common::api::search(&s, "SELECT a FROM rp").len(), 2);
}

#[test]
fn metrics_ingest_not_counted_cases() {
    let f = Fixture::new();
    let fault = WalFault::new(&f);
    let s = fault.start(&f, &[]);
    ingest_ok(&s, "m2", r#"[{"a":1}]"#);
    let before = metrics(&s);

    let mut statuses = vec![ingest(&s, "m2", "[5,6]").status];
    statuses.push(ingest_with(&s, "m2", &[("Content-Encoding", "br")], br#"[{"a":2}]"#).status);
    let element = format!("[{}]", sent_element("src", minute_of(now_us()), 1));
    statuses.push(sent(&s, "m2", &element).status);
    fault.inject("write");
    statuses.push(ingest(&s, "m2", r#"[{"a":3}]"#).status);
    fault.clear();
    assert_eq!(statuses, [400, 415, 200, 503]);

    let after = metrics(&s);
    assert_eq!(
        ingest_total(&after, "m2", "rejected") - ingest_total(&before, "m2", "rejected"),
        2.0,
        "{after}"
    );
    assert_eq!(
        ingest_total(&after, "m2", "accepted"),
        ingest_total(&before, "m2", "accepted"),
        "{after}"
    );
}

// ---------------------------------------------------------------------------
// 提交计数
// ---------------------------------------------------------------------------

fn commits_total(text: &str, stream: &str, result: &str) -> f64 {
    sample(
        text,
        &format!(
            "amberlog_manifest_commits_total{{org=\"default\",stream=\"{stream}\",result=\"{result}\"}}"
        ),
    )
    .unwrap_or(0.0)
}

/// 20 秒内按 2 秒节拍计的失败次数应在 [9, 11] 之内。
fn assert_per_interval(f0: f64, f1: f64) {
    let d = f1 - f0;
    assert!(
        (9.0..=11.0).contains(&d),
        "F1 − F0 = {d}（F0 = {f0}，F1 = {f1}）"
    );
}

const FLUSH_2S: common::Kv = ("flush.interval", Some("\"2s\""));

#[test]
fn metrics_commits_ok() {
    let f = Fixture::new();
    let s = f.start(&[("flush.interval", Some("\"1s\""))]);
    let r = ingest(&s, "ok1", "[]");
    assert_eq!(r.status, 200, "{}", r.text());
    let first = commits_total(&metrics(&s), "ok1", "ok");
    assert!(first >= 1.0, "创世快照应计入 ok：{first}");
    ingest_ok(&s, "ok1", r#"[{"a":1}]"#);
    std::thread::sleep(Duration::from_secs(5));
    let second = commits_total(&metrics(&s), "ok1", "ok");
    assert!(second - first >= 1.0, "{first} → {second}");
}

#[test]
fn metrics_commit_failed_per_interval_genesis_pending() {
    let inst = S3Instance::new();
    let s = inst.start(&[FLUSH_2S]);
    inst.relay.stop();
    let r = ingest(&s, "g", "[]");
    assert_eq!(r.status, 200, "{}", r.text());
    let f0 = commits_total(&metrics(&s), "g", "failed");
    std::thread::sleep(Duration::from_secs(20));
    let f1 = commits_total(&metrics(&s), "g", "failed");
    assert_per_interval(f0, f1);
}

#[test]
fn metrics_commit_failed_per_interval_records_pending() {
    let inst = S3Instance::new();
    let s = inst.start(&[FLUSH_2S]);
    ingest_ok(&s, "rp2", r#"[{"a":0}]"#);
    // 等这条记录的快照提交：ok 计数在创世快照之后再加 1
    let deadline = Instant::now() + Duration::from_secs(20);
    while commits_total(&metrics(&s), "rp2", "ok") < 2.0 {
        assert!(Instant::now() < deadline, "rp2 的记录未在 20 秒内提交");
        std::thread::sleep(Duration::from_millis(200));
    }
    inst.relay.stop();
    ingest_ok(&s, "rp2", r#"[{"a":1}]"#);
    let text = metrics(&s);
    let (f0, k0) = (
        commits_total(&text, "rp2", "failed"),
        commits_total(&text, "rp2", "ok"),
    );
    std::thread::sleep(Duration::from_secs(20));
    let f1 = commits_total(&metrics(&s), "rp2", "failed");
    inst.relay.resume();
    std::thread::sleep(Duration::from_secs(10));
    let text = metrics(&s);
    let (f2, k2) = (
        commits_total(&text, "rp2", "failed"),
        commits_total(&text, "rp2", "ok"),
    );
    std::thread::sleep(Duration::from_secs(10));
    let f3 = commits_total(&metrics(&s), "rp2", "failed");
    assert_per_interval(f0, f1);
    assert!(k2 - k0 >= 1.0, "恢复后应提交成功：K0 = {k0}，K2 = {k2}");
    assert_eq!(f3, f2, "恢复后不再计失败");
}

#[test]
fn metrics_commit_failed_per_interval_suspended() {
    let f = Fixture::new();
    let mut s = f.start(&[FLUSH_1H]);
    ingest_ok(&s, "sv", r#"{"a":1}"#);
    s.sigkill();
    set_current_format_version(&f.dataset("sv"), 3);

    let s = f.start(&[FLUSH_2S]);
    assert_eq!(s.wait_readyz(200, Duration::from_secs(10)).status, 200);
    let f0 = commits_total(&metrics(&s), "sv", "failed");
    std::thread::sleep(Duration::from_secs(20));
    let f1 = commits_total(&metrics(&s), "sv", "failed");
    assert_per_interval(f0, f1);
}

/// 把数据集的当前快照文件中的 `format_version` 就地改为 `version`。
fn set_current_format_version(ds: &common::store::Ds, version: u64) {
    let n = ds.current().expect("当前快照存在");
    let mut snap = ds.snapshot(n);
    snap["format_version"] = serde_json::json!(version);
    ds.write_json(&snapshot_rel(n), &snap);
}

/// 两个 stream 各摄取一条记录、经 SIGTERM 停机提交（`flush.interval` 为 `1h`）。
fn committed_pair(f: &Fixture, a: &str, b: &str) {
    let mut s = f.start(&[FLUSH_1H]);
    ingest_ok(&s, a, r#"[{"a":1}]"#);
    ingest_ok(&s, b, r#"[{"a":1}]"#);
    s.signal("TERM");
    assert_eq!(s.wait_exit(Duration::from_secs(30)), Some(Some(0)));
}

fn disabled_lines(text: &str, stream: &str) -> Vec<String> {
    text.lines()
        .filter(|l| {
            l.starts_with("amberlog_dataset_disabled{")
                && l.contains(&format!("stream=\"{stream}\""))
        })
        .map(str::to_owned)
        .collect()
}

#[test]
fn metrics_dataset_disabled() {
    let f = Fixture::new();
    committed_pair(&f, "app", "web");
    let app = f.dataset("app");
    let head = app.head();
    let bucket = head["buckets"][0].clone();
    app.remove(bucket["manifest"].as_str().unwrap());

    let s = f.start(&[]);
    let body = serde_json::json!({"query": {
        "sql": "SELECT * FROM app",
        "start_time": bucket["time_min"],
        "end_time": bucket["time_max"].as_i64().unwrap() + 1,
    }})
    .to_string();
    let r = s.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", common::BEARER)],
        body.as_bytes(),
    );
    assert_eq!(r.status, 500, "{}", r.text());
    let text = metrics(&s);
    assert_eq!(
        disabled_lines(&text, "app"),
        vec![
            "amberlog_dataset_disabled{org=\"default\",stream=\"app\",state=\"manifest_unreadable\"} 1"
                .to_owned()
        ],
        "{text}"
    );
    assert!(disabled_lines(&text, "web").is_empty(), "{text}");
}

#[test]
fn metrics_dataset_disabled_format_version() {
    let f = Fixture::new();
    committed_pair(&f, "fv", "ok2");
    set_current_format_version(&f.dataset("fv"), 3);

    let s = f.start(&[]);
    assert_eq!(s.wait_readyz(200, Duration::from_secs(10)).status, 200);
    let text = metrics(&s);
    assert_eq!(
        disabled_lines(&text, "fv"),
        vec![
            "amberlog_dataset_disabled{org=\"default\",stream=\"fv\",state=\"format_version\"} 1"
                .to_owned()
        ],
        "{text}"
    );
    assert!(disabled_lines(&text, "ok2").is_empty(), "{text}");
}

/// `amberlog_manifest_commits_total{org="default",stream="app",result=<result>}` 的值。
fn app_commits(s: &Server, result: &str) -> Option<f64> {
    common::store::metric(
        s,
        &format!(
            "amberlog_manifest_commits_total{{org=\"default\",stream=\"app\",result=\"{result}\"}}"
        ),
    )
}

#[test]
fn metrics_compaction_commit_counted() {
    use common::store::{FLUSH_1S, HOUR, hour_of, seed_small_files};
    let f = Fixture::new();
    seed_small_files(&f, "app", hour_of(now_us()) - 2 * HOUR, 3, 10);
    // 重启后不再摄取：本进程里唯一的提交是封口桶的合并
    let s = f.start(&[("lifecycle.seal_grace", Some("\"1s\"")), FLUSH_1S]);
    let compactions_ok = || {
        common::store::metric(
            &s,
            "amberlog_compactions_total{org=\"default\",stream=\"app\",result=\"ok\"}",
        )
    };
    let (n, done) = common::api::poll(Duration::from_secs(60), compactions_ok, |n| *n == Some(1.0));
    assert!(done, "60 秒内没有一次成功的合并：{n:?}");
    assert_eq!(app_commits(&s, "ok"), Some(1.0));
    assert_eq!(app_commits(&s, "failed").unwrap_or(0.0), 0.0);
}

#[test]
fn metrics_tombstone_commit_counted() {
    use common::store::{FLUSH_1S, kill_at_marker, stop};
    let f = Fixture::new();
    let ds = f.dataset("app");
    let mut s = f.start(&[FLUSH_1S]);
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
    ds.wait_records(1, Duration::from_secs(15));
    stop(&mut s);
    let before: BTreeSet<String> = ds.keys_under("data/").into_iter().collect();
    // 上传数据文件之后、写出桶 manifest 之前崩溃，留下从未进链的孤儿数据文件
    let wal = format!("\"{}\"", f.root().join("wal-crash").display());
    let (mut s, marker) = f.start_paused(
        &[FLUSH_1S, ("wal.dir", Some(wal.as_str()))],
        "commit.after_data_files",
    );
    ingest_ok(&s, "app", r#"[{"a":2}]"#);
    kill_at_marker(&mut s, &marker);
    let orphan = ds
        .keys_under("data/")
        .into_iter()
        .find(|k| !before.contains(k))
        .expect("崩溃留下一个孤儿数据文件");

    // 以新的空 wal.dir 重启、不再摄取：本进程里唯一的提交是巡检的留痕提交
    let wal = format!("\"{}\"", f.root().join("wal-2").display());
    let s = f.start(&[
        FLUSH_1S,
        ("hot_fsck.interval", Some("\"5s\"")),
        ("hot_fsck.min_orphan_age", Some("\"1s\"")),
        ("wal.dir", Some(wal.as_str())),
    ]);
    let (gone, done) = common::api::poll(
        Duration::from_secs(30),
        || !ds.exists(&orphan),
        |gone| *gone,
    );
    assert!(done && gone, "30 秒内 {orphan} 未被巡检删除");
    assert_eq!(app_commits(&s, "ok"), Some(1.0));
    assert_eq!(app_commits(&s, "failed").unwrap_or(0.0), 0.0);
}

/// 入湖在 `lake.after_cold_commit` 暂停与热层写入失败都是 `fault-injection` 下的注入（`TESTBED_FAULT_GATE`、
/// `TESTBED_FAULT_STORE`）。
#[cfg(feature = "fault-injection")]
#[test]
fn metrics_lake_handover_commit_failed_counted() {
    let f = Fixture::new();
    let gate = f.root().join("fault-gate");
    std::fs::write(&gate, "hold").unwrap();
    let faults = f.root().join("fault-store");
    std::fs::write(&faults, "").unwrap();
    let mut cmd = f.serve(&[
        ("flush.interval", Some("\"1s\"")),
        ("lifecycle.lake_watermark", Some("\"1h\"")),
    ]);
    cmd.env("TESTBED_FAULT_GATE", "lake.after_cold_commit")
        .env("TESTBED_FAULT_GATE_FILE", &gate)
        .env("TESTBED_FAULT_STORE", &faults);
    let s = Server::start(cmd);
    let hour = 3_600_000_000;
    let start = now_us().div_euclid(hour) * hour - 3 * hour;
    let r = ingest(&s, "app", &format!(r#"[{{"_timestamp":{}}}]"#, start + 1));
    assert_eq!(r.status, 200, "{}", r.text());
    let deadline = Instant::now() + Duration::from_secs(130);
    while std::fs::read_to_string(&gate).is_ok_and(|g| g.trim() == "hold") {
        assert!(
            Instant::now() < deadline,
            "入湖未走到暂停点：{:#?}",
            s.stderr_lines()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let series = |result: &str| {
        format!(
            "amberlog_manifest_commits_total{{org=\"default\",stream=\"app\",result=\"{result}\"}}"
        )
    };
    let text = metrics(&s);
    let (f0, k0) = (
        sample(&text, &series("failed")).unwrap_or(0.0),
        sample(&text, &series("ok")).unwrap_or(0.0),
    );
    std::fs::write(&faults, "write default/app/ 403\n").unwrap();
    std::fs::write(&gate, "go").unwrap();
    std::thread::sleep(Duration::from_secs(130));
    let text = metrics(&s);
    let (f1, k1) = (
        sample(&text, &series("failed")).unwrap_or(0.0),
        sample(&text, &series("ok")).unwrap_or(0.0),
    );
    assert!(f1 - f0 >= 1.0, "F0 {f0}，F1 {f1}");
    assert_eq!(k1, k0);
}

// ---------------------------------------------------------------------------
// 只读目录模式（REQ-AMB-OBS-HEALTH-METRICS-V5 ①②）
// ---------------------------------------------------------------------------

#[test]
fn readyz_catalog_mode_readonly_probe() {
    let inst = S3Instance::new();
    let creds = common::s3::session_credentials(&inst.read_only_policy("_iceberg/"));
    let c = Server::start(inst.catalog(&[("wal.dir", None)], &creds));
    assert_eq!(c.wait_readyz(200, Duration::from_secs(60)).status, 200);

    inst.relay.stop();
    std::thread::sleep(Duration::from_secs(35));
    let v = c.get("/readyz").assert_envelope(503, "not_ready");
    let checks = &v["details"]["checks"];
    assert_eq!(checks["wal"], true);
    assert_eq!(checks["object_store"], false);
    assert_eq!(checks["shutting_down"], false);
}

#[test]
fn metrics_catalog_mode_minimal() {
    let f = Fixture::new();
    let c = Server::start(f.catalog(&[]));
    assert_eq!(c.wait_readyz(200, Duration::from_secs(60)).status, 200);
    let r = c.get_auth("/metrics");
    assert_eq!(r.status, 200, "{}", r.text());
    let text = r.text();
    assert!(
        text.lines().any(|l| l.starts_with("amberlog_build_info")),
        "{text}"
    );
    assert!(text.lines().any(|l| l == "amberlog_ready 1"), "{text}");
}

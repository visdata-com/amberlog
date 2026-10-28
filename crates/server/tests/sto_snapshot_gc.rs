//! REQ-AMB-STO-SNAPSHOT-GC 的系统测试：以子进程运行 `amberlog`，对象存储用
//! `file://` 临时目录，测试直接读、删、写回目录下的快照文件来构造断档与重现快照，读 `/metrics` 与 stderr 检查计数与
//! warn 日志。
//!
//! 一个新数据集的第一次摄取先写出创世快照 1，再写出含该记录的快照 2；场景所说「已为 stream `app` 写出快照 1 至 6」
//! 由创世快照与其后 5 次摄取写出（[`six`]）。块写「与某场景相同的实例与数据」的场景各自重复准备步骤。

mod common;

use std::time::{Duration, Instant};

use common::api::{ingest_ok, now_us, search_resp};
use common::store::{
    Ds, FLUSH_1S, HOUR, commits_total, hour_of, ingest_times, kill_at_marker, metric, snapshot_rel,
    stop, wait_snapshots_gone,
};
use common::{BEARER, Fixture, Kv, Server, http};
use serde_json::{Value, json};

const INTERVAL_1H: Kv = ("hot_fsck.interval", Some("\"1h\""));
const INTERVAL_5S: Kv = ("hot_fsck.interval", Some("\"5s\""));
const MIN_AGE_1S: Kv = ("hot_fsck.min_orphan_age", Some("\"1s\""));
const KEEP_2: Kv = ("hot_retention.snapshots", Some("2"));
const KEEP_3: Kv = ("hot_retention.snapshots", Some("3"));
const KEEP_10: Kv = ("hot_retention.snapshots", Some("10"));
const AGE_1S: Kv = ("hot_retention.age", Some("\"1s\""));
const AGE_60S: Kv = ("hot_retention.age", Some("\"60s\""));
const AGE_1H: Kv = ("hot_retention.age", Some("\"1h\""));

/// 场景「等待 15 秒」。
const WAIT: Duration = Duration::from_secs(15);

/// 以 `hot_fsck.interval` 为 `1h`（期间不回收）运行实例，写出快照 1 至 6（创世快照与 5 次摄取），以 SIGTERM 停止。
fn six(f: &Fixture) {
    let mut s = f.start(&[INTERVAL_1H, FLUSH_1S]);
    let ds = f.dataset("app");
    assert_eq!(ingest_times(&s, &ds, 1, 5), 6);
    stop(&mut s);
    assert_eq!(ds.snapshot_ids(), (1..=6).collect::<Vec<_>>());
}

/// 以测试专用时钟偏移 `offset_s` 秒启动（`fault-injection` 特性下的 `TESTBED_CLOCK_OFFSET`）。
fn start_offset(f: &Fixture, config: &[Kv], offset_s: i64) -> Server {
    let mut cmd = f.serve(config);
    cmd.env("TESTBED_CLOCK_OFFSET", offset_s.to_string());
    Server::start(cmd)
}

fn assert_exist(ds: &Ds, ids: impl IntoIterator<Item = u64>) {
    for id in ids {
        assert!(
            ds.exists(&snapshot_rel(id)),
            "快照 {id} 应存在：{:?}",
            ds.snapshot_ids()
        );
    }
}

fn assert_gone(ds: &Ds, ids: impl IntoIterator<Item = u64>) {
    for id in ids {
        assert!(
            !ds.exists(&snapshot_rel(id)),
            "快照 {id} 应不存在：{:?}",
            ds.snapshot_ids()
        );
    }
}

/// `amberlog_snapshot_gc_deleted_total{org="default",stream="app",kind=<kind>}`。
fn gc_deleted(s: &Server, kind: &str) -> Option<f64> {
    metric(
        s,
        &format!(
            "amberlog_snapshot_gc_deleted_total{{org=\"default\",stream=\"app\",kind=\"{kind}\"}}"
        ),
    )
}

/// stderr 中 `level` 为 `warn`、文本含 `default` 与 `app` 的 JSON 行。
fn warn_lines(s: &Server) -> Vec<String> {
    s.stderr_lines()
        .into_iter()
        .filter(|l| {
            serde_json::from_str::<Value>(l).is_ok_and(|v| v["level"] == "warn")
                && l.contains("default")
                && l.contains("app")
        })
        .collect()
}

/// `/metrics` 是否含 `stream="app"` 的 `amberlog_dataset_disabled` 序列。
fn app_disabled(s: &Server) -> bool {
    s.get_auth("/metrics")
        .text()
        .lines()
        .any(|l| l.starts_with("amberlog_dataset_disabled{") && l.contains("stream=\"app\""))
}

/// 一个新的空 `wal.dir` 的配置取值（TOML 字符串）。
fn fresh_wal(f: &Fixture, name: &str) -> String {
    format!("\"{}\"", f.root().join(name).display())
}

fn with<'a>(base: &[Kv<'a>], more: &[Kv<'a>]) -> Vec<Kv<'a>> {
    base.iter().chain(more).copied().collect()
}

// ---------------------------------------------------------------------------
// retain-start
// ---------------------------------------------------------------------------

/// 场景「按时长保留时早于保留时长的第一个快照本身也保留」。块写「等待 70 秒后再摄取 2 次」与「重启后等待 12 秒」：
/// 以测试专用时钟偏移 70 秒启动写出快照 5、6 的实例与回收的实例，代替真实等待 70 秒（`created_at` 由写方时钟写出，
/// 偏移同时作用于写出快照 5、6 与回收的进程），断言不变。
#[test]
fn snapshot_gc_age_keeps_first_older_snapshot() {
    let f = Fixture::new();
    let ds = f.dataset("app");
    let mut s = f.start(&[INTERVAL_1H, FLUSH_1S]);
    assert_eq!(ingest_times(&s, &ds, 1, 3), 4);
    stop(&mut s);
    let mut s = start_offset(&f, &[INTERVAL_1H, FLUSH_1S], 70);
    assert_eq!(ingest_times(&s, &ds, 4, 2), 6);
    stop(&mut s);

    let _s = start_offset(&f, &[KEEP_2, AGE_60S, INTERVAL_5S, FLUSH_1S], 70);
    std::thread::sleep(Duration::from_secs(12));
    assert_exist(&ds, [4, 5, 6]);
    assert_gone(&ds, [1, 2, 3]);
}

#[test]
fn snapshot_gc_within_age_kept() {
    let f = Fixture::new();
    let ds = f.dataset("app");
    let s = f.start(&[KEEP_2, AGE_1H, INTERVAL_5S, FLUSH_1S]);
    ingest_times(&s, &ds, 1, 6);
    std::thread::sleep(WAIT);
    let n = ds.current().unwrap();
    assert_eq!(ds.snapshot_ids(), (1..=n).collect::<Vec<_>>());
}

// ---------------------------------------------------------------------------
// resurrected
// ---------------------------------------------------------------------------

/// 「重现快照被回收删除」与「删除重现快照时输出 warn 日志」的共同步骤：保存快照 1 的字节，摄取 6 次，等快照 1 被回收，
/// 写回快照 1 并等待 15 秒。
fn resurrect(f: &Fixture) -> Server {
    let ds = f.dataset("app");
    let s = f.start(&[KEEP_3, AGE_1S, INTERVAL_5S, FLUSH_1S]);
    ingest_times(&s, &ds, 1, 1);
    let saved = ds.read(&snapshot_rel(1));
    ingest_times(&s, &ds, 2, 5);
    wait_snapshots_gone(&ds, [1], Duration::from_secs(60));
    ds.write(&snapshot_rel(1), &saved);
    std::thread::sleep(WAIT);
    s
}

#[test]
fn snapshot_gc_resurrected_deleted() {
    let f = Fixture::new();
    let s = resurrect(&f);
    assert_gone(&f.dataset("app"), [1]);
    assert_eq!(gc_deleted(&s, "resurrected_snapshot"), Some(1.0));
}

#[test]
fn snapshot_gc_resurrected_warn_log() {
    let f = Fixture::new();
    let s = resurrect(&f);
    assert!(
        warn_lines(&s)
            .iter()
            .any(|l| l.contains("default/app") && l.contains("\"snapshot_id\":1")),
        "{:#?}",
        s.stderr_lines()
    );
}

// ---------------------------------------------------------------------------
// term-bottom
// ---------------------------------------------------------------------------

/// 块写「分 6 次摄取」，而断言点名快照 5、6 是回收后剩下的两个：按创世快照与 5 次摄取写出快照 1 至 6。
#[test]
fn snapshot_gc_raised_retention_bottom_not_abandoned() {
    let f = Fixture::new();
    let ds = f.dataset("app");
    let mut s = f.start(&[KEEP_2, AGE_1S, INTERVAL_5S, FLUSH_1S]);
    assert_eq!(ingest_times(&s, &ds, 1, 5), 6);
    wait_snapshots_gone(&ds, 1..=4, Duration::from_secs(60));
    stop(&mut s);

    let s = f.start(&[KEEP_10, AGE_1H, INTERVAL_5S]);
    std::thread::sleep(WAIT);
    assert_exist(&ds, [5, 6]);
    assert_eq!(warn_lines(&s), Vec::<String>::new());
}

// ---------------------------------------------------------------------------
// schedule
// ---------------------------------------------------------------------------

#[test]
fn snapshot_gc_first_run_after_one_interval() {
    let f = Fixture::new();
    six(&f);
    let ds = f.dataset("app");
    let ready = Instant::now();
    let _s = f.start(&[KEEP_2, AGE_1S, ("hot_fsck.interval", Some("\"20s\""))]);
    std::thread::sleep((ready + Duration::from_secs(10)).saturating_duration_since(Instant::now()));
    assert_exist(&ds, 1..=6);
    std::thread::sleep((ready + Duration::from_secs(35)).saturating_duration_since(Instant::now()));
    assert_gone(&ds, 1..=4);
    assert_exist(&ds, [5, 6]);
}

// ---------------------------------------------------------------------------
// before-fsck
// ---------------------------------------------------------------------------

/// 崩溃点 `commit.after_data_upload` 即实现中的 `commit.after_data_files`（上传数据文件之后、写出桶 manifest 之前）。
#[test]
fn snapshot_gc_before_fsck() {
    let f = Fixture::new();
    six(&f);
    let ds = f.dataset("app");
    let before: Vec<String> = ds.keys_under("data/");
    let (mut s, marker) = f.start_paused(&[INTERVAL_1H, FLUSH_1S], "commit.after_data_files");
    ingest_ok(&s, "app", r#"[{"seq":6}]"#);
    kill_at_marker(&mut s, &marker);
    let crashed: Vec<String> = ds
        .keys_under("data/")
        .into_iter()
        .filter(|k| !before.contains(k))
        .collect();
    assert_eq!(crashed.len(), 1, "{crashed:?}");
    let file = &crashed[0];

    let wal = fresh_wal(&f, "wal-2");
    let (_s, gate) = f.start_gated(
        &[
            KEEP_2,
            AGE_1S,
            INTERVAL_5S,
            MIN_AGE_1S,
            ("wal.dir", Some(wal.as_str())),
        ],
        "snapshot_gc.before_delete",
    );
    std::thread::sleep(WAIT);
    assert!(gate.held(), "回收应已停在暂停点");
    assert!(ds.exists(file));
    assert_exist(&ds, 1..=6);
    gate.release();
    std::thread::sleep(WAIT);
    assert!(!ds.exists(file));
    assert_gone(&ds, 1..=4);
}

// ---------------------------------------------------------------------------
// skip-disabled
// ---------------------------------------------------------------------------

#[test]
fn snapshot_gc_skips_disabled_dataset() {
    let f = Fixture::new();
    six(&f);
    let ds = f.dataset("app");
    let head = ds.snapshot(6);
    let h = head["buckets"][0].clone();
    ds.remove(h["manifest"].as_str().unwrap());
    let start = h["time_min"].as_i64().unwrap().div_euclid(HOUR) * HOUR;

    let s = f.start(&[KEEP_2, AGE_1S, INTERVAL_5S]);
    let body = json!({"query": {"sql": "SELECT * FROM app", "start_time": start, "end_time": start + HOUR}})
        .to_string();
    let r = s.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        body.as_bytes(),
    );
    assert_eq!(r.status, 500, "{}", r.text());
    std::thread::sleep(WAIT);
    assert_exist(&ds, 1..=6);
    assert!(
        [
            gc_deleted(&s, "snapshot"),
            gc_deleted(&s, "resurrected_snapshot")
        ]
        .iter()
        .all(|v| v.is_none_or(|v| v == 0.0))
    );
}

// ---------------------------------------------------------------------------
// no-start-on-shutdown
// ---------------------------------------------------------------------------

#[test]
fn snapshot_gc_no_start_on_shutdown() {
    let f = Fixture::new();
    six(&f);
    let ds = f.dataset("app");
    let (mut s, _marker) = f.start_paused(&[KEEP_2, AGE_1S, INTERVAL_5S], "shutdown.before_exit");
    s.signal("TERM");
    std::thread::sleep(WAIT);
    assert_exist(&ds, 1..=6);
    s.sigkill();
}

// ---------------------------------------------------------------------------
// delete-below
// ---------------------------------------------------------------------------

#[test]
fn snapshot_gc_contiguous_suffix() {
    let f = Fixture::new();
    let ds = f.dataset("app");
    let s = f.start(&[KEEP_3, AGE_1S, INTERVAL_5S, FLUSH_1S]);
    let n = ingest_times(&s, &ds, 1, 6);
    std::thread::sleep(WAIT);
    assert_eq!(ds.snapshot_ids(), vec![n - 2, n - 1, n]);
    let r = search_resp(&s, "SELECT * FROM app");
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["total"], 6);
}

// ---------------------------------------------------------------------------
// order
// ---------------------------------------------------------------------------

#[test]
fn snapshot_gc_crash_mid_run_no_gap() {
    let f = Fixture::new();
    six(&f);
    let ds = f.dataset("app");
    let (mut s, marker) = f.start_paused(
        &[KEEP_2, AGE_1S, INTERVAL_5S],
        "snapshot_gc.after_each_delete",
    );
    kill_at_marker(&mut s, &marker);
    assert_gone(&ds, [1]);
    assert_exist(&ds, 2..=6);
}

// ---------------------------------------------------------------------------
// only-snapshots
// ---------------------------------------------------------------------------

#[test]
fn snapshot_gc_only_snapshot_files() {
    let f = Fixture::new();
    let ds = f.dataset("app");
    let s = f.start(&[
        KEEP_2,
        AGE_1S,
        INTERVAL_5S,
        ("hot_fsck.min_orphan_age", Some("\"1h\"")),
        FLUSH_1S,
    ]);
    let mut kept: Vec<String> = Vec::new();
    for i in 0..6 {
        ingest_times(&s, &ds, i, 1);
        let head = ds.head();
        for b in head["buckets"].as_array().unwrap() {
            kept.push(b["manifest"].as_str().unwrap().to_owned());
        }
        for e in ds.file_entries(&head) {
            kept.push(e["path"].as_str().unwrap().to_owned());
        }
    }
    let counters = ds.keys_under("counters/");
    std::thread::sleep(WAIT);
    for k in kept.iter().chain(&counters) {
        assert!(ds.exists(k), "{k} 应存在");
    }
    assert!(ds.exists("manifest/_current"));
}

// ---------------------------------------------------------------------------
// head
// ---------------------------------------------------------------------------

#[test]
fn snapshot_gc_hint_and_tail_kept() {
    let f = Fixture::new();
    let ds = f.dataset("app");
    let (s, faults) = f.start_with_store_faults(&[KEEP_2, AGE_1S, INTERVAL_5S, FLUSH_1S]);
    ingest_times(&s, &ds, 1, 5);
    let n = ds.wait_hint_at_tail(Duration::from_secs(15));
    faults.set("write default/app/manifest/_current 503\n");
    ingest_ok(&s, "app", r#"[{"seq":6}]"#);
    ds.wait_snapshot(n + 1, Duration::from_secs(15));
    std::thread::sleep(WAIT);
    assert_exist(&ds, [n, n + 1]);
    assert_eq!(ds.hint(), Some(n));
    assert_gone(&ds, 1..n);
}

// ---------------------------------------------------------------------------
// existence
// ---------------------------------------------------------------------------

#[test]
fn snapshot_gc_dataset_still_exists() {
    let f = Fixture::new();
    let ds = f.dataset("app");
    let mut s = f.start(&[KEEP_2, AGE_1S, INTERVAL_5S, FLUSH_1S]);
    ingest_times(&s, &ds, 1, 6);
    std::thread::sleep(WAIT);
    assert_gone(&ds, [1]);
    stop(&mut s);

    let wal = fresh_wal(&f, "wal-2");
    let s = f.start(&[
        KEEP_2,
        AGE_1S,
        INTERVAL_5S,
        FLUSH_1S,
        ("wal.dir", Some(wal.as_str())),
    ]);
    let r = search_resp(&s, "SELECT * FROM app");
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["total"], 6);
    let hint = ds.hint().expect("提示存在");
    assert!(ds.exists(&snapshot_rel(hint)));
}

// ---------------------------------------------------------------------------
// in-use
// ---------------------------------------------------------------------------

#[test]
fn snapshot_gc_inflight_query_snapshot_kept() {
    let f = Fixture::new();
    let ds = f.dataset("app");
    let (s, gate) = f.start_gated(
        &[KEEP_2, AGE_1S, INTERVAL_5S, MIN_AGE_1S, FLUSH_1S],
        "query.after_hot_snapshot",
    );
    let ts = now_us();
    let h = hour_of(ts);
    for i in 0..3 {
        ingest_ok(
            &s,
            "app",
            &format!(r#"[{{"_timestamp":{},"seq":{i}}}]"#, ts + i),
        );
        ds.wait_records(i as u64 + 1, Duration::from_secs(15));
        std::thread::sleep(Duration::from_millis(1500));
    }
    let snap = ds.current().unwrap();
    let m = ds.snapshot(snap)["buckets"][0]["manifest"]
        .as_str()
        .unwrap()
        .to_owned();
    let addr = s.addr;
    let body = json!({"query": {"sql": "SELECT * FROM app", "start_time": h, "end_time": h + HOUR, "size": 100}})
        .to_string();
    let q = std::thread::spawn(move || {
        http(
            addr,
            "POST",
            "/api/default/_search",
            &[("Authorization", BEARER)],
            body.as_bytes(),
        )
    });
    gate.wait_held(&s, Duration::from_secs(15));
    for i in 3..8 {
        ingest_ok(
            &s,
            "app",
            &format!(r#"[{{"_timestamp":{},"seq":{i}}}]"#, ts + i),
        );
        ds.wait_records(i as u64 + 1, Duration::from_secs(15));
        std::thread::sleep(Duration::from_millis(1500));
    }
    std::thread::sleep(WAIT);
    assert_exist(&ds, [snap]);
    assert!(ds.exists(&m), "{m}");
    assert_gone(&ds, 1..snap);

    gate.release();
    let r = q.join().unwrap();
    assert_eq!(r.status, 200, "{}", r.text());
    assert!(
        r.json()["hits"]
            .as_array()
            .unwrap()
            .iter()
            .any(|hit| hit["seq"] == 0),
        "{}",
        r.text()
    );
    let body =
        json!({"query": {"sql": "SELECT * FROM app", "start_time": h, "end_time": h + HOUR}})
            .to_string();
    let r = s.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        body.as_bytes(),
    );
    assert_eq!(r.status, 200, "{}", r.text());
}

// ---------------------------------------------------------------------------
// no-write
// ---------------------------------------------------------------------------

#[test]
fn snapshot_gc_writes_nothing() {
    let f = Fixture::new();
    let ds = f.dataset("app");
    let s = f.start(&[KEEP_2, AGE_1S, INTERVAL_5S, FLUSH_1S]);
    let n = ingest_times(&s, &ds, 1, 6);
    let c = commits_total(&s, "app", "ok");
    std::thread::sleep(WAIT);
    assert_eq!(ds.current(), Some(n));
    assert!(!ds.exists(&snapshot_rel(n + 1)));
    assert_eq!(commits_total(&s, "app", "ok"), c);
    for m in ds.bucket_manifests(&ds.snapshot(n)) {
        for d in m["deletions"].as_array().unwrap() {
            let p = d["path"].as_str().unwrap();
            assert!(
                amberlog_storage::layout::parse_snapshot_path(p).is_none(),
                "{p}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// incomplete、no-disable、abandon-warn
// ---------------------------------------------------------------------------

/// 写出快照 1 至 6 后删除快照 3，以 `hot_retention.snapshots` 为 `2`、`hot_retention.age` 为 `1h`、
/// `hot_fsck.interval` 为 `5s` 重启并等待 15 秒（其间不对 `app` 发起查询或摄取）。
fn gap_restart(f: &Fixture) -> Server {
    six(f);
    f.dataset("app").remove(&snapshot_rel(3));
    let s = f.start(&[KEEP_2, AGE_1H, INTERVAL_5S]);
    std::thread::sleep(WAIT);
    s
}

#[test]
fn snapshot_gc_incomplete_abandons() {
    let f = Fixture::new();
    let _s = gap_restart(&f);
    assert_exist(&f.dataset("app"), [1, 2, 4, 5, 6]);
}

#[test]
fn snapshot_gc_read_failure_not_disabled() {
    let f = Fixture::new();
    let s = gap_restart(&f);
    assert!(!app_disabled(&s));
    assert!(
        !s.json_lines()
            .iter()
            .any(|v| v["event"] == "dataset_disabled" && v["dataset"] == "default/app"),
        "{:#?}",
        s.stderr_lines()
    );
}

#[test]
fn snapshot_gc_abandon_warn_log() {
    let f = Fixture::new();
    let s = gap_restart(&f);
    assert!(!warn_lines(&s).is_empty(), "{:#?}", s.stderr_lines());
    assert_exist(&f.dataset("app"), [1, 2, 4, 5, 6]);
}

// ---------------------------------------------------------------------------
// store-unavailable
// ---------------------------------------------------------------------------

#[test]
fn snapshot_gc_store_unavailable_abandons() {
    let f = Fixture::new();
    six(&f);
    let ds = f.dataset("app");
    let (s, faults) = f.start_with_store_faults(&[KEEP_2, AGE_1S, INTERVAL_5S]);
    // 快照文件名是零填充 20 位编号，以 `0` 起头；不命中 `_current`、`buckets/`、`retired/` 与 `cold-deletions/`
    faults.set("delete default/app/manifest/0 503\n");
    std::thread::sleep(WAIT);
    assert_exist(&ds, 1..=6);
    assert!(!app_disabled(&s));
    faults.set("");
    std::thread::sleep(WAIT);
    assert_gone(&ds, 1..=4);
}

// ---------------------------------------------------------------------------
// wal-unchanged
// ---------------------------------------------------------------------------

/// 实例 X 以 W1 写出快照 N 后，在 `commit.after_snapshot_create` 处带着 `{"seq":7}` 崩溃；实例 Y 以空的 W2 摄取 8、9、10
/// 并回收掉快照 N+1；实例 Z 以 W1 启动。
#[test]
fn snapshot_gc_stale_wal_intent_no_duplicate() {
    let f = Fixture::new();
    let ds = f.dataset("app");
    let mut s = f.start(&[FLUSH_1S]);
    let n = ingest_times(&s, &ds, 0, 1);
    stop(&mut s);
    assert_eq!(ds.hint(), Some(n));
    let (mut x, marker) = f.start_paused(&[FLUSH_1S], "commit.after_snapshot_create");
    ingest_ok(&x, "app", r#"[{"seq":7}]"#);
    kill_at_marker(&mut x, &marker);
    assert!(ds.exists(&snapshot_rel(n + 1)));

    let w2 = fresh_wal(&f, "wal-2");
    let y_cfg = [KEEP_2, AGE_1S, INTERVAL_5S, FLUSH_1S];
    let mut y = f.start(&with(&y_cfg, &[("wal.dir", Some(w2.as_str()))]));
    ingest_times(&y, &ds, 8, 3);
    wait_snapshots_gone(&ds, [n + 1], Duration::from_secs(60));
    stop(&mut y);

    let z = f.start(&y_cfg);
    z.wait_readyz(200, Duration::from_secs(30));
    std::thread::sleep(Duration::from_secs(3));
    let r = s_seq(&z, "SELECT seq FROM app WHERE seq >= 7");
    assert_eq!(r, vec![7, 8, 9, 10]);
}

/// 执行 `sql`（`size` 为 10000），返回各 hit 的 `seq`（升序）。
fn s_seq(s: &Server, sql: &str) -> Vec<i64> {
    let r = search_resp(s, sql);
    assert_eq!(r.status, 200, "{}", r.text());
    let mut seqs: Vec<i64> = r.json()["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["seq"].as_i64().unwrap())
        .collect();
    seqs.sort();
    seqs
}

// ---------------------------------------------------------------------------
// metrics
// ---------------------------------------------------------------------------

#[test]
fn snapshot_gc_deleted_counter() {
    let f = Fixture::new();
    let ds = f.dataset("app");
    let s = f.start(&[KEEP_3, AGE_1S, INTERVAL_5S, FLUSH_1S]);
    let n = ingest_times(&s, &ds, 1, 6);
    std::thread::sleep(WAIT);
    assert_eq!(gc_deleted(&s, "snapshot"), Some((n - 3) as f64));
    assert!(gc_deleted(&s, "resurrected_snapshot").is_none_or(|v| v == 0.0));
}

//! REQ-AMB-STO-MANIFEST-COMMIT 的系统测试：以子进程启动已构建的 `amberlog`。
//!
//! 条件写、引用不变量与并发提交在 amberlog-storage 的集成测试里对两类后端直接验证。崩溃类场景用暂停点
//! `commit.after_data_files`、`commit.after_snapshot_create`：进程停机时的 flush 走到该步骤前阻塞，测试见到标记后 SIGKILL；
//! 段号跳过一场景用 `commit.after_segment_write`（TTL 写出冷层变更段之后），冷层文件到期靠时钟偏移重启。
//! 「对象存储已停止」用 tests/common/s3.rs 的 TCP 转发（被测进程经它连 MinIO 台架）。

mod common;

use std::collections::BTreeSet;
use std::process::Output;
use std::time::Duration;

use common::api::{FLUSH_1H, ingest, ingest_ok, poll, search};
use common::lake::{COLD_TTL, COLD_TTL_OFFSET_S};
use common::s3::S3Instance;
use common::store::{
    Ds, FLUSH_1S, HOUR, Remote, commits_total, ingest_times, kill_at_marker, sha256, snapshot_rel,
    wait_snapshots_gone,
};
use common::{Fixture, Kv, Server, wait_file};

const WAIT: Duration = Duration::from_secs(15);

fn stop(server: &mut Server) {
    server.signal("TERM");
    assert_eq!(
        server.wait_exit(Duration::from_secs(30)),
        Some(Some(0)),
        "{:#?}",
        server.stderr_lines()
    );
}

/// 以 `flush.interval` 为 `1h` 启动，摄取 `body`，SIGTERM 停机（停机 flush 并提交）。
fn committed(fx: &Fixture, stream: &str, body: &str) {
    let mut server = fx.start(&[FLUSH_1H]);
    ingest_ok(&server, stream, body);
    stop(&mut server);
}

/// 以暂停点 `step` 启动，摄取 `body` 得 200 后 SIGTERM，停机提交走到 `step` 前被 SIGKILL。
fn crash_at_shutdown_commit(fx: &Fixture, stream: &str, body: &str, step: &str) {
    let (mut server, marker) = fx.start_paused(&[FLUSH_1H], step);
    ingest_ok(&server, stream, body);
    server.signal("TERM");
    kill_at_marker(&mut server, &marker);
}

fn new_wal_dir(fx: &Fixture) -> String {
    format!("\"{}\"", fx.root().join("wal-new").display())
}

fn values(server: &Server, stream: &str) -> BTreeSet<String> {
    search(server, &format!("SELECT k FROM {stream}"))
        .iter()
        .map(|h| h["k"].as_str().unwrap().to_owned())
        .collect()
}

#[test]
fn commit_s3_conditional_put_required() {
    let f = Fixture::new();
    let Output { status, stderr, .. } = f
        .serve(&[
            ("object_store.url", Some("\"s3://bucket/prefix\"")),
            ("object_store.s3.conditional_put", None),
        ])
        .output()
        .expect("启动 amberlog");
    let stderr = String::from_utf8_lossy(&stderr);
    assert_eq!(status.code(), Some(2), "stderr:\n{stderr}");
    assert!(
        stderr
            .lines()
            .any(|l| l.contains("object_store.s3.conditional_put") && l.contains("missing")),
        "stderr 应有一行说明缺少 object_store.s3.conditional_put:\n{stderr}"
    );
}

// ---------------------------------------------------------------------------
// conditional-writes
// ---------------------------------------------------------------------------

/// 数据集的每个快照（按快照号升序）：快照号、经其 `buckets` 可达的 `files` 条目数与 `summary.record_count`。
fn shape(ds: &Ds) -> Vec<(u64, usize, u64)> {
    ds.snapshot_ids()
        .into_iter()
        .map(|id| {
            let s = ds.snapshot(id);
            (
                id,
                ds.file_entries(&s).len(),
                s["summary"]["record_count"].as_u64().unwrap(),
            )
        })
        .collect()
}

/// 三轮：每轮 4 个线程同时向 `pa`、`pb` 各摄取 5 批（时间戳分布在以 `h0` 起的两个小时桶），随后 SIGTERM 停机提交。
fn parity_workload(h0: i64, start: &dyn Fn() -> Server) {
    for round in 0..3 {
        let mut server = start();
        let addr = server.addr;
        let threads: Vec<_> = (0..4)
            .map(|t| {
                std::thread::spawn(move || {
                    for i in 0..5 {
                        for stream in ["pa", "pb"] {
                            let ts = h0
                                + ((t + i) % 2) as i64 * HOUR
                                + t as i64 * 60_000_000
                                + i as i64 * 1_000_000;
                            let body = format!(r#"[{{"_timestamp":{ts},"k":"{round}-{t}-{i}"}}]"#);
                            let r = common::http(
                                addr,
                                "POST",
                                &format!("/api/default/{stream}/_json"),
                                &[("Authorization", common::BEARER)],
                                body.as_bytes(),
                            );
                            assert_eq!(r.status, 200, "{}", r.text());
                        }
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        stop(&mut server);
    }
}

/// 两个后端写同样的两个小时桶：此刻前 2 小时与前 1 小时所在的桶。`lifecycle.seal_grace` 为 `24h`，两桶在测试期间
/// 一直开放、不会被合并，「期间未发生合并与巡检留痕」的前提不依赖进程活不到第一次合并检查（巡检默认 1 小时）。
#[test]
fn commit_backend_parity() {
    let h0 = common::store::hour_of(common::api::now_us()) - 2 * HOUR;
    let config = [FLUSH_1H, ("lifecycle.seal_grace", Some("\"24h\""))];
    let local = Fixture::new();
    parity_workload(h0, &|| local.start(&config));

    let remote = S3Instance::new();
    parity_workload(h0, &|| remote.start(&config));

    for stream in ["pa", "pb"] {
        let file = shape(&local.dataset(stream));
        let mirror = remote.fixture.root().join("mirror").join(stream);
        let s3 = shape(&Remote::new(remote.store(), stream).mirror(&mirror));
        // 逐快照比较：快照数、各快照的 `files` 条目总数与 `summary.record_count` 都一致
        assert_eq!(file, s3, "{stream}");
        assert_eq!(file.last().map(|x| x.2), Some(60), "{stream}");
    }
}

// ---------------------------------------------------------------------------
// write-order
// ---------------------------------------------------------------------------

#[test]
fn commit_crash_before_snapshot() {
    let fx = Fixture::new();
    committed(&fx, "app", r#"[{"k":"a"}]"#);
    let ds = fx.dataset("app");
    let n = ds.current().unwrap();
    let before: BTreeSet<String> = ds.keys_under("data/").into_iter().collect();
    crash_at_shutdown_commit(&fx, "app", r#"[{"k":"b"}]"#, "commit.after_data_files");
    let orphans: Vec<String> = ds
        .keys_under("data/")
        .into_iter()
        .filter(|k| !before.contains(k))
        .collect();
    assert!(!orphans.is_empty(), "暂停点之前应已上传数据文件");

    let server = fx.start(&[FLUSH_1H]);
    assert_eq!(server.wait_readyz(200, Duration::from_secs(10)).status, 200);
    assert_eq!(ds.current(), Some(n));
    assert_eq!(
        values(&server, "app"),
        BTreeSet::from(["a".into(), "b".into()])
    );
    let referenced: BTreeSet<String> = ds
        .file_entries(&ds.snapshot(n))
        .iter()
        .map(|e| e["path"].as_str().unwrap().to_owned())
        .collect();
    for o in &orphans {
        assert!(!referenced.contains(o), "{o}");
    }
}

// ---------------------------------------------------------------------------
// startup-and-tail
// ---------------------------------------------------------------------------

#[test]
fn commit_crash_after_snapshot_before_hint() {
    let fx = Fixture::new();
    committed(&fx, "app", r#"[{"k":"a"}]"#);
    let ds = fx.dataset("app");
    let n = ds.wait_hint_at_tail(WAIT);
    crash_at_shutdown_commit(&fx, "app", r#"[{"k":"b"}]"#, "commit.after_snapshot_create");
    assert!(ds.exists(&common::store::snapshot_rel(n + 1)));
    assert_eq!(ds.hint(), Some(n), "提示尚未写为 N+1");

    let wal = new_wal_dir(&fx);
    let server = fx.start(&[FLUSH_1S, ("wal.dir", Some(wal.as_str()))]);
    ingest_ok(&server, "app", r#"[{"k":"c"}]"#);
    ds.wait_records(3, WAIT);
    let next = common::store::snapshot_rel(n + 2);
    assert!(ds.exists(&next), "快照 N+2 不存在");
    assert_eq!(ds.json(&next)["parent_snapshot_id"], n + 1);
    assert_eq!(
        values(&server, "app"),
        BTreeSet::from(["a".into(), "b".into(), "c".into()])
    );
}

#[test]
fn commit_orphan_schema_baseline() {
    let fx = Fixture::new();
    committed(&fx, "ob", r#"[{"a":1}]"#);
    let ds = fx.dataset("ob");
    let n = ds.current().unwrap();
    assert!(
        !ds.head()["schema"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["name"] == "c")
    );
    crash_at_shutdown_commit(&fx, "ob", r#"{"c":1}"#, "commit.after_snapshot_create");
    assert_eq!(ds.hint(), Some(n), "提示尚未写为 N+1");

    let wal = new_wal_dir(&fx);
    let y = fx.start(&[FLUSH_1H, ("wal.dir", Some(wal.as_str()))]);
    assert_eq!(y.wait_readyz(200, Duration::from_secs(10)).status, 200);
    // 链尾快照 N+1 的 schema（含 Int64 列 `c`）先于受理记录取作基线：字符串取值落入影子列 `c__utf8`
    // （REQ-AMB-DM-SCHEMA-DRIFT；早期版本为 400 `type_conflict`）
    let r = ingest(&y, "ob", r#"{"c":"x"}"#);
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(
        search(&y, "SELECT c__utf8 FROM ob WHERE c__utf8 = 'x'").len(),
        1
    );
    assert!(ds.current().unwrap() > n);
}

// ---------------------------------------------------------------------------
// when-to-commit
// ---------------------------------------------------------------------------

#[test]
fn commit_no_empty_snapshot() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1S]);
    ingest_ok(&server, "ne", r#"[{"a":1}]"#);
    let ds = fx.dataset("ne");
    ds.wait_records(1, WAIT);
    let count = ds.keys_under("manifest/").len();
    let current = ds.read("manifest/_current");
    std::thread::sleep(Duration::from_secs(10));
    assert_eq!(ds.keys_under("manifest/").len(), count);
    assert_eq!(ds.read("manifest/_current"), current);
}

#[test]
fn commit_snapshot_slot_never_overwritten() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1S, ("storage.commit_max_retries", Some("0"))]);
    let ds = fx.dataset("so");
    ingest_ok(&server, "so", r#"[{"a":1}]"#);
    ds.wait_records(1, WAIT);
    // 前提：提示与当前快照均为 N
    let n = ds.wait_hint_at_tail(WAIT);
    let slot = common::store::snapshot_rel(n + 1);
    ds.write(&slot, b"{}");
    let f0 = commits_total(&server, "so", "failed");
    ingest_ok(&server, "so", r#"[{"a":2}]"#);
    std::thread::sleep(Duration::from_secs(5));
    assert_eq!(ds.read(&slot), b"{}");
    assert!(commits_total(&server, "so", "failed") > f0);
    assert_eq!(ds.hint(), Some(n));
    assert!(
        !ds.exists(&common::store::snapshot_rel(n + 2)),
        "写出了快照 N+2"
    );
}

// ---------------------------------------------------------------------------
// ingest-guarantee
// ---------------------------------------------------------------------------

#[test]
fn commit_store_outage_ingest_continues() {
    let inst = S3Instance::new();
    let server = inst.start(&[("flush.interval", Some("\"2s\""))]);
    let remote = Remote::new(inst.store(), "oo");
    ingest_ok(&server, "oo", r#"[{"k":"before"}]"#);
    let deadline = std::time::Instant::now() + WAIT;
    let n = loop {
        if let Some(n) = remote.current()
            && remote.json(&common::store::snapshot_rel(n))["summary"]["record_count"] == 1
        {
            break n;
        }
        assert!(std::time::Instant::now() < deadline, "未等到首次提交");
        std::thread::sleep(Duration::from_millis(100));
    };

    inst.relay.stop();
    for i in 0..20 {
        let r = ingest(&server, "oo", &format!(r#"[{{"k":"new-{i}"}}]"#));
        assert_eq!(r.status, 200, "第 {i} 次：{}", r.text());
        std::thread::sleep(Duration::from_secs(1));
    }
    inst.relay.resume();
    std::thread::sleep(Duration::from_secs(15));

    assert!(remote.current().unwrap() > n);
    let got: Vec<String> = search(&server, "SELECT k FROM oo")
        .iter()
        .map(|h| h["k"].as_str().unwrap().to_owned())
        .filter(|k| k != "before")
        .collect();
    let distinct: BTreeSet<&String> = got.iter().collect();
    assert_eq!(got.len(), 20, "{got:?}");
    assert_eq!(distinct.len(), 20, "{got:?}");
}

/// 合并提交停在暂停点 `commit.compaction_before_snapshot`（已以链头为父定下新桶 manifest、尚未写快照，持有该数据集的
/// 提交入口）。停住期间照常摄取，flush 提交须排在它后面；没有进程内串行时 flush 会先写出同号快照，合并提交因冲突
/// 在重试上限 0 下计入 `failed`。
#[test]
fn commit_in_process_serialized() {
    let fx = Fixture::new();
    let (server, gate) = fx.start_gated(
        &[FLUSH_1S, ("storage.commit_max_retries", Some("0"))],
        "commit.compaction_before_snapshot",
    );
    let ds = fx.dataset("cs");
    let now = common::api::now_us();
    let hour = 3_600_000_000;
    let b = (now - 3 * hour).div_euclid(hour) * hour;
    let label = common::store::bucket_label(b);
    for i in 0..2 {
        ingest_ok(
            &server,
            "cs",
            &format!(r#"[{{"_timestamp":{},"i":{i}}}]"#, b + i),
        );
        std::thread::sleep(Duration::from_secs(5));
    }
    let f0 = commits_total(&server, "cs", "failed");
    let compactions_ok = || {
        common::store::metric(
            &server,
            "amberlog_compactions_total{org=\"default\",stream=\"cs\",result=\"ok\"}",
        )
        .unwrap_or(0.0)
    };
    assert_eq!(compactions_ok(), 0.0, "循环开始前合并不应已提交");
    // (停住时的循环序号, 停住时的 `_current`)
    let mut held: Option<(u64, Option<u64>)> = None;
    let mut released = false;
    for i in 0..90 {
        ingest_ok(&server, "cs", &format!(r#"[{{"k":{i}}}]"#));
        if !released && gate.held() {
            match held {
                None => held = Some((i, ds.current())),
                Some((at, current)) if i >= at + 5 => {
                    // 停住的 5 秒里每秒都有新记录、flush 每秒一次：没有一次 flush 提交越过停住的合并提交
                    assert_eq!(ds.current(), current, "合并提交停住期间链头前进了");
                    assert_eq!(compactions_ok(), 0.0);
                    gate.release();
                    released = true;
                }
                Some(_) => {}
            }
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    assert!(released, "90 秒内合并提交没有走到暂停点（或停住不足 5 秒）");
    assert_eq!(compactions_ok(), 1.0, "合并应在循环期间提交恰一次");
    let head = ds.head();
    let m = ds.bucket_manifest(&head, &label).expect("桶 B 在快照中");
    assert_eq!(m["files"].as_array().unwrap().len(), 1, "{m}");
    assert_eq!(commits_total(&server, "cs", "failed"), f0);
    let chain = ds.chain();
    for pair in chain.windows(2) {
        assert_eq!(
            pair[0]["snapshot_id"].as_u64().unwrap(),
            pair[1]["snapshot_id"].as_u64().unwrap() + 1
        );
    }
}

// ---------------------------------------------------------------------------
// hint（REQ-AMB-STO-MANIFEST-COMMIT-V3「提示的写法」）
// ---------------------------------------------------------------------------

/// 超过一个 `flush.interval`（`1s`）的等待。
const PAST_FLUSH: Duration = Duration::from_millis(2500);

/// 提示对 `stream` 的写入一律以 HTTP 503 失败。
fn hint_writes_fail(stream: &str) -> String {
    format!("put default/{stream}/manifest/_current 503\n")
}

/// `ds` 的快照 `id` 新增了数据文件（`files` 中有 `added_in` 为 `id` 的条目）。
fn assert_added(ds: &Ds, id: u64) {
    assert!(
        !ds.added_entries(id).is_empty(),
        "快照 {id} 没有新增数据文件"
    );
}

#[test]
fn commit_hint_updated_after_snapshot() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1S]);
    let ds = fx.dataset("app");
    ingest_ok(&server, "app", r#"[{"k":"a"}]"#);
    ds.wait_records(1, WAIT);
    let n = ds.wait_hint_at_tail(WAIT);
    ingest_ok(&server, "app", r#"[{"k":"b"}]"#);
    ds.wait_snapshot(n + 1, WAIT);
    std::thread::sleep(PAST_FLUSH);
    assert!(!ds.exists(&snapshot_rel(n + 2)));
    assert_eq!(ds.hint(), Some(n + 1));
}

#[test]
fn commit_hint_write_failure_not_commit_failure() {
    let fx = Fixture::new();
    let (server, faults) = fx.start_with_store_faults(&[FLUSH_1S]);
    let ds = fx.dataset("app");
    ingest_ok(&server, "app", r#"[{"k":"a"}]"#);
    ds.wait_records(1, WAIT);
    let n = ds.wait_hint_at_tail(WAIT);
    faults.set(&hint_writes_fail("app"));
    let ok0 = commits_total(&server, "app", "ok");
    ingest_ok(&server, "app", r#"[{"k":"b"}]"#);
    ds.wait_snapshot(n + 1, WAIT);
    std::thread::sleep(PAST_FLUSH);
    assert_eq!(ds.snapshot(n + 1)["parent_snapshot_id"], n);
    assert_added(&ds, n + 1);
    assert_eq!(search(&server, "SELECT k FROM app WHERE k = 'b'").len(), 1);
    assert_eq!(commits_total(&server, "app", "ok"), ok0 + 1);
    assert_eq!(ds.hint(), Some(n), "提示写入一律失败");
}

#[test]
fn commit_hint_backfill_failure_blocks_next_snapshot() {
    let fx = Fixture::new();
    let (server, faults) = fx.start_with_store_faults(&[FLUSH_1S]);
    let ds = fx.dataset("app");
    ingest_ok(&server, "app", r#"[{"k":"a"}]"#);
    ds.wait_records(1, WAIT);
    let n = ds.wait_hint_at_tail(WAIT);
    faults.set(&hint_writes_fail("app"));
    ingest_ok(&server, "app", r#"[{"k":"b"}]"#);
    ds.wait_snapshot(n + 1, WAIT);
    std::thread::sleep(2 * PAST_FLUSH);
    ingest_ok(&server, "app", r#"[{"k":"c"}]"#);
    std::thread::sleep(2 * PAST_FLUSH);
    assert_eq!(ds.snapshot(n + 1)["parent_snapshot_id"], n);
    assert!(
        !ds.exists(&snapshot_rel(n + 2)),
        "提示补写未成功却创建了快照 N+2"
    );
    assert_eq!(ds.hint(), Some(n));
}

#[test]
fn commit_hint_lagging_tail_is_current() {
    let fx = Fixture::new();
    let ds = fx.dataset("app");
    let mut x = fx.start(&[FLUSH_1S]);
    ingest_ok(&x, "app", r#"[{"k":"a"}]"#);
    ds.wait_records(1, WAIT);
    ingest_ok(&x, "app", r#"[{"k":"r"}]"#);
    ds.wait_records(2, WAIT);
    stop(&mut x);
    let n = ds.current().unwrap();
    assert!(n >= 2);
    assert_eq!(
        ds.snapshot(n)["summary"]["record_count"],
        2,
        "R 只在快照 N 新增"
    );
    ds.write_json(
        "manifest/_current",
        &serde_json::json!({"snapshot_id": n - 1}),
    );

    let server = fx.start(&[FLUSH_1S]);
    assert_eq!(server.wait_readyz(200, WAIT).status, 200);
    assert_eq!(search(&server, "SELECT k FROM app WHERE k = 'r'").len(), 1);
    ingest_ok(&server, "app", r#"[{"k":"x"}]"#);
    ds.wait_snapshot(n + 1, WAIT);
    std::thread::sleep(PAST_FLUSH);
    assert_eq!(ds.snapshot(n + 1)["parent_snapshot_id"], n);
    assert!(ds.hint().unwrap() >= n, "提示 {:?}", ds.hint());
}

#[test]
fn commit_create_exists_readback_same_digest() {
    let fx = Fixture::new();
    let (server, faults) = fx.start_with_store_faults(&[FLUSH_1S]);
    let ds = fx.dataset("app");
    ingest_ok(&server, "app", r#"[{"k":"a"}]"#);
    ds.wait_records(1, WAIT);
    let n = ds.wait_hint_at_tail(WAIT);
    // 照常执行对快照 N+1 的第一次创建请求，但把应答换成 503
    faults.set(&format!(
        "create default/app/{} after-503 times=1\n",
        snapshot_rel(n + 1)
    ));
    ingest_ok(&server, "app", r#"[{"k":"b"}]"#);
    ds.wait_snapshot(n + 1, WAIT);
    std::thread::sleep(2 * PAST_FLUSH);
    assert_added(&ds, n + 1);
    assert!(!ds.exists(&snapshot_rel(n + 2)), "读回摘要相等却换号重写");
    assert_eq!(search(&server, "SELECT k FROM app WHERE k = 'b'").len(), 1);
}

/// 冷层文件到期以 `retention.period` 为 `6h` 运行，入湖之后以时钟偏移重启使其到期（[`Fixture::lake_expiring`]）；
/// 重启时以暂停点 `commit.after_segment_write` 运行，TTL 冷层提交成功、写出冷层变更段之后、创建引用它的快照之前被 SIGKILL。
#[test]
fn commit_segment_exists_skips_to_next_number() {
    let fx = Fixture::new();
    let handed = fx.lake_expiring();
    let marker = fx.root().join("segment-marker");
    let mut s = fx.start_offset(
        &COLD_TTL,
        COLD_TTL_OFFSET_S,
        &[
            (
                "TESTBED_FAULT_PAUSE",
                "commit.after_segment_write".to_owned(),
            ),
            ("TESTBED_FAULT_MARKER", marker.display().to_string()),
        ],
    );
    assert!(
        wait_file(&marker, Duration::from_secs(7 * 60)),
        "未走到 commit.after_segment_write：{:#?}",
        s.stderr_lines()
    );
    s.sigkill();
    let ds = fx.dataset("app");
    let first = "manifest/cold-deletions/00000000000000000001.json";
    assert!(ds.exists(first));
    assert!(ds.head()["cold_deletion_segment"].is_null());
    let digest = sha256(&ds.read(first));

    let s = fx.start_offset(&COLD_TTL, COLD_TTL_OFFSET_S, &[]);
    let (head, ok) = poll(
        Duration::from_secs(7 * 60),
        || ds.head(),
        |h| !h["cold_deletion_segment"].is_null(),
    );
    assert!(ok, "{head}\n{:#?}", s.stderr_lines());
    let second = "manifest/cold-deletions/00000000000000000002.json";
    assert_eq!(head["cold_deletion_segment"]["path"], second);
    let seg = ds.json(second);
    assert_eq!(seg["segment_id"], 2);
    assert!(seg["previous"].is_null(), "{seg}");
    let paths: BTreeSet<&str> = seg["deletions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["path"].as_str().unwrap())
        .collect();
    for p in handed.keys() {
        assert!(paths.contains(p.as_str()), "{p}：{seg}");
    }
    assert_eq!(sha256(&ds.read(first)), digest);
}

// ---------------------------------------------------------------------------
// REQ-AMB-STO-MANIFEST-COMMIT-V4：快照回收之后不分叉、不重提
// ---------------------------------------------------------------------------

const GC_FAST: [Kv<'static>; 4] = [
    ("hot_retention.snapshots", Some("2")),
    ("hot_retention.age", Some("\"1s\"")),
    ("hot_fsck.interval", Some("\"5s\"")),
    FLUSH_1S,
];

#[test]
fn commit_hint_deleted_after_gc_no_new_genesis() {
    let f = Fixture::new();
    let ds = f.dataset("app");
    let mut s = f.start(&GC_FAST);
    ingest_times(&s, &ds, 1, 6);
    wait_snapshots_gone(&ds, [1], Duration::from_secs(60));
    let n = ds.current().unwrap();
    stop(&mut s);
    ds.remove("manifest/_current");

    let s = f.start(&GC_FAST);
    s.wait_readyz(200, Duration::from_secs(30));
    let _ = ingest(&s, "app", r#"[{"seq":7}]"#);
    std::thread::sleep(Duration::from_secs(3));
    assert!(!ds.exists(&snapshot_rel(1)));
    assert!(!ds.exists(&snapshot_rel(n + 1)));
    assert!(!ds.exists("manifest/_current"));
}

#[test]
fn commit_unresolved_intent_snapshot_collected_not_recommitted() {
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

    let w2 = format!("\"{}\"", f.root().join("wal-2").display());
    let mut y_cfg = GC_FAST.to_vec();
    y_cfg.push(("wal.dir", Some(w2.as_str())));
    let mut y = f.start(&y_cfg);
    ingest_times(&y, &ds, 8, 3);
    wait_snapshots_gone(&ds, [n + 1], Duration::from_secs(60));
    let m = ds.current().unwrap();
    stop(&mut y);

    let z = f.start(&GC_FAST);
    z.wait_readyz(200, Duration::from_secs(30));
    std::thread::sleep(Duration::from_secs(3));
    assert!(!ds.exists(&snapshot_rel(m + 1)));
    let hits = search(&z, "SELECT seq FROM app WHERE seq = 7");
    assert_eq!(hits.len(), 1, "{hits:?}");
}

// ---------------------------------------------------------------------------
// 数据集之间（REQ-AMB-STO-MANIFEST-COMMIT-V5）
// ---------------------------------------------------------------------------

/// `app` 数据集前缀之下的写入挂起（对象存储注入的 `stall`，即 `commit.stall_dataset`）期间，`web` 的记录照常进入已提交快照。
/// 先摄取 `app` 并等过两个 `flush.interval`，使 `app` 的提交已挂在对象存储写入上，再摄取 `web`。
#[cfg(feature = "fault-injection")]
#[test]
fn manifest_commit_datasets_do_not_wait() {
    let fx = Fixture::new();
    let (server, faults) = fx.start_with_store_faults(&[FLUSH_1S]);
    let (app, web) = (fx.dataset("app"), fx.dataset("web"));
    ingest_ok(&server, "app", r#"[{"seq":0}]"#);
    ingest_ok(&server, "web", r#"[{"seq":0}]"#);
    let count = |ds: &Ds| -> u64 {
        ds.current().map_or(0, |id| {
            ds.snapshot(id)["summary"]["record_count"].as_u64().unwrap()
        })
    };
    app.wait_records(1, Duration::from_secs(30));
    web.wait_records(1, Duration::from_secs(30));
    let (app_before, web_before) = (app.current(), count(&web));

    faults.set("write default/app/ stall\n");
    let batch = |from: u64| {
        format!(
            "[{}]",
            (from..from + 10)
                .map(|seq| format!(r#"{{"seq":{seq}}}"#))
                .collect::<Vec<_>>()
                .join(",")
        )
    };
    ingest_ok(&server, "app", &batch(1));
    std::thread::sleep(Duration::from_secs(3));
    ingest_ok(&server, "web", &batch(1));
    web.wait_records(web_before + 10, Duration::from_secs(60));
    assert_eq!(app.current(), app_before, "挂起期间 app 的当前快照不变");

    faults.set("");
    app.wait_records(11, Duration::from_secs(60));
}

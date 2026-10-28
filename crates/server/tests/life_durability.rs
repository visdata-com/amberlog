//! REQ-AMB-LIFE-DURABILITY-V3 的系统测试：以子进程运行 `amberlog`，
//! 经 `_json` 摄取、经 `_search` 查询，发信号、改写 `wal.dir` 下的段文件、在 `fault-injection` 暂停点强杀来构造状态；
//! 「对象存储不可达」经 [`S3Instance`] 的进程内转发关停 MinIO 台架的连接；计数对象直接读 `file://` 对象存储。

mod common;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use amberlog_common::dataset::Dataset;
use amberlog_common::record::{FieldValue, Record};
use amberlog_storage::wal::{FileWal, Wal, WalEntry};
use common::api::{
    FLUSH_1H, SlowRequest, counter_window, ingest_ok, search, sent, sent_element, settle_minute,
};
use common::s3::S3Instance;
use common::{BEARER, Fixture, HeldPort, Kv, Resp, Server, wait_file};
use object_store::ObjectStoreExt;

const PAUSE_BEFORE_LISTEN: &str = "开始监听之前";
const PAUSE_BEFORE_CONFIG: &str = "装载配置之前";
const PAUSE_AFTER_INTENT: &str = "commit.after_intent";

fn serve_on(fx: &Fixture, port: u16, overrides: &[common::Kv]) -> std::process::Command {
    let addr = format!("\"127.0.0.1:{port}\"");
    let mut all = vec![("server.http_addr", Some(addr.as_str()))];
    all.extend_from_slice(overrides);
    fx.serve(&all)
}

/// 向 `stream` 摄取 `n` 条带唯一 `seq`（0..n）的记录，每批 `batch` 条，全部须得 200。
fn ingest_seq(server: &Server, stream: &str, n: u64, batch: u64) {
    for start in (0..n).step_by(batch as usize) {
        let records: Vec<String> = (start..(start + batch).min(n))
            .map(|seq| format!(r#"{{"seq":{seq}}}"#))
            .collect();
        ingest_ok(server, stream, &format!("[{}]", records.join(",")));
    }
}

/// `SELECT seq FROM <stream>` 的 `seq` 值，断言两两不同。
fn distinct_seqs(server: &Server, stream: &str) -> BTreeSet<u64> {
    let hits = search(server, &format!("SELECT seq FROM {stream}"));
    let seqs: BTreeSet<u64> = hits.iter().map(|h| h["seq"].as_u64().unwrap()).collect();
    assert_eq!(seqs.len(), hits.len(), "有重复的 seq");
    seqs
}

fn segments(dir: &Path) -> Vec<PathBuf> {
    let mut all: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "wal"))
        .collect();
    all.sort();
    all
}

fn largest_segment(dir: &Path) -> PathBuf {
    segments(dir)
        .into_iter()
        .max_by_key(|p| std::fs::metadata(p).unwrap().len())
        .expect("至少一个段文件")
}

fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

/// `file://` 对象存储中 `stream` 的当前快照（按找链尾确定）的 `summary.record_count`；尚无任何快照为 `None`。
fn current_record_count(fx: &Fixture, stream: &str) -> Option<u64> {
    let ds = fx.dataset(stream);
    let id = ds.current()?;
    ds.snapshot(id)["summary"]["record_count"].as_u64()
}

/// `inst` 的对象存储中 `amberlog/<rel>` 是否存在（测试进程直连台架，不经转发）。
fn s3_object_exists(inst: &S3Instance, rel: &str) -> bool {
    let store = inst.store();
    let path = object_store::path::Path::from(format!("{}/{rel}", store.root));
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            match store.inner.head(&path).await {
                Ok(_) => true,
                Err(object_store::Error::NotFound { .. }) => false,
                Err(e) => panic!("读 {path} 失败：{e}"),
            }
        })
}

/// 发信号并等进程以退出码 0 退出。
fn stop(server: &mut Server, signal: &str) {
    server.signal(signal);
    assert_eq!(
        server.wait_exit(Duration::from_secs(30)),
        Some(Some(0)),
        "{:#?}",
        server.stderr_lines()
    );
}

/// 停机开始后，新的 `/api/` 请求得 503：反复发 `_search` 直到不再是 200（至多 5 秒）。
fn search_until_rejected(server: &Server) -> Resp {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let r = server.request(
            "POST",
            "/api/default/_search",
            &[("Authorization", BEARER)],
            common::api::search_body("SELECT seq FROM app").as_bytes(),
        );
        // 停机闸门生效之前，查询可能先得 200 或 400（在途批次还没建出 `app`）：等到 503 为止
        if r.status == 503 || Instant::now() >= deadline {
            return r;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

// ---------------------------------------------------------------------------
// 确认与可查、不重复
// ---------------------------------------------------------------------------

#[test]
fn wal_sigkill_replay_zero_loss() {
    let fx = Fixture::new();
    let mut server = fx.start(&[]);
    ingest_seq(&server, "app", 1000, 100);
    server.sigkill();

    let server = fx.start(&[]);
    assert_eq!(server.wait_readyz(200, Duration::from_secs(10)).status, 200);
    assert_eq!(distinct_seqs(&server, "app"), (0..1000).collect());
}

#[test]
fn wal_no_duplicate_after_restart() {
    let fx = Fixture::new();
    let flush = [
        ("flush.max_records", Some("100")),
        ("flush.interval", Some("\"2s\"")),
    ];
    let mut server = fx.start(&flush);
    ingest_seq(&server, "app", 1000, 100);
    // 强杀前轮询到全部 1000 条已提交（当前快照计入 1000 条），一见到就强杀
    let deadline = Instant::now() + Duration::from_secs(30);
    while current_record_count(&fx, "app") != Some(1000) {
        assert!(Instant::now() < deadline, "30 秒内未见 1000 条全部提交");
        std::thread::sleep(Duration::from_millis(20));
    }
    server.sigkill();

    let server = fx.start(&flush);
    assert_eq!(server.wait_readyz(200, Duration::from_secs(10)).status, 200);
    assert_eq!(distinct_seqs(&server, "app"), (0..1000).collect());
    std::thread::sleep(Duration::from_secs(3));
    assert_eq!(current_record_count(&fx, "app"), Some(1000));
}

// ---------------------------------------------------------------------------
// 运行期的 WAL 段回收
// ---------------------------------------------------------------------------

#[test]
fn wal_segment_retained_until_commit() {
    let inst = S3Instance::new();
    let server = inst.start(&[
        ("flush.interval", Some("\"1s\"")),
        ("flush.max_records", Some("100")),
    ]);
    ingest_ok(&server, "app", r#"{"seq":0}"#);
    inst.relay.stop();

    // 每批约 1 MiB，共约 20 MiB：跨过几个 flush.interval，段文件写满后换新段
    let pad = "x".repeat(1000);
    let wal = inst.fixture.wal_dir();
    let mut counts = vec![segments(&wal).len()];
    for b in 0..20u64 {
        let records: Vec<String> = (0..1000)
            .map(|i| format!(r#"{{"seq":{},"pad":"{pad}"}}"#, 1 + b * 1000 + i))
            .collect();
        ingest_ok(&server, "app", &format!("[{}]", records.join(",")));
        counts.push(segments(&wal).len());
        std::thread::sleep(Duration::from_millis(150));
    }
    assert!(
        counts.windows(2).all(|w| w[0] <= w[1]),
        "段文件数量在摄取期间减少：{counts:?}"
    );
    assert!(
        counts.last().unwrap() > &1,
        "摄取量应使 WAL 换过段：{counts:?}"
    );
}

// ---------------------------------------------------------------------------
// 优雅停机
// ---------------------------------------------------------------------------

#[test]
fn shutdown_sigterm_exit0() {
    let fx = Fixture::new();
    let mut server = fx.start(&[FLUSH_1H]);
    ingest_seq(&server, "app", 10, 5);
    server.signal("TERM");
    assert_eq!(
        server.wait_exit(Duration::from_secs(30)),
        Some(Some(0)),
        "{:#?}",
        server.stderr_lines()
    );

    let server = fx.start(&[FLUSH_1H]);
    assert_eq!(distinct_seqs(&server, "app"), (0..10).collect());
}

#[test]
fn shutdown_sigint_same_as_sigterm() {
    let fx = Fixture::new();
    let mut server = fx.start(&[FLUSH_1H]);
    ingest_ok(&server, "si", r#"[{"a":1}]"#);
    stop(&mut server, "INT");
    assert_eq!(current_record_count(&fx, "si"), Some(1));
}

#[test]
fn shutdown_flushes_and_commits() {
    let fx = Fixture::new();
    let mut server = fx.start(&[FLUSH_1H]);
    ingest_ok(&server, "sf", r#"[{"a":1},{"a":2}]"#);
    ingest_ok(&server, "sf", r#"[{"a":3}]"#);
    stop(&mut server, "TERM");
    assert_eq!(current_record_count(&fx, "sf"), Some(3));
}

#[test]
fn shutdown_commit_failure_exit0_wal_kept() {
    let inst = S3Instance::new();
    let mut server = inst.start(&[FLUSH_1H]);
    ingest_ok(&server, "sk", r#"[{"k":"kept"}]"#);
    inst.relay.stop();
    stop(&mut server, "TERM");

    inst.relay.resume();
    let server = inst.start(&[FLUSH_1H]);
    assert_eq!(server.wait_readyz(200, Duration::from_secs(10)).status, 200);
    assert_eq!(
        search(&server, "SELECT k FROM sk WHERE k = 'kept'").len(),
        1
    );
}

#[test]
fn sigint_on_an_idle_server_exits_0() {
    let fx = Fixture::new();
    let mut server = fx.start(&[]);
    server.signal("INT");
    assert_eq!(server.wait_exit(Duration::from_secs(12)), Some(Some(0)));
}

#[test]
fn shutdown_drains_inflight_rejects_new() {
    let fx = Fixture::new();
    let mut server = fx.start(&[]);
    let slow = SlowRequest::start(&server, "app", r#"[{"seq":1},{"seq":2}]"#);
    server.signal("TERM");
    search_until_rejected(&server).assert_envelope(503, "unavailable");
    let r = slow.finish();
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(
        server.wait_exit(Duration::from_secs(30)),
        Some(Some(0)),
        "{:#?}",
        server.stderr_lines()
    );
}

#[test]
fn shutdown_timeout_bounds_drain() {
    let fx = Fixture::new();
    let mut server = fx.start(&[("server.shutdown_timeout", Some("\"2s\""))]);
    let _slow = SlowRequest::start(&server, "app", r#"[{"seq":1}]"#);
    server.signal("TERM");
    assert_eq!(
        server.wait_exit(Duration::from_secs(12)),
        Some(Some(0)),
        "{:#?}",
        server.stderr_lines()
    );
}

/// 在途请求在时限内已得出应答、还没写出时进入停机：进程等应答写完再退出，客户端收到完整的 200
/// （REQ-AMB-LIFE-DURABILITY-V4「让在途请求在 `server.shutdown_timeout` 内完成」）。不对应块场景。
#[test]
fn shutdown_waits_for_response_write() {
    let fx = Fixture::new();
    let (mut server, gate) = fx.start_gated(
        &[FLUSH_1H, ("server.shutdown_timeout", Some("\"30s\""))],
        "gateway.after_response",
    );
    let addr = server.addr;
    let r1 = std::thread::spawn(move || {
        common::http(
            addr,
            "POST",
            "/api/default/rw/_json",
            &[("Authorization", BEARER)],
            br#"[{"a":1}]"#,
        )
    });
    gate.wait_held(&server, Duration::from_secs(30));

    server.signal("TERM");
    // 排空、flush 与写出计数都不等这条应答；应答停住期间进程不退出
    assert_eq!(
        server.wait_exit(Duration::from_secs(3)),
        None,
        "{:#?}",
        server.stderr_lines()
    );
    gate.release();
    let r1 = r1.join().unwrap();
    assert_eq!(r1.status, 200, "{}", r1.text());
    assert_eq!(
        server.wait_exit(Duration::from_secs(30)),
        Some(Some(0)),
        "{:#?}",
        server.stderr_lines()
    );
}

#[test]
fn shutdown_writes_counters() {
    let fx = Fixture::new();
    let mut server = fx.start(&[FLUSH_1H]);
    let w = settle_minute(Duration::from_secs(5), false);
    ingest_ok(&server, "sc", r#"[{"a":1},{"a":2}]"#);
    let r = sent(
        &server,
        "sc",
        &format!("[{}]", sent_element("agent-1", w, 5)),
    );
    assert_eq!(r.status, 200, "{}", r.text());
    stop(&mut server, "TERM");

    let entry = counter_window(&fx, "sc", w).expect("停机后计数对象中有该分钟的条目");
    assert_eq!(entry["received"], 2, "{entry}");
    assert_eq!(entry["durable"], 2, "{entry}");
    assert_eq!(entry["sent"], serde_json::json!({"agent-1": 5}), "{entry}");
}

#[test]
fn shutdown_double_signal_exits_immediately() {
    let fx = Fixture::new();
    let mut server = fx.start(&[]);
    let _slow = SlowRequest::start(&server, "app", r#"[{"seq":1}]"#);
    server.signal("TERM");
    // 确认已进入停机、仍在排空
    search_until_rejected(&server).assert_envelope(503, "unavailable");
    server.signal("TERM");
    let code = server
        .wait_exit(Duration::from_secs(1))
        .expect("二次信号后 1 秒内退出");
    assert_ne!(code, Some(0), "{:#?}", server.stderr_lines());
}

#[test]
fn wal_double_signal_restart_queryable() {
    let fx = Fixture::new();
    let mut server = fx.start(&[FLUSH_1H]);
    ingest_ok(&server, "app", r#"[{"k":"x"}]"#);
    let _slow = SlowRequest::start(&server, "app", r#"[{"k":"slow"}]"#);
    server.signal("TERM");
    search_until_rejected(&server).assert_envelope(503, "unavailable");
    server.signal("TERM");
    let code = server.wait_exit(Duration::from_secs(5)).expect("进程退出");
    assert_ne!(code, Some(0));

    let server = fx.start(&[FLUSH_1H]);
    assert_eq!(server.wait_readyz(200, Duration::from_secs(10)).status, 200);
    assert_eq!(search(&server, "SELECT k FROM app WHERE k = 'x'").len(), 1);
}

#[test]
fn shutdown_signal_before_listen_exit0() {
    let fx = Fixture::new();
    let mut server = fx.start(&[FLUSH_1H]);
    ingest_ok(&server, "bl", r#"[{"k":"b"}]"#);
    server.sigkill();

    for signal in ["TERM", "INT"] {
        let held = HeldPort::new();
        let marker = fx.root().join(format!("paused-{signal}"));
        let mut cmd = serve_on(&fx, held.port(), &[FLUSH_1H]);
        cmd.env("TESTBED_FAULT_PAUSE", PAUSE_BEFORE_LISTEN)
            .env("TESTBED_FAULT_MARKER", &marker);
        let mut paused = Server::spawn(cmd);
        assert!(
            wait_file(&marker, Duration::from_secs(30)),
            "未走到暂停点：{:#?}",
            paused.stderr_lines()
        );
        paused.signal(signal);
        let code = paused.wait_exit(Duration::from_secs(5));
        assert_eq!(
            code,
            Some(Some(0)),
            "SIG{signal}：{:#?}",
            paused.stderr_lines()
        );
        held.assert_never_listened(code.flatten(), &paused);
        assert!(
            !paused
                .stderr_lines()
                .iter()
                .any(|l| l.contains("\"event\":\"ready\""))
        );
    }

    let server = fx.start(&[FLUSH_1H]);
    assert_eq!(search(&server, "SELECT k FROM bl WHERE k = 'b'").len(), 1);
}

/// 比「开始监听之前」更早的一段（装载配置之前）收到信号，同样以退出码 0 中止启动（REQ-AMB-LIFE-DURABILITY-V2）。
#[test]
fn shutdown_signal_before_config_exit0() {
    let fx = Fixture::new();
    for signal in ["TERM", "INT"] {
        let held = HeldPort::new();
        let marker = fx.root().join(format!("paused-config-{signal}"));
        let mut cmd = serve_on(&fx, held.port(), &[FLUSH_1H]);
        cmd.env("TESTBED_FAULT_PAUSE", PAUSE_BEFORE_CONFIG)
            .env("TESTBED_FAULT_MARKER", &marker);
        let mut paused = Server::spawn(cmd);
        assert!(
            wait_file(&marker, Duration::from_secs(30)),
            "未走到暂停点：{:#?}",
            paused.stderr_lines()
        );
        paused.signal(signal);
        let code = paused.wait_exit(Duration::from_secs(5));
        assert_eq!(
            code,
            Some(Some(0)),
            "SIG{signal}：{:#?}",
            paused.stderr_lines()
        );
        held.assert_never_listened(code.flatten(), &paused);
    }
}

// ---------------------------------------------------------------------------
// 重启时的 WAL 重放
// ---------------------------------------------------------------------------

#[test]
fn readyz_implies_replay_done() {
    let fx = Fixture::new();
    let mut server = fx.start(&[]);
    ingest_seq(&server, "app", 1000, 100);
    server.sigkill();

    // 监听 127.0.0.1:0，从 ready 事件取实际端口再轮询 /readyz：不先选端口再交给被测进程，免得端口在它绑定之前被
    // 并行的其他测试占走
    let restarted = fx.start(&[]);
    assert_eq!(
        restarted.wait_readyz(200, Duration::from_secs(30)).status,
        200,
        "{:#?}",
        restarted.stderr_lines()
    );
    assert_eq!(distinct_seqs(&restarted, "app").len(), 1000);
}

#[test]
fn replay_no_revalidation() {
    let fx = Fixture::new();
    let mut server = fx.start(&[FLUSH_1H]);
    let ts = now_us() + 23 * 3600 * 1_000_000;
    ingest_ok(
        &server,
        "rv",
        &format!(r#"[{{"_timestamp":{ts},"k":"future"}}]"#),
    );
    server.sigkill();

    let server = fx.start(&[FLUSH_1H, ("ingest.max_future", Some("\"1h\""))]);
    assert_eq!(server.wait_readyz(200, Duration::from_secs(10)).status, 200);
    assert_eq!(
        search(&server, "SELECT k FROM rv WHERE k = 'future'").len(),
        1
    );
}

#[test]
fn replay_genesis_recommit() {
    let inst = S3Instance::new();
    let mut server = inst.start(&[FLUSH_1H]);
    inst.relay.stop();
    ingest_ok(&server, "app2", r#"[{"k":"g"}]"#);
    server.sigkill();
    let genesis = format!("default/app2/{}", common::store::snapshot_rel(1));
    assert!(!s3_object_exists(&inst, &genesis));

    inst.relay.resume();
    let server = inst.start(&[FLUSH_1H]);
    assert_eq!(server.wait_readyz(200, Duration::from_secs(10)).status, 200);
    assert!(s3_object_exists(&inst, &genesis));
    assert_eq!(search(&server, "SELECT k FROM app2 WHERE k = 'g'").len(), 1);
}

#[test]
fn wal_corrupt_tail_truncate() {
    let fx = Fixture::new();
    let mut server = fx.start(&[FLUSH_1H]);
    ingest_seq(&server, "app", 100, 10);
    server.sigkill();

    // 模拟强杀时尚在进行、未被确认的一次追加：以存储层的 WAL 实现在尾部补一个条目，再截去它的末尾若干字节
    let rt = tokio::runtime::Runtime::new().unwrap();
    let wal = FileWal::open(&fx.wal_dir());
    wal.replay(&mut |_, _| {}).unwrap();
    rt.block_on(wal.append(vec![WalEntry::Records {
        dataset: Dataset::new("default", "app"),
        records: vec![Record {
            timestamp_us: 1_000_000,
            fields: vec![("seq".into(), FieldValue::Int64(100_000))],
        }],
    }]))
    .unwrap();
    drop(wal);
    let last = segments(&fx.wal_dir()).pop().unwrap();
    let len = std::fs::metadata(&last).unwrap().len();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&last)
        .unwrap()
        .set_len(len - 5)
        .unwrap();

    let server = fx.start(&[FLUSH_1H]);
    assert_eq!(server.wait_readyz(200, Duration::from_secs(10)).status, 200);
    assert_eq!(distinct_seqs(&server, "app"), (0..100).collect());
}

#[test]
fn wal_non_tail_corruption_exit3() {
    let fx = Fixture::new();
    let mut server = fx.start(&[FLUSH_1H]);
    ingest_seq(&server, "app", 1000, 10);
    server.sigkill();

    let seg = largest_segment(&fx.wal_dir());
    let mut bytes = std::fs::read(&seg).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] = !bytes[mid];
    std::fs::write(&seg, &bytes).unwrap();

    let held = HeldPort::new();
    let mut restarted = Server::spawn(serve_on(&fx, held.port(), &[FLUSH_1H]));
    let code = restarted.wait_exit(Duration::from_secs(30));
    assert_eq!(code, Some(Some(3)), "{:#?}", restarted.stderr_lines());
    held.assert_never_listened(code.flatten(), &restarted);
    assert!(
        !restarted
            .stderr_lines()
            .iter()
            .any(|l| l.contains("\"event\":\"ready\""))
    );
    assert_eq!(std::fs::read(&seg).unwrap(), bytes, "退出码 3 时不截断");
}

// ---------------------------------------------------------------------------
// 过时 wal.dir 的滞留记录
// ---------------------------------------------------------------------------

/// 按两个过时 `wal.dir` 场景的步骤构造 X、Y：X 在 W1 上提交 `{"seq":0}`、重启后确认 `batch` 并被强杀，
/// Y 以空的 W2 与同一对象存储确认 `other` 并停机。返回 W1 上的配置（含 `extra`）。
fn stale_on_w1<'a>(
    fx: &Fixture,
    w1: &'a str,
    extra: &[Kv<'a>],
    batch: &str,
    other: &str,
) -> Vec<Kv<'a>> {
    let w2 = format!("\"{}\"", fx.root().join("w2").display());
    let mut on_w1 = vec![FLUSH_1H, ("wal.dir", Some(w1))];
    on_w1.extend_from_slice(extra);
    let mut on_w2 = vec![FLUSH_1H, ("wal.dir", Some(w2.as_str()))];
    on_w2.extend_from_slice(extra);

    let mut x = fx.start(&on_w1);
    ingest_ok(&x, "app", r#"{"seq":0}"#);
    stop(&mut x, "TERM");
    let mut x = fx.start(&on_w1);
    ingest_ok(&x, "app", batch);
    x.sigkill();

    let mut y = fx.start(&on_w2);
    ingest_ok(&y, "app", other);
    stop(&mut y, "TERM");
    on_w1
}

/// Z 的 stderr 中唯一一条 `wal_stale` 事件。
fn only_wal_stale(z: &Server) -> serde_json::Value {
    let stale: Vec<serde_json::Value> = z
        .json_lines()
        .into_iter()
        .filter(|l| l["event"] == "wal_stale")
        .collect();
    assert_eq!(stale.len(), 1, "{stale:?}");
    assert_eq!(stale[0]["level"], "warn");
    assert_eq!(stale[0]["dataset"], "default/app");
    stale[0].clone()
}

#[test]
fn wal_stale_recover_into_shadow() {
    let fx = Fixture::new();
    let w1 = format!("\"{}\"", fx.root().join("w1").display());
    // X 确认的 host 为整数，Y 把 host 定为字符串并提交
    let on_w1 = stale_on_w1(
        &fx,
        &w1,
        &[],
        r#"[{"seq":1,"host":123},{"seq":2}]"#,
        r#"{"seq":3,"host":"web1"}"#,
    );

    // Z：挂回 W1，类型不同的滞留记录落入影子列收回
    let mut z = fx.start(&on_w1);
    assert_eq!(z.wait_readyz(200, Duration::from_secs(10)).status, 200);
    assert_eq!(distinct_seqs(&z, "app"), BTreeSet::from([0, 1, 2, 3]));
    let hits = search(&z, "SELECT seq FROM app WHERE host__int64 = 123");
    assert_eq!(hits, [serde_json::json!({"seq": 1})]);
    let stale = only_wal_stale(&z);
    assert_eq!(
        (stale["recovered"].as_u64(), stale["discarded"].as_u64()),
        (Some(2), Some(0))
    );

    // 再以同一配置重启 Z：不再判过时，收回的恰好一份
    z.sigkill();
    let z = fx.start(&on_w1);
    assert_eq!(z.wait_readyz(200, Duration::from_secs(10)).status, 200);
    assert_eq!(distinct_seqs(&z, "app"), BTreeSet::from([0, 1, 2, 3]));
    assert_eq!(search(&z, "SELECT seq FROM app").len(), 4);
}

#[test]
fn wal_stale_discard_over_column_cap() {
    let fx = Fixture::new();
    let w1 = format!("\"{}\"", fx.root().join("w1").display());
    let cap: Kv = ("ingest.max_columns", Some("3"));
    // X 确认的 seq 1 带新列 a（列数到 3）；Y 以同一上限登记了 b，链上列数已到 3
    let on_w1 = stale_on_w1(
        &fx,
        &w1,
        &[cap],
        r#"[{"seq":1,"a":1},{"seq":2}]"#,
        r#"{"seq":3,"b":"x"}"#,
    );

    let z = fx.start(&on_w1);
    assert_eq!(z.wait_readyz(200, Duration::from_secs(10)).status, 200);
    assert_eq!(distinct_seqs(&z, "app"), BTreeSet::from([0, 2, 3]));
    assert_eq!(search(&z, "SELECT seq FROM app").len(), 3);
    let stale = only_wal_stale(&z);
    assert_eq!(
        (stale["recovered"].as_u64(), stale["discarded"].as_u64()),
        (Some(1), Some(1))
    );
}

#[test]
fn wal_stale_intent_digest() {
    let fx = Fixture::new();
    let w1 = format!("\"{}\"", fx.root().join("w1").display());
    let w2 = format!("\"{}\"", fx.root().join("w2").display());
    let on_w1 = [FLUSH_1H, ("wal.dir", Some(w1.as_str()))];
    let on_w2 = [FLUSH_1H, ("wal.dir", Some(w2.as_str()))];

    // X：W1 上提交快照 N，重启后确认 seq 7，在下一次提交记下意图之后、写出快照 N+1 之前被强杀
    let mut x = fx.start(&on_w1);
    ingest_ok(&x, "app", r#"{"seq":0}"#);
    stop(&mut x, "TERM");
    let marker = fx.root().join("paused");
    let mut cmd = fx.serve(&[("flush.interval", Some("\"1s\"")), on_w1[1]]);
    cmd.env("TESTBED_FAULT_PAUSE", PAUSE_AFTER_INTENT)
        .env("TESTBED_FAULT_MARKER", &marker);
    let mut x = Server::start(cmd);
    ingest_ok(&x, "app", r#"{"seq":7}"#);
    assert!(
        wait_file(&marker, Duration::from_secs(30)),
        "未走到暂停点：{:#?}",
        x.stderr_lines()
    );
    x.sigkill();

    // Y：空的 W2，同一对象存储，提交 seq 8
    let mut y = fx.start(&on_w2);
    ingest_ok(&y, "app", r#"{"seq":8}"#);
    stop(&mut y, "TERM");

    // Z：挂回 W1
    let z = fx.start(&on_w1);
    assert_eq!(z.wait_readyz(200, Duration::from_secs(10)).status, 200);
    let hits = search(&z, "SELECT seq FROM app WHERE seq >= 7");
    let seqs: Vec<u64> = hits.iter().map(|h| h["seq"].as_u64().unwrap()).collect();
    assert_eq!(seqs.len(), 2, "{hits:?}");
    assert_eq!(
        seqs.into_iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([7, 8])
    );
}

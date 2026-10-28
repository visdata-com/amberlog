//! REQ-AMB-ING-HOT-BACKPRESSURE-V5 的系统测试：以子进程运行 `amberlog`。
//! 「对象存储被停止」用 MinIO 台架加进程内 TCP 转发（`common::s3`），「WAL 写入被置为失败」用 `TESTBED_FAULT_WAL`
//! 控制文件；「进程的常驻内存」取被测进程 `/proc/<pid>/status` 的 `VmRSS`。以「上一场景的实例」为前提的场景由各自的测试
//! 重新构造前置状态。分段、额度归还与读回场景用可放行暂停点 `flush.after_write_commit`、`overflow.read_pause`
//! （`fault-injection`），已提交的记录由测试自己的对象存储客户端读当前快照所列数据文件的 `seq` 列求得。

mod common;

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use common::api::{FLUSH_1H, WalFault, ingest, ingest_ok, search};
use common::s3::S3Instance;
use common::store::{Remote, snapshot_rel};
use common::{Fixture, Kv, Server};
use serde_json::{Value, json};

/// 一个 65536 字节、不可压缩的随机字符串（xorshift，按 `seed` 确定）。
fn random_text(seed: u64) -> String {
    random_text_of(seed, 65536)
}

/// 一个 `len` 字节、不可压缩的随机字符串（xorshift，按 `seed` 确定）。
fn random_text_of(seed: u64, len: usize) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            ALPHABET[(x % ALPHABET.len() as u64) as usize] as char
        })
        .collect()
}

/// `seqs` 各一条记录的批次：带唯一的 `seq` 与一个 65536 字节的随机字符串字段。
fn big_batch(seqs: std::ops::Range<u64>) -> String {
    let records: Vec<Value> = seqs
        .map(|seq| json!({"seq": seq, "blob": random_text(seq + 1)}))
        .collect();
    Value::Array(records).to_string()
}

fn proc_status_kb(pid: u32, key: &str) -> u64 {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
    status
        .lines()
        .find_map(|l| l.strip_prefix(key))
        .and_then(|rest| rest.trim_start_matches(':').split_whitespace().next())
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("/proc/{pid}/status 中没有 {key}"))
}

/// `/metrics` 中 `amberlog_wal_bytes` 的取值。
fn wal_bytes_metric(s: &Server) -> u64 {
    let r = s.get_auth("/metrics");
    assert_eq!(r.status, 200, "{}", r.text());
    r.text()
        .lines()
        .find_map(|l| l.strip_prefix("amberlog_wal_bytes "))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or_else(|| panic!("没有 amberlog_wal_bytes 样本：{}", r.text()))
}

/// 目录下（含子目录）全部普通文件的长度之和。
fn dir_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| {
            let p = e.unwrap().path();
            let m = p.symlink_metadata().unwrap();
            if m.is_dir() {
                dir_bytes(&p)
            } else if m.is_file() {
                m.len()
            } else {
                0
            }
        })
        .sum()
}

fn backlog_events(s: &Server) -> Vec<Value> {
    s.json_lines()
        .into_iter()
        .filter(|l| l["event"] == "wal_backlog_high")
        .collect()
}

const LIMIT_1MIB: Kv = ("ingest.max_backlog_memory_bytes", Some("1048576"));

/// 用到 [`backlog_then_recover`] 的测试互斥：两个各约 64 MiB 的积压同时提交会互相争用 CPU 与磁盘，使「60 秒内全部可查」
/// 只剩几秒余量（单独跑约 40 秒，两个并行约 63 秒）。场景只写一个实例，不并行不改变它的含义。
fn heavy() -> MutexGuard<'static, ()> {
    static HEAVY: Mutex<()> = Mutex::new(());
    HEAVY.lock().unwrap_or_else(|e| e.into_inner())
}

/// 场景「积压超过内存上限后照常受理，恢复后全部可查」：对象存储停止期间摄取 64 批共 1024 条（约 64 MiB），
/// 断言受理、`wal.dir` 字节数与常驻内存，恢复后等全部可查。返回被测进程（对象存储已恢复、积压已全部提交）。
fn backlog_then_recover(inst: &S3Instance) -> Server {
    let s = inst.start(&[LIMIT_1MIB]);
    assert_eq!(s.wait_readyz(200, Duration::from_secs(30)).status, 200);
    inst.relay.stop();

    let rss_before = proc_status_kb(s.pid(), "VmRSS");
    for b in 0..64 {
        let r = ingest(&s, "app", &big_batch(b * 16..b * 16 + 16));
        assert_eq!(r.status, 200, "第 {b} 批：{}", r.text());
    }
    let rss_after = proc_status_kb(s.pid(), "VmRSS");
    assert!(
        rss_after.saturating_sub(rss_before) <= 64 * 1024,
        "常驻内存 {rss_after} kB，第一个批次发送前 {rss_before} kB"
    );
    let wal_bytes = wal_bytes_metric(&s);
    assert!(wal_bytes >= 33_554_432, "amberlog_wal_bytes = {wal_bytes}");

    inst.relay.resume();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let hits = search(&s, "SELECT seq FROM app");
        let distinct: BTreeSet<u64> = hits.iter().map(|h| h["seq"].as_u64().unwrap()).collect();
        if hits.len() == 1024 && distinct.len() == 1024 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "60 秒内未见全部 1024 条（{} 个 hit）",
            hits.len()
        );
        std::thread::sleep(Duration::from_millis(500));
    }
    s
}

// ---------------------------------------------------------------------------
// 对象存储停止期间的积压
// ---------------------------------------------------------------------------

#[test]
fn backpressure_memory_bounded() {
    let _heavy = heavy();
    let inst = S3Instance::new();
    backlog_then_recover(&inst);
}

#[test]
fn backpressure_memory_room_visible() {
    let _heavy = heavy();
    let inst = S3Instance::new();
    let s = backlog_then_recover(&inst);
    let remote = Remote::new(inst.store(), "app");
    let committed = || {
        let id = remote.current().expect("当前快照存在");
        remote.json(&snapshot_rel(id))["summary"]["record_count"]
            .as_u64()
            .unwrap()
    };
    assert_eq!(committed(), 1024);
    // 查到新记录时它还不在已提交快照里，才说明结果来自内存：被错当成溢出的记录要等提交才可查（查询不读 WAL）。
    // 摄取与查到之间恰好赶上一次 flush 时分不清，换一个 `seq` 再试，至多 3 次
    let mut from_memory = false;
    for seq in [5000, 5001, 5002] {
        let before = committed();
        ingest_ok(&s, "app", &format!(r#"[{{"seq":{seq}}}]"#));
        let deadline = Instant::now() + Duration::from_secs(10);
        while search(&s, &format!("SELECT seq FROM app WHERE seq = {seq}")).len() != 1 {
            assert!(Instant::now() < deadline, "seq {seq} 10 秒内不可查");
            std::thread::sleep(Duration::from_millis(200));
        }
        if committed() == before {
            from_memory = true;
            break;
        }
    }
    assert!(from_memory, "三次都在提交之后才查到：新记录没有留在内存");
}

// ---------------------------------------------------------------------------
// 水位告警
// ---------------------------------------------------------------------------

const ALERT_1MIB: Kv = ("wal.alert_bytes", Some("1048576"));

/// 场景「本地积压越过水位时告警，就绪不受影响」，返回被测进程。
fn alert_once(fx: &Fixture) -> Server {
    let s = fx.start(&[ALERT_1MIB, FLUSH_1H]);
    for b in 0..2 {
        ingest_ok(&s, "app", &big_batch(b * 16..b * 16 + 16));
    }
    std::thread::sleep(Duration::from_secs(15));
    let events = backlog_events(&s);
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(events[0]["level"], "warn");
    assert_eq!(events[0]["alert_bytes"], 1_048_576);
    assert!(events[0]["wal_bytes"].as_u64().unwrap() > 1_048_576);
    assert_eq!(s.get("/readyz").status, 200);
    s
}

#[test]
fn backpressure_wal_alert() {
    let fx = Fixture::new();
    let s = alert_once(&fx);
    ingest_ok(&s, "app", &big_batch(100..116));
    std::thread::sleep(Duration::from_secs(15));
    assert_eq!(backlog_events(&s).len(), 1);
}

#[test]
fn backpressure_alert_after_restart() {
    let fx = Fixture::new();
    let mut s = alert_once(&fx);
    s.sigkill();
    let s = fx.start(&[ALERT_1MIB, FLUSH_1H]);
    assert_eq!(s.wait_readyz(200, Duration::from_secs(30)).status, 200);
    std::thread::sleep(Duration::from_secs(15));
    assert_eq!(backlog_events(&s).len(), 1);
}

// ---------------------------------------------------------------------------
// 边界
// ---------------------------------------------------------------------------

#[test]
fn backpressure_wal_full_503() {
    let inst = S3Instance::new();
    let fault = WalFault::new(&inst.fixture);
    let owned = inst.config(&[]);
    let kv: Vec<Kv> = owned.iter().map(|(k, v)| (*k, v.as_deref())).collect();
    let mut cmd = inst.fixture.serve(&kv);
    cmd.env("TESTBED_FAULT_WAL", fault.path());
    let s = Server::start(cmd);
    ingest_ok(&s, "app", r#"{"a":2}"#);
    assert_eq!(s.wait_readyz(200, Duration::from_secs(30)).status, 200);
    inst.relay.stop();
    fault.inject("write");
    ingest(&s, "app", r#"[{"a":1}]"#).assert_envelope(503, "unavailable");
    fault.clear();
    inst.relay.resume();
    assert_eq!(s.wait_readyz(200, Duration::from_secs(30)).status, 200);
    assert!(search(&s, "SELECT a FROM app WHERE a = 1").is_empty());
}

// ---------------------------------------------------------------------------
// 溢出记录读不出
// ---------------------------------------------------------------------------

/// 场景「运行期读出溢出记录时 WAL 已损坏则以退出码 3 退出」。对象存储停止期间可能已物化过一代（读出成功、提交失败、
/// 留作在途批次），改写区域所在段中其后各代的读出同样越不过改写处，故不论物化发生在改写之前还是之后，进程都要退出。
///
/// 把目录下（含子目录）普通文件中第一处 `needle` 的字节逐个取反；返回改写的文件。
fn corrupt_first(dir: &Path, needle: &[u8]) -> Option<std::path::PathBuf> {
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    entries.sort();
    for p in entries {
        if p.is_dir() {
            if let Some(hit) = corrupt_first(&p, needle) {
                return Some(hit);
            }
            continue;
        }
        let mut bytes = std::fs::read(&p).unwrap();
        if let Some(at) = bytes.windows(needle.len()).position(|w| w == needle) {
            for b in &mut bytes[at..at + needle.len()] {
                *b = !*b;
            }
            std::fs::write(&p, &bytes).unwrap();
            return Some(p);
        }
    }
    None
}

#[test]
fn backpressure_overflow_read_corrupt_exit3() {
    let inst = S3Instance::new();
    let mut s = inst.start(&[LIMIT_1MIB]);
    assert_eq!(s.wait_readyz(200, Duration::from_secs(30)).status, 200);
    inst.relay.stop();
    for b in 0..64 {
        let r = ingest(&s, "app", &big_batch(b * 16..b * 16 + 16));
        assert_eq!(r.status, 200, "第 {b} 批：{}", r.text());
    }
    // 第一个批次第一条记录（seq 0）的随机字符串所在区域
    let needle = random_text(1);
    let segment = corrupt_first(&inst.fixture.wal_dir(), &needle.as_bytes()[..256])
        .expect("WAL 中找到第一个批次的记录");
    // 第二个批次第一条记录（seq 16）在同一段中、改写区域之后
    let later = random_text(17);
    let found_later = std::fs::read(&segment)
        .unwrap()
        .windows(64)
        .any(|w| w == &later.as_bytes()[..64]);
    assert!(found_later, "改写区域之后同一段中还有其他记录");

    inst.relay.resume();
    assert_eq!(
        s.wait_exit(Duration::from_secs(60)),
        Some(Some(3)),
        "{:#?}",
        s.stderr_lines()
    );

    let owned = inst.config(&[LIMIT_1MIB]);
    let kv: Vec<Kv> = owned.iter().map(|(k, v)| (*k, v.as_deref())).collect();
    let mut restarted = Server::spawn(inst.fixture.serve(&kv));
    assert_eq!(
        restarted.wait_exit(Duration::from_secs(60)),
        Some(Some(3)),
        "{:#?}",
        restarted.stderr_lines()
    );
}

// ---------------------------------------------------------------------------
// 需求场景之外的补充测试
// ---------------------------------------------------------------------------

#[test]
fn backpressure_wal_bytes_matches_dir() {
    let fx = Fixture::new();
    let s = fx.start(&[FLUSH_1H]);
    let extra = fx.wal_dir().join("extra");
    std::fs::create_dir_all(&extra).unwrap();
    std::fs::write(extra.join("file"), vec![0u8; 12345]).unwrap();
    for seq in 0..8 {
        ingest_ok(&s, "app", &format!(r#"[{{"seq":{seq}}}]"#));
    }
    let metric = wal_bytes_metric(&s);
    assert_eq!(metric, dir_bytes(&fx.wal_dir()));
    assert!(metric >= 12345);
}

// ---------------------------------------------------------------------------
// 提交的分段、额度归还与读回
// ---------------------------------------------------------------------------

const MAX_100: Kv = ("flush.max_records", Some("100"));
const FLUSH_1S: Kv = ("flush.interval", Some("\"1s\""));

/// `seqs` 各一条记录的批次：带唯一的 `seq` 与一个 1024 字节的随机字符串字段。
fn kib_batch(seqs: std::ops::Range<u64>) -> String {
    let records: Vec<Value> = seqs
        .map(|seq| json!({"seq": seq, "blob": random_text_of(seq + 1, 1024)}))
        .collect();
    Value::Array(records).to_string()
}

/// `/metrics` 中 `amberlog_backlog_memory_bytes` 的取值。
fn backlog_memory(s: &Server) -> u64 {
    common::store::metric(s, "amberlog_backlog_memory_bytes")
        .unwrap_or_else(|| panic!("没有 amberlog_backlog_memory_bytes 样本")) as u64
}

#[cfg(feature = "fault-injection")]
/// 以本实例的对象存储配置外加 `extra` 启动，另设环境变量 `env`；等到 `/readyz` 返回 200。
fn start_with_env(inst: &S3Instance, extra: &[Kv], env: &[(&str, &Path)]) -> Server {
    let owned = inst.config(extra);
    let kv: Vec<Kv> = owned.iter().map(|(k, v)| (*k, v.as_deref())).collect();
    let mut cmd = inst.fixture.serve(&kv);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let s = Server::start(cmd);
    assert_eq!(s.wait_readyz(200, Duration::from_secs(30)).status, 200);
    s
}

#[cfg(feature = "fault-injection")]
/// 步骤 `step` 的可放行暂停点控制文件（`hold` 只停第一个调用，`hold-each` 逐次停住每个调用；见 `amberlog_storage::fault::gate`）。
struct StepGate {
    step: &'static str,
    file: std::path::PathBuf,
    each: bool,
}

#[cfg(feature = "fault-injection")]
impl StepGate {
    fn hold(fx: &Fixture, step: &'static str) -> StepGate {
        StepGate::new(fx, step, false)
    }

    fn hold_each(fx: &Fixture, step: &'static str) -> StepGate {
        StepGate::new(fx, step, true)
    }

    fn new(fx: &Fixture, step: &'static str, each: bool) -> StepGate {
        let file = fx.root().join(format!("gate-{step}"));
        std::fs::write(&file, if each { "hold-each" } else { "hold" }).unwrap();
        StepGate { step, file, each }
    }

    fn env(&self) -> String {
        format!("{}={}", self.step, self.file.display())
    }

    fn content(&self) -> String {
        std::fs::read_to_string(&self.file)
            .unwrap_or_default()
            .trim()
            .to_owned()
    }

    fn held(&self) -> bool {
        self.content() == "held"
    }

    /// 等有调用停在该步骤（内容为 `held`）。
    fn wait_held(&self, s: &Server, limit: Duration) {
        let deadline = Instant::now() + limit;
        while !self.held() {
            assert!(
                Instant::now() < deadline,
                "{} 秒内没有停在 {}：{:#?}",
                limit.as_secs(),
                self.step,
                s.stderr_lines()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// 放行停住的那一个；逐次停住时等它改回（内容不再是 `go`）再返回，下一次放行不会与它赛跑。
    fn release(&self) {
        std::fs::write(&self.file, "go").unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.each && self.content() == "go" {
            assert!(Instant::now() < deadline, "{} 放行后未改回", self.step);
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

/// 测试自己的对象存储客户端所见的 `app` 当前快照：(快照号, `summary.record_count`)；尚无快照为 `(None, 0)`。
fn committed_count(remote: &Remote) -> (Option<u64>, u64) {
    match remote.current() {
        Some(id) => (
            Some(id),
            remote.json(&snapshot_rel(id))["summary"]["record_count"]
                .as_u64()
                .unwrap(),
        ),
        None => (None, 0),
    }
}

#[cfg(feature = "fault-injection")]
/// 当前快照所列数据文件中 `seq` 列的全部取值（暂停期间新确认的记录不在快照里，所以不读 `_search`）。
fn committed_seqs(remote: &Remote, scratch: &Path) -> Vec<u64> {
    use arrow::array::{Array, Int64Array};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let Some(id) = remote.current() else {
        return Vec::new();
    };
    let _ = std::fs::remove_dir_all(scratch);
    let ds = remote.mirror(scratch);
    let mut seqs = Vec::new();
    for entry in ds.file_entries(&ds.snapshot(id)) {
        let f = std::fs::File::open(ds.path(entry["path"].as_str().unwrap())).unwrap();
        for batch in ParquetRecordBatchReaderBuilder::try_new(f)
            .unwrap()
            .build()
            .unwrap()
        {
            let batch = batch.unwrap();
            let col = batch.column_by_name("seq").unwrap();
            let col = col.as_any().downcast_ref::<Int64Array>().unwrap();
            seqs.extend((0..col.len()).map(|i| col.value(i) as u64));
        }
    }
    seqs.sort_unstable();
    seqs
}

#[cfg(feature = "fault-injection")]
/// 两个分段场景共同的前提：以 `flush.max_records` 为 100、`flush.interval` 为 1 秒启动，`/readyz` 返回 200 之后对象存储
/// 被停止，写入提交在暂停点 `flush.after_write_commit` 暂停；摄取 350 条（7 个批次）后记下内存积压字节数，再恢复对象存储。
fn segmented_backlog(inst: &S3Instance, gate: &StepGate) -> (Server, u64) {
    let s = start_with_env(
        inst,
        &[MAX_100, FLUSH_1S],
        &[("TESTBED_FAULT_GATES", Path::new(&gate.env()))],
    );
    inst.relay.stop();
    for b in 0..7 {
        let r = ingest(&s, "app", &kib_batch(b * 50..b * 50 + 50));
        assert_eq!(r.status, 200, "第 {b} 批：{}", r.text());
    }
    let m = backlog_memory(&s);
    inst.relay.resume();
    (s, m)
}

#[cfg(feature = "fault-injection")]
#[test]
fn backpressure_quota_released_per_commit() {
    let inst = S3Instance::new();
    let gate = StepGate::hold(&inst.fixture, "flush.after_write_commit");
    let (s, m) = segmented_backlog(&inst, &gate);
    gate.wait_held(&s, Duration::from_secs(60));
    let remote = Remote::new(inst.store(), "app");
    assert_eq!(committed_count(&remote).1, 100, "第一次写入提交恰含 100 条");
    let after = backlog_memory(&s);
    assert!(after < m, "第一次写入提交之后 {after}，摄取后 {m}");
    gate.release();
}

#[cfg(feature = "fault-injection")]
#[test]
fn backpressure_commit_segments_in_ack_order() {
    let inst = S3Instance::new();
    let gate = StepGate::hold_each(&inst.fixture, "flush.after_write_commit");
    let (s, _) = segmented_backlog(&inst, &gate);
    let remote = Remote::new(inst.store(), "app");
    let scratch = inst.fixture.root().join("mirror");
    let mut stops = 0;
    loop {
        gate.wait_held(&s, Duration::from_secs(60));
        stops += 1;
        let seqs = committed_seqs(&remote, &scratch);
        let prefix: Vec<u64> = (0..seqs.len() as u64).collect();
        assert_eq!(seqs, prefix, "第 {stops} 次停住时已提交的 seq 不是前缀");
        let done = seqs.len() == 350;
        gate.release();
        if done {
            break;
        }
        assert!(stops < 350, "停住 {stops} 次仍未全部提交");
    }
    assert!(stops >= 4, "350 条至少分 4 次提交，实际 {stops} 次");
}

#[test]
fn backpressure_commit_capped_by_max_records() {
    let inst = S3Instance::new();
    let s = inst.start(&[MAX_100, FLUSH_1S]);
    assert_eq!(s.wait_readyz(200, Duration::from_secs(30)).status, 200);
    let remote = Remote::new(inst.store(), "app");
    let (before, _) = committed_count(&remote);
    inst.relay.stop();
    for b in 0..7 {
        let r = ingest(
            &s,
            "app",
            &format!(
                "[{}]",
                (b * 50..b * 50 + 50)
                    .map(|seq| format!(r#"{{"seq":{seq}}}"#))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        );
        assert_eq!(r.status, 200, "第 {b} 批：{}", r.text());
    }
    inst.relay.resume();
    let deadline = Instant::now() + Duration::from_secs(60);
    let now = loop {
        let (id, n) = committed_count(&remote);
        if n == 350 {
            break id.unwrap();
        }
        assert!(Instant::now() < deadline, "60 秒内只提交到 {n} 条");
        std::thread::sleep(Duration::from_millis(500));
    };
    // 沿快照链自恢复前的当前快照（没有时自 1 号快照、按 0 条起算）到此刻的当前快照
    let mut prev = before.map_or(0, |id| {
        remote.json(&snapshot_rel(id))["summary"]["record_count"]
            .as_u64()
            .unwrap()
    });
    for id in before.map_or(1, |b| b + 1)..=now {
        let n = remote.json(&snapshot_rel(id))["summary"]["record_count"]
            .as_u64()
            .unwrap();
        assert!(n - prev <= 100, "快照 {id} 比上一个多 {} 条", n - prev);
        prev = n;
    }
}

#[test]
fn backpressure_quota_released_after_commit() {
    let inst = S3Instance::new();
    let s = inst.start(&[FLUSH_1S]);
    assert_eq!(s.wait_readyz(200, Duration::from_secs(30)).status, 200);
    inst.relay.stop();
    let r = ingest(&s, "app", &kib_batch(0..100));
    assert_eq!(r.status, 200, "{}", r.text());
    let m = backlog_memory(&s);
    assert!(m > 102_400, "amberlog_backlog_memory_bytes = {m}");
    inst.relay.resume();
    let deadline = Instant::now() + Duration::from_secs(60);
    while backlog_memory(&s) != 0 {
        assert!(
            Instant::now() < deadline,
            "60 秒内未回落到 0：{}",
            backlog_memory(&s)
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

#[test]
fn backpressure_memory_metric_zero_at_start() {
    let fx = Fixture::new();
    let s = fx.start(&[]);
    let r = s.get_auth("/metrics");
    assert_eq!(r.status, 200, "{}", r.text());
    assert!(
        r.text()
            .lines()
            .any(|l| l == "amberlog_backlog_memory_bytes 0"),
        "{}",
        r.text()
    );
}

#[cfg(feature = "fault-injection")]
#[test]
fn backpressure_readback_does_not_block_ack() {
    let _heavy = heavy();
    let inst = S3Instance::new();
    let gate = StepGate::hold(&inst.fixture, "overflow.read_pause");
    let s = start_with_env(
        &inst,
        &[LIMIT_1MIB],
        &[("TESTBED_FAULT_GATES", Path::new(&gate.env()))],
    );
    inst.relay.stop();
    for b in 0..64 {
        let r = ingest(&s, "app", &big_batch(b * 16..b * 16 + 16));
        assert_eq!(r.status, 200, "第 {b} 批：{}", r.text());
    }
    inst.relay.resume();
    gate.wait_held(&s, Duration::from_secs(60));
    for stream in ["web", "app"] {
        let t = Instant::now();
        let r = ingest(&s, stream, r#"[{"k":1}]"#);
        assert_eq!(r.status, 200, "{stream}：{}", r.text());
        assert!(
            t.elapsed() < Duration::from_secs(5),
            "{stream} 等了 {:?} 才确认",
            t.elapsed()
        );
    }
    assert!(gate.held(), "确认期间读出仍停在暂停点");
    gate.release();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let hits = search(&s, "SELECT count(*) AS n FROM app");
        if hits.first().and_then(|h| h["n"].as_u64()) == Some(1025) {
            break;
        }
        assert!(Instant::now() < deadline, "60 秒内 n 未到 1025：{hits:?}");
        std::thread::sleep(Duration::from_millis(500));
    }
}

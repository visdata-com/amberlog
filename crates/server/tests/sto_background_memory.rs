//! REQ-AMB-STO-BACKGROUND-MEMORY-V3 的系统测试：封口合并、冷层合并的峰值以目标大小为界，三种后台任务做完以后匿名常驻内存
//! 回落。
//!
//! 被测进程是测试启动的 `amberlog serve` 子进程，读它的 `/proc/<pid>/status` 中的 `RssAnon`（只为 Linux 编写）。
//! 「开始前的值」在暂停点读：`compaction.after_select`、`lake.before_read`、`cold_compaction.after_select` 逐次停住（`hold-each`），
//! `cold_compaction.before_run` 只停第一次（`hold`）。采样线程自第一次暂停起每 20 毫秒读一次；被测进程退出或超过总时限都判失败。

#![cfg(feature = "fault-injection")]

mod common;

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::api::{FLUSH_1H, ingest_ok, now_us};
use common::lake::ColdDir;
use common::store::{Ds, HOUR, bucket_label, has_mergeable_run, hour_of, stop};
use common::{Fixture, Kv, Server};

/// 场景约定：`auth.enabled` 为 `false`，`hot_fsck.interval` 为 `24h`（被测期间不进行巡检、快照回收与冷层孤儿清理）；
/// `storage.prefix` 取缺省的 `amberlog`，不配置告警规则与 `retention.period`。
const CONVENTION: [Kv; 3] = [
    ("auth.enabled", Some("false")),
    ("auth.token", None),
    ("hot_fsck.interval", Some("\"24h\"")),
];

/// 峰值判据与回落判据的固定余量（REQ-AMB-STO-BACKGROUND-MEMORY-PEAK-SEAL、-PEAK-COLD、-RETURN）。
const PEAK_SLACK: u64 = 402_653_184;
const RETURN_SLACK: u64 = 536_870_912;

const DAY: i64 = 24 * HOUR;

/// 以场景约定加 `overrides` 启动，启用暂停点 `gates`，等到 `ready`。
fn start(f: &Fixture, overrides: &[Kv], gates: &[&Gate]) -> Server {
    let mut all: Vec<Kv> = CONVENTION.to_vec();
    all.extend_from_slice(overrides);
    let mut cmd = f.serve(&all);
    if !gates.is_empty() {
        let pairs: Vec<String> = gates
            .iter()
            .map(|g| format!("{}={}", g.step, g.file.display()))
            .collect();
        cmd.env("TESTBED_FAULT_GATES", pairs.join(";"));
    }
    Server::start(cmd)
}

// ---------------------------------------------------------------------------
// 暂停点、采样与指标
// ---------------------------------------------------------------------------

/// 一个可放行暂停点的控制文件（见 `amberlog_storage::fault::gate`）。
struct Gate {
    step: &'static str,
    file: PathBuf,
    each: bool,
}

impl Gate {
    /// 只停第一个走到 `step` 的调用。
    fn hold(f: &Fixture, step: &'static str) -> Gate {
        Gate::new(f, step, false)
    }

    /// 逐次停住每个走到 `step` 的调用。
    fn hold_each(f: &Fixture, step: &'static str) -> Gate {
        Gate::new(f, step, true)
    }

    fn new(f: &Fixture, step: &'static str, each: bool) -> Gate {
        let file = f.root().join(format!("gate-{step}"));
        std::fs::write(&file, if each { "hold-each" } else { "hold" }).unwrap();
        Gate { step, file, each }
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

    /// 放行停住的那一个；逐次停住时等内容不再是 `go`（已改回或下一个已停住）再返回。
    fn release(&self) {
        std::fs::write(&self.file, "go").unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.each && self.content() == "go" {
            assert!(Instant::now() < deadline, "{} 放行后未改回", self.step);
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

/// 进程 `pid` 的 `/proc/<pid>/status` 中 `RssAnon` 的字节数（按字段名取）；进程已不在时为 `None`。
fn rss_anon(pid: u32) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let kb: u64 = status
        .lines()
        .find_map(|l| l.strip_prefix("RssAnon:"))?
        .trim()
        .trim_end_matches("kB")
        .trim()
        .parse()
        .ok()?;
    Some(kb * 1024)
}

/// 被测进程此刻的匿名常驻内存；读不到即进程已退出，判失败。
fn rss_now(s: &Server) -> u64 {
    rss_anon(s.pid()).unwrap_or_else(|| panic!("被测进程已退出：{:#?}", s.stderr_lines()))
}

/// 每 20 毫秒读一次匿名常驻内存的采样线程。
struct Sampler {
    samples: Arc<Mutex<Vec<(Instant, u64)>>>,
    gone: Arc<AtomicBool>,
    done: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Sampler {
    fn start(pid: u32) -> Sampler {
        let samples: Arc<Mutex<Vec<(Instant, u64)>>> = Arc::default();
        let gone = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let thread = {
            let (samples, gone, done) =
                (Arc::clone(&samples), Arc::clone(&gone), Arc::clone(&done));
            std::thread::spawn(move || {
                while !done.load(Ordering::SeqCst) {
                    match rss_anon(pid) {
                        Some(v) => samples.lock().unwrap().push((Instant::now(), v)),
                        None => {
                            gone.store(true, Ordering::SeqCst);
                            return;
                        }
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
            })
        };
        Sampler {
            samples,
            gone,
            done,
            thread: Some(thread),
        }
    }

    /// 采样线程发现被测进程已退出时判失败。
    fn assert_alive(&self, s: &Server) {
        assert!(
            !self.gone.load(Ordering::SeqCst),
            "被测进程已退出：{:#?}",
            s.stderr_lines()
        );
    }

    fn stop(mut self) -> Vec<(Instant, u64)> {
        self.done.store(true, Ordering::SeqCst);
        if let Some(t) = self.thread.take() {
            t.join().unwrap();
        }
        std::mem::take(&mut *self.samples.lock().unwrap())
    }
}

impl Drop for Sampler {
    fn drop(&mut self) {
        self.done.store(true, Ordering::SeqCst);
    }
}

/// `/metrics` 的文本（场景约定下认证关闭）。
fn metrics(s: &Server) -> String {
    let r = s.get("/metrics");
    assert_eq!(r.status, 200, "{}", r.text());
    r.text()
}

/// 样本 `series`（指标名连同标签）的值；没有该样本为 0。
fn value(text: &str, series: &str) -> f64 {
    text.lines()
        .find_map(|l| l.strip_prefix(series)?.strip_prefix(' '))
        .map_or(0.0, |v| v.trim().parse().expect("样本值是数"))
}

/// 以 `prefix`（指标名与开头几个标签）开头的各样本之和。
fn sum(text: &str, prefix: &str) -> f64 {
    text.lines()
        .filter(|l| l.starts_with(prefix))
        .filter_map(|l| l.rsplit_once(' ')?.1.trim().parse::<f64>().ok())
        .sum()
}

/// 一次次暂停与计数的记录：每次暂停的时刻与该次的开始前的值，计数最后一次变化的时刻。
struct Runs {
    pauses: Vec<(Instant, u64)>,
    last: f64,
    changed: Instant,
}

impl Runs {
    fn new(count: f64) -> Runs {
        Runs {
            pauses: Vec::new(),
            last: count,
            changed: Instant::now(),
        }
    }

    /// 暂停点 `gate` 上有调用停住时读开始前的值、放行；计数 `count` 变了时记下时刻。
    fn step(&mut self, s: &Server, gate: &Gate, sampler: &mut Option<Sampler>, count: f64) {
        if gate.held() {
            let before = rss_now(s);
            let at = Instant::now();
            sampler.get_or_insert_with(|| Sampler::start(s.pid()));
            self.pauses.push((at, before));
            gate.release();
        }
        if count != self.last {
            self.last = count;
            self.changed = Instant::now();
        }
    }

    /// 每一次自暂停时起至下一次暂停（最后一次至计数最后一次变化）所采各值，都不超过该次的开始前的值加 `bound`。
    fn assert_peaks(&self, samples: &[(Instant, u64)], bound: u64, what: &str) {
        for (i, &(at, before)) in self.pauses.iter().enumerate() {
            let until = self.pauses.get(i + 1).map_or(self.changed, |p| p.0);
            let peak = samples
                .iter()
                .filter(|(t, _)| *t >= at && *t <= until)
                .map(|(_, v)| *v)
                .max()
                .unwrap_or(before)
                .max(before);
            eprintln!(
                "第 {} 次{what}：开始前 {} MiB，峰值 {} MiB，上限 {} MiB",
                i + 1,
                before >> 20,
                peak >> 20,
                (before + bound) >> 20
            );
            assert!(
                peak <= before + bound,
                "第 {} 次{what}峰值 {} MiB，超过开始前的值 {} MiB 加 {} MiB",
                i + 1,
                peak >> 20,
                before >> 20,
                bound >> 20
            );
        }
    }

    /// 计数最后一次变化 10 秒之后读得的匿名常驻内存，不超过第一次的开始前的值加 512 MiB。
    fn assert_return(&self, s: &Server) {
        let wait =
            (self.changed + Duration::from_secs(10)).saturating_duration_since(Instant::now());
        std::thread::sleep(wait);
        let after = rss_now(s);
        let first = self.pauses[0].1;
        eprintln!(
            "{} 次；第一次开始前 {} MiB，做完 10 秒后 {} MiB",
            self.pauses.len(),
            first >> 20,
            after >> 20
        );
        assert!(
            after <= first + RETURN_SLACK,
            "做完 10 秒后 RssAnon {} MiB，超过第一次开始前的值 {} MiB 加 512 MiB",
            after >> 20,
            first >> 20
        );
    }
}

/// 总时限内被测进程退出即判失败。
fn assert_running(s: &mut Server, deadline: Instant, what: &str) {
    assert!(
        s.wait_exit(Duration::ZERO).is_none(),
        "被测进程已退出：{:#?}",
        s.stderr_lines()
    );
    assert!(Instant::now() < deadline, "超过总时限：{what}");
}

/// xorshift 伪随机数，生成随机十六进制字段。
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    /// `n` 个随机十六进制字符（`n` 为 16 的倍数）。
    fn hex(&mut self, n: usize) -> String {
        (0..n / 16)
            .map(|_| format!("{:016x}", self.next()))
            .collect()
    }
}

/// 向 `app` 摄取 `n` 条 `_timestamp` 在 `[start, start + 1 小时)` 内均匀分布、`msg` 为 200 个随机十六进制字符的记录，
/// 每个请求 `per_request` 条，须全部得 200。
fn ingest_random(s: &Server, rng: &mut Rng, start: i64, n: i64, per_request: i64) {
    for first in (0..n).step_by(per_request as usize) {
        let rows: Vec<String> = (first..(first + per_request).min(n))
            .map(|i| {
                format!(
                    r#"{{"_timestamp":{},"msg":"{}"}}"#,
                    start + i * HOUR / n,
                    &rng.hex(208)[..200]
                )
            })
            .collect();
        ingest_ok(s, "app", &format!("[{}]", rows.join(",")));
    }
}

// ---------------------------------------------------------------------------
// peak-seal
// ---------------------------------------------------------------------------

/// 当前快照中桶 `label` 各数据文件的大小（字节）。
fn file_sizes(ds: &Ds, label: &str) -> Vec<u64> {
    ds.bucket_manifest(&ds.head(), label).expect("桶在快照中")["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["size_bytes"].as_u64().unwrap())
        .collect()
}

/// 封口场景的造数：两小时前那个小时之内 800000 条，每条 `msg` 为 10 位序号后接同一个 8 字符片段重复 125 次、`trace_id` 为
/// 32 个随机十六进制字符；`flush.max_records` 为 50000（16 个小文件），封口宽限 24 小时使写入期间不合并。返回桶的起点。
fn seed_seal(f: &Fixture) -> i64 {
    const N: i64 = 800_000;
    const PER_REQUEST: i64 = 5_000;
    let start = hour_of(now_us()) - 2 * HOUR;
    let mut s = start_plain(
        f,
        &[
            ("lifecycle.seal_grace", Some("\"24h\"")),
            ("flush.max_records", Some("50000")),
            FLUSH_1H,
        ],
    );
    let pattern = "abcdefgh".repeat(125);
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    for first in (0..N).step_by(PER_REQUEST as usize) {
        let rows: Vec<String> = (first..first + PER_REQUEST)
            .map(|n| {
                format!(
                    r#"{{"_timestamp":{},"msg":"{n:010}{pattern}","trace_id":"{}"}}"#,
                    start + n * 1_000,
                    rng.hex(32)
                )
            })
            .collect();
        ingest_ok(&s, "app", &format!("[{}]", rows.join(",")));
    }
    f.dataset("app")
        .wait_records(N as u64, Duration::from_secs(180));
    stop(&mut s);
    start
}

fn start_plain(f: &Fixture, overrides: &[Kv]) -> Server {
    start(f, overrides, &[])
}

#[test]
fn bgmem_seal_peak_and_return() {
    let f = Fixture::new();
    let label = bucket_label(seed_seal(&f));
    let ds = f.dataset("app");
    let sizes = file_sizes(&ds, &label);
    let t = sizes.iter().sum::<u64>() / sizes.len() as u64 * 5 / 2;
    assert!(
        sizes.len() >= 8 && sizes.iter().all(|&n| 3 * n > t && 2 * n < t),
        "该桶数据文件不足 8 个或不都在 T/3 与 T/2 之间（T = {t}）：{sizes:?}"
    );
    let t_s = t.to_string();
    let gate = Gate::hold_each(&f, "compaction.after_select");
    let mut s = start(
        &f,
        &[
            ("lifecycle.seal_grace", Some("\"1s\"")),
            FLUSH_1H,
            ("compaction.target_file_bytes", Some(t_s.as_str())),
        ],
        &[&gate],
    );
    let ok = "amberlog_compactions_total{org=\"default\",stream=\"app\",result=\"ok\"}";
    let mut runs = Runs::new(value(&metrics(&s), ok));
    let mut sampler = None;
    let deadline = Instant::now() + Duration::from_secs(600);
    while has_mergeable_run(&file_sizes(&ds, &label), t) {
        assert_running(&mut s, deadline, "封口合并未收敛");
        if let Some(sm) = &sampler {
            Sampler::assert_alive(sm, &s);
        }
        let count = value(&metrics(&s), ok);
        runs.step(&s, &gate, &mut sampler, count);
        std::thread::sleep(Duration::from_millis(20));
    }
    // 收敛以后计数可能还差最后一次递增：再看 3 秒
    let settle = Instant::now() + Duration::from_secs(3);
    while Instant::now() < settle {
        let count = value(&metrics(&s), ok);
        runs.step(&s, &gate, &mut sampler, count);
        std::thread::sleep(Duration::from_millis(20));
    }
    let samples = sampler.expect("封口合并没有走到暂停点").stop();
    eprintln!("目标大小 T = {} KiB", t >> 10);
    assert!(runs.last >= 4.0, "封口合并只做了 {} 次", runs.last);
    runs.assert_peaks(&samples, 4 * t + PEAK_SLACK, "封口合并");
    runs.assert_return(&s);
}

// ---------------------------------------------------------------------------
// peak-cold
// ---------------------------------------------------------------------------

/// D 日（此刻所在 UTC 日的前两日）的起点。
fn day_d() -> i64 {
    now_us().div_euclid(DAY) * DAY - 2 * DAY
}

/// D 日的 `YYYY-MM-DD` 写法。
fn d_name() -> String {
    let days = day_d().div_euclid(DAY);
    // 1970-01-01 起的天数换算成公历日期（Howard Hinnant 的 civil_from_days）
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    format!("{:04}-{m:02}-{d:02}", if m <= 2 { y + 1 } else { y })
}

/// D 日分区当前的冷层存活数据文件：(大小, 是否为入湖新增)。
fn d_files(cold: &ColdDir) -> Vec<(u64, bool)> {
    if cold.version().is_none() {
        return Vec::new();
    }
    let meta = cold.current();
    let Some(current) = meta["current-snapshot-id"].as_i64() else {
        return Vec::new();
    };
    let snapshots = ColdDir::snapshots(&meta);
    let lake: BTreeSet<i64> = snapshots
        .iter()
        .filter(|s| s["summary"]["amberlog.operation"] == "lake")
        .filter_map(|s| s["snapshot-id"].as_i64())
        .collect();
    let snapshot = snapshots
        .iter()
        .find(|s| s["snapshot-id"].as_i64() == Some(current))
        .expect("当前快照在元数据中");
    let prefix = format!("data/_timestamp_day={}/", d_name());
    cold.entries(&meta, snapshot)
        .into_iter()
        .filter(|e| !e.deleted && e.path.starts_with(&prefix))
        .map(|e| (e.file_size_in_bytes, lake.contains(&e.snapshot_id)))
        .collect()
}

#[test]
fn bgmem_cold_peak_and_return() {
    const T: u64 = 67_108_864;
    const PER_HOUR: i64 = 280_000;
    let f = Fixture::new();
    let before_run = Gate::hold(&f, "cold_compaction.before_run");
    let after_select = Gate::hold_each(&f, "cold_compaction.after_select");
    let mut s = start(
        &f,
        &[
            ("compaction.target_file_bytes", Some("67108864")),
            ("lifecycle.seal_grace", Some("\"1s\"")),
            ("lifecycle.lake_watermark", Some("\"2s\"")),
            FLUSH_1H,
            ("flush.max_records", Some("280000")),
            ("cold_compaction.check_interval", Some("\"10s\"")),
            ("ingest.max_backlog_memory_bytes", Some("6442450944")),
        ],
        &[&before_run, &after_select],
    );
    let mut rng = Rng(0x2545_f491_4f6c_dd1d);
    for h in 0..12 {
        ingest_random(&s, &mut rng, day_d() + h * HOUR, PER_HOUR, 20_000);
    }
    let cold = f.cold("app");
    let deadline = Instant::now() + Duration::from_secs(900);
    loop {
        assert_running(&mut s, deadline, "12 个桶没有全部入湖");
        let m = metrics(&s);
        if value(&m, "amberlog_backlog_memory_bytes") == 0.0
            && sum(&m, "amberlog_hot_buckets{org=\"default\",stream=\"app\",") == 0.0
            && d_files(&cold).iter().filter(|(_, lake)| *lake).count() == 12
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    let laked: Vec<u64> = d_files(&cold).iter().map(|(n, _)| *n).collect();
    assert!(
        laked.len() == 12 && laked.iter().all(|n| (26_843_545..=33_554_432).contains(n)),
        "入湖新增文件不都在目标大小的 0.4 至 0.5 倍之间：{laked:?}"
    );

    let ok = "amberlog_manifest_commits_total{org=\"default\",stream=\"app\",result=\"ok\"}";
    let commits0 = value(&metrics(&s), ok);
    let mut runs = Runs::new(commits0);
    let mut sampler = None;
    assert!(
        before_run.held(),
        "冷层合并检查没有停在 cold_compaction.before_run"
    );
    before_run.release();
    let deadline = Instant::now() + Duration::from_secs(900);
    while d_files(&cold)
        .iter()
        .filter(|(n, _)| *n < 33_554_432)
        .count()
        >= 2
    {
        assert_running(&mut s, deadline, "冷层合并未收敛");
        if let Some(sm) = &sampler {
            Sampler::assert_alive(sm, &s);
        }
        let count = value(&metrics(&s), ok);
        runs.step(&s, &after_select, &mut sampler, count);
        std::thread::sleep(Duration::from_millis(20));
    }
    // 合并记录所在的热层提交在冷层提交之后，相隔多久随机器快慢（较慢的机器上约 8 秒）：等计数追上已开始的合并次数
    // （每次合并一次热层提交），至多 120 秒；没追上时由下面的断言判定
    let settle = Instant::now() + Duration::from_secs(120);
    while runs.last - commits0 < runs.pauses.len() as f64 && Instant::now() < settle {
        assert_running(&mut s, deadline, "冷层合并未收敛");
        let count = value(&metrics(&s), ok);
        runs.step(&s, &after_select, &mut sampler, count);
        std::thread::sleep(Duration::from_millis(20));
    }
    let samples = sampler.expect("冷层合并没有走到暂停点").stop();
    assert!(
        runs.last - commits0 >= 4.0,
        "放行以后热层提交只增加了 {}",
        runs.last - commits0
    );
    runs.assert_peaks(&samples, 12 * T + PEAK_SLACK, "冷层合并");
    runs.assert_return(&s);
}

// ---------------------------------------------------------------------------
// return
// ---------------------------------------------------------------------------

#[test]
fn bgmem_lake_return() {
    const N: i64 = 1_000_000;
    let f = Fixture::new();
    let start_us = hour_of(now_us()) - 3 * HOUR;
    let mut s = start_plain(
        &f,
        &[
            ("lifecycle.seal_grace", Some("\"24h\"")),
            FLUSH_1H,
            ("flush.max_records", Some("100000")),
        ],
    );
    ingest_random(&s, &mut Rng(0xd1b5_4a32_d192_ed03), start_us, N, 20_000);
    f.dataset("app")
        .wait_records(N as u64, Duration::from_secs(180));
    stop(&mut s);

    let gate = Gate::hold_each(&f, "lake.before_read");
    let mut s = start(
        &f,
        &[
            ("lifecycle.seal_grace", Some("\"1s\"")),
            ("lifecycle.lake_watermark", Some("\"2s\"")),
            FLUSH_1H,
        ],
        &[&gate],
    );
    let deadline = Instant::now() + Duration::from_secs(120);
    while !gate.held() {
        assert_running(&mut s, deadline, "入湖没有停在 lake.before_read");
        std::thread::sleep(Duration::from_millis(20));
    }
    let before = rss_now(&s);
    gate.release();
    let deadline = Instant::now() + Duration::from_secs(600);
    loop {
        assert_running(&mut s, deadline, "入湖没有完成");
        if sum(
            &metrics(&s),
            "amberlog_hot_buckets{org=\"default\",stream=\"app\",",
        ) == 0.0
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    std::thread::sleep(Duration::from_secs(10));
    let after = rss_now(&s);
    eprintln!(
        "入湖开始前 {} MiB，完成 10 秒后 {} MiB",
        before >> 20,
        after >> 20
    );
    assert!(
        after <= before + RETURN_SLACK,
        "入湖完成 10 秒后 RssAnon {} MiB，超过开始前的值 {} MiB 加 512 MiB",
        after >> 20,
        before >> 20
    );
}

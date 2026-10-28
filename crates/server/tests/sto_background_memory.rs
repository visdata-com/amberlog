//! 后台任务（封口合并）的内存：不对应需求场景的回归测试。
//!
//! 封口合并在 tokio 工作线程上解码合并输入，用完释放后留在各线程的 glibc 分配区里；不把空闲内存还给系统时，
//! 一次次合并落在不同的分配区上，匿名常驻内存逐级堆积、回不到合并之前；合并时
//! 把全部输入解码、拼成一个批次再写，单次合并就要压缩输入的约 30 倍。需求块对后台任务的内存没有条款，这里核对
//! 两项性质：合并做完以后常驻内存回落；合并边读边写，单次峰值只与压缩大小有关。
//!
//! 被测进程是测试启动的 `amberlog serve` 子进程，读它的 `/proc/<pid>/status`（只为 Linux 编写）。

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use common::api::{FLUSH_1H, ingest_ok, now_us};
use common::store::{Ds, HOUR, bucket_label, has_mergeable_run, hour_of, metric, stop};
use common::{Fixture, Kv, Server};
use serde_json::{Value, json};

/// 进程 `pid` 的 `/proc/<pid>/status` 中 `RssAnon` 的字节数。
fn rss_anon(pid: u32) -> u64 {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
    let kb: u64 = status
        .lines()
        .find_map(|l| l.strip_prefix("RssAnon:"))
        .expect("RssAnon 行")
        .trim()
        .trim_end_matches("kB")
        .trim()
        .parse()
        .unwrap();
    kb * 1024
}

/// 当前快照中桶 `label` 各数据文件的大小（字节）。
fn file_sizes(ds: &Ds, label: &str) -> Vec<u64> {
    ds.bucket_manifest(&ds.head(), label).expect("桶在快照中")["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["size_bytes"].as_u64().unwrap())
        .collect()
}

fn compactions_ok(s: &Server) -> f64 {
    metric(
        s,
        "amberlog_compactions_total{org=\"default\",stream=\"app\",result=\"ok\"}",
    )
    .unwrap_or(0.0)
}

/// 每个小文件的记录数（`flush.max_records`，按条数整批 flush）。
const PER_FILE: i64 = 50_000;
/// 小文件个数。
const FILES: i64 = 16;
/// 一次摄取请求的记录数（请求体约 5.5 MB，在缺省 32 MiB 的上限之内）。
const PER_REQUEST: i64 = 5_000;

/// 在两小时前的桶里写出 [`FILES`] 个各 [`PER_FILE`] 条的小文件：每条约 1 KB、高度可压缩（解码后约为压缩后的几十倍），
/// 封口宽限 24 小时使写入期间不合并。返回桶的起点。
fn seed(f: &Fixture) -> i64 {
    let start = hour_of(now_us()) - 2 * HOUR;
    let per_file = PER_FILE.to_string();
    let mut s = f.start(&[
        ("lifecycle.seal_grace", Some("\"24h\"")),
        ("flush.max_records", Some(per_file.as_str())),
        FLUSH_1H,
    ]);
    let pattern = "abcdefgh".repeat(125);
    let ds = f.dataset("app");
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    for i in 0..FILES * PER_FILE / PER_REQUEST {
        let rows: Vec<Value> = (i * PER_REQUEST..(i + 1) * PER_REQUEST)
            .map(|n| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                json!({
                    "_timestamp": start + n * 1_000,
                    "msg": format!("{n:010}{pattern}"),
                    "trace_id": format!("{state:016x}{:016x}", state.rotate_left(29)),
                })
            })
            .collect();
        ingest_ok(&s, "app", &Value::Array(rows).to_string());
    }
    ds.wait_records((FILES * PER_FILE) as u64, Duration::from_secs(120));
    stop(&mut s);
    start
}

/// 封口合并做完以后，匿名常驻内存回落到合并开始之前附近，不随合并次数逐级堆积；合并期间的峰值不超过开始时加上
/// 归还阈值（256 MiB；低于它的空闲内存不归还，会带进下一次合并）、一次合并在用的余量（128 MiB：写入器正在编码的行组、
/// 解码中的一批与分配器碎片）与 8 倍目标大小（判据沿用 REQ-AMB-STO-SEAL-COMPACTION-V3 时的写法：那时一次合并的压缩
/// 输入至多约 2 倍目标大小，输入与输出的压缩字节各一份；V4 起合并输入之和不大于目标大小，判据不变）。
///
/// 目标大小取小文件平均大小的 2.5 倍：每个文件约为目标大小的 0.4 倍，是小文件（小于目标大小的一半），任两个构成可合并
/// 的一组、任三个之和超过目标大小，所以两两合并 8 次（每次解码约 110 MB），产物约 0.8 倍、不再是小文件，之后没有相邻
/// 可合并组（REQ-AMB-STO-SEAL-COMPACTION-V4）。下界取 4 次（同 REQ-AMB-STO-BACKGROUND-MEMORY-V2）。
#[test]
fn compaction_memory_returned_after_merges() {
    let f = Fixture::new();
    let start = seed(&f);
    let label = bucket_label(start);
    let ds = f.dataset("app");
    let sizes = file_sizes(&ds, &label);
    assert!(sizes.len() as i64 >= FILES / 2, "{sizes:?}");
    let target = sizes.iter().sum::<u64>() / sizes.len() as u64 * 5 / 2;
    let target_s = target.to_string();
    let config: [Kv; 3] = [
        ("lifecycle.seal_grace", Some("\"1s\"")),
        ("compaction.target_file_bytes", Some(target_s.as_str())),
        FLUSH_1H,
    ];
    let s = f.start(&config);
    let pid = s.pid();
    let before = rss_anon(pid);
    // 每 20 毫秒采一次峰值，直到合并收敛
    let done = Arc::new(AtomicBool::new(false));
    let sampler = {
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            let mut peak = 0;
            while !done.load(Ordering::SeqCst) {
                peak = peak.max(rss_anon(pid));
                std::thread::sleep(Duration::from_millis(20));
            }
            peak
        })
    };
    let deadline = Instant::now() + Duration::from_secs(600);
    loop {
        let sizes = file_sizes(&ds, &label);
        if !has_mergeable_run(&sizes, target) {
            break;
        }
        assert!(Instant::now() < deadline, "合并未收敛：{sizes:?}");
        std::thread::sleep(Duration::from_millis(100));
    }
    done.store(true, Ordering::SeqCst);
    let peak = sampler.join().unwrap().max(before);
    let merges = compactions_ok(&s);
    // 最后一次合并之后的归还在阻塞线程池里做
    std::thread::sleep(Duration::from_secs(3));
    let after = rss_anon(pid);
    eprintln!(
        "目标大小 {} KiB；合并 {merges} 次；RssAnon 开始 {} MiB，峰值 {} MiB，合并完 {} MiB",
        target >> 10,
        before >> 20,
        peak >> 20,
        after >> 20
    );
    assert!(merges >= 4.0, "合并只做了 {merges} 次");
    assert!(
        after <= before + (512 << 20),
        "合并完 RssAnon {} MiB，开始时 {} MiB（峰值 {} MiB，合并 {merges} 次）",
        after >> 20,
        before >> 20,
        peak >> 20
    );
    let bound = before + (256 << 20) + (128 << 20) + 8 * target;
    assert!(
        peak <= bound,
        "合并期间 RssAnon 峰值 {} MiB，超过 {} MiB（开始时 {} MiB，目标大小 {} KiB）",
        peak >> 20,
        bound >> 20,
        before >> 20,
        target >> 10
    );
}

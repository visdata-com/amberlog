//! REQ-AMB-LIFE-RETIREMENT 的系统测试：以子进程先起 `amberlog serve`、
//! 再起 `amberlog retire`，对象存储用 `file://` 临时目录，测试直接读对象检查快照、桶 manifest、冷层元数据与退役记录。
//! 场景约定（「退役前的实例」「运行 `retire`」「报告」「直读」）见 `common::retire`。

// 多数场景要用测试专用的注入（暂停点、对象存储注入），只在 `fault-injection` 特性下编译
#![cfg_attr(not(feature = "fault-injection"), allow(dead_code, unused_imports))]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::{Duration, Instant};

use common::api::now_us;
use common::retire::{Ctl, Ran, Seq, direct_seqs, finish, in_hour_ago, now_ts};
use common::store::{Ds, stop};
use common::{Fixture, HeldPort, Server, amberlog};
use serde_json::{Value, json};

const RECORD: &str = "amberlog/_system/retirement.json";
/// 退役记录相对 `<storage.prefix>/` 的键（对象存储注入与请求记录的写法）。
const RECORD_REL: &str = "_system/retirement.json";
const FLUSH_1H: common::Kv = ("flush.interval", Some("\"1h\""));
const WAIT: Duration = Duration::from_secs(120);

const AFTER_REPLAY: &str = "retire.after_replay";
const AFTER_BUCKET: &str = "retire.after_bucket_lake";
const BEFORE_DELETE: &str = "retire.before_physical_delete";
const BEFORE_RECORD: &str = "retire.before_record_write";
const ALL_GATES: &[&str] = &[AFTER_REPLAY, AFTER_BUCKET, BEFORE_DELETE, BEFORE_RECORD];

// ---------------------------------------------------------------------------
// 读对象
// ---------------------------------------------------------------------------

/// 退役记录（不存在为 `None`）。
fn record(f: &Fixture) -> Option<Value> {
    let p = f.store_dir().join(RECORD);
    std::fs::read(&p)
        .ok()
        .map(|b| serde_json::from_slice(&b).expect("退役记录是 JSON"))
}

fn record_exists(f: &Fixture) -> bool {
    f.store_dir().join(RECORD).exists()
}

/// `base` 之下全部文件（相对键 → 完整字节）；对象存储本地后端的暂存文件（名字带 `#`）不算。
fn files(base: &Path) -> BTreeMap<String, Vec<u8>> {
    fn walk(base: &Path, p: &Path, out: &mut BTreeMap<String, Vec<u8>>) {
        let Ok(rd) = std::fs::read_dir(p) else {
            return;
        };
        for e in rd.flatten() {
            let path = e.path();
            if path.is_dir() {
                walk(base, &path, out);
            } else if !e.file_name().to_string_lossy().contains('#')
                && let Ok(bytes) = std::fs::read(&path)
            {
                let rel = path
                    .strip_prefix(base)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned();
                out.insert(rel, bytes);
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(base, base, &mut out);
    out
}

/// 对象存储中 `amberlog/` 之下的全部对象。
fn objects(f: &Fixture) -> BTreeMap<String, Vec<u8>> {
    files(&f.store_dir().join("amberlog"))
}

/// 启动探测对象（`_system/` 之下除退役记录外的对象）。
fn is_probe(key: &str) -> bool {
    key == "_system/readyz-probe" || key.starts_with("_system/create-only-probe-")
}

/// `wal.dir` 之下的全部文件。
fn wal_files(f: &Fixture) -> BTreeMap<String, Vec<u8>> {
    files(&f.wal_dir())
}

fn wal_segments(f: &Fixture) -> Vec<String> {
    wal_files(f)
        .into_keys()
        .filter(|k| k.ends_with(".wal"))
        .collect()
}

/// 当前快照的存活数据文件条目。
fn live(ds: &Ds) -> Vec<Value> {
    ds.file_entries(&ds.head())
}

/// 完整删除台账中 `reason` 为 `reason` 的热层数据文件路径。
fn ledger_paths(ds: &Ds, reason: &str) -> Vec<String> {
    ds.ledger()
        .iter()
        .filter(|d| {
            d["reason"] == reason
                && !d["path"]
                    .as_str()
                    .unwrap()
                    .starts_with("data/_timestamp_day=")
        })
        .map(|d| d["path"].as_str().unwrap().to_owned())
        .collect()
}

/// 冷层变更段 `deletions` 所记的冷层数据文件路径（相对冷层表目录）。
fn cold_deleted(ds: &Ds) -> Vec<String> {
    ds.segments(&ds.head()["cold_deletion_segment"])
        .iter()
        .flat_map(|s| s["deletions"].as_array().unwrap().clone())
        .map(|d| d["path"].as_str().unwrap().to_owned())
        .collect()
}

/// 冷层当前版本中 `amberlog.operation` 为 `op` 的快照。
fn cold_snapshots(f: &Fixture, stream: &str, op: &str) -> Vec<Value> {
    let cold = f.cold(stream);
    if cold.version().is_none() {
        return Vec::new();
    }
    common::lake::ColdDir::snapshots(&cold.current())
        .into_iter()
        .filter(|s| s["summary"]["amberlog.operation"] == op)
        .collect()
}

fn sha256(bytes: &[u8]) -> String {
    amberlog_storage::chain::Digest::of(bytes).to_string()
}

fn sorted(mut v: Vec<i64>) -> Vec<i64> {
    v.sort_unstable();
    v
}

fn distinct(v: &[i64]) -> bool {
    v.iter().collect::<BTreeSet<_>>().len() == v.len()
}

/// `amberlog --version` 输出中的 `<semver>`。
fn semver(f: &Fixture) -> String {
    let out = amberlog(f.root()).arg("--version").output().unwrap();
    let line = String::from_utf8(out.stdout).unwrap();
    line.split_whitespace().nth(1).unwrap().to_owned()
}

/// 报告中数据集 `dataset` 的元素。
fn ds_of<'a>(rep: &'a Value, dataset: &str) -> &'a Value {
    rep["datasets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|d| d["dataset"] == dataset)
        .unwrap_or_else(|| panic!("报告中没有 {dataset}：{rep}"))
}

fn ok(r: &Ran) -> Value {
    assert_eq!(r.code, Some(0), "{:#?}\n{:#?}", r.stdout, r.stderr);
    r.report()
}

fn keys(v: &Value) -> BTreeSet<String> {
    v.as_object().unwrap().keys().cloned().collect()
}

fn set(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|s| (*s).to_owned()).collect()
}

/// 退役前的实例已向 `app` 摄取落在此刻前第 40 小时与第 30 小时的记录各 20 条，随后停止实例。
fn two_buckets(f: &Fixture) {
    let mut s = f.before(&[]);
    let mut q = Seq::default();
    q.ingest(&s, "app", &in_hour_ago(40, 20));
    q.ingest(&s, "app", &in_hour_ago(30, 20));
    stop(&mut s);
}

#[test]
fn retire_report_fields() {
    let f = Fixture::new();
    two_buckets(&f);
    let r = f.run_retire(&[]);
    assert_eq!(r.code, Some(0), "{:#?}", r.stderr);
    let rep = r.report();
    assert_eq!(
        keys(&rep),
        set(&["status", "interrupted", "record", "datasets", "exceptions"])
    );
    assert_eq!(rep["status"], "retired");
    assert_eq!(rep["interrupted"], false);
    assert_eq!(rep["record"], RECORD);
    assert_eq!(rep["exceptions"], serde_json::json!([]));
    assert_eq!(direct_seqs(&f, "app", true).len(), 40);
}

// ---------------------------------------------------------------------------
// 搭建
// ---------------------------------------------------------------------------

/// 退役前的实例已向 `stream` 摄取 `n` 条落在此刻前第 `hours` 小时的记录，随后停止实例。
fn ingested(f: &Fixture, stream: &str, hours: i64, n: usize) {
    let mut s = f.before(&[]);
    Seq::default().ingest(&s, stream, &in_hour_ago(hours, n));
    stop(&mut s);
}

/// 运行 `retire`（带注入与暂停点控制）。
fn retire_with(
    f: &Fixture,
    ctl: &Ctl,
    extra: &[common::Kv],
    gates: &[&str],
    pause: Option<&str>,
) -> Server {
    Server::spawn(ctl.apply(f.retire(extra), gates, pause))
}

/// 「退役记录的字段」的前提：`app` 50 条、`web` 20 条落在此刻前第 30 小时，停止实例；`retire` 另配 `retention.period`
/// 为 `720h`。运行 `retire` 并返回（t0，结果，t1）。
fn retired_two(f: &Fixture) -> (i64, Ran, i64) {
    let mut s = f.before(&[]);
    let mut q = Seq::default();
    q.ingest(&s, "app", &in_hour_ago(30, 50));
    q.ingest(&s, "web", &in_hour_ago(30, 20));
    stop(&mut s);
    let t0 = now_us();
    let r = f.run_retire(&[("retention.period", Some("\"720h\""))]);
    let t1 = now_us();
    (t0, r, t1)
}

// ---------------------------------------------------------------------------
// B · 退役记录
// ---------------------------------------------------------------------------

#[cfg(feature = "fault-injection")]
#[test]
fn retire_record_written_last() {
    let f = Fixture::new();
    ingested(&f, "app", 30, 50);
    let ctl = Ctl::new(&f);
    ctl.hold(BEFORE_RECORD);
    let mut p = retire_with(&f, &ctl, &[], ALL_GATES, None);
    ctl.wait_held(BEFORE_RECORD, &p, WAIT);
    assert!(!record_exists(&f));
    assert_eq!(live(&f.dataset("app")), Vec::<Value>::new());
    ctl.release(BEFORE_RECORD);
    let r = finish(&mut p, WAIT);
    ok(&r);
    assert!(record_exists(&f));
    let last_write = ctl
        .requests()
        .into_iter()
        .rfind(|l| l.starts_with("put ") || l.starts_with("create "))
        .unwrap();
    assert_eq!(last_write, format!("create {RECORD_REL}"));
}

/// 写出时以「已存在」失败（另一个 `retire` 同时写出了退役记录）报告 `already_retired` 并以 0 退出。
#[cfg(feature = "fault-injection")]
#[test]
fn retire_record_create_conflict_already_retired() {
    let f = Fixture::new();
    ingested(&f, "app", 30, 10);
    let ctl = Ctl::new(&f);
    ctl.hold(BEFORE_RECORD);
    let mut p = retire_with(&f, &ctl, &[], ALL_GATES, None);
    ctl.wait_held(BEFORE_RECORD, &p, WAIT);
    std::fs::write(f.store_dir().join(RECORD), b"{}").unwrap();
    ctl.release(BEFORE_RECORD);
    let rep = ok(&finish(&mut p, WAIT));
    assert_eq!(rep["status"], "already_retired");
    assert_eq!(rep["record"], RECORD);
    assert_eq!(std::fs::read(f.store_dir().join(RECORD)).unwrap(), b"{}");
}

#[cfg(feature = "fault-injection")]
#[test]
fn retire_record_write_failed() {
    let f = Fixture::new();
    ingested(&f, "app", 30, 30);
    let ctl = Ctl::new(&f);
    ctl.faults(&format!("create {RECORD_REL} 403"));
    let r = finish(&mut retire_with(&f, &ctl, &[], &[], None), WAIT);
    assert_eq!(r.code, Some(6), "{:#?}", r.stderr);
    let rep = r.report();
    assert_eq!(rep["status"], "incomplete");
    assert_eq!(ds_of(&rep, "default/app")["settled"], true);
    assert_eq!(
        rep["exceptions"],
        json!([{"dataset": null, "kind": "record_write_failed", "bucket": null, "paths": ["_system/retirement.json"]}])
    );
    ctl.faults("");
    let rep = ok(&finish(&mut retire_with(&f, &ctl, &[], &[], None), WAIT));
    assert_eq!(ds_of(&rep, "default/app")["buckets_laked"], 0);
    assert_eq!(ds_of(&rep, "default/app")["files_deleted"], 0);
    assert!(record_exists(&f));
}

#[test]
fn retire_record_fields() {
    let f = Fixture::new();
    let (t0, r, t1) = retired_two(&f);
    ok(&r);
    let rec = record(&f).unwrap();
    assert_eq!(
        keys(&rec),
        set(&[
            "format_version",
            "retired_at",
            "amberlog_version",
            "retention_period",
            "datasets"
        ])
    );
    assert_eq!(rec["format_version"], 1);
    let at = rec["retired_at"].as_i64().expect("UTC 微秒整数");
    assert!(t0 <= at && at <= t1, "{t0} {at} {t1}");
    assert_eq!(rec["amberlog_version"], semver(&f));
    assert_eq!(rec["retention_period"], "720h");
}

#[test]
fn retire_record_retention_null() {
    let f = Fixture::new();
    ingested(&f, "app", 0, 10);
    ok(&f.run_retire(&[]));
    assert_eq!(record(&f).unwrap()["retention_period"], Value::Null);
}

#[test]
fn retire_record_datasets() {
    let f = Fixture::new();
    let (_, r, _) = retired_two(&f);
    ok(&r);
    let rec = record(&f).unwrap();
    let datasets = rec["datasets"].as_array().unwrap();
    let names: Vec<&str> = datasets
        .iter()
        .map(|d| d["dataset"].as_str().unwrap())
        .collect();
    assert_eq!(names, ["default/app", "default/web"]);
    for d in datasets {
        let stream = d["dataset"]
            .as_str()
            .unwrap()
            .strip_prefix("default/")
            .unwrap();
        let ds = f.dataset(stream);
        let id = ds.current().unwrap();
        let path = format!("manifest/{id:020}.json");
        assert_eq!(
            d["snapshot"],
            json!({"snapshot_id": id, "path": path, "content_digest": sha256(&ds.read(&path))})
        );
        let cold = f.cold(stream);
        let n = cold.version().unwrap();
        let mpath = format!("metadata/v{n}.metadata.json");
        assert_eq!(
            d["cold"],
            json!({"metadata_version": n, "path": mpath, "content_digest": sha256(&cold.read(&mpath))})
        );
    }
}

#[test]
fn retire_empty_prefix() {
    let f = Fixture::new();
    std::fs::create_dir_all(f.wal_dir()).unwrap();
    ok(&f.run_retire(&[]));
    assert_eq!(record(&f).unwrap()["datasets"], json!([]));
}

/// 以「运行 `retire`」的同一配置（另含 `retention.period` 为 `720h`）全时段导出 `default/app` 到夹具下的新路径 `name`，
/// 断言以 0 退出。
fn export_same_config(f: &Fixture, name: &str) {
    let out = f.root().join(name);
    assert!(!out.exists());
    let o = amberlog(f.root())
        .arg("evidence")
        .arg("export")
        .arg("--config")
        .arg(f.retire_config(&[("retention.period", Some("\"720h\""))]))
        .args([
            "--dataset",
            "default/app",
            "--output",
            out.to_str().unwrap(),
        ])
        .stdin(std::process::Stdio::null())
        .output()
        .unwrap();
    assert_eq!(
        o.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&o.stderr)
    );
}

#[test]
fn retire_record_unchanged_on_rerun() {
    let f = Fixture::new();
    ok(&retired_two(&f).1);
    let before = std::fs::read(f.store_dir().join(RECORD)).unwrap();
    ok(&f.run_retire(&[("retention.period", Some("\"720h\""))]));
    export_same_config(&f, "e.tar");
    assert_eq!(std::fs::read(f.store_dir().join(RECORD)).unwrap(), before);
}

// ---------------------------------------------------------------------------
// C · 启动与总次序
// ---------------------------------------------------------------------------

#[test]
fn retire_already_retired_no_writes() {
    let f = Fixture::new();
    ok(&retired_two(&f).1);
    let before: BTreeMap<String, Vec<u8>> = objects(&f)
        .into_iter()
        .filter(|(k, _)| !is_probe(k))
        .collect();
    let rep = ok(&f.run_retire(&[("retention.period", Some("\"720h\""))]));
    assert_eq!(rep["status"], "already_retired");
    assert_eq!(rep["record"], RECORD);
    assert_eq!(
        (&rep["datasets"], &rep["exceptions"]),
        (&json!([]), &json!([]))
    );
    let after: BTreeMap<String, Vec<u8>> = objects(&f)
        .into_iter()
        .filter(|(k, _)| !is_probe(k))
        .collect();
    assert!(before == after, "对象有变动");
}

#[test]
fn retire_unparsable_record_counts_as_retired() {
    let f = Fixture::new();
    ingested(&f, "app", 0, 10);
    std::fs::create_dir_all(f.store_dir().join("amberlog/_system")).unwrap();
    std::fs::write(f.store_dir().join(RECORD), b"{").unwrap();
    let wal = wal_files(&f);
    let rep = ok(&f.run_retire(&[]));
    assert_eq!(rep["status"], "already_retired");
    assert!(wal == wal_files(&f), "WAL 段有变动");
}

/// 退役前的实例已向 `app` 摄取 30 条落在此刻前第 30 小时的记录并已提交快照，再摄取 20 条此刻的记录并收到 HTTP 200，
/// 随后被强杀。
fn committed_then_killed(f: &Fixture) {
    let mut q = Seq::default();
    let mut s = f.before(&[]);
    q.ingest(&s, "app", &in_hour_ago(30, 30));
    f.dataset("app").wait_records(30, WAIT);
    q.ingest(&s, "app", &now_ts(20));
    s.sigkill();
}

#[cfg(feature = "fault-injection")]
#[test]
fn retire_order() {
    let f = Fixture::new();
    committed_then_killed(&f);
    let ds = f.dataset("app");
    let ctl = Ctl::new(&f);
    for g in [AFTER_REPLAY, BEFORE_DELETE, BEFORE_RECORD] {
        ctl.hold(g);
    }
    let mut p = retire_with(&f, &ctl, &[], ALL_GATES, None);
    ctl.wait_held(AFTER_REPLAY, &p, WAIT);
    let records: u64 = live(&ds)
        .iter()
        .map(|e| e["record_count"].as_u64().unwrap())
        .sum();
    assert_eq!(records, 50);
    assert_eq!(f.cold("app").version(), None);
    ctl.release(AFTER_REPLAY);
    ctl.wait_held(BEFORE_DELETE, &p, WAIT);
    assert_eq!(live(&ds), Vec::<Value>::new());
    let moved = ledger_paths(&ds, "lake");
    assert!(!moved.is_empty());
    assert!(moved.iter().all(|m| ds.exists(m)), "{moved:?}");
    ctl.release(BEFORE_DELETE);
    ctl.wait_held(BEFORE_RECORD, &p, WAIT);
    assert!(moved.iter().all(|m| !ds.exists(m)), "{moved:?}");
    assert!(!record_exists(&f));
    ctl.release(BEFORE_RECORD);
    ok(&finish(&mut p, WAIT));
}

#[cfg(feature = "fault-injection")]
#[test]
fn retire_scope_list_fails_exit_3() {
    let f = Fixture::new();
    let mut s = f.before(&[]);
    Seq::default().ingest(&s, "app", &now_ts(20));
    s.sigkill();
    let wal = wal_files(&f);
    let ctl = Ctl::new(&f);
    ctl.faults("list * 403");
    let r = finish(&mut retire_with(&f, &ctl, &[], &[], None), WAIT);
    assert_eq!(r.code, Some(3), "{:#?}", r.stderr);
    assert_eq!(r.stdout, Vec::<String>::new());
    assert!(wal == wal_files(&f), "WAL 段有变动");
    assert!(!record_exists(&f));
}

#[cfg(feature = "fault-injection")]
#[test]
fn retire_no_listen() {
    let f = Fixture::new();
    ingested(&f, "app", 0, 10);
    // 端口由测试占住：`retire` 若去监听，必然以退出码 4 失败、stderr 含 `cannot listen on`
    let held = HeldPort::new();
    let addr = held.addr();
    let ctl = Ctl::new(&f);
    ctl.hold(BEFORE_RECORD);
    let extra = [
        ("server.http_addr", Some(addr.as_str())),
        ("wal.alert_bytes", Some("1")),
    ];
    let mut p = retire_with(&f, &ctl, &extra, ALL_GATES, None);
    ctl.wait_held(BEFORE_RECORD, &p, WAIT);
    ctl.release(BEFORE_RECORD);
    let r = finish(&mut p, WAIT);
    ok(&r);
    held.assert_never_listened(r.code, &p);
    assert_eq!(r.events("starting").len(), 1);
    for e in ["ready", "lake_backlog_high", "wal_backlog_high"] {
        assert_eq!(r.events(e), Vec::<Value>::new(), "{e}");
    }
}

/// 写入实例专用的配置项（监听地址、查询并发上限、flush 间隔）不起作用；`compaction.target_file_bytes` 起作用（入湖的冷层文件
/// 大小上限）。端口 P 由测试进程自己占住：`retire` 若去监听，必然失败。
///
/// 场景只规定 300 条与 `_timestamp`：只含 `_timestamp` 与 `seq` 的 300 条编码后不到 4096 字节、只成一个文件，所以每条另带一个
/// 64 字符的伪随机字段 `blob`，使数据量超过 4096 字节。
#[test]
fn retire_config_keys() {
    let f = Fixture::new();
    let mut s = f.before(&[]);
    let ts = in_hour_ago(30, 300);
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    let records: Vec<String> = ts
        .iter()
        .enumerate()
        .map(|(i, t)| {
            let blob: String = (0..64)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    char::from(b'a' + (x % 26) as u8)
                })
                .collect();
            format!(r#"{{"_timestamp":{t},"seq":{i},"blob":"{blob}"}}"#)
        })
        .collect();
    common::api::ingest_ok(&s, "app", &format!("[{}]", records.join(",")));
    stop(&mut s);
    let held = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let addr = format!("\"{}\"", held.local_addr().unwrap());
    ok(&f.run_retire(&[
        ("server.http_addr", Some(addr.as_str())),
        ("query.max_concurrency", Some("1")),
        FLUSH_1H,
        ("compaction.target_file_bytes", Some("4096")),
    ]));
    let cold = f.cold("app");
    let meta = cold.current();
    let lakes = common::lake::ColdDir::lake_snapshots(&meta, &label_of(ts[0]));
    let added: Vec<common::lake::Entry> = lakes.iter().flat_map(|l| cold.added(&meta, l)).collect();
    assert!(added.len() > 1, "{added:?}");
    for e in &added {
        assert!(e.file_size_in_bytes <= 4096 || e.record_count == 1, "{e:?}");
    }
    assert_eq!(direct_seqs(&f, "app", true).len(), 300);
}

/// 数据文件 `bytes` 中列 `column` 逐行组是否带 bloom（列块的 `bloom_filter_offset`）。文件中没有该列时为空。
fn bloom_row_groups(bytes: &[u8], column: &str) -> Vec<bool> {
    let n = bytes.len();
    assert_eq!(&bytes[n - 4..], b"PAR1", "不是 Parquet 文件");
    let len = u32::from_le_bytes(bytes[n - 8..n - 4].try_into().unwrap()) as usize;
    let meta =
        parquet::file::metadata::ParquetMetaDataReader::decode_metadata(&bytes[n - 8 - len..n - 8])
            .expect("尾部元数据可解析");
    let schema = meta.file_metadata().schema_descr();
    let Some(i) = (0..schema.num_columns()).find(|&i| schema.column(i).name() == column) else {
        return Vec::new();
    };
    meta.row_groups()
        .iter()
        .map(|g| g.column(i).bloom_filter_offset().is_some())
        .collect()
}

/// `index_hints` 在 `retire` 下起作用（REQ-AMB-LIFE-RETIREMENT-CONFIG-KEYS）：退役入湖的冷层数据文件按 `retire` 读入的
/// 索引提示写等值 bloom（REQ-AMB-QRY-EQUALITY-BLOOM）。退役前的实例不声明索引提示，只在 `retire` 的配置里声明。
#[test]
fn retire_index_hints_bloom() {
    let f = Fixture::new();
    let mut s = f.before(&[]);
    let ts = in_hour_ago(30, 20);
    let records: Vec<String> = ts
        .iter()
        .enumerate()
        .map(|(i, t)| format!(r#"{{"_timestamp":{t},"seq":{i},"trace_id":"t{i}","msg":"m{i}"}}"#))
        .collect();
    common::api::ingest_ok(&s, "app", &format!("[{}]", records.join(",")));
    stop(&mut s);
    ok(&f.run_retire(&[("index_hints", Some(r#"{ "default/app" = ["trace_id"] }"#))]));
    let cold = f.cold("app");
    let meta = cold.current();
    let lakes = common::lake::ColdDir::lake_snapshots(&meta, &label_of(ts[0]));
    let added: Vec<common::lake::Entry> = lakes.iter().flat_map(|l| cold.added(&meta, l)).collect();
    assert!(!added.is_empty(), "{meta}");
    for e in &added {
        let bytes = cold.read(&e.path);
        let declared = bloom_row_groups(&bytes, "trace_id");
        assert!(
            !declared.is_empty() && declared.iter().all(|b| *b),
            "{}：trace_id 应有 bloom",
            e.path
        );
        assert!(
            bloom_row_groups(&bytes, "msg").iter().all(|b| !b),
            "{}：msg 不应有 bloom",
            e.path
        );
    }
    assert_eq!(direct_seqs(&f, "app", true).len(), 20);
}

#[test]
fn retire_no_periodic_work() {
    let f = Fixture::new();
    let mut s = f.before(&[]);
    let mut q = Seq::default();
    q.ingest(&s, "app", &in_hour_ago(150, 10));
    q.ingest(&s, "app", &now_ts(10));
    stop(&mut s);
    let ds = f.dataset("app");
    let before: BTreeSet<u64> = ds.snapshot_ids().into_iter().collect();
    let extra = [
        ("lifecycle.lake_watermark", Some("\"48h\"")),
        ("retention.period", Some("\"100h\"")),
        ("retention.check_interval", Some("\"5s\"")),
        ("hot_fsck.interval", Some("\"5s\"")),
        ("storage.delete_delay", Some("\"20s\"")),
    ];
    ok(&f.run_retire(&extra));
    let after: BTreeSet<u64> = ds.snapshot_ids().into_iter().collect();
    assert_eq!(direct_seqs(&f, "app", true).len(), 20);
    for op in ["ttl", "compaction"] {
        assert_eq!(cold_snapshots(&f, "app", op), Vec::<Value>::new(), "{op}");
    }
    assert!(before.is_subset(&after), "{before:?} {after:?}");
    assert_eq!(ds.keys_under("manifest/retired/"), Vec::<String>::new());
}

#[test]
fn retire_writes_only_listed_objects() {
    let f = Fixture::new();
    ingested(&f, "app", 30, 30);
    let before: BTreeSet<String> = objects(&f).into_keys().collect();
    let wal_before: BTreeSet<String> = wal_files(&f).into_keys().collect();
    ok(&f.run_retire(&[]));
    let allowed = [
        "_system/",
        "default/app/data/",
        "default/app/manifest/",
        "default/app/counters/",
        "_iceberg/default/app/data/",
        "_iceberg/default/app/metadata/",
    ];
    for k in objects(&f).into_keys().filter(|k| !before.contains(k)) {
        assert!(allowed.iter().any(|a| k.starts_with(a)), "{k}");
        if k.starts_with("_system/") {
            assert!(k == RECORD_REL || is_probe(&k), "{k}");
        }
    }
    for k in wal_files(&f)
        .into_keys()
        .filter(|k| !wal_before.contains(k))
    {
        assert!(k.ends_with(".wal"), "wal.dir 新文件 {k}");
    }
}

#[test]
fn retire_keeps_metadata_and_orphans() {
    let f = Fixture::new();
    ingested(&f, "app", 30, 30);
    let ds = f.dataset("app");
    let some = live(&ds)[0]["path"].as_str().unwrap().to_owned();
    let orphan = format!(
        "{}/orphan-{}.parquet",
        some.rsplit_once('/').unwrap().0,
        std::process::id()
    );
    ds.write(&orphan, &ds.read(&some));
    let kept: Vec<String> = ds
        .keys_under("manifest/")
        .into_iter()
        .chain(ds.keys_under("counters/"))
        .collect();
    ok(&f.run_retire(&[]));
    for k in &kept {
        assert!(ds.exists(k), "{k}");
    }
    assert!(ds.exists(&orphan));
}

// ---------------------------------------------------------------------------
// D · 重放 WAL 并提交
// ---------------------------------------------------------------------------

#[test]
fn retire_replay_commits_and_counters() {
    let f = Fixture::new();
    // 「在下一次 flush 之前被强杀」：只为本场景把实例的 `flush.interval` 设为 `1h`，使记录只在 WAL 中（偏离场景约定的
    // `1s`，只影响测试时机）
    let mut s = f.before(&[FLUSH_1H]);
    let mut q = Seq::default();
    let ts = now_ts(20);
    q.ingest(&s, "app", &ts);
    q.ingest(&s, "fresh", &now_ts(5));
    s.sigkill();
    ok(&f.run_retire(&[]));
    let app = direct_seqs(&f, "app", true);
    let fresh = direct_seqs(&f, "fresh", true);
    assert_eq!((app.len(), fresh.len()), (20, 5));
    assert!(distinct(&app) && distinct(&fresh));
    let hour = common::store::hour_of(ts[0]);
    let counted = common::api::counter_bytes(&f, "app", hour).expect("计数对象存在");
    let obj: Value = serde_json::from_slice(&counted).unwrap();
    // 块场景写的「各窗口 `successful`」即计数对象各窗口的 `durable`（已获 HTTP 200 确认的记录数，
    // REQ-AMB-OBS-INGEST-COUNTERS-V2）；计数对象没有名为 `successful` 的字段
    let durable: u64 = obj["windows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|w| w["durable"].as_u64().unwrap())
        .sum();
    assert_eq!(durable, 20);
    assert_eq!(wal_segments(&f), Vec::<String>::new());
}

#[test]
fn retire_wal_corrupt_exit_3() {
    let f = Fixture::new();
    let mut s = f.before(&[]);
    let mut q = Seq::default();
    for _ in 0..4 {
        q.ingest(&s, "app", &now_ts(5));
    }
    s.sigkill();
    let seg = f.wal_dir().join(&wal_segments(&f)[0]);
    let mut bytes = std::fs::read(&seg).unwrap();
    let mid = bytes.len() / 3;
    for b in &mut bytes[mid..mid + 4] {
        *b = !*b;
    }
    std::fs::write(&seg, &bytes).unwrap();
    let r = f.run_retire(&[]);
    assert_eq!(r.code, Some(3), "{:#?}", r.stderr);
    assert_eq!(r.stdout, Vec::<String>::new());
    assert!(!record_exists(&f));
}

/// 从 REQ-AMB-LIFE-RETIREMENT-REPLAY-WAL-FAIL 推出：删除 WAL 段失败经 `TESTBED_FAULT_WAL` 的 `delete` 注入。
/// 撤除注入后重跑按 RERUN 续做。
#[test]
fn retire_wal_delete_fail_exit_3() {
    let f = Fixture::new();
    let mut s = f.before(&[]);
    let mut q = Seq::default();
    for _ in 0..4 {
        q.ingest(&s, "app", &now_ts(5));
    }
    s.sigkill();
    let control = f.root().join("wal-fault");
    std::fs::write(&control, "delete").unwrap();
    let mut cmd = f.retire(&[]);
    cmd.env("TESTBED_FAULT_WAL", &control);
    let r = common::retire::run(cmd);
    assert_eq!(r.code, Some(3), "{:#?}", r.stderr);
    assert_eq!(r.stdout, Vec::<String>::new());
    assert!(!record_exists(&f));
    assert!(!wal_segments(&f).is_empty());

    std::fs::remove_file(&control).unwrap();
    let r = f.run_retire(&[]);
    assert_eq!(r.code, Some(0), "{:#?}", r.stderr);
    assert!(record_exists(&f));
}

// ---------------------------------------------------------------------------
// E · 补做、补写与全量入湖
// ---------------------------------------------------------------------------

/// 以 `lifecycle.lake_watermark` 为 `1h` 运行的实例的配置（`lifecycle.seal_grace` 取 `1s`，使水位约束成立）。
const LAKE_1H: &[common::Kv] = &[
    ("lifecycle.seal_grace", Some("\"1s\"")),
    ("lifecycle.lake_watermark", Some("\"1h\"")),
];

/// 另加 `retention.period` 为 `2h`、`retention.check_interval` 为 `10s`。
const TTL_2H: &[common::Kv] = &[
    ("lifecycle.seal_grace", Some("\"1s\"")),
    ("lifecycle.lake_watermark", Some("\"1h\"")),
    ("retention.period", Some("\"2h\"")),
    ("retention.check_interval", Some("\"10s\"")),
];

fn label_of(ts: i64) -> String {
    common::store::bucket_label(ts)
}

/// 数据集当前快照中桶 `label` 的桶 manifest。
fn bucket(ds: &Ds, label: &str) -> Option<Value> {
    ds.bucket_now(label)
}

/// 桶 `label` 的第一条交接记录出现于其中的快照编号（入湖交接提交写出的快照）。
fn handover_snapshot(ds: &Ds, label: &str) -> u64 {
    for id in ds.snapshot_ids() {
        let s = ds.snapshot(id);
        let Some(b) = s["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .find(|b| b["bucket"] == label)
        else {
            continue;
        };
        let m = ds.restore(b["manifest"].as_str().unwrap());
        if !m["handovers"].as_array().unwrap().is_empty() {
            return id;
        }
    }
    panic!("桶 {label} 没有交接记录")
}

/// 冷层当前版本中 `lake` 快照的 `amberlog.bucket`，按 `sequence-number` 升序。
fn lake_buckets(f: &Fixture, stream: &str) -> Vec<(i64, String, i64)> {
    let mut out: Vec<(i64, String, i64)> = cold_snapshots(f, stream, "lake")
        .iter()
        .map(|s| {
            (
                s["sequence-number"].as_i64().unwrap(),
                s["summary"]["amberlog.bucket"].as_str().unwrap().to_owned(),
                s["snapshot-id"].as_i64().unwrap(),
            )
        })
        .collect();
    out.sort();
    out
}

/// 以 `extra` 启动带注入控制的 `amberlog serve`（不强求 `ready` 之后的行为）。
fn serve_with(
    f: &Fixture,
    ctl: &Ctl,
    extra: &[common::Kv],
    gates: &[&str],
    pause: Option<&str>,
) -> Server {
    Server::start(ctl.apply(f.before_cmd(extra), gates, pause))
}

/// 以 `extra` 启动实例，摄取 `ts` 后等该桶入湖完成，再停止实例。
fn laked(f: &Fixture, extra: &[common::Kv], q: &mut Seq, ts: &[i64]) {
    let mut s = f.before(extra);
    q.ingest(&s, "app", ts);
    f.dataset("app").wait_handovers(&label_of(ts[0]), 1, WAIT);
    stop(&mut s);
}

#[cfg(feature = "fault-injection")]
#[test]
fn retire_recover_handover_window() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    let mut s = serve_with(&f, &ctl, LAKE_1H, &[], Some("lake.after_cold_commit"));
    let ts = in_hour_ago(3, 30);
    Seq::default().ingest(&s, "app", &ts);
    ctl.kill_at_pause(&mut s, WAIT);
    ok(&f.run_retire(LAKE_1H));
    let b = label_of(ts[0]);
    let laked: Vec<_> = lake_buckets(&f, "app")
        .into_iter()
        .filter(|x| x.1 == b)
        .collect();
    assert_eq!(laked.len(), 1, "{laked:?}");
    let m = bucket(&f.dataset("app"), &b).unwrap();
    let h = m["handovers"].as_array().unwrap();
    assert_eq!(h.len(), 1, "{m}");
    assert_eq!(h[0]["iceberg_snapshot_id"], laked[0].2);
    assert_eq!(direct_seqs(&f, "app", true).len(), 30);
}

#[cfg(feature = "fault-injection")]
#[test]
fn retire_recover_ttl_trace() {
    let f = Fixture::new();
    let mut q = Seq::default();
    laked(&f, LAKE_1H, &mut q, &in_hour_ago(3, 10));
    let ctl = Ctl::new(&f);
    let mut s = serve_with(&f, &ctl, TTL_2H, &[], Some("ttl.after_cold_commit"));
    ctl.kill_at_pause(&mut s, WAIT);
    let ttl = cold_snapshots(&f, "app", "ttl");
    assert_eq!(ttl.len(), 1);
    let rep = ok(&f.run_retire(TTL_2H));
    let ds = f.dataset("app");
    let traced = ds
        .segments(&ds.head()["cold_deletion_segment"])
        .iter()
        .flat_map(|s| s["deletions"].as_array().unwrap().clone())
        .any(|d| d["reason"] == "ttl" && d["cold_removed_in"] == ttl[0]["snapshot-id"]);
    assert!(traced, "{rep}");
}

/// 前一 UTC 日 `h` 时 `m` 分（UTC 微秒）。
fn yesterday_at(h: i64, m: i64) -> i64 {
    use common::api::{HOUR_US, MINUTE_US};
    let day = 24 * HOUR_US;
    now_us().div_euclid(day) * day - day + h * HOUR_US + m * MINUTE_US
}

/// 冷层合并只合并已够格的日分区（该日结束时刻加上入湖水位之后）：此刻在 UTC 日开始后不足 1 小时 5 分钟时，`lake_watermark`
/// 为 `1h` 的前一 UTC 日还不够格。这时返回一个测试专用时钟偏移（秒，`TESTBED_CLOCK_OFFSET`），把进程时钟推到够格之后；偏移
/// 不超过 65 分钟，进程看到的前一 UTC 日不变。其余时刻返回 `None`。
fn eligible_offset_secs() -> Option<i64> {
    use common::api::{HOUR_US, MINUTE_US};
    let since = now_us().rem_euclid(24 * HOUR_US);
    let want = HOUR_US + 5 * MINUTE_US;
    (since < want).then(|| (want - since) / 1_000_000 + 1)
}

#[cfg(feature = "fault-injection")]
#[test]
fn retire_recover_compaction_trace() {
    let f = Fixture::new();
    let mut q = Seq::default();
    // 两个桶各由一个实例入湖：冷层合并的检查间隔取默认值，第一轮（启动后 2 秒）时日分区至多一个文件，不合并
    for ts in [yesterday_at(1, 0), yesterday_at(2, 30)] {
        laked(
            &f,
            LAKE_1H,
            &mut q,
            &(0..10).map(|i| ts + i * 1_000_000).collect::<Vec<_>>(),
        );
    }
    let ctl = Ctl::new(&f);
    let cc = [
        LAKE_1H[0],
        LAKE_1H[1],
        ("cold_compaction.check_interval", Some("\"10s\"")),
    ];
    // 冷层合并的实例与 retire 用同一时钟偏移（有的话），时刻先后与不加偏移时一致
    let offset = eligible_offset_secs();
    let with_offset = |mut cmd: std::process::Command| {
        if let Some(o) = offset {
            cmd.env("TESTBED_CLOCK_OFFSET", o.to_string());
        }
        cmd
    };
    let mut s = Server::start(with_offset(ctl.apply(
        f.before_cmd(&cc),
        &[],
        Some("cold_compaction.after_cold_commit"),
    )));
    ctl.kill_at_pause(&mut s, WAIT);
    let merges = cold_snapshots(&f, "app", "compaction");
    assert_eq!(merges.len(), 1, "{merges:?}");
    let id = merges[0]["snapshot-id"].as_i64().unwrap();
    let ds = f.dataset("app");
    assert!(ds.head()["cold_deletion_segment"].is_null());
    let cold = f.cold("app");
    let outputs: BTreeSet<String> = cold
        .entries(&cold.current(), &merges[0])
        .into_iter()
        .filter(|e| !e.deleted && e.snapshot_id == id)
        .map(|e| e.path)
        .collect();
    assert!(!outputs.is_empty());
    let r = common::retire::run(with_offset(f.retire(LAKE_1H)));
    assert_eq!(r.code, Some(0), "{:#?}", r.stderr);
    let segments = ds.segments(&ds.head()["cold_deletion_segment"]);
    let seg = segments
        .iter()
        .find(|s| {
            s["deletions"]
                .as_array()
                .unwrap()
                .iter()
                .any(|d| d["reason"] == "compaction" && d["cold_removed_in"] == id)
        })
        .unwrap_or_else(|| panic!("没有该次冷层合并的冷层变更段元素：{segments:#?}"));
    let added: BTreeSet<String> = seg["additions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["path"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(added, outputs, "{seg}");
}

/// 运行期间冷层写入失败的退役前实例：向 `app` 摄取此刻（`open`）、此刻前第 30 小时（`sealed`）、此刻前第 210 小时
/// （`pending_lake`）的记录各 20 条，停止实例，冷层写入恢复。返回三个桶。
fn three_states(f: &Fixture, ctl: &Ctl) -> Vec<String> {
    ctl.faults("write _iceberg/ 403");
    let mut s = serve_with(f, ctl, &[], &[], None);
    let mut q = Seq::default();
    let mut labels = Vec::new();
    for ts in [now_ts(20), in_hour_ago(30, 20), in_hour_ago(210, 20)] {
        q.ingest(&s, "app", &ts);
        labels.push(label_of(ts[0]));
    }
    f.dataset("app").wait_records(60, WAIT);
    stop(&mut s);
    ctl.faults("");
    labels.sort();
    labels
}

#[cfg(feature = "fault-injection")]
#[test]
fn retire_lake_all_bucket_states() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    let labels = three_states(&f, &ctl);
    ok(&f.run_retire(&[]));
    let mut laked: Vec<String> = lake_buckets(&f, "app").into_iter().map(|x| x.1).collect();
    laked.sort();
    assert_eq!(laked, labels);
    let seqs = direct_seqs(&f, "app", true);
    assert_eq!(seqs.len(), 60);
    assert!(distinct(&seqs));
    let ds = f.dataset("app");
    assert_eq!(live(&ds), Vec::<Value>::new());
    for l in &labels {
        let m = bucket(&ds, l).unwrap();
        assert!(
            m["deletions"]
                .as_array()
                .unwrap()
                .iter()
                .any(|d| d["reason"] == "lake"),
            "{m}"
        );
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn retire_lake_order() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    ctl.hold("lake.after_cold_commit");
    let mut s = serve_with(&f, &ctl, LAKE_1H, &["lake.after_cold_commit"], None);
    let mut q = Seq::default();
    let b4 = in_hour_ago(4, 10);
    q.ingest(&s, "app", &b4);
    ctl.wait_held("lake.after_cold_commit", &s, WAIT);
    let b5 = in_hour_ago(5, 10);
    let b3 = in_hour_ago(3, 10);
    q.ingest(&s, "app", &b5);
    q.ingest(&s, "app", &b3);
    s.sigkill();
    ok(&f.run_retire(LAKE_1H));
    let ds = f.dataset("app");
    let (h4, h5, h3) = (
        handover_snapshot(&ds, &label_of(b4[0])),
        handover_snapshot(&ds, &label_of(b5[0])),
        handover_snapshot(&ds, &label_of(b3[0])),
    );
    assert!(h4 < h5 && h4 < h3, "{h4} {h5} {h3}");
    let order: Vec<String> = lake_buckets(&f, "app").into_iter().map(|x| x.1).collect();
    let after_b4: Vec<&String> = order
        .iter()
        .skip_while(|b| **b != label_of(b4[0]))
        .skip(1)
        .collect();
    assert_eq!(after_b4, [&label_of(b5[0]), &label_of(b3[0])], "{order:?}");
}

#[cfg(feature = "fault-injection")]
#[test]
fn retire_lake_done() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    three_states(&f, &ctl);
    ctl.hold(BEFORE_DELETE);
    let mut p = retire_with(&f, &ctl, &[], ALL_GATES, None);
    ctl.wait_held(BEFORE_DELETE, &p, WAIT);
    let ds = f.dataset("app");
    for m in ds.bucket_manifests(&ds.head()) {
        assert_eq!(m["files"], json!([]), "{m}");
    }
    // 每个 `lake` 快照的桶都没有 `added_in` 不大于其 `amberlog.hot_snapshot_id` 的存活数据文件（`files` 都空即成立）
    assert_eq!(cold_snapshots(&f, "app", "lake").len(), 3);
    ctl.release(BEFORE_DELETE);
    ok(&finish(&mut p, WAIT));
}

#[cfg(feature = "fault-injection")]
#[test]
fn retire_external_read_matches() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    ctl.faults("write _iceberg/ 403");
    let mut s = serve_with(&f, &ctl, &[], &[], None);
    let mut q = Seq::default();
    for ts in [now_ts(100), in_hour_ago(30, 100), in_hour_ago(210, 100)] {
        q.ingest(&s, "app", &ts);
    }
    f.dataset("app").wait_records(300, WAIT);
    let count = common::api::search(&s, "SELECT count(*) AS n FROM app");
    assert_eq!(count[0]["n"], 300);
    // 块场景的 `WHERE seq % 7 = 0` 是表达式，查询端点不受理（只认列名与字面量）：取 `ORDER BY seq` 的全部结果后在测试里
    // 按 `seq % 7 = 0` 过滤，所比的仍是同一组记录
    let sevens: Vec<i64> = common::api::search(&s, "SELECT seq FROM app ORDER BY seq")
        .iter()
        .map(|h| h["seq"].as_i64().unwrap())
        .filter(|x| x % 7 == 0)
        .collect();
    q.ingest(&s, "app", &now_ts(40));
    s.sigkill();
    ctl.faults("");
    ok(&f.run_retire(&[]));
    let seqs = direct_seqs(&f, "app", true);
    assert_eq!(seqs.len(), 340);
    let got: Vec<i64> = sorted(
        seqs.iter()
            .copied()
            .filter(|x| x % 7 == 0 && *x < 300)
            .collect(),
    );
    assert_eq!(got, sevens);
    assert!(distinct(&seqs));
}

// ---------------------------------------------------------------------------
// F · 冷层提示与物理删除
// ---------------------------------------------------------------------------

#[cfg(feature = "fault-injection")]
#[test]
fn retire_cold_hint_fixed() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    let mut s = serve_with(&f, &ctl, LAKE_1H, &[], None);
    let mut q = Seq::default();
    let first = in_hour_ago(5, 10);
    q.ingest(&s, "app", &first);
    f.dataset("app")
        .wait_handovers(&label_of(first[0]), 1, WAIT);
    ctl.faults("write _iceberg/default/app/metadata/version-hint.text 403");
    let second = in_hour_ago(3, 10);
    q.ingest(&s, "app", &second);
    f.dataset("app")
        .wait_handovers(&label_of(second[0]), 1, WAIT);
    stop(&mut s);
    let cold = f.cold("app");
    let n = cold.version().unwrap();
    assert_eq!(
        String::from_utf8(cold.read("metadata/version-hint.text"))
            .unwrap()
            .trim(),
        (n - 1).to_string()
    );
    ctl.faults("");
    ok(&f.run_retire(LAKE_1H));
    let n = cold.version().unwrap();
    assert_eq!(
        String::from_utf8(cold.read("metadata/version-hint.text"))
            .unwrap()
            .trim(),
        n.to_string()
    );
    assert_eq!(
        direct_seqs(&f, "app", true).len(),
        direct_seqs(&f, "app", false).len()
    );
}

#[test]
fn retire_deletes_pending_files() {
    let f = Fixture::new();
    let mut q = Seq::default();
    let mut s = f.before(LAKE_1H);
    let (a, b) = (in_hour_ago(5, 10), in_hour_ago(3, 10));
    q.ingest(&s, "app", &a);
    q.ingest(&s, "app", &b);
    let ds = f.dataset("app");
    ds.wait_handovers(&label_of(a[0]), 1, WAIT);
    ds.wait_handovers(&label_of(b[0]), 1, WAIT);
    stop(&mut s);
    // TTL 把第 5 小时的冷层文件移出冷层存活文件集并写出冷层变更段（storage.delete_delay 为 1h，文件仍在）
    let mut s = f.before(TTL_2H);
    let deadline = Instant::now() + Duration::from_secs(300);
    while cold_deleted(&ds).is_empty() {
        assert!(Instant::now() < deadline, "TTL 未写出冷层变更段");
        std::thread::sleep(Duration::from_millis(500));
    }
    stop(&mut s);
    let cold = f.cold("app");
    let hot: Vec<String> = ledger_paths(&ds, "lake")
        .into_iter()
        .filter(|p| ds.exists(p))
        .collect();
    let colds: Vec<String> = cold_deleted(&ds)
        .into_iter()
        .filter(|p| cold.exists(p))
        .collect();
    assert!(!hot.is_empty() && !colds.is_empty(), "{hot:?} {colds:?}");
    let rep = ok(&f.run_retire(TTL_2H));
    assert!(ledger_paths(&ds, "lake").iter().all(|p| !ds.exists(p)));
    assert!(cold_deleted(&ds).iter().all(|p| !cold.exists(p)));
    assert_eq!(
        ds_of(&rep, "default/app")["files_deleted"],
        (hot.len() + colds.len()) as u64
    );
}

#[test]
fn retire_waits_delete_delay() {
    let f = Fixture::new();
    ingested(&f, "app", 30, 30);
    let ds = f.dataset("app");
    let mut p = Server::spawn(f.retire(&[("storage.delete_delay", Some("\"20s\""))]));
    // 等入湖交接提交写出，再在其后 15 秒内看入湖输入都仍存在
    let deadline = Instant::now() + WAIT;
    let inputs = loop {
        let lake = ledger_paths(&ds, "lake");
        if !lake.is_empty() {
            break lake;
        }
        assert!(
            Instant::now() < deadline,
            "未见入湖交接提交：{:#?}",
            p.stderr_lines()
        );
        std::thread::sleep(Duration::from_millis(100));
    };
    let deleted_at = ds.ledger().iter().find(|d| d["reason"] == "lake").unwrap()["deleted_at"]
        .as_i64()
        .unwrap();
    while now_us() < deleted_at + 15_000_000 {
        assert!(inputs.iter().all(|i| ds.exists(i)), "15 秒内入湖输入被删");
        std::thread::sleep(Duration::from_millis(500));
    }
    let r = finish(&mut p, WAIT);
    let t1 = now_us();
    ok(&r);
    for d in ds.ledger().iter().filter(|d| d["reason"] == "lake") {
        assert!(d["deleted_at"].as_i64().unwrap() + 20_000_000 <= t1, "{d}");
    }
}

// ---------------------------------------------------------------------------
// G · 例外
// ---------------------------------------------------------------------------

#[test]
fn retire_disabled_dataset_exception() {
    let f = Fixture::new();
    let mut s = f.before(&[]);
    let mut q = Seq::default();
    q.ingest(&s, "old", &now_ts(10));
    q.ingest(&s, "app", &now_ts(20));
    stop(&mut s);
    // 早期版本写出的第 1 版格式：当前快照的 `format_version` 为 1
    let old = f.dataset("old");
    let n = old.current().unwrap();
    let mut snap = old.snapshot(n);
    snap["format_version"] = json!(1);
    old.write_json(&format!("manifest/{n:020}.json"), &snap);
    let data: Vec<String> = old.keys_under("data/");
    let r = f.run_retire(&[]);
    assert_eq!(r.code, Some(5), "{:#?}", r.stderr);
    let rep = r.report();
    assert_eq!(rep["status"], "exceptions");
    assert_eq!(
        rep["exceptions"],
        json!([{"dataset": "default/old", "kind": "dataset_disabled", "bucket": null, "paths": []}])
    );
    assert_eq!(direct_seqs(&f, "app", true).len(), 20);
    assert!(!record_exists(&f));
    assert!(data.iter().all(|k| old.exists(k)));
}

/// 退役前的实例已向 `app` 摄取落在此刻前第 40 小时（桶 B1）与第 30 小时（桶 B2）的记录各 20 条，随后停止实例；改写 B1 的
/// 一个存活数据文件 F 为另一个合法的 Parquet 文件。返回（B1，B2，F 的路径，F 的原字节，B2 的入湖输入）。
fn tampered(f: &Fixture) -> (String, String, String, Vec<u8>, Vec<String>) {
    let mut s = f.before(&[]);
    let mut q = Seq::default();
    let (b1, b2) = (in_hour_ago(40, 20), in_hour_ago(30, 20));
    q.ingest(&s, "app", &b1);
    q.ingest(&s, "app", &b2);
    stop(&mut s);
    let ds = f.dataset("app");
    let (l1, l2) = (label_of(b1[0]), label_of(b2[0]));
    let m1 = bucket(&ds, &l1).unwrap();
    let path = m1["files"][0]["path"].as_str().unwrap().to_owned();
    let original = ds.read(&path);
    common::lake::alter_parquet_rows(&ds.path(&path));
    let inputs: Vec<String> = bucket(&ds, &l2).unwrap()["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["path"].as_str().unwrap().to_owned())
        .collect();
    (l1, l2, path, original, inputs)
}

#[test]
fn retire_digest_mismatch_exception() {
    let f = Fixture::new();
    let (l1, l2, path, _, inputs) = tampered(&f);
    let r = f.run_retire(&[]);
    assert_eq!(r.code, Some(5), "{:#?}", r.stderr);
    let rep = r.report();
    assert_eq!(rep["status"], "exceptions");
    assert_eq!(
        rep["exceptions"],
        json!([{"dataset": "default/app", "kind": "digest_mismatch", "bucket": l1, "paths": [path]}])
    );
    let ev = r.events("digest_mismatch");
    assert_eq!(ev.len(), 1, "{ev:?}");
    assert_eq!(ev[0]["path"], path);
    assert_eq!(ev[0]["tier"], "hot");
    let laked: Vec<String> = lake_buckets(&f, "app").into_iter().map(|x| x.1).collect();
    assert!(laked.contains(&l2), "{laked:?}");
    let ds = f.dataset("app");
    assert!(
        bucket(&ds, &l1).unwrap()["files"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["path"] == path)
    );
    assert!(inputs.iter().all(|i| !ds.exists(i)));
    assert_eq!(
        ds_of(&rep, "default/app")["files_deleted"],
        inputs.len() as u64
    );
    assert!(!record_exists(&f));
}

#[test]
fn retire_input_missing_exception() {
    let f = Fixture::new();
    ingested(&f, "app", 30, 20);
    let ds = f.dataset("app");
    let l = live(&ds);
    let path = l[0]["path"].as_str().unwrap().to_owned();
    let label = label_of(in_hour_ago(30, 1)[0]);
    ds.remove(&path);
    let r = f.run_retire(&[]);
    assert_eq!(r.code, Some(5), "{:#?}", r.stderr);
    assert_eq!(
        r.report()["exceptions"],
        json!([{"dataset": "default/app", "kind": "input_missing", "bucket": label, "paths": [path]}])
    );
    assert_eq!(r.events("digest_mismatch"), Vec::<Value>::new());
}

#[test]
fn retire_rerun_after_repair() {
    let f = Fixture::new();
    let (_, _, path, original, _) = tampered(&f);
    assert_eq!(f.run_retire(&[]).code, Some(5));
    f.dataset("app").write(&path, &original);
    let rep = ok(&f.run_retire(&[]));
    assert_eq!(rep["status"], "retired");
    assert_eq!(direct_seqs(&f, "app", true).len(), 40);
}

#[cfg(feature = "fault-injection")]
#[test]
fn retire_store_unavailable() {
    let f = Fixture::new();
    ingested(&f, "app", 30, 30);
    let ctl = Ctl::new(&f);
    ctl.hold(AFTER_REPLAY);
    let mut p = retire_with(&f, &ctl, &[], ALL_GATES, None);
    ctl.wait_held(AFTER_REPLAY, &p, WAIT);
    ctl.faults("* * 503");
    ctl.release(AFTER_REPLAY);
    let r = finish(&mut p, WAIT);
    assert_eq!(r.code, Some(6), "{:#?}", r.stderr);
    let rep = r.report();
    assert_eq!(
        (&rep["status"], &rep["interrupted"]),
        (&json!("incomplete"), &json!(false))
    );
    assert!(
        rep["exceptions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["dataset"] == "default/app" && e["kind"] == "store_unavailable"),
        "{rep}"
    );
    ctl.faults("");
    ok(&finish(&mut retire_with(&f, &ctl, &[], &[], None), WAIT));
    assert_eq!(direct_seqs(&f, "app", true).len(), 30);
}

#[cfg(feature = "fault-injection")]
#[test]
fn retire_delete_failed() {
    let f = Fixture::new();
    ingested(&f, "app", 30, 30);
    let inputs: Vec<String> = live(&f.dataset("app"))
        .iter()
        .map(|e| e["path"].as_str().unwrap().to_owned())
        .collect();
    let ctl = Ctl::new(&f);
    ctl.faults("delete * 403");
    let r = finish(&mut retire_with(&f, &ctl, &[], &[], None), WAIT);
    assert_eq!(r.code, Some(6), "{:#?}", r.stderr);
    let rep = r.report();
    assert_eq!(rep["status"], "incomplete");
    let ex = rep["exceptions"].as_array().unwrap();
    assert_eq!(ex.len(), 1, "{rep}");
    assert_eq!(ex[0]["kind"], "delete_failed");
    assert!(
        ex[0]["paths"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| inputs.iter().any(|i| p == i)),
        "{rep}"
    );
    assert!(!record_exists(&f));
}

#[cfg(feature = "fault-injection")]
#[test]
fn retire_unsettled() {
    let f = Fixture::new();
    let mut q = Seq::default();
    laked(&f, LAKE_1H, &mut q, &in_hour_ago(3, 10));
    let ctl = Ctl::new(&f);
    let mut s = serve_with(&f, &ctl, TTL_2H, &[], Some("ttl.after_cold_commit"));
    ctl.kill_at_pause(&mut s, WAIT);
    // 从那次 TTL 冷层提交的快照摘要中删去 `amberlog.ttl_cutoff`（其余不变）
    let cold = f.cold("app");
    let n = cold.version().unwrap();
    let rel = format!("metadata/v{n}.metadata.json");
    let mut meta: Value = serde_json::from_slice(&cold.read(&rel)).unwrap();
    for s in meta["snapshots"].as_array_mut().unwrap() {
        if s["summary"]["amberlog.operation"] == "ttl" {
            s["summary"]
                .as_object_mut()
                .unwrap()
                .remove("amberlog.ttl_cutoff");
        }
    }
    std::fs::write(cold.path(&rel), serde_json::to_vec(&meta).unwrap()).unwrap();
    let r = f.run_retire(TTL_2H);
    assert_eq!(r.code, Some(5), "{:#?}", r.stderr);
    let rep = r.report();
    assert_eq!(rep["status"], "exceptions");
    let ex = rep["exceptions"].as_array().unwrap();
    assert_eq!(ex.len(), 1, "{rep}");
    assert_eq!(
        (&ex[0]["dataset"], &ex[0]["kind"]),
        (&json!("default/app"), &json!("unsettled"))
    );
    assert!(!record_exists(&f));
}

// ---------------------------------------------------------------------------
// H · 报告、退出码与信号
// ---------------------------------------------------------------------------

#[test]
fn retire_report_datasets() {
    let f = Fixture::new();
    two_buckets(&f);
    let inputs = live(&f.dataset("app")).len() as u64;
    let rep = ok(&f.run_retire(&[]));
    let datasets = rep["datasets"].as_array().unwrap();
    assert_eq!(datasets.len(), 1);
    let d = &datasets[0];
    assert_eq!(
        keys(d),
        set(&[
            "dataset",
            "settled",
            "snapshot_id",
            "cold_metadata_version",
            "buckets_laked",
            "files_deleted"
        ])
    );
    let rec = record(&f).unwrap();
    assert_eq!(d["dataset"], "default/app");
    assert_eq!(d["settled"], true);
    assert_eq!(
        d["snapshot_id"],
        rec["datasets"][0]["snapshot"]["snapshot_id"]
    );
    assert_eq!(
        d["cold_metadata_version"],
        rec["datasets"][0]["cold"]["metadata_version"]
    );
    assert_eq!(d["buckets_laked"], 2);
    assert_eq!(d["files_deleted"], inputs);
}

#[test]
fn retire_report_exceptions_sorted() {
    let f = Fixture::new();
    let mut s = f.before(&[]);
    let mut q = Seq::default();
    let (w1, w2, a) = (
        in_hour_ago(40, 10),
        in_hour_ago(30, 10),
        in_hour_ago(30, 10),
    );
    q.ingest(&s, "web", &w1);
    q.ingest(&s, "web", &w2);
    q.ingest(&s, "app", &a);
    stop(&mut s);
    for (stream, ts) in [("web", &w2), ("web", &w1), ("app", &a)] {
        let ds = f.dataset(stream);
        let m = bucket(&ds, &label_of(ts[0])).unwrap();
        common::lake::alter_parquet_rows(&ds.path(m["files"][0]["path"].as_str().unwrap()));
    }
    let r = f.run_retire(&[]);
    assert_eq!(r.code, Some(5), "{:#?}", r.stderr);
    let rep = r.report();
    let ex = rep["exceptions"].as_array().unwrap();
    let order: Vec<(&str, &str)> = ex
        .iter()
        .map(|e| {
            (
                e["dataset"].as_str().unwrap(),
                e["bucket"].as_str().unwrap(),
            )
        })
        .collect();
    let (la, l1, l2) = (label_of(a[0]), label_of(w1[0]), label_of(w2[0]));
    assert_eq!(
        order,
        [
            ("default/app", la.as_str()),
            ("default/web", l1.as_str()),
            ("default/web", l2.as_str())
        ]
    );
    for e in ex {
        assert_eq!(e["kind"], "digest_mismatch");
        assert_eq!(keys(e), set(&["dataset", "kind", "bucket", "paths"]));
    }
}

#[test]
fn retire_startup_failure_no_report() {
    let f = Fixture::new();
    let r = f.run_retire(&[("wal.dir", None)]);
    assert_eq!(r.code, Some(2), "{:#?}", r.stderr);
    assert_eq!(r.stdout, Vec::<String>::new());
}

#[test]
fn retire_exit_5() {
    let f = Fixture::new();
    tampered(&f);
    assert_eq!(f.run_retire(&[]).code, Some(5));
}

#[cfg(feature = "fault-injection")]
#[test]
fn retire_sigterm_incomplete() {
    let f = Fixture::new();
    two_buckets(&f);
    let ctl = Ctl::new(&f);
    ctl.hold(AFTER_BUCKET);
    let mut p = retire_with(&f, &ctl, &[], ALL_GATES, None);
    ctl.wait_held(AFTER_BUCKET, &p, WAIT);
    p.signal("TERM");
    std::thread::sleep(Duration::from_millis(200));
    ctl.release(AFTER_BUCKET);
    let r = finish(&mut p, WAIT);
    assert_eq!(r.code, Some(6), "{:#?}", r.stderr);
    let rep = r.report();
    assert_eq!(
        (&rep["status"], &rep["interrupted"]),
        (&json!("incomplete"), &json!(true))
    );
    assert_eq!(cold_snapshots(&f, "app", "lake").len(), 1);
    assert!(!record_exists(&f));
    ok(&f.run_retire(&[]));
    assert_eq!(direct_seqs(&f, "app", true).len(), 40);
}

#[test]
fn retire_sigterm_during_wait() {
    let f = Fixture::new();
    ingested(&f, "app", 30, 30);
    let ds = f.dataset("app");
    let mut p = Server::spawn(f.retire(&[("storage.delete_delay", Some("\"1h\""))]));
    let deadline = Instant::now() + WAIT;
    let inputs = loop {
        let lake = ledger_paths(&ds, "lake");
        if !lake.is_empty() {
            break lake;
        }
        assert!(
            Instant::now() < deadline,
            "未见入湖交接提交：{:#?}",
            p.stderr_lines()
        );
        std::thread::sleep(Duration::from_millis(100));
    };
    std::thread::sleep(Duration::from_secs(10));
    p.signal("TERM");
    let r = finish(&mut p, Duration::from_secs(10));
    assert_eq!(r.code, Some(6), "{:#?}", r.stderr);
    assert_eq!(r.report()["interrupted"], true);
    assert!(inputs.iter().all(|i| ds.exists(i)));
    assert!(!record_exists(&f));
}

#[cfg(feature = "fault-injection")]
#[test]
fn retire_second_signal() {
    let f = Fixture::new();
    two_buckets(&f);
    let ctl = Ctl::new(&f);
    ctl.hold(AFTER_BUCKET);
    let mut p = retire_with(&f, &ctl, &[], ALL_GATES, None);
    ctl.wait_held(AFTER_BUCKET, &p, WAIT);
    p.signal("TERM");
    std::thread::sleep(Duration::from_secs(1));
    p.signal("TERM");
    let code = p.wait_exit(Duration::from_secs(5));
    assert!(matches!(code, Some(Some(c)) if c != 0), "{code:?}");
}

// ---------------------------------------------------------------------------
// I · 重跑即续做
// ---------------------------------------------------------------------------

/// 运行 `retire`、在崩溃点 `step` 强杀，再次运行 `retire` 并返回第二次的报告（断言以 0 退出）。
fn kill_then_rerun(f: &Fixture, step: &str) -> Value {
    let ctl = Ctl::new(f);
    let mut p = retire_with(f, &ctl, &[], &[], Some(step));
    ctl.kill_at_pause(&mut p, WAIT);
    ok(&f.run_retire(&[]))
}

#[cfg(feature = "fault-injection")]
#[test]
fn retire_rerun_after_replay_kill() {
    let f = Fixture::new();
    committed_then_killed(&f);
    kill_then_rerun(&f, AFTER_REPLAY);
    let seqs = direct_seqs(&f, "app", true);
    assert_eq!(seqs.len(), 50);
    assert!(distinct(&seqs));
}

#[cfg(feature = "fault-injection")]
#[test]
fn retire_rerun_after_bucket_kill() {
    let f = Fixture::new();
    two_buckets(&f);
    let rep = kill_then_rerun(&f, AFTER_BUCKET);
    assert_eq!(ds_of(&rep, "default/app")["buckets_laked"], 1);
    let seqs = direct_seqs(&f, "app", true);
    assert_eq!(seqs.len(), 40);
    assert!(distinct(&seqs));
    assert_eq!(cold_snapshots(&f, "app", "lake").len(), 2);
}

#[cfg(feature = "fault-injection")]
#[test]
fn retire_rerun_after_cold_commit_kill() {
    let f = Fixture::new();
    ingested(&f, "app", 30, 20);
    kill_then_rerun(&f, "lake.after_cold_commit");
    let b = label_of(in_hour_ago(30, 1)[0]);
    assert_eq!(
        lake_buckets(&f, "app").iter().filter(|x| x.1 == b).count(),
        1
    );
    assert_eq!(direct_seqs(&f, "app", true).len(), 20);
}

#[cfg(feature = "fault-injection")]
#[test]
fn retire_rerun_before_delete_kill() {
    let f = Fixture::new();
    ingested(&f, "app", 30, 30);
    let rep = kill_then_rerun(&f, BEFORE_DELETE);
    let d = ds_of(&rep, "default/app");
    assert_eq!(d["buckets_laked"], 0);
    assert!(d["files_deleted"].as_u64().unwrap() > 0, "{rep}");
    let ds = f.dataset("app");
    assert!(ledger_paths(&ds, "lake").iter().all(|p| !ds.exists(p)));
}

#[cfg(feature = "fault-injection")]
#[test]
fn retire_rerun_before_record_kill() {
    let f = Fixture::new();
    ingested(&f, "app", 30, 30);
    let rep = kill_then_rerun(&f, BEFORE_RECORD);
    assert_eq!(rep["status"], "retired");
    let d = ds_of(&rep, "default/app");
    assert_eq!(
        (&d["buckets_laked"], &d["files_deleted"]),
        (&json!(0), &json!(0))
    );
    assert!(record_exists(&f));
    assert_eq!(direct_seqs(&f, "app", true).len(), 30);
}

// ---------------------------------------------------------------------------
// J · `serve` 与已退役的存储前缀
// ---------------------------------------------------------------------------

/// 以与该实例相同的配置（监听地址为测试占住的回环端口）运行 `amberlog serve` 直到退出，断言它没有试过监听。
fn serve_refused(f: &Fixture, extra: &[common::Kv]) -> Ran {
    let held = HeldPort::new();
    let addr = held.addr();
    let mut kv: Vec<common::Kv> = extra.to_vec();
    kv.push(("server.http_addr", Some(addr.as_str())));
    let mut p = Server::spawn(f.before_cmd(&kv));
    let r = finish(&mut p, Duration::from_secs(30));
    held.assert_never_listened(r.code, &p);
    r
}

#[test]
fn retire_serve_refuses() {
    let f = Fixture::new();
    ok(&retired_two(&f).1);
    let before = files(&f.store_dir().join("amberlog/default"));
    let r = serve_refused(&f, &[]);
    assert_eq!(r.code, Some(2), "{:#?}", r.stderr);
    assert!(
        r.stderr.iter().any(|l| l.contains(RECORD)),
        "{:#?}",
        r.stderr
    );
    assert!(
        before == files(&f.store_dir().join("amberlog/default")),
        "对象有变动"
    );
}

#[test]
fn retire_serve_refuses_unparsable_record() {
    let f = Fixture::new();
    let mut s = f.before(&[]);
    Seq::default().ingest(&s, "app", &now_ts(20));
    s.sigkill();
    std::fs::create_dir_all(f.store_dir().join("amberlog/_system")).unwrap();
    std::fs::write(f.store_dir().join(RECORD), b"{").unwrap();
    let wal = wal_files(&f);
    let r = serve_refused(&f, &[]);
    assert_eq!(r.code, Some(2), "{:#?}", r.stderr);
    assert!(
        r.stderr.iter().any(|l| l.contains(RECORD)),
        "{:#?}",
        r.stderr
    );
    assert!(wal == wal_files(&f), "WAL 段有变动");
}

#[test]
fn retire_revive_after_record_deleted() {
    let f = Fixture::new();
    ok(&retired_two(&f).1);
    let r1 = record(&f).unwrap()["retired_at"].as_i64().unwrap();
    std::fs::remove_file(f.store_dir().join(RECORD)).unwrap();
    let mut s = f.before(&[]);
    assert_eq!(s.wait_readyz(200, Duration::from_secs(10)).status, 200);
    Seq(1000).ingest(&s, "app", &now_ts(10));
    stop(&mut s);
    assert!(
        !s.stderr_lines()
            .iter()
            .any(|l| l.contains("retirement.json"))
    );
    let rep = ok(&f.run_retire(&[("retention.period", Some("\"720h\""))]));
    assert_eq!(rep["status"], "retired");
    assert!(record(&f).unwrap()["retired_at"].as_i64().unwrap() > r1);
    assert_eq!(direct_seqs(&f, "app", true).len(), 60);
}

#[test]
fn retire_catalog_and_evidence_unaffected() {
    let f = Fixture::new();
    ok(&retired_two(&f).1);
    let c = Server::start(f.catalog(&[]));
    assert_eq!(c.wait_readyz(200, Duration::from_secs(60)).status, 200);
    let table = c.get_auth("/iceberg/v1/amberlog/namespaces/default/tables/app");
    assert_eq!(table.status, 200, "{}", table.text());
    export_same_config(&f, "e.tar");
}

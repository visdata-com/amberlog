//! REQ-AMB-STO-HOT-FSCK-V4 的系统测试：以子进程运行 `amberlog`，
//! 对象存储用 `file://` 临时目录，测试直接读写目录下的对象来构造外部删改与检查留痕。
//!
//! 崩溃残余用 `fault-injection` 特性下的暂停点制造：`commit.after_data_files`（块所称崩溃点
//! `commit.after_data_upload`：上传数据文件之后、写出桶 manifest 之前）、`commit.after_bucket_manifests`（块所称
//! `commit.after_bucket_manifest`：上传桶 manifest 之后、创建快照之前）、`hot_fsck.after_tombstone`（留痕所在快照创建
//! 成功之后、物理删除之前）。重启用新的空 `wal.dir`，崩溃前的记录不再重放。
//!
//! `hot_fsck_referenced_objects_kept` 是长时场景：环境变量 `TESTBED_SOAK_HOURS` 存在时跑满
//! 10 分钟，缺省跑 2 分钟的缩短版。

mod common;

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use common::api::{ingest_ok, now_us};
use common::store::{
    Ds, FLUSH_1S, HOUR, bucket_label, commits_total, hour_of, kill_at_marker, metric, sha256,
    snapshot_rel, stop, three_ingests, wait_snapshots_gone,
};
use common::{BEARER, Fixture, Kv};
use serde_json::{Value, json};

const INTERVAL_5S: Kv = ("hot_fsck.interval", Some("\"5s\""));
const MIN_AGE_1S: Kv = ("hot_fsck.min_orphan_age", Some("\"1s\""));

/// 一个新的空 `wal.dir` 的配置取值（TOML 字符串）。
fn fresh_wal(f: &Fixture, name: &str) -> String {
    format!("\"{}\"", f.root().join(name).display())
}

/// 先正常提交一条记录（当前快照为 N），再在暂停点 `step` 处崩溃；返回崩溃留下的新数据文件路径。
fn crash_at(f: &Fixture, config: &[Kv], step: &str) -> Vec<String> {
    let mut all = vec![FLUSH_1S];
    all.extend_from_slice(config);
    let mut s = f.start(&all);
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
    let ds = f.dataset("app");
    ds.wait_records(1, Duration::from_secs(15));
    stop(&mut s);
    let before: BTreeSet<String> = ds.keys_under("data/").into_iter().collect();

    let wal = fresh_wal(f, "wal-crash");
    all.push(("wal.dir", Some(wal.as_str())));
    let (mut s, marker) = f.start_paused(&all, step);
    ingest_ok(&s, "app", r#"[{"a":2}]"#);
    kill_at_marker(&mut s, &marker);
    ds.keys_under("data/")
        .into_iter()
        .filter(|k| !before.contains(k))
        .collect()
}

/// 快照 `s` 经 `buckets` 可达的全部 `deletions` 条目。
fn deletions(ds: &Ds, s: &Value) -> Vec<Value> {
    ds.bucket_manifests(s)
        .into_iter()
        .flat_map(|m| m["deletions"].as_array().unwrap().clone())
        .collect()
}

/// `/metrics` 中 `amberlog_hot_fsck_deleted_total` 的全部样本行。
fn fsck_deleted_lines(s: &common::Server) -> Vec<String> {
    s.get_auth("/metrics")
        .text()
        .lines()
        .filter(|l| l.starts_with("amberlog_hot_fsck_deleted_total{"))
        .map(str::to_owned)
        .collect()
}

/// 与数据文件 `path` 同一小时目录、编号只差末位字符的路径（仍符合数据文件路径模板）。
fn sibling(path: &str) -> String {
    let (dir, name) = path.rsplit_once('/').unwrap();
    let id = name.strip_suffix(".parquet").unwrap();
    let last = if id.ends_with('0') { '1' } else { '0' };
    format!("{dir}/{}{last}.parquet", &id[..id.len() - 1])
}

#[test]
fn hot_fsck_orphan_data_file_tombstoned() {
    let f = Fixture::new();
    let orphans = crash_at(&f, &[INTERVAL_5S, MIN_AGE_1S], "commit.after_data_files");
    assert_eq!(orphans.len(), 1, "{orphans:?}");
    let ds = f.dataset("app");
    let file = &orphans[0];
    let digest = sha256(&ds.read(file));
    let n = ds.current().unwrap();

    let wal = fresh_wal(&f, "wal-2");
    let _s = f.start(&[
        FLUSH_1S,
        INTERVAL_5S,
        MIN_AGE_1S,
        ("wal.dir", Some(wal.as_str())),
    ]);
    std::thread::sleep(Duration::from_secs(15));
    assert!(!ds.exists(file), "{file} 仍存在");
    let head = ds.head();
    let entry = deletions(&ds, &head)
        .into_iter()
        .find(|d| d["path"] == file.as_str())
        .unwrap_or_else(|| panic!("没有 {file} 的留痕"));
    assert_eq!(entry["reason"], "orphan");
    assert_eq!(entry["content_digest"], digest.as_str());
    let removed_in = ds.snapshot(entry["removed_in"].as_u64().unwrap());
    assert_eq!(entry["deleted_at"], removed_in["created_at"]);
    // 巡检只写留痕提交：链只前进了这一次（不写修复或回滚快照）
    assert_eq!(ds.current(), Some(n + 1));
    assert_eq!(entry["removed_in"].as_u64(), Some(n + 1));
}

#[test]
fn hot_fsck_orphan_bucket_manifest_deleted() {
    let f = Fixture::new();
    let ds = f.dataset("app");
    crash_at(
        &f,
        &[INTERVAL_5S, MIN_AGE_1S],
        "commit.after_bucket_manifests",
    );
    let referenced: BTreeSet<String> = ds
        .snapshot_ids()
        .into_iter()
        .flat_map(|id| {
            ds.snapshot(id)["buckets"]
                .as_array()
                .unwrap()
                .iter()
                .map(|b| b["manifest"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        })
        .collect();
    let orphans: Vec<String> = ds
        .keys_under("manifest/buckets/")
        .into_iter()
        .filter(|k| !referenced.contains(k))
        .collect();
    assert_eq!(orphans.len(), 1, "{orphans:?}");
    let m = &orphans[0];

    let wal = fresh_wal(&f, "wal-2");
    let _s = f.start(&[
        FLUSH_1S,
        INTERVAL_5S,
        MIN_AGE_1S,
        ("wal.dir", Some(wal.as_str())),
    ]);
    std::thread::sleep(Duration::from_secs(15));
    assert!(!ds.exists(m), "{m} 仍存在");
    assert!(
        !deletions(&ds, &ds.head())
            .iter()
            .any(|d| d["path"] == m.as_str()),
        "桶 manifest 不应记入 deletions"
    );
}

#[test]
fn hot_fsck_tombstoned_file_deleted_after_restart() {
    let f = Fixture::new();
    let config = [INTERVAL_5S, MIN_AGE_1S];
    let orphans = crash_at(&f, &config, "commit.after_data_files");
    assert_eq!(orphans.len(), 1);
    let file = &orphans[0];
    let ds = f.dataset("app");

    let wal2 = fresh_wal(&f, "wal-2");
    let (mut s, marker) = f.start_paused(
        &[
            FLUSH_1S,
            INTERVAL_5S,
            MIN_AGE_1S,
            ("wal.dir", Some(wal2.as_str())),
        ],
        "hot_fsck.after_tombstone",
    );
    kill_at_marker(&mut s, &marker);
    assert!(ds.exists(file), "留痕之后、删除之前 F 应仍在");
    // 第二次崩溃确在留痕所在快照创建成功之后：当前快照已有 F 的 orphan 留痕
    let tombstones: Vec<Value> = deletions(&ds, &ds.head())
        .into_iter()
        .filter(|d| d["path"] == file.as_str())
        .collect();
    assert_eq!(tombstones.len(), 1, "{tombstones:?}");
    assert_eq!(tombstones[0]["reason"], "orphan");

    let wal3 = fresh_wal(&f, "wal-3");
    let s = f.start(&[
        FLUSH_1S,
        INTERVAL_5S,
        MIN_AGE_1S,
        ("wal.dir", Some(wal3.as_str())),
    ]);
    std::thread::sleep(Duration::from_secs(15));
    assert!(!ds.exists(file), "{file} 仍存在");
    let n = deletions(&ds, &ds.head())
        .iter()
        .filter(|d| d["path"] == file.as_str())
        .count();
    assert_eq!(n, 1);
    assert_eq!(
        metric(
            &s,
            "amberlog_hot_fsck_deleted_total{org=\"default\",stream=\"app\",kind=\"data_file\"}"
        ),
        Some(1.0)
    );
}

#[test]
fn hot_fsck_min_age_respected() {
    let f = Fixture::new();
    let config = [INTERVAL_5S, ("hot_fsck.min_orphan_age", Some("\"1h\""))];
    let orphans = crash_at(&f, &config, "commit.after_data_files");
    assert_eq!(orphans.len(), 1);
    let file = &orphans[0];
    let ds = f.dataset("app");

    // 对照：同一小时目录下另放一个上传完成时刻在 2 小时前的孤儿 G，它满足安全时长，巡检运行过就会被删除
    let control = sibling(file);
    ds.write(&control, &ds.read(file));
    std::fs::File::options()
        .write(true)
        .open(ds.path(&control))
        .unwrap()
        .set_modified(std::time::SystemTime::now() - Duration::from_secs(2 * 3600))
        .unwrap();

    let wal = fresh_wal(&f, "wal-2");
    let mut all = vec![FLUSH_1S, ("wal.dir", Some(wal.as_str()))];
    all.extend_from_slice(&config);
    let _s = f.start(&all);
    std::thread::sleep(Duration::from_secs(15));
    assert!(ds.exists(file));
    let entries = deletions(&ds, &ds.head());
    assert!(!entries.iter().any(|d| d["path"] == file.as_str()));
    assert!(
        !ds.exists(&control),
        "对照孤儿 {control} 未被删除：巡检没有运行"
    );
    assert!(
        entries
            .iter()
            .any(|d| d["path"] == control.as_str() && d["reason"] == "orphan"),
        "{entries:?}"
    );
}

/// 孤儿数据文件 F 与被删除的当前快照所引用的小时桶 H 的桶 manifest；返回 F、H 的起点与当前快照号。
fn orphan_and_missing_manifest(f: &Fixture) -> (String, i64, u64) {
    let orphans = crash_at(f, &[INTERVAL_5S, MIN_AGE_1S], "commit.after_data_files");
    assert_eq!(orphans.len(), 1);
    let ds = f.dataset("app");
    let head = ds.head();
    let bucket = &head["buckets"][0];
    ds.remove(bucket["manifest"].as_str().unwrap());
    let start = bucket["time_min"].as_i64().unwrap().div_euclid(HOUR) * HOUR;
    (orphans[0].clone(), start, ds.current().unwrap())
}

/// 以 `lifecycle.seal_grace` 为 `24h` 向 `app` 的小时桶 `start` 各提交一次、共写出 2 个小文件后 SIGTERM 停止
/// （以默认 `seal_grace` 重启后该桶已封口、满足合并条件）。
fn two_small_files(f: &Fixture, start: i64) {
    let ds = f.dataset("app");
    let n0 = ds.head()["summary"]["record_count"].as_u64().unwrap();
    let mut s = f.start(&[("lifecycle.seal_grace", Some("\"24h\"")), FLUSH_1S]);
    for i in 0..2 {
        ingest_ok(
            &s,
            "app",
            &format!(r#"[{{"_timestamp":{},"s":{i}}}]"#, start + i),
        );
        ds.wait_records(n0 + i as u64 + 1, Duration::from_secs(15));
    }
    stop(&mut s);
}

#[test]
fn hot_fsck_abandons_on_missing_bucket_manifest() {
    let f = Fixture::new();
    let (file, _, current) = orphan_and_missing_manifest(&f);
    let ds = f.dataset("app");
    let manifest_keys = ds.keys_under("manifest/");
    let wal = fresh_wal(&f, "wal-2");
    let s = f.start(&[
        FLUSH_1S,
        INTERVAL_5S,
        MIN_AGE_1S,
        ("wal.dir", Some(wal.as_str())),
    ]);
    std::thread::sleep(Duration::from_secs(15));
    assert!(ds.exists(&file));
    assert_eq!(ds.current(), Some(current));
    // 不修复：没有为缺失的桶 manifest 写出新的快照或桶 manifest
    assert_eq!(ds.keys_under("manifest/"), manifest_keys);
    assert!(
        !s.json_lines()
            .iter()
            .any(|v| v["event"] == "dataset_disabled" && v["dataset"] == "default/app"),
        "{:#?}",
        s.stderr_lines()
    );
}

/// 构造与上一场景相同（孤儿 F、桶 H 的桶 manifest 被删），查询使数据集停用之后把桶 H 的桶 manifest 原样放回：
/// 此后读链已能读全，巡检若不跳过停用的数据集就会为 F 发起留痕提交（停用下提交失败，计入 `failed`）。另备一个
/// 已封口、含 2 个小文件的桶 S，合并若不跳过停用的数据集同样会发起提交。
#[test]
fn hot_fsck_skips_disabled_dataset() {
    let f = Fixture::new();
    let orphans = crash_at(&f, &[INTERVAL_5S, MIN_AGE_1S], "commit.after_data_files");
    assert_eq!(orphans.len(), 1);
    let file = orphans[0].clone();
    let ds = f.dataset("app");
    let h_label = ds.head()["buckets"][0]["bucket"]
        .as_str()
        .unwrap()
        .to_owned();
    let s_start = hour_of(now_us()) - 3 * HOUR;
    let s_label = bucket_label(s_start);
    two_small_files(&f, s_start);

    let head = ds.head();
    let h = head["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|b| b["bucket"] == h_label.as_str())
        .unwrap()
        .clone();
    let h_manifest = h["manifest"].as_str().unwrap().to_owned();
    let h_bytes = ds.read(&h_manifest);
    ds.remove(&h_manifest);
    let start = h["time_min"].as_i64().unwrap().div_euclid(HOUR) * HOUR;
    let current = ds.current().unwrap();

    let wal = fresh_wal(&f, "wal-2");
    let s = f.start(&[
        FLUSH_1S,
        INTERVAL_5S,
        MIN_AGE_1S,
        ("wal.dir", Some(wal.as_str())),
    ]);
    let body = json!({"query": {"sql": "SELECT * FROM app", "start_time": start, "end_time": start + HOUR}})
        .to_string();
    let r = s.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        body.as_bytes(),
    );
    assert_eq!(r.status, 500, "{}", r.text());
    ds.write(&h_manifest, &h_bytes);
    std::thread::sleep(Duration::from_secs(15));
    assert!(ds.exists(&file));
    assert_eq!(ds.current(), Some(current));
    // 停用期间巡检与合并都没有发起提交、没有删除
    assert_eq!(commits_total(&s, "app", "failed"), 0);
    assert_eq!(fsck_deleted_lines(&s), Vec::<String>::new());
    let m = ds
        .bucket_manifest(&ds.head(), &s_label)
        .expect("桶 S 在快照中");
    assert_eq!(m["files"].as_array().unwrap().len(), 2, "{m}");
    assert_eq!(
        metric(
            &s,
            "amberlog_compactions_total{org=\"default\",stream=\"app\",result=\"ok\"}"
        ),
        None
    );
}

/// 在途提交已上传的对象不被删除也不留痕：数据提交在上传数据文件与桶 manifest 之后、写快照之前停在
/// 可放行暂停点 `commit.after_bucket_manifests`，停留期间经过两次以上巡检、上传已超过安全时长；放行后再经两次巡检。
/// 没有在途保护时，停留期间的巡检会把桶 manifest 当孤儿记下，等提交完成后删掉已被引用的它。
#[test]
fn hot_fsck_inflight_objects_kept() {
    let f = Fixture::new();
    let (s, gate) = f.start_gated(
        &[FLUSH_1S, INTERVAL_5S, MIN_AGE_1S],
        "commit.after_bucket_manifests",
    );
    let ds = f.dataset("app");
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
    gate.wait_held(&s, Duration::from_secs(15));
    let current = ds.current();
    let data = ds.keys_under("data/");
    let manifests = ds.keys_under("manifest/buckets/");
    assert_eq!(data.len(), 1, "{data:?}");
    assert_eq!(manifests.len(), 1, "{manifests:?}");

    std::thread::sleep(Duration::from_secs(12));
    for k in data.iter().chain(&manifests) {
        assert!(ds.exists(k), "在途对象 {k} 被删除");
    }
    assert_eq!(ds.current(), current, "提交停住期间链头前进了");

    gate.release();
    ds.wait_records(1, Duration::from_secs(15));
    std::thread::sleep(Duration::from_secs(12));
    let head = ds.head();
    let referenced: Vec<String> = head["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["manifest"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(referenced, manifests);
    let files: Vec<String> = ds
        .file_entries(&head)
        .iter()
        .map(|e| e["path"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(files, data);
    for k in data.iter().chain(&manifests) {
        assert!(ds.exists(k), "{k} 被删除");
    }
    assert_eq!(deletions(&ds, &head), Vec::<Value>::new());
    for l in fsck_deleted_lines(&s) {
        assert!(l.ends_with(" 0"), "{l}");
    }
}

#[test]
fn hot_fsck_non_template_objects_kept() {
    let f = Fixture::new();
    let config = [FLUSH_1S, INTERVAL_5S, MIN_AGE_1S];
    let mut s = f.start(&config);
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
    let ds = f.dataset("app");
    ds.wait_records(1, Duration::from_secs(15));
    stop(&mut s);
    ds.write("data/2026/08/22/10/not-an-id.parquet", b"x");
    ds.write("manifest/buckets/junk.json", b"{}");
    let mut kept: Vec<String> = ds.snapshot_ids().into_iter().map(snapshot_rel).collect();
    kept.push("manifest/_current".into());
    kept.extend(ds.keys_under("counters/"));
    kept.push("data/2026/08/22/10/not-an-id.parquet".into());
    kept.push("manifest/buckets/junk.json".into());

    let _s = f.start(&config);
    std::thread::sleep(Duration::from_secs(15));
    for k in &kept {
        assert!(ds.exists(k), "{k} 被删除");
    }
}

#[test]
fn hot_fsck_referenced_objects_kept() {
    let minutes = if std::env::var_os("TESTBED_SOAK_HOURS").is_some() {
        10
    } else {
        2
    };
    let f = Fixture::new();
    let s = f.start(&[("hot_fsck.interval", Some("\"10s\"")), MIN_AGE_1S, FLUSH_1S]);
    let deadline = Instant::now() + Duration::from_secs(minutes * 60);
    let mut seq = 0u64;
    while Instant::now() < deadline {
        ingest_ok(&s, "app", &format!(r#"[{{"seq":{seq}}}]"#));
        seq += 1;
        std::thread::sleep(Duration::from_millis(100));
    }
    let ds = f.dataset("app");
    let head = ds.head();
    for b in head["buckets"].as_array().unwrap() {
        assert!(ds.exists(b["manifest"].as_str().unwrap()));
    }
    for e in ds.file_entries(&head) {
        let p = e["path"].as_str().unwrap();
        assert!(ds.exists(p), "{p} 被删除");
    }
    let text = s.get_auth("/metrics").text();
    for l in text
        .lines()
        .filter(|l| l.starts_with("amberlog_hot_fsck_deleted_total{"))
    {
        assert!(l.ends_with(" 0"), "{l}");
    }
}

const SEAL_10M: Kv = ("lifecycle.seal_grace", Some("\"10m\""));
const LAKE_1H: Kv = ("lifecycle.lake_watermark", Some("\"1h\""));

/// 留痕归档场景的配置：`seal_grace` 10m、`lake_watermark` 1h、巡检周期 5 秒、安全时长 1 秒。
#[cfg(feature = "fault-injection")]
fn retire_config() -> Vec<Kv<'static>> {
    vec![FLUSH_1S, SEAL_10M, LAKE_1H, INTERVAL_5S, MIN_AGE_1S]
}

/// 留痕归档段的前提由真实的入湖与归档写出：入湖后以时钟偏移 2 小时重启（`fault-injection` 下的
/// `TESTBED_CLOCK_OFFSET`），使已入湖的桶变空满 1 小时而被归档（[`Fixture::retire_bucket`]）。
#[cfg(feature = "fault-injection")]
#[test]
fn hot_fsck_abandons_on_missing_retired_segment() {
    use common::lake::RETIRE_OFFSET_S;
    let f = Fixture::new();
    let config = retire_config();
    let (mut s, r) = f.retire_bucket(&config, 10);
    stop(&mut s);
    // 孤儿数据文件 F：上传数据文件之后、写出桶 manifest 之前崩溃
    let ds = f.dataset("app");
    let before: BTreeSet<String> = ds.keys_under("data/").into_iter().collect();
    let wal = fresh_wal(&f, "wal-crash");
    let mut crash: Vec<Kv> = config.clone();
    crash.push(("wal.dir", Some(wal.as_str())));
    let marker = f.root().join("fault-marker");
    let mut s = f.start_offset(
        &crash,
        RETIRE_OFFSET_S,
        &[
            ("TESTBED_FAULT_PAUSE", "commit.after_data_files".to_owned()),
            ("TESTBED_FAULT_MARKER", marker.display().to_string()),
        ],
    );
    ingest_ok(
        &s,
        "app",
        &format!(r#"[{{"_timestamp":{},"a":2}}]"#, now_us()),
    );
    kill_at_marker(&mut s, &marker);
    let orphans: Vec<String> = ds
        .keys_under("data/")
        .into_iter()
        .filter(|k| !before.contains(k))
        .collect();
    assert_eq!(orphans.len(), 1);
    let current = ds.current().unwrap();
    ds.remove(r.segment["path"].as_str().unwrap());

    let wal = fresh_wal(&f, "wal-2");
    let mut restart: Vec<Kv> = config.clone();
    restart.push(("wal.dir", Some(wal.as_str())));
    let s = f.start_offset(&restart, RETIRE_OFFSET_S, &[]);
    std::thread::sleep(Duration::from_secs(15));
    assert!(ds.exists(&orphans[0]));
    assert_eq!(ds.current(), Some(current));
    assert!(
        !s.json_lines()
            .iter()
            .any(|v| v["event"] == "dataset_disabled" && v["dataset"] == "default/app"),
        "{:#?}",
        s.stderr_lines()
    );
}

#[cfg(feature = "fault-injection")]
#[test]
fn hot_fsck_retired_bucket_manifest_kept() {
    let f = Fixture::new();
    let (s, r) = f.retire_bucket(&retire_config(), 10);
    std::thread::sleep(Duration::from_secs(15));
    assert!(
        f.dataset("app").exists(&r.manifest),
        "{} 被删除",
        r.manifest
    );
    let v = metric(
        &s,
        "amberlog_hot_fsck_deleted_total{org=\"default\",stream=\"app\",kind=\"bucket_manifest\"}",
    );
    assert!(v.is_none_or(|v| v == 0.0), "{v:?}");
}

/// 以 `seal_grace` 10m、`lake_watermark` 1h（`storage.delete_delay` 默认 1h）向此刻前第 3 小时的桶 B 写 10 条，等到交接
/// 完成后 SIGKILL。返回 B 的标签与入湖前的数据文件（记有 `lake` 留痕、仍存在）。
fn lake_then_kill(f: &Fixture) -> (String, Vec<String>) {
    let b = hour_of(now_us()) - 3 * HOUR;
    let label = bucket_label(b);
    let mut s = f.start(&[FLUSH_1S, SEAL_10M, LAKE_1H]);
    ingest_ok(&s, "app", &format!(r#"[{{"_timestamp":{},"a":1}}]"#, b + 1));
    let ds = f.dataset("app");
    let m = ds.wait_handovers(&label, 1, Duration::from_secs(150));
    s.sigkill();
    let files: Vec<String> = m["deletions"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|d| d["reason"] == "lake")
        .map(|d| d["path"].as_str().unwrap().to_owned())
        .collect();
    assert!(!files.is_empty(), "{m}");
    for p in &files {
        assert!(ds.exists(p), "{p} 已被删除");
    }
    (label, files)
}

#[test]
fn hot_fsck_lake_tombstoned_file_redeleted() {
    let f = Fixture::new();
    let (_, files) = lake_then_kill(&f);
    let wal = fresh_wal(&f, "wal-2");
    let _s = f.start(&[
        FLUSH_1S,
        SEAL_10M,
        LAKE_1H,
        ("storage.delete_delay", Some("\"1s\"")),
        INTERVAL_5S,
        MIN_AGE_1S,
        ("wal.dir", Some(wal.as_str())),
    ]);
    std::thread::sleep(Duration::from_secs(15));
    let ds = f.dataset("app");
    let all = deletions(&ds, &ds.head());
    for p in &files {
        assert!(!ds.exists(p), "{p} 仍存在");
        let hits: Vec<&Value> = all.iter().filter(|d| d["path"] == p.as_str()).collect();
        assert_eq!(hits.len(), 1, "{all:?}");
        assert_eq!(hits[0]["reason"], "lake");
    }
}

/// 以 `seal_grace` 1s 向此刻前第 2 小时的桶分三次写 30 条，等到合并留痕后 SIGKILL（`storage.delete_delay` 默认 1h）。
/// 返回合并输入的路径（记有 `compaction` 留痕、仍存在）。
fn compact_then_kill(f: &Fixture) -> Vec<String> {
    let b = hour_of(now_us()) - 2 * HOUR;
    let mut s = f.start(&[FLUSH_1S, ("lifecycle.seal_grace", Some("\"1s\""))]);
    let ds = f.dataset("app");
    three_ingests(&s, &ds, b);
    let m = ds.wait_compaction(&bucket_label(b), Duration::from_secs(120));
    s.sigkill();
    let files: Vec<String> = m["deletions"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|d| d["reason"] == "compaction")
        .map(|d| d["path"].as_str().unwrap().to_owned())
        .collect();
    for p in &files {
        assert!(ds.exists(p), "{p} 已被删除");
    }
    files
}

#[test]
fn hot_fsck_compaction_tombstoned_file_redeleted() {
    let f = Fixture::new();
    let files = compact_then_kill(&f);
    let wal = fresh_wal(&f, "wal-2");
    let _s = f.start(&[
        FLUSH_1S,
        ("storage.delete_delay", Some("\"1s\"")),
        INTERVAL_5S,
        MIN_AGE_1S,
        ("wal.dir", Some(wal.as_str())),
    ]);
    std::thread::sleep(Duration::from_secs(15));
    let ds = f.dataset("app");
    let all = deletions(&ds, &ds.head());
    for p in &files {
        assert!(!ds.exists(p), "{p} 仍存在");
        let hits: Vec<&Value> = all.iter().filter(|d| d["path"] == p.as_str()).collect();
        assert_eq!(hits.len(), 1, "{all:?}");
        assert_eq!(hits[0]["reason"], "compaction");
    }
}

#[test]
fn hot_fsck_delete_delay_respected() {
    let f = Fixture::new();
    let (_, files) = lake_then_kill(&f);
    let wal = fresh_wal(&f, "wal-2");
    let _s = f.start(&[
        FLUSH_1S,
        SEAL_10M,
        LAKE_1H,
        INTERVAL_5S,
        MIN_AGE_1S,
        ("wal.dir", Some(wal.as_str())),
    ]);
    std::thread::sleep(Duration::from_secs(15));
    let ds = f.dataset("app");
    for p in &files {
        assert!(ds.exists(p), "{p} 被删除");
    }
}

// ---------------------------------------------------------------------------
// REQ-AMB-STO-HOT-FSCK-V3：可达快照与断档即放弃（与快照回收同周期，先回收后巡检）
// ---------------------------------------------------------------------------

const INTERVAL_1H: Kv = ("hot_fsck.interval", Some("\"1h\""));
const KEEP_2: Kv = ("hot_retention.snapshots", Some("2"));
const KEEP_10: Kv = ("hot_retention.snapshots", Some("10"));
const AGE_1S: Kv = ("hot_retention.age", Some("\"1s\""));
const AGE_1H: Kv = ("hot_retention.age", Some("\"1h\""));

fn with<'a>(base: &[Kv<'a>], more: &[Kv<'a>]) -> Vec<Kv<'a>> {
    base.iter().chain(more).copied().collect()
}

/// 当前小时桶的一条记录。
fn h_record(ts: i64, seq: u64) -> String {
    format!(r#"[{{"_timestamp":{ts},"seq":{seq}}}]"#)
}

/// 以 `config` 与原 `wal.dir` 启动一个停在 `commit.after_data_files` 的实例，摄取 1 条落在 `ts` 所在小时桶的记录，在该次
/// 提交上传数据文件之后、写出桶 manifest 之前 SIGKILL；返回上传了的数据文件 F。
fn crash_one(f: &Fixture, config: &[Kv], ts: i64) -> String {
    let ds = f.dataset("app");
    let before = ds.keys_under("data/");
    let (mut s, marker) = f.start_paused(config, "commit.after_data_files");
    ingest_ok(&s, "app", &h_record(ts, 99));
    kill_at_marker(&mut s, &marker);
    let crashed: Vec<String> = ds
        .keys_under("data/")
        .into_iter()
        .filter(|k| !before.contains(k))
        .collect();
    assert_eq!(crashed.len(), 1, "{crashed:?}");
    crashed[0].clone()
}

/// 当前快照中 `label` 桶的桶 manifest 的 `deletions` 是否含 `path` 为 `file`、`reason` 为 `orphan` 的条目。
fn orphan_tombstoned(ds: &Ds, label: &str, file: &str) -> bool {
    ds.bucket_manifest(&ds.head(), label).is_some_and(|m| {
        m["deletions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d["path"] == file && d["reason"] == "orphan")
    })
}

/// 块写「已分 6 次摄取，写出快照 1 至 6」：按创世快照与 5 次摄取写出；「快照 1、2 所引用的 H 的桶
/// manifest」取引用 H 的头两个快照（2、3）所引用的。
#[test]
fn hot_fsck_abandons_on_gap_above_retain_start() {
    let f = Fixture::new();
    let ds = f.dataset("app");
    let ts = now_us();
    let mut s = f.start(&[INTERVAL_1H, FLUSH_1S]);
    let mut manifests = Vec::new();
    for i in 0..5 {
        ingest_ok(&s, "app", &h_record(ts + i, i as u64));
        ds.wait_records(i as u64 + 1, Duration::from_secs(15));
        if manifests.len() < 2 {
            manifests.push(
                ds.head()["buckets"][0]["manifest"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
            );
        }
        std::thread::sleep(Duration::from_millis(1500));
    }
    stop(&mut s);
    assert_eq!(ds.snapshot_ids(), (1..=6).collect::<Vec<_>>());
    let file = crash_one(&f, &[INTERVAL_1H, FLUSH_1S], ts);
    ds.remove(&snapshot_rel(4));

    let wal = fresh_wal(&f, "wal-2");
    let s = f.start(&[
        KEEP_10,
        AGE_1H,
        INTERVAL_5S,
        MIN_AGE_1S,
        ("wal.dir", Some(wal.as_str())),
    ]);
    std::thread::sleep(Duration::from_secs(15));
    assert!(ds.exists(&file));
    for m in &manifests {
        assert!(ds.exists(m), "{m}");
    }
    assert_eq!(fsck_deleted_lines(&s), Vec::<String>::new());
    assert!(
        !s.json_lines()
            .iter()
            .any(|v| v["event"] == "dataset_disabled" && v["dataset"] == "default/app"),
        "{:#?}",
        s.stderr_lines()
    );
}

/// 块写「同一进程在该次提交的崩溃点被 SIGKILL」：先以 SIGTERM 停止，再以停在崩溃点的实例（同一 `wal.dir`）摄取，
/// 对象存储状态相同。
#[test]
fn hot_fsck_raised_retention_bottom_not_abandoned() {
    let f = Fixture::new();
    let ds = f.dataset("app");
    let ts = now_us();
    let label = bucket_label(ts);
    let config = [KEEP_2, AGE_1S, INTERVAL_5S, MIN_AGE_1S, FLUSH_1S];
    let mut s = f.start(&config);
    for i in 0..6 {
        ingest_ok(&s, "app", &h_record(ts + i, i as u64));
        ds.wait_records(i as u64 + 1, Duration::from_secs(15));
        std::thread::sleep(Duration::from_millis(1500));
    }
    wait_snapshots_gone(&ds, 1..=4, Duration::from_secs(60));
    stop(&mut s);
    let file = crash_one(&f, &config, ts);

    let wal = fresh_wal(&f, "wal-2");
    let _s = f.start(&[
        KEEP_10,
        AGE_1H,
        INTERVAL_5S,
        MIN_AGE_1S,
        ("wal.dir", Some(wal.as_str())),
    ]);
    std::thread::sleep(Duration::from_secs(15));
    assert!(!ds.exists(&file));
    assert!(orphan_tombstoned(&ds, &label, &file));
}

#[test]
fn hot_fsck_abandon_warn_log() {
    let f = Fixture::new();
    let (file, _, _) = orphan_and_missing_manifest(&f);
    let ds = f.dataset("app");
    let wal = fresh_wal(&f, "wal-2");
    let s = f.start(&[
        FLUSH_1S,
        INTERVAL_5S,
        MIN_AGE_1S,
        ("wal.dir", Some(wal.as_str())),
    ]);
    std::thread::sleep(Duration::from_secs(15));
    assert!(
        s.stderr_lines().iter().any(|l| {
            serde_json::from_str::<Value>(l).is_ok_and(|v| v["level"] == "warn")
                && l.contains("default")
                && l.contains("app")
        }),
        "{:#?}",
        s.stderr_lines()
    );
    assert!(ds.exists(&file));
}

/// 分 `n` 次各向新 stream `app` 摄取 1 条落在 `ts` 所在小时桶 H 的记录，每次收到 HTTP 200 之后等到当前快照中 H 的
/// `record_count` 等于已摄取的条数再发下一次；返回每次之后当前快照中 H 的头片路径（P1 至 Pn）。
fn ingest_heads(s: &common::Server, ds: &Ds, ts: i64, n: u64) -> Vec<String> {
    let label = bucket_label(ts);
    let mut heads = Vec::new();
    for i in 0..n {
        ingest_ok(s, "app", &h_record(ts + i as i64, i));
        let deadline = Instant::now() + Duration::from_secs(30);
        let head = loop {
            let h = ds.current().map(|_| ds.head()).and_then(|head| {
                head["buckets"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|b| b["bucket"] == label.as_str() && b["record_count"] == i + 1)
                    .map(|b| b["manifest"].as_str().unwrap().to_owned())
            });
            if let Some(h) = h {
                break h;
            }
            assert!(
                Instant::now() < deadline,
                "第 {} 次摄取 30 秒内未提交",
                i + 1
            );
            std::thread::sleep(Duration::from_millis(50));
        };
        heads.push(head);
    }
    heads
}

#[test]
fn hot_fsck_gc_orphaned_bucket_manifests_deleted() {
    let f = Fixture::new();
    let ds = f.dataset("app");
    let ts = now_us();
    let s = f.start(&[KEEP_2, AGE_1S, INTERVAL_5S, MIN_AGE_1S, FLUSH_1S]);
    let heads = ingest_heads(&s, &ds, ts, 70);
    // 第 65 份是全量片，第 66 至 70 份是以它为基准片的增量片
    assert_eq!(ds.json(&heads[64])["kind"], "full");
    std::thread::sleep(Duration::from_secs(20));
    for (i, m) in heads.iter().enumerate() {
        if i >= 64 {
            assert!(ds.exists(m), "P{} {m} 应存在", i + 1);
        } else {
            assert!(!ds.exists(m), "P{} {m} 应已被删除", i + 1);
        }
    }
    let deleted = metric(
        &s,
        "amberlog_hot_fsck_deleted_total{org=\"default\",stream=\"app\",kind=\"bucket_manifest\"}",
    )
    .unwrap_or(0.0);
    assert!(deleted >= 64.0, "{deleted}");
}

/// 块写「快照 1 及其所引用的 H 的桶 manifest」：创世快照 1 不引用任何桶，取引用 H 的第一个快照（2）及其头片。
#[test]
fn hot_fsck_snapshots_below_gap_not_reachable() {
    let f = Fixture::new();
    let ds = f.dataset("app");
    let ts = now_us();
    let label = bucket_label(ts);
    let config = [KEEP_2, AGE_1S, INTERVAL_5S, MIN_AGE_1S, FLUSH_1S];
    let mut s = f.start(&config);
    let heads = ingest_heads(&s, &ds, ts, 1);
    let first = ds.current().unwrap();
    let saved = ds.read(&snapshot_rel(first));
    let first_manifest = heads[0].clone();
    ingest_heads_from(&s, &ds, ts, 1, 70);
    let deadline = Instant::now() + Duration::from_secs(180);
    while ds.exists(&snapshot_rel(first)) || ds.exists(&first_manifest) {
        assert!(Instant::now() < deadline, "快照 {first} 与其头片未被删除");
        std::thread::sleep(Duration::from_millis(100));
    }
    stop(&mut s);
    let file = crash_one(&f, &config, ts);
    ds.write(&snapshot_rel(first), &saved);

    let wal = fresh_wal(&f, "wal-2");
    let (_s, faults) =
        f.start_with_store_faults(&with(&config, &[("wal.dir", Some(wal.as_str()))]));
    faults.set(&format!("delete default/app/{} 403\n", snapshot_rel(first)));
    std::thread::sleep(Duration::from_secs(15));
    assert!(!ds.exists(&file));
    assert!(orphan_tombstoned(&ds, &label, &file));
    assert!(ds.exists(&snapshot_rel(first)));
}

/// 同 [`ingest_heads`]，自第 `from + 1` 次起到第 `to` 次。
fn ingest_heads_from(s: &common::Server, ds: &Ds, ts: i64, from: u64, to: u64) {
    let label = bucket_label(ts);
    for i in from..to {
        ingest_ok(s, "app", &h_record(ts + i as i64, i));
        let deadline = Instant::now() + Duration::from_secs(30);
        while !ds.current().is_some_and(|_| {
            ds.head()["buckets"]
                .as_array()
                .unwrap()
                .iter()
                .any(|b| b["bucket"] == label.as_str() && b["record_count"] == i + 1)
        }) {
            assert!(
                Instant::now() < deadline,
                "第 {} 次摄取 30 秒内未提交",
                i + 1
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

#[test]
fn hot_fsck_abandons_on_missing_chain_piece() {
    let f = Fixture::new();
    let ds = f.dataset("app");
    let ts = now_us();
    let label = bucket_label(ts);
    let config = [INTERVAL_5S, MIN_AGE_1S, FLUSH_1S];
    let mut s = f.start(&config);
    ingest_heads(&s, &ds, ts, 2);
    stop(&mut s);
    let file = crash_one(&f, &config, ts);
    let current = ds.current().unwrap();
    let head = ds.json(
        ds.head()["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .find(|b| b["bucket"] == label.as_str())
            .unwrap()["manifest"]
            .as_str()
            .unwrap(),
    );
    assert_eq!(head["kind"], "delta", "{head}");
    ds.remove(head["previous"]["path"].as_str().unwrap());

    let wal = fresh_wal(&f, "wal-2");
    let s = f.start(&with(&config, &[("wal.dir", Some(wal.as_str()))]));
    std::thread::sleep(Duration::from_secs(15));
    assert!(ds.exists(&file));
    assert_eq!(ds.current(), Some(current));
    for l in fsck_deleted_lines(&s) {
        if l.contains("stream=\"app\"") {
            assert!(l.ends_with(" 0"), "{l}");
        }
    }
}

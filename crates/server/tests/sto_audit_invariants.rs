//! REQ-AMB-STO-AUDIT-INVARIANTS-V4 的系统测试：以子进程运行
//! `amberlog`，对象存储用 `file://` 临时目录，测试直接读写目录下的对象来检查或构造状态。离线查验（篡改可检出、无误报）
//! 在 amberlog-storage 的集成测试里直接调用。
//!
//! 除特意构造的以外，各测试只写此刻起 3 小时以内的小时桶：默认 `lifecycle.lake_watermark`（`72h`）下它们在测试期间不入湖。
//!
//! `audit_no_deletion_outside_listed_deleters` 是长时场景：平时的测试运行跑缩短版，跑满时长的长时运行另行执行。
//! 时长取环境变量 `TESTBED_SOAK_HOURS`（小时，可为小数；跑满时长时设为 24），缺省跑 2 分钟的缩短版。
//! 设了 `TESTBED_SOAK_HOURS` 时按默认配置运行、`_timestamp` 取当前时刻，桶随真实时间推进；缩短版另把巡检周期设为
//! 10 秒、安全时长设为 1 秒，并让每批一半记录落在此刻前 3 小时（已封口）的桶里，使合并与巡检都在 2 分钟内运行；`flush.interval` 为 1 秒、24 个桶轮流写入的形态
//! 由「加速运行」场景覆盖——早期版本的桶不封口，每次提交都整份重写受影响桶的 manifest，那种形态持续 24 小时，
//! 桶 manifest 的总量随运行时长平方增长（1.5 小时即约 32 GB），任何测试机的磁盘都撑不住。

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use common::api::{FLUSH_1H, ingest_ok, now_us, search};
use common::lake::ColdDir;
use common::store::{
    Ds, FLUSH_1S, H10, HOUR, bucket_label, hour_of, rewrite_parquet_uncompressed, sha256,
    snapshot_rel, three_ingests,
};
use common::{BEARER, Fixture, Kv, Server, wait_file};
use serde_json::{Value, json};

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

fn is_digest(v: &Value) -> bool {
    v.as_str()
        .and_then(|s| s.strip_prefix("sha256:"))
        .is_some_and(|h| {
            h.len() == 64
                && h.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
}

/// 依次摄取各批，每批之后等到提交（当前快照含至今全部记录）。
fn ingest_and_commit(server: &Server, ds: &Ds, stream: &str, batches: &[String]) {
    let mut total = 0;
    for b in batches {
        let r = ingest_ok(server, stream, b);
        total += r["status"][0]["successful"]
            .as_u64()
            .expect("successful 计数");
        ds.wait_records(total, WAIT);
    }
}

fn at(ts: i64, k: &str) -> String {
    format!(r#"[{{"_timestamp":{ts},"k":"{k}"}}]"#)
}

// ---------------------------------------------------------------------------
// single-chain
// ---------------------------------------------------------------------------

#[test]
fn audit_chain_linear() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1S]);
    let ds = fx.dataset("app");
    ingest_and_commit(
        &server,
        &ds,
        "app",
        &[r#"[{"k":"a"}]"#.into(), r#"[{"k":"b"}]"#.into()],
    );
    let chain = ds.chain();
    let ids: Vec<u64> = chain
        .iter()
        .map(|s| s["snapshot_id"].as_u64().unwrap())
        .collect();
    assert_eq!(ids, [3, 2, 1]);
    assert_eq!(chain[2]["parent_snapshot_id"], Value::Null);
}

#[test]
fn audit_parent_digest() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1S]);
    let ds = fx.dataset("app");
    ingest_and_commit(
        &server,
        &ds,
        "app",
        &[r#"[{"k":"a"}]"#.into(), r#"[{"k":"b"}]"#.into()],
    );
    assert_eq!(ds.current(), Some(3));
    let s1 = ds.read(&snapshot_rel(1));
    let s2 = ds.read(&snapshot_rel(2));
    assert_eq!(ds.snapshot(2)["parent_digest"], sha256(&s1).as_str());
    assert_eq!(ds.snapshot(3)["parent_digest"], sha256(&s2).as_str());
    let genesis = ds.snapshot(1);
    assert!(
        genesis.as_object().unwrap().contains_key("parent_digest"),
        "{genesis}"
    );
    assert_eq!(genesis["parent_digest"], Value::Null);
}

#[test]
fn audit_current_only_advances() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1S]);
    let ds = fx.dataset("ca");
    let addr = server.addr;
    let writer = std::thread::spawn(move || {
        for i in 0..30 {
            let r = common::http(
                addr,
                "POST",
                "/api/default/ca/_json",
                &[("Authorization", BEARER)],
                format!(r#"[{{"i":{i}}}]"#).as_bytes(),
            );
            assert_eq!(r.status, 200, "{}", r.text());
            std::thread::sleep(Duration::from_secs(1));
        }
    });
    let mut seen = Vec::new();
    while !writer.is_finished() {
        // 每一轮先读提示、再按找链尾确定当前快照
        let hint = ds.hint();
        if let Some(id) = ds.current() {
            assert!(
                hint.is_none_or(|h| h <= id),
                "提示 {hint:?} 越过了当前快照 {id}"
            );
            seen.push(id);
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    writer.join().unwrap();
    assert!(seen.windows(2).all(|w| w[0] <= w[1]), "{seen:?}");
    assert!(seen.last().copied().unwrap_or(0) > 2, "{seen:?}");
    for k in seen.iter().collect::<BTreeSet<_>>() {
        if *k > 1 {
            assert_eq!(ds.snapshot(*k)["parent_snapshot_id"], json!(k - 1), "{k}");
        }
    }
}

// ---------------------------------------------------------------------------
// digests
// ---------------------------------------------------------------------------

#[test]
fn audit_digest_fixed_at_write() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1S]);
    let ds = fx.dataset("app");
    ingest_and_commit(
        &server,
        &ds,
        "app",
        &[r#"[{"k":"a"}]"#.into(), r#"[{"k":"b"}]"#.into()],
    );
    let head = ds.head();
    let entries = ds.file_entries(&head);
    assert!(!entries.is_empty());
    // 自创世快照向后，文件首次出现的快照
    let mut chain = ds.chain();
    chain.reverse();
    for f in entries {
        let path = f["path"].as_str().unwrap();
        assert_eq!(f["content_digest"], sha256(&ds.read(path)), "{path}");
        let first = chain
            .iter()
            .find(|s| ds.file_entries(s).iter().any(|e| e["path"] == path))
            .unwrap();
        assert_eq!(f["added_in"], first["snapshot_id"], "{path}");
    }
}

#[test]
fn audit_bucket_digest_format() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1S]);
    let h = hour_of(now_us());
    ingest_ok(
        &server,
        "bd",
        &format!(
            r#"[{{"_timestamp":{}}},{{"_timestamp":{}}}]"#,
            h - 2 * HOUR + 1,
            h - HOUR + 1
        ),
    );
    let ds = fx.dataset("bd");
    let s = ds.wait_records(2, WAIT);
    let buckets = s["buckets"].as_array().unwrap();
    assert_eq!(buckets.len(), 2);
    for b in buckets {
        assert!(is_digest(&b["content_digest"]), "{b}");
        let rel = b["manifest"].as_str().unwrap();
        assert_eq!(b["content_digest"], sha256(&ds.read(rel)), "{rel}");
        for f in ds.restore(rel)["files"].as_array().unwrap() {
            assert!(is_digest(&f["content_digest"]), "{f}");
        }
    }
}

#[test]
fn audit_file_entry_digest_stable() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1S]);
    let ds = fx.dataset("fs");
    // 此刻前第 3 小时的桶：已封口（会被合并）、未达入湖水位
    let b = hour_of(now_us()) - 3 * HOUR;
    let batches: Vec<String> = (0..4)
        .map(|i| at(b + i * 60_000_000, &i.to_string()))
        .collect();
    ingest_and_commit(&server, &ds, "fs", &batches);
    let versions = ds.keys_under(&format!("manifest/buckets/{}-", bucket_label(b)));
    assert!(versions.len() >= 4, "{versions:?}");
    let mut by_path: BTreeMap<String, BTreeSet<(String, u64)>> = BTreeMap::new();
    for v in &versions {
        // 各清单片列出的文件条目：全量片的 `files`，增量片的 `files_added`
        let p = ds.json(v);
        let key = if p["kind"] == "delta" {
            "files_added"
        } else {
            "files"
        };
        for f in p[key].as_array().unwrap() {
            by_path
                .entry(f["path"].as_str().unwrap().to_owned())
                .or_default()
                .insert((
                    f["content_digest"].as_str().unwrap().to_owned(),
                    f["added_in"].as_u64().unwrap(),
                ));
        }
    }
    // 已封口的桶会被合并，合并输出是另外的路径
    assert!(by_path.len() >= 4, "{by_path:?}");
    for (path, variants) in by_path {
        assert_eq!(variants.len(), 1, "{path}：{variants:?}");
    }
}

#[test]
fn audit_data_digest_mismatch_no_online_effect() {
    let fx = Fixture::new();
    let mut server = fx.start(&[FLUSH_1H]);
    ingest_ok(&server, "dm", r#"[{"k":"a","n":1},{"k":"b","n":2}]"#);
    stop(&mut server);
    let server = fx.start(&[FLUSH_1H]);
    let sql = "SELECT k, n FROM dm ORDER BY k";
    let before = search(&server, sql);
    assert_eq!(before.len(), 2);
    let mut server = server;
    stop(&mut server);

    let ds = fx.dataset("dm");
    let files = ds.keys_under("data/");
    assert_eq!(files.len(), 1);
    rewrite_parquet_uncompressed(&ds.path(&files[0]));

    let server = fx.start(&[FLUSH_1H]);
    assert_eq!(server.wait_readyz(200, Duration::from_secs(10)).status, 200);
    assert_eq!(search(&server, sql), before);
}

// ---------------------------------------------------------------------------
// deletion-ledger
// ---------------------------------------------------------------------------

#[test]
fn audit_deletions_array_empty_r0() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1S]);
    let ds = fx.dataset("de");
    // 此刻与此前 1、2 小时的三个桶，各一个数据文件：不合并、不入湖、没有 TTL
    let h = hour_of(now_us());
    let batches: Vec<String> = (0..3)
        .map(|i| at(h - i * HOUR + 1, &i.to_string()))
        .collect();
    ingest_and_commit(&server, &ds, "de", &batches);
    let manifests = ds.keys_under("manifest/buckets/");
    assert!(manifests.len() >= 3);
    for m in manifests {
        assert_eq!(ds.json(&m)["deletions"], json!([]), "{m}");
    }
}

#[test]
fn audit_tombstone_bucket_ledger() {
    // 桶 B 为此刻前第 3 小时、桶 C 为此刻前第 2 小时：都已封口、未达入湖水位
    let b_start = hour_of(now_us()) - 3 * HOUR;
    let c_start = b_start + HOUR;
    let (b_label, c_label) = (bucket_label(b_start), bucket_label(c_start));
    let fx = Fixture::new();
    let mut server = fx.start(&[FLUSH_1H]);
    ingest_ok(
        &server,
        "tb",
        &format!(
            r#"[{{"_timestamp":{},"k":"b"}},{{"_timestamp":{},"k":"c"}}]"#,
            b_start + 600_000_000,
            c_start + 600_000_000
        ),
    );
    stop(&mut server);

    // 第 4 版快照 N+1：桶 B 的文件全部移除、留痕记在其新头片（`ledger_previous` 为 null 的全量片）的 deletions 里；
    // 桶 C 不变
    let ds = fx.dataset("tb");
    let n = ds.current().unwrap();
    let parent = ds.snapshot(n);
    assert_eq!(parent["format_version"], 4, "{parent}");
    let created_at = parent["created_at"].as_i64().unwrap() + 1;
    let buckets = parent["buckets"].as_array().unwrap().clone();
    let (b, c): (Vec<Value>, Vec<Value>) = buckets
        .into_iter()
        .partition(|x| x["bucket"] == b_label.as_str());
    let c = c
        .into_iter()
        .find(|x| x["bucket"] == c_label.as_str())
        .unwrap();
    let old = ds.restore(b[0]["manifest"].as_str().unwrap());
    let f = old["files"][0].clone();
    let tomb = json!({
        "format_version": 4,
        "bucket": b_label,
        "kind": "full",
        "files": [],
        "deletions": [{
            "path": f["path"],
            "content_digest": f["content_digest"],
            "reason": "compaction",
            "added_in": f["added_in"],
            "max_timestamp": f["max_timestamp"],
            "cutoff": null,
            "deleted_at": created_at,
            "removed_in": n + 1,
        }],
        "handovers": [],
        "ledger_previous": null,
    });
    let bytes = tomb.to_string();
    let rel = format!(
        "manifest/buckets/{b_label}-{}.json",
        amberlog_storage::layout::new_object_id()
    );
    ds.write(&rel, bytes.as_bytes());
    let b_elem = json!({
        "bucket": b_label,
        "manifest": rel,
        "content_digest": sha256(bytes.as_bytes()),
        "record_count": 0,
        "size_bytes": 0,
        "time_min": null,
        "time_max": null,
    });
    ds.append_snapshot(|s| {
        s["created_at"] = json!(created_at);
        s["buckets"] = json!([b_elem, c.clone()]);
        s["summary"]["record_count"] = c["record_count"].clone();
        s["summary"]["size_bytes"] = c["size_bytes"].clone();
        s["summary"]["time_min"] = c["time_min"].clone();
        s["summary"]["time_max"] = c["time_max"].clone();
    });

    let mut server = fx.start(&[FLUSH_1H]);
    assert_eq!(server.wait_readyz(200, Duration::from_secs(10)).status, 200);
    let body = json!({"query": {"sql": "SELECT * FROM tb", "start_time": b_start, "end_time": b_start + HOUR}});
    let r = server.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        body.to_string().as_bytes(),
    );
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["scan_files"], 0);

    // 让系统自己写出下一个快照（再摄取一条进桶 C 并优雅停机），对系统写的快照断言：
    // 空桶 B 的留痕仍在台账中，`summary.time_min` 等于其余桶 `time_min` 的最小值
    ingest_ok(
        &server,
        "tb",
        &format!(r#"[{{"_timestamp":{},"k":"d"}}]"#, c_start + 1_200_000_000),
    );
    stop(&mut server);
    let head = ds.head();
    assert!(
        head["snapshot_id"].as_u64().unwrap() > n + 1,
        "系统应已写出新快照：{head}"
    );
    let b_now = head["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|x| x["bucket"] == b_label.as_str())
        .unwrap_or_else(|| panic!("空桶 B 应仍在快照中：{head}"));
    let b_manifest = ds.restore(b_now["manifest"].as_str().unwrap());
    assert!(
        b_manifest["deletions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d["path"] == f["path"]),
        "B 的留痕应仍在：{b_manifest}"
    );
    let others_min = head["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|x| x["time_min"].as_i64())
        .min();
    assert!(others_min.is_some(), "{head}");
    assert_eq!(head["summary"]["time_min"].as_i64(), others_min);
}

/// 持续摄取 `duration`：每 100 毫秒一批 5 条，第 `i` 批第 `j` 条的 `_timestamp` 取 `ts(i, j)`；
/// 每 5 秒记下一次数据集前缀下出现过的对象键。返回记下的键。
fn churn(
    server: &Server,
    ds: &Ds,
    stream: &str,
    duration: Duration,
    ts: fn(u64, u64) -> i64,
) -> BTreeSet<String> {
    let addr = server.addr;
    let path = format!("/api/default/{stream}/_json");
    let deadline = Instant::now() + duration;
    let writer = std::thread::spawn(move || {
        let mut i: u64 = 0;
        while Instant::now() < deadline {
            let records: Vec<String> = (0..5)
                .map(|j| format!(r#"{{"_timestamp":{},"i":{i}}}"#, ts(i, j)))
                .collect();
            let r = common::http(
                addr,
                "POST",
                &path,
                &[("Authorization", BEARER)],
                format!("[{}]", records.join(",")).as_bytes(),
            );
            assert_eq!(r.status, 200, "{}", r.text());
            i += 1;
            std::thread::sleep(Duration::from_millis(100));
        }
    });
    let mut seen = BTreeSet::new();
    while !writer.is_finished() {
        seen.extend(ds.keys());
        std::thread::sleep(Duration::from_secs(5));
    }
    writer.join().unwrap();
    seen.extend(ds.keys());
    seen
}

/// 记下的键仍都存在，快照 1..=当前快照都在，`_system/` 下没有对象。
fn assert_nothing_deleted(ds: &Ds, seen: &BTreeSet<String>) {
    for k in seen {
        assert!(ds.exists(k), "{k} 被删除");
    }
    let current = ds.current().unwrap();
    for id in 1..=current {
        assert!(ds.exists(&snapshot_rel(id)), "快照 {id} 不存在");
    }
    assert_eq!(ds.keys_under("_system/"), Vec::<String>::new());
}

#[test]
fn audit_no_deletion_outside_listed_deleters() {
    let soak = std::env::var("TESTBED_SOAK_HOURS").ok();
    let fx = Fixture::new();
    let ds = fx.dataset("soak");
    let seen = match soak {
        Some(h) => {
            let hours = h.parse::<f64>().expect("TESTBED_SOAK_HOURS 为小时数");
            let mut server = fx.start(&[]);
            let seen = churn(
                &server,
                &ds,
                "soak",
                Duration::from_secs_f64(hours * 3600.0),
                |_, _| now_us(),
            );
            stop(&mut server);
            seen
        }
        None => {
            // 缩短版：合并与巡检都在 2 分钟内运行
            let mut server = fx.start(&[
                ("hot_fsck.interval", Some("\"10s\"")),
                ("hot_fsck.min_orphan_age", Some("\"1s\"")),
            ]);
            let seen = churn(&server, &ds, "soak", Duration::from_secs(120), |_, j| {
                if j % 2 == 0 {
                    now_us()
                } else {
                    now_us() - 3 * HOUR
                }
            });
            let compacted = common::store::metric(
                &server,
                "amberlog_compactions_total{org=\"default\",stream=\"soak\",result=\"ok\"}",
            )
            .unwrap_or(0.0);
            assert!(compacted >= 1.0, "缩短版 2 分钟内没有发生合并");
            stop(&mut server);
            seen
        }
    };
    // 曾出现在任一桶 manifest `files` 中的路径
    let listed: BTreeSet<String> = ds
        .keys_under("manifest/buckets/")
        .iter()
        .flat_map(|m| {
            let p = ds.json(m);
            let key = if p["kind"] == "delta" {
                "files_added"
            } else {
                "files"
            };
            p[key]
                .as_array()
                .unwrap()
                .iter()
                .map(|f| f["path"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(!listed.is_empty());
    // 封口合并的输入在 `storage.delete_delay` 之后被物理删除（REQ-AMB-STO-AUDIT-INVARIANTS-V4）：仍存在，或完整删除台账中有
    // 它的 `compaction` 留痕，二者居一
    let compacted: BTreeSet<String> = ds
        .ledger()
        .iter()
        .filter(|d| d["reason"] == "compaction")
        .map(|d| d["path"].as_str().unwrap().to_owned())
        .collect();
    for p in &listed {
        assert!(
            ds.exists(p) || compacted.contains(p),
            "{p} 被删除且没有 compaction 留痕"
        );
    }
    let kept: BTreeSet<String> = seen.difference(&compacted).cloned().collect();
    assert_nothing_deleted(&ds, &kept);
}

#[test]
fn audit_no_deletion_accelerated() {
    let fx = Fixture::new();
    let mut server = fx.start(&[FLUSH_1S, ("flush.max_records", Some("50"))]);
    let ds = fx.dataset("nd");
    // `_timestamp` 轮流落在 2026-08-22 的 24 个 UTC 小时桶里
    let seen = churn(&server, &ds, "nd", Duration::from_secs(180), |i, j| {
        H10 - 10 * HOUR + ((i * 5 + j) % 24) as i64 * HOUR + 1_000_000
    });
    stop(&mut server);
    assert!(seen.iter().filter(|k| k.starts_with("data/")).count() >= 24);
    assert_nothing_deleted(&ds, &seen);
}

// ---------------------------------------------------------------------------
// 入湖、留痕归档与延后物理删除
// ---------------------------------------------------------------------------

const SEAL_10M: common::Kv = ("lifecycle.seal_grace", Some("\"10m\""));
const LAKE_1H: common::Kv = ("lifecycle.lake_watermark", Some("\"1h\""));
const LAKE_WAIT: Duration = Duration::from_secs(150);

/// 场景「交接记录中的冷层文件摘要与冷层文件一致」的配置，另加 `extra`。
fn lake_config(extra: &[common::Kv<'static>]) -> Vec<common::Kv<'static>> {
    let mut c = vec![FLUSH_1S, SEAL_10M, LAKE_1H];
    c.extend_from_slice(extra);
    c
}

/// 此刻前 3 小时所在小时桶 B 的起点与 100 条记录。
fn hundred_in_b() -> (i64, String) {
    let b = hour_of(now_us()) - 3 * HOUR;
    let recs: Vec<String> = (0..100)
        .map(|i| format!(r#"{{"_timestamp":{},"seq":{i}}}"#, b + 1 + i * 1000))
        .collect();
    (b, format!("[{}]", recs.join(",")))
}

/// 冷层表中 `amberlog.bucket` 为 `label` 的那次 `lake` 提交的快照（恰一个）。
fn lake_commit(f: &Fixture, label: &str) -> Value {
    let meta = f.cold("app").current();
    let snaps = common::lake::ColdDir::lake_snapshots(&meta, label);
    assert_eq!(snaps.len(), 1, "{meta}");
    snaps[0].clone()
}

fn file_digests(snap: &Value) -> BTreeMap<String, String> {
    serde_json::from_str(snap["summary"]["amberlog.file_digests"].as_str().unwrap()).unwrap()
}

/// 交接记录 `h` 的 `files` 的摘要等于冷层文件独立计算的摘要，也等于冷层提交 `snap` 的 `amberlog.file_digests`。
fn assert_handover_digests(f: &Fixture, h: &Value, snap: &Value) {
    let cold = f.cold("app");
    let digests = file_digests(snap);
    let files = h["files"].as_array().unwrap();
    assert!(!files.is_empty(), "{h}");
    for x in files {
        let p = x["path"].as_str().unwrap();
        assert_eq!(x["content_digest"], sha256(&cold.read(p)).as_str(), "{p}");
        assert_eq!(x["content_digest"], digests[p].as_str(), "{p}");
    }
}

/// 写入 100 条到 B 并等到交接完成，返回实例、B 的标签、交接后 B 的桶 manifest 与入湖前 B 的 `files` 条目。
fn lake_hundred(f: &Fixture, extra: &[common::Kv<'static>]) -> (Server, String, Value, Vec<Value>) {
    let s = f.start(&lake_config(extra));
    let (b, body) = hundred_in_b();
    let label = bucket_label(b);
    ingest_ok(&s, "app", &body);
    let ds = f.dataset("app");
    let m = ds.wait_handovers(&label, 1, LAKE_WAIT);
    let id = m["handovers"][0]["snapshot_id"].as_u64().unwrap();
    let inputs = ds.bucket_at(id - 1, &label).unwrap()["files"]
        .as_array()
        .unwrap()
        .clone();
    (s, label, m, inputs)
}

fn input_paths(inputs: &[Value]) -> Vec<String> {
    inputs
        .iter()
        .map(|e| e["path"].as_str().unwrap().to_owned())
        .collect()
}

/// B 的 `lake` 留痕所在快照的 `created_at`。
fn lake_created_at(ds: &Ds, m: &Value) -> i64 {
    let id = m["handovers"][0]["snapshot_id"].as_u64().unwrap();
    ds.snapshot(id)["created_at"].as_i64().unwrap()
}

fn sleep_until(us: i64) {
    let wait = us - now_us();
    if wait > 0 {
        std::thread::sleep(Duration::from_micros(wait as u64));
    }
}

#[test]
fn audit_handover_cold_digest() {
    let f = Fixture::new();
    let (_s, label, m, _) = lake_hundred(&f, &[]);
    let h = m["handovers"].as_array().unwrap();
    assert_eq!(h.len(), 1, "{m}");
    assert_handover_digests(&f, &h[0], &lake_commit(&f, &label));
}

#[cfg(feature = "fault-injection")]
#[test]
fn audit_recovered_handover_digest_from_cold_commit() {
    let f = Fixture::new();
    let config = lake_config(&[]);
    let (mut s, marker) = f.start_paused(&config, "lake.after_cold_commit");
    let (b, body) = hundred_in_b();
    let label = bucket_label(b);
    ingest_ok(&s, "app", &body);
    assert!(
        common::wait_file(&marker, LAKE_WAIT),
        "{:#?}",
        s.stderr_lines()
    );
    s.sigkill();
    let _s = f.start(&config);
    let m = f.dataset("app").wait_handovers(&label, 1, LAKE_WAIT);
    let h = m["handovers"].as_array().unwrap();
    assert_eq!(h.len(), 1, "{m}");
    let snap = lake_commit(&f, &label);
    assert_eq!(h[0]["iceberg_snapshot_id"], snap["snapshot-id"]);
    assert_handover_digests(&f, &h[0], &snap);
}

/// 场景「入湖移出的文件留痕为 lake 并带 added_in」的断言。
fn assert_lake_tombstones(ds: &Ds, m: &Value, inputs: &[Value]) {
    assert_eq!(m["files"], json!([]), "{m}");
    let deletions = m["deletions"].as_array().unwrap();
    for e in inputs {
        let hits: Vec<&Value> = deletions
            .iter()
            .filter(|d| d["reason"] == "lake" && d["path"] == e["path"])
            .collect();
        assert_eq!(hits.len(), 1, "{m}");
        let d = hits[0];
        for k in ["content_digest", "added_in", "max_timestamp"] {
            assert_eq!(d[k], e[k], "{k}：{d}");
        }
        assert_eq!(d["cutoff"], Value::Null, "{d}");
        let removed = ds.snapshot(d["removed_in"].as_u64().unwrap());
        assert_eq!(d["deleted_at"], removed["created_at"], "{d}");
    }
}

#[test]
fn audit_lake_tombstone_added_in() {
    let f = Fixture::new();
    let (_s, _, m, inputs) = lake_hundred(&f, &[]);
    assert_lake_tombstones(&f.dataset("app"), &m, &inputs);
}

/// 留痕归档的前提由真实的入湖与归档写出：入湖后以时钟偏移 2 小时重启（`fault-injection` 下的
/// `TESTBED_CLOCK_OFFSET`），使已入湖的桶变空满 1 小时而被归档（[`Fixture::retire_bucket`]）。
#[cfg(feature = "fault-injection")]
fn retired(f: &Fixture) -> (Server, common::lake::Retired) {
    let (s, r) = f.retire_bucket(&lake_config(&[]), 100);
    let ds = f.dataset("app");
    let lake = ds.json(&r.manifest);
    assert_lake_tombstones(&ds, &lake, &r.inputs);
    (s, r)
}

/// 台账中所记每个数据文件都有 `lake` 留痕条目。
#[cfg(feature = "fault-injection")]
fn assert_ledger_has(ds: &Ds, inputs: &[Value]) {
    let ledger = ds.ledger();
    for e in inputs {
        assert!(
            ledger
                .iter()
                .any(|d| d["path"] == e["path"] && d["reason"] == "lake"),
            "{} 不在台账中：{ledger:?}",
            e["path"]
        );
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn audit_retired_ledger_complete() {
    let f = Fixture::new();
    let (_s, r) = retired(&f);
    let ds = f.dataset("app");
    assert_ledger_has(&ds, &r.inputs);
    let hit = ds
        .segments(&ds.head()["retired_segment"])
        .into_iter()
        .flat_map(|s| s["buckets"].as_array().unwrap().clone())
        .find(|b| b["bucket"] == r.label.as_str())
        .expect("某个留痕归档段含 B");
    assert_eq!(hit["manifest"], r.manifest.as_str());
    assert_eq!(
        hit["content_digest"],
        sha256(&ds.read(&r.manifest)).as_str()
    );
}

#[cfg(feature = "fault-injection")]
#[test]
fn audit_retired_bucket_restarts_empty() {
    let f = Fixture::new();
    let (s, r) = retired(&f);
    let ds = f.dataset("app");
    let tail = ds.current().unwrap();
    ingest_ok(
        &s,
        "app",
        &format!(r#"[{{"_timestamp":{},"seq":100}}]"#, r.bucket + 500_000_000),
    );
    std::thread::sleep(Duration::from_secs(10));
    // 首个引用该记录所在数据文件的快照
    let first = (tail + 1..=ds.current().unwrap())
        .map(|id| ds.snapshot(id))
        .find(|s| ds.bucket_manifest(s, &r.label).is_some())
        .expect("B 重新出现在 buckets 中");
    let m = ds.bucket_manifest(&first, &r.label).unwrap();
    let files = m["files"].as_array().unwrap();
    assert_eq!(files.len(), 1, "{m}");
    assert_eq!(files[0]["record_count"], 1);
    assert_eq!(m["deletions"], json!([]));
    assert_eq!(m["handovers"], json!([]));
    assert_ledger_has(&ds, &r.inputs);
}

#[test]
fn audit_delete_delay_respected() {
    let f = Fixture::new();
    let (_s, _, m, inputs) = lake_hundred(&f, &[]);
    let ds = f.dataset("app");
    sleep_until(lake_created_at(&ds, &m) + 60_000_000);
    for p in input_paths(&inputs) {
        assert!(ds.exists(&p), "{p} 被删除");
    }
}

#[test]
fn audit_deleted_after_delay() {
    let f = Fixture::new();
    let (_s, _, m, inputs) = lake_hundred(&f, &[("storage.delete_delay", Some("\"10s\""))]);
    let ds = f.dataset("app");
    let deadline = lake_created_at(&ds, &m) + 3_610_000_000;
    let paths = input_paths(&inputs);
    // 轮询到全部删除即止，至多到留痕所在快照创建后 1 小时 10 秒
    while paths.iter().any(|p| ds.exists(p)) {
        assert!(now_us() < deadline, "{paths:?} 未在时限内删除");
        std::thread::sleep(Duration::from_secs(1));
    }
}

#[test]
fn audit_compaction_input_deleted_after_delay() {
    let f = Fixture::new();
    let s = f.start(&[
        ("lifecycle.seal_grace", Some("\"1s\"")),
        FLUSH_1S,
        ("storage.delete_delay", Some("\"30s\"")),
    ]);
    let ds = f.dataset("app");
    let b = hour_of(now_us()) - 2 * HOUR;
    three_ingests(&s, &ds, b);
    let m = ds.wait_compaction(&bucket_label(b), Duration::from_secs(120));
    let tombstones: Vec<&Value> = m["deletions"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|d| d["reason"] == "compaction")
        .collect();
    let paths: Vec<String> = tombstones
        .iter()
        .map(|d| d["path"].as_str().unwrap().to_owned())
        .collect();
    // 记有它们的快照创建于 `deleted_at`（REQ-AMB-STO-AUDIT-INVARIANTS-V4：留痕的 `deleted_at` 即所在快照的 `created_at`）
    let created = tombstones[0]["deleted_at"].as_i64().unwrap();
    sleep_until(created + 10_000_000);
    for p in &paths {
        assert!(ds.exists(p), "{p} 在 10 秒时已被删除");
    }
    // 轮询到全部删除即止，至多到该快照创建后 1 小时 40 秒
    let deadline = created + 3_640_000_000;
    while paths.iter().any(|p| ds.exists(p)) {
        assert!(now_us() < deadline, "{paths:?} 未在时限内删除");
        std::thread::sleep(Duration::from_secs(1));
    }
    let ledger = ds.ledger();
    for p in &paths {
        assert!(
            ledger.iter().any(|d| d["path"] == p.as_str()
                && d["reason"] == "compaction"
                && d["cutoff"].is_null()),
            "{p}：{ledger:?}"
        );
    }
}

/// 长时场景：设了 `TESTBED_SOAK_HOURS` 时按场景以 `storage.delete_delay` 5m 跑满 1 小时
/// 10 分钟；缺省跑缩短版：`storage.delete_delay` 10s，等到留痕所在快照创建后 150 秒（越过延后与两轮物理删除周期）。
///
/// 运行中的实例缓存已读过的桶 manifest，删除 C 的桶 manifest 之后查询读不到这次删除；所以入湖完成后以 SIGTERM 停止，
/// 删除之后以同一配置重启、随即查询使数据集停用（重启不改变要断言的对象）。
#[test]
fn audit_disabled_dataset_no_physical_delete() {
    let soak = std::env::var("TESTBED_SOAK_HOURS").is_ok();
    let (delay, wait) = if soak {
        ("\"5m\"", 4_200_000_000)
    } else {
        ("\"10s\"", 150_000_000)
    };
    let f = Fixture::new();
    let config = lake_config(&[("storage.delete_delay", Some(delay))]);
    let mut s = f.start(&config);
    let (b, body) = hundred_in_b();
    let label = bucket_label(b);
    ingest_ok(&s, "app", &body);
    let c = now_us();
    ingest_ok(&s, "app", &format!(r#"[{{"_timestamp":{c},"seq":1000}}]"#));
    let ds = f.dataset("app");
    let m = ds.wait_handovers(&label, 1, LAKE_WAIT);
    stop(&mut s);
    let id = m["handovers"][0]["snapshot_id"].as_u64().unwrap();
    let paths = input_paths(
        ds.bucket_at(id - 1, &label).unwrap()["files"]
            .as_array()
            .unwrap(),
    );
    let created = lake_created_at(&ds, &m);
    for p in &paths {
        assert!(ds.exists(p), "{p} 在停用之前已被删除");
    }

    // 删除当前快照中 C 的桶 manifest，查询 C 使数据集停用
    let head = ds.head();
    let rc = head["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["bucket"] == bucket_label(c).as_str())
        .expect("C 在当前快照中")
        .clone();
    ds.remove(rc["manifest"].as_str().unwrap());
    let s = f.start(&config);
    let start = hour_of(c);
    let q = json!({"query": {"sql": "SELECT * FROM app", "start_time": start, "end_time": start + HOUR}})
        .to_string();
    let r = s.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        q.as_bytes(),
    );
    assert_eq!(r.status, 500, "{}", r.text());
    assert!(
        now_us() < created + 300_000_000,
        "停用晚于留痕所在快照创建后 5 分钟"
    );

    sleep_until(created + wait);
    for p in &paths {
        assert!(ds.exists(p), "{p} 被删除");
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn audit_shutdown_no_new_physical_delete() {
    let f = Fixture::new();
    let config = lake_config(&[("storage.delete_delay", Some("\"30s\""))]);
    let (mut s, marker) = f.start_paused(&config, "shutdown.before_exit");
    let (b, body) = hundred_in_b();
    let label = bucket_label(b);
    ingest_ok(&s, "app", &body);
    let ds = f.dataset("app");
    let m = ds.wait_handovers(&label, 1, LAKE_WAIT);
    let created = lake_created_at(&ds, &m);
    s.signal("TERM");
    assert!(
        now_us() < created + 10_000_000,
        "SIGTERM 晚于留痕所在快照创建后 10 秒"
    );
    assert!(common::wait_file(&marker, Duration::from_secs(60)));
    let id = m["handovers"][0]["snapshot_id"].as_u64().unwrap();
    let paths = input_paths(
        ds.bucket_at(id - 1, &label).unwrap()["files"]
            .as_array()
            .unwrap(),
    );
    sleep_until(created + 90_000_000);
    for p in &paths {
        assert!(ds.exists(p), "{p} 被删除");
    }
    s.sigkill();
}

// ---------------------------------------------------------------------------
// REQ-AMB-STO-AUDIT-INVARIANTS-V4：冷层合并的留痕与合并记录摘要
// ---------------------------------------------------------------------------

/// 冷层合并场景的配置：`lifecycle.seal_grace` 为 `1s`、`lifecycle.lake_watermark` 为 `2s`、`cold_compaction.check_interval`
/// 为 `10s`，未配置 `retention.period`；另以 `flush.interval` 为 `1s` 使摄取尽快提交。
const COLD_MERGE: [Kv<'static>; 4] = [
    ("flush.interval", Some("\"1s\"")),
    ("lifecycle.seal_grace", Some("\"1s\"")),
    ("lifecycle.lake_watermark", Some("\"2s\"")),
    ("cold_compaction.check_interval", Some("\"10s\"")),
];

/// 冷层合并场景的「等待至」上限。
const MERGE_WAIT: Duration = Duration::from_secs(180);

/// 前一个 UTC 日第 `h` 小时的起点。
fn yesterday_hour(h: i64) -> i64 {
    let day = 24 * HOUR;
    now_us().div_euclid(day) * day - day + h * HOUR
}

/// 向 `app` 摄取前一个 UTC 日第 1 小时与第 2 小时的各 10 条记录，等两个小时桶都入湖，返回两次交接记录中各冷层数据文件的
/// `files` 元素（`path`、`content_digest`、`max_timestamp` 等）。
fn two_laked_hours(s: &Server, ds: &Ds) -> Vec<Value> {
    let mut out = Vec::new();
    for (k, h) in [(0u64, 1i64), (10, 2)] {
        let start = yesterday_hour(h);
        let body: Vec<String> = (0..10u64)
            .map(|i| {
                format!(
                    r#"{{"_timestamp":{},"seq":{}}}"#,
                    start + i as i64 * 1_000_000,
                    k + i
                )
            })
            .collect();
        ingest_ok(s, "app", &format!("[{}]", body.join(",")));
    }
    for h in [1, 2] {
        let m = ds.wait_handovers(&bucket_label(yesterday_hour(h)), 1, MERGE_WAIT);
        out.extend(
            m["handovers"][0]["files"]
                .as_array()
                .unwrap()
                .iter()
                .cloned(),
        );
    }
    out
}

/// 冷层当前版本中 `amberlog.operation` 为 `compaction` 的快照。
fn cold_compactions(cold: &ColdDir) -> Vec<Value> {
    if cold.version().is_none() {
        return Vec::new();
    }
    ColdDir::snapshots(&cold.current())
        .into_iter()
        .filter(|s| s["summary"]["amberlog.operation"] == "compaction")
        .collect()
}

/// 等冷层当前版本中出现合并冷层提交 c、且当前快照可达的冷层变更段中出现 `cold_removed_in` 为 c 的元素；返回 c 与这些
/// 冷层变更段。
fn wait_merge_traced(f: &Fixture, s: &Server) -> (Value, Vec<Value>) {
    let ds = f.dataset("app");
    let cold = f.cold("app");
    let deadline = Instant::now() + MERGE_WAIT;
    loop {
        if let Some(c) = cold_compactions(&cold).into_iter().next() {
            let id = c["snapshot-id"].as_i64().unwrap();
            let segs: Vec<Value> = ds
                .segments(&ds.head()["cold_deletion_segment"])
                .into_iter()
                .filter(|seg| {
                    seg["deletions"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|d| d["cold_removed_in"] == id)
                })
                .collect();
            if !segs.is_empty() {
                return (c, segs);
            }
        }
        assert!(
            Instant::now() < deadline,
            "{} 秒内未见记有合并的冷层变更段：{:#?}",
            MERGE_WAIT.as_secs(),
            s.stderr_lines()
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// 含合并移出元素的冷层变更段的 `additions`：不为空，每个元素的摘要等于独立计算的值，也等于 c 的 `amberlog.file_digests`。
fn assert_additions(cold: &ColdDir, c: &Value, segs: &[Value]) {
    let digests = file_digests(c);
    let additions: Vec<&Value> = segs
        .iter()
        .flat_map(|seg| seg["additions"].as_array().unwrap())
        .collect();
    assert!(!additions.is_empty(), "{segs:#?}");
    for a in additions {
        let path = a["path"].as_str().unwrap();
        let digest = a["content_digest"].as_str().unwrap();
        assert_eq!(digest, sha256(&cold.read(path)), "{path}");
        assert_eq!(
            Some(digest),
            digests.get(path).map(String::as_str),
            "{path}：{digests:?}"
        );
    }
}

#[test]
fn audit_cold_compaction_traced_with_merge_digest() {
    let f = Fixture::new();
    let ds = f.dataset("app");
    let s = f.start(&COLD_MERGE);
    let recorded = two_laked_hours(&s, &ds);
    let (c, segs) = wait_merge_traced(&f, &s);
    let id = c["snapshot-id"].as_i64().unwrap();
    let removed: Vec<&Value> = segs
        .iter()
        .flat_map(|seg| seg["deletions"].as_array().unwrap())
        .filter(|d| d["cold_removed_in"] == id)
        .collect();
    // 每个被 c 移除的冷层数据文件恰有一个 `compaction` 留痕，取值等于交接时所记
    let mut seen = BTreeSet::new();
    for d in &removed {
        assert_eq!(d["reason"], "compaction", "{d}");
        assert!(d["cutoff"].is_null(), "{d}");
        let path = d["path"].as_str().unwrap();
        assert!(seen.insert(path.to_owned()), "{path} 有不止一个留痕");
        let r = recorded
            .iter()
            .find(|e| e["path"] == path)
            .unwrap_or_else(|| panic!("{path} 不在交接记录中：{recorded:#?}"));
        assert_eq!(d["content_digest"], r["content_digest"], "{path}");
        assert_eq!(d["max_timestamp"], r["max_timestamp"], "{path}");
    }
    assert!(!removed.is_empty());
    assert_additions(&f.cold("app"), &c, &segs);
}

/// 崩溃点 `cold_compaction.after_cold_commit`：合并冷层提交之后、合并记录所在的热层提交之前。
#[test]
fn audit_recovered_merge_record_digest_from_cold_commit() {
    let f = Fixture::new();
    let ds = f.dataset("app");
    let (mut s, marker) = f.start_paused(&COLD_MERGE, "cold_compaction.after_cold_commit");
    two_laked_hours(&s, &ds);
    kill_at_marker_within(&mut s, &marker, MERGE_WAIT);
    assert_eq!(
        cold_compactions(&f.cold("app")).len(),
        1,
        "崩溃时冷层已有合并冷层提交 c"
    );

    let s = f.start(&COLD_MERGE);
    let (c, segs) = wait_merge_traced(&f, &s);
    assert_additions(&f.cold("app"), &c, &segs);
}

/// 等暂停点标记出现后 SIGKILL（至多 `limit`）。
fn kill_at_marker_within(server: &mut Server, marker: &std::path::Path, limit: Duration) {
    assert!(
        wait_file(marker, limit),
        "未走到暂停点：{:#?}",
        server.stderr_lines()
    );
    server.sigkill();
}

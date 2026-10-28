//! REQ-AMB-STO-EVIDENCE-EXPORT 的系统测试：以子进程运行写入实例
//! `amberlog serve` 与 `amberlog evidence export`，对象存储为 `file://` 临时目录，测试直接读、改、删其中的对象构造外部删改。
//! 场景里的 `/tmp/<名>` 一律落在每个测试自己的临时目录之下（[`Fixture::tmp`]）。

mod common;

use std::collections::BTreeSet;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use common::Fixture;
use common::api::now_us;
use common::evidence::{
    MIN_ORPHAN_1S, RECYCLE, W, commit_until, files_lines, member_json, objects_of, untar, w_with,
};
use common::store::{HOUR, hour_of, sha256, snapshot_rel, stop};
use serde_json::{Value, json};

/// 以 W 启动、写入 50 条此刻的记录并停止（「stream `app` 有热层数据，W 已停止」）。
fn hot_stopped(fx: &Fixture) {
    let mut w = fx.start(&W);
    fx.hot_data(&w, 50);
    stop(&mut w);
}

/// 「stream `app` 已入湖且有热层数据，W 已停止」。
fn laked_and_hot(fx: &Fixture) -> String {
    let mut w = fx.start(&W);
    let label = fx.laked(&w);
    fx.hot_data(&w, 50);
    stop(&mut w);
    label
}

/// 对象存储中 `amberlog/` 之下全部对象（相对路径 → SHA-256）。
fn store_digests(fx: &Fixture) -> Vec<(String, String)> {
    let base = fx.store_dir().join("amberlog");
    common::lake::keys(&base)
        .into_iter()
        .map(|k| {
            let d = sha256(&std::fs::read(base.join(&k)).unwrap());
            (k, d)
        })
        .collect()
}

/// `dir` 中文件名含 `needle` 的文件。
fn names_with(dir: &Path, needle: &str) -> Vec<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.contains(needle))
        .collect()
}

#[test]
fn evidence_export_read_only() {
    let fx = Fixture::new();
    laked_and_hot(&fx);
    let before = store_digests(&fx);
    let d = fx.root().join("no-such-wal");
    let wal = format!("\"{}\"", d.display());
    let mut conf = W.to_vec();
    conf.push(("wal.dir", Some(&wal)));
    fx.export_app(&conf, "e.tar", &[], 0);
    assert_eq!(store_digests(&fx), before);
    assert!(!d.exists());
}

#[test]
fn evidence_export_while_serving() {
    let fx = Fixture::new();
    let w = fx.start(&W);
    fx.laked(&w);
    fx.hot_data(&w, 50);
    let (_, r) = fx.export_app(&W, "e.tar", &[], 0);
    let receipt = r.json();
    assert_eq!(receipt["dataset"], "default/app");
    assert!(receipt["snapshot"]["snapshot_id"].as_u64().unwrap() >= 1);
    assert!(receipt["cold"]["metadata_version"].as_u64().unwrap() >= 1);
}

#[test]
fn evidence_export_args_usage() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let a = fx.tmp("a.tar");
    let b = fx.tmp("b.tar");
    for args in [
        vec!["--output", a.to_str().unwrap()],
        vec!["--dataset", "default/app"],
        vec![
            "--dataset",
            "default/app",
            "--output",
            b.to_str().unwrap(),
            "--verbose",
        ],
    ] {
        let r = fx.export(&W, &args);
        assert_eq!(r.code, Some(2), "{args:?}");
        assert!(
            r.stderr.contains("usage: amberlog"),
            "{args:?}: {}",
            r.stderr
        );
    }
    assert!(!a.exists() && !b.exists());
}

#[test]
fn evidence_export_args_repeated_and_equals() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let p = |n: &str| fx.tmp(n).to_str().unwrap().to_owned();
    let (r1, r2, r3, r4) = (p("r1.tar"), p("r2.tar"), p("r3.tar"), p("r4.tar"));
    let r = fx.export(
        &W,
        &[
            "--dataset",
            "default/app",
            "--dataset",
            "default/app",
            "--output",
            &r1,
        ],
    );
    assert_eq!(r.code, Some(2));
    assert!(r.stderr.contains("usage: amberlog"), "{}", r.stderr);
    let r = fx.export(
        &W,
        &[
            "--dataset=default/app",
            "--output",
            &r2,
            &format!("--output={r3}"),
        ],
    );
    assert_eq!(r.code, Some(2));
    assert!(!r.stderr.is_empty());
    for f in [&r1, &r2, &r3] {
        assert!(!Path::new(f).exists());
    }
    // `--config=w.toml` 与等号写法：用写好的 w.toml 直接运行
    std::fs::write(fx.root().join("w.toml"), fx.toml(&W)).unwrap();
    let out = common::amberlog(fx.root())
        .args([
            "evidence",
            "export",
            "--config=w.toml",
            "--dataset=default/app",
            &format!("--output={r4}"),
        ])
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(Path::new(&r4).exists());
}

#[test]
fn evidence_export_dataset_arg_invalid() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let out = fx.tmp("d.tar");
    for ds in ["app", "default/App", "other/app"] {
        let r = fx.export(&W, &["--dataset", ds, "--output", out.to_str().unwrap()]);
        assert_eq!(r.code, Some(2), "{ds}");
        assert!(r.stderr.contains("--dataset"), "{ds}: {}", r.stderr);
    }
}

#[test]
fn evidence_export_range_not_whole_hour() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let out = fx.tmp("r.tar");
    for from in [
        "2026-10-01T00:30:00Z",
        "2026-10-01T00:00:00+08:00",
        "2026-10-01",
    ] {
        let r = fx.export(
            &W,
            &[
                "--dataset",
                "default/app",
                "--from",
                from,
                "--to",
                "2026-10-02T00:00:00Z",
                "--output",
                out.to_str().unwrap(),
            ],
        );
        assert_eq!(r.code, Some(2), "{from}");
        assert!(r.stderr.contains("--from"), "{from}: {}", r.stderr);
    }
}

#[test]
fn evidence_export_range_pair() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let out = fx.tmp("r.tar");
    let out = out.to_str().unwrap();
    for range in [
        vec!["--from", "2026-10-01T00:00:00Z"],
        vec![
            "--from",
            "2026-10-02T00:00:00Z",
            "--to",
            "2026-10-01T00:00:00Z",
        ],
        vec![
            "--from",
            "2026-10-01T00:00:00Z",
            "--to",
            "2026-10-01T00:00:00Z",
        ],
    ] {
        let mut args = vec!["--dataset", "default/app", "--output", out];
        args.extend(range.iter().copied());
        let r = fx.export(&W, &args);
        assert_eq!(r.code, Some(2), "{range:?}");
        assert!(
            r.stderr.contains("--from") && r.stderr.contains("--to"),
            "{range:?}: {}",
            r.stderr
        );
    }
}

#[test]
fn evidence_export_output_in_prefix() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let obj = fx.store_dir();
    let link = fx.tmp("link");
    std::os::unix::fs::symlink(&obj, &link).unwrap();
    let inside = obj.join("amberlog").join("e.tar");
    let via_link = link.join("amberlog").join("e.tar");
    for out in [&inside, &via_link] {
        let r = fx.export(
            &W,
            &[
                "--dataset",
                "default/app",
                "--output",
                out.to_str().unwrap(),
            ],
        );
        assert_eq!(r.code, Some(2), "{}", out.display());
        assert!(r.stderr.contains("--output"), "{}", r.stderr);
    }
    assert!(!inside.exists());
}

#[test]
fn evidence_export_dataset_missing() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let out = fx.tmp("n.tar");
    let r = fx.export(
        &W,
        &[
            "--dataset",
            "default/nosuch",
            "--output",
            out.to_str().unwrap(),
        ],
    );
    assert_eq!(r.code, Some(2), "{}", r.stderr);
    assert!(r.stderr.contains("default/nosuch"), "{}", r.stderr);
    assert!(!out.exists());
    assert!(names_with(out.parent().unwrap(), "n.tar").is_empty());
}

#[test]
fn evidence_export_output_exists() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let out = fx.tmp("exist.tar");
    std::fs::write(&out, "keep").unwrap();
    let r = fx.export(
        &W,
        &[
            "--dataset",
            "default/app",
            "--output",
            out.to_str().unwrap(),
        ],
    );
    assert_eq!(r.code, Some(2));
    assert!(r.stderr.contains(out.to_str().unwrap()), "{}", r.stderr);
    assert_eq!(std::fs::read_to_string(&out).unwrap(), "keep");
}

#[test]
fn evidence_export_output_parent_invalid() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let nodir = fx.tmp("nodir");
    let plain = fx.tmp("plain");
    std::fs::write(&plain, "keep").unwrap();
    for out in [nodir.join("e.tar"), plain.join("e.tar")] {
        let r = fx.export(
            &W,
            &[
                "--dataset",
                "default/app",
                "--output",
                out.to_str().unwrap(),
            ],
        );
        assert_eq!(r.code, Some(2));
        assert!(r.stderr.contains(out.to_str().unwrap()), "{}", r.stderr);
    }
    assert!(!nodir.exists());
    assert_eq!(std::fs::read_to_string(&plain).unwrap(), "keep");
}

#[test]
fn evidence_export_probe_fail_no_file() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    // 台架停止对象存储服务：`file://` 所指目录不存在即对象存储不可访问（REQ-AMB-CFG-STARTUP）
    std::fs::rename(fx.store_dir(), fx.root().join("store-gone")).unwrap();
    let out = fx.tmp("p.tar");
    let r = fx.export(
        &W,
        &[
            "--dataset",
            "default/app",
            "--output",
            out.to_str().unwrap(),
        ],
    );
    assert_eq!(r.code, Some(3), "{}", r.stderr);
    assert!(!out.exists());
    assert!(names_with(out.parent().unwrap(), "p.tar").is_empty());
}

#[test]
fn evidence_export_exported_at() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let t1 = now_us();
    let (_, r) = fx.export_app(&W, "e.tar", &[], 0);
    let t2 = now_us();
    let at = r.json()["exported_at"].as_i64().unwrap();
    assert!(t1 <= at && at <= t2, "{t1} {at} {t2}");
}

#[test]
fn evidence_export_pin_tail_not_hint() {
    let fx = Fixture::new();
    let mut w = fx.start(&W);
    fx.hot_data(&w, 25);
    fx.hot_data(&w, 25);
    stop(&mut w);
    let ds = fx.dataset("app");
    let k = ds.current().unwrap();
    assert!(k >= 2);
    ds.write("manifest/_current", br#"{"snapshot_id": 1}"#);
    let (_, r) = fx.export_app(&W, "e.tar", &[], 0);
    let receipt = r.json();
    assert_eq!(receipt["snapshot"]["snapshot_id"], k);
    assert_eq!(
        receipt["snapshot"]["content_digest"],
        sha256(&ds.read(&snapshot_rel(k)))
    );
    let s = ds.snapshot(k);
    assert_eq!(receipt["retired_segment"], s["retired_segment"]);
    assert_eq!(receipt["cold_deletion_segment"], s["cold_deletion_segment"]);
}

#[test]
fn evidence_export_hint_unusable_exit6() {
    let fx = Fixture::new();
    let mut w = fx.start(&W);
    fx.hot_data(&w, 25);
    fx.hot_data(&w, 25);
    stop(&mut w);
    let ds = fx.dataset("app");
    assert!(ds.current().unwrap() >= 2);
    ds.write("manifest/_current", b"not json");
    ds.remove(&snapshot_rel(1));
    let out = fx.tmp("hint.tar");
    let r = fx.export(
        &W,
        &[
            "--dataset",
            "default/app",
            "--output",
            out.to_str().unwrap(),
        ],
    );
    assert_eq!(r.code, Some(6), "{}", r.stderr);
    assert!(r.stderr.contains("manifest/_current"), "{}", r.stderr);
    assert!(!out.exists());
    assert!(r.stdout.is_empty());
}

#[test]
fn evidence_export_no_cold_table() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let (p, r) = fx.export_app(&W, "e.tar", &[], 0);
    assert_eq!(r.json()["cold"], Value::Null);
    let (_, files) = untar(&p);
    assert!(files_lines(&files).iter().all(|l| l["tier"] != "cold"));
}

#[test]
fn evidence_export_format_v1_exit6() {
    let fx = Fixture::new();
    // 早期版本写出的第 1 版快照（`format_version` 为 1）与提示
    let ds = fx.dataset("old");
    ds.write(
        &snapshot_rel(1),
        serde_json::to_string(&json!({
            "format_version": 1,
            "snapshot_id": 1,
            "parent_snapshot_id": null,
            "created_at": now_us(),
            "schema": [],
            "buckets": [],
            "summary": {"dataset": "default/old", "record_count": 0, "size_bytes": 0, "time_min": null, "time_max": null},
        }))
        .unwrap()
        .as_bytes(),
    );
    ds.write("manifest/_current", br#"{"snapshot_id": 1}"#);
    let out = fx.tmp("old.tar");
    let r = fx.export(
        &W,
        &[
            "--dataset",
            "default/old",
            "--output",
            out.to_str().unwrap(),
        ],
    );
    assert_eq!(r.code, Some(6), "{}", r.stderr);
    assert!(r.stderr.contains("format_version"), "{}", r.stderr);
    assert!(!out.exists());
    assert!(r.stdout.is_empty());
}

#[test]
fn evidence_export_format_v2_exit6() {
    let fx = Fixture::new();
    // 较早版本写出的第 2 版快照（`format_version` 为 2）与提示
    let ds = fx.dataset("old2");
    ds.write(
        &snapshot_rel(1),
        serde_json::to_string(&json!({
            "format_version": 2,
            "snapshot_id": 1,
            "parent_snapshot_id": null,
            "parent_digest": null,
            "created_at": now_us(),
            "schema": [{"name": "_timestamp", "type": "Int64", "field_id": 1}],
            "buckets": [],
            "summary": {"dataset": "default/old2", "record_count": 0, "size_bytes": 0, "time_min": null, "time_max": null,
                "cold": {"record_count": 0, "size_bytes": 0, "time_min": null, "time_max": null}},
            "retired_segment": null,
            "cold_deletion_segment": null,
        }))
        .unwrap()
        .as_bytes(),
    );
    ds.write("manifest/_current", br#"{"snapshot_id": 1}"#);
    let out = fx.tmp("old2.tar");
    let r = fx.export(
        &W,
        &[
            "--dataset",
            "default/old2",
            "--output",
            out.to_str().unwrap(),
        ],
    );
    assert_eq!(r.code, Some(6), "{}", r.stderr);
    assert!(r.stderr.contains("format_version"), "{}", r.stderr);
    assert!(!out.exists());
    assert!(r.stdout.is_empty());
}

#[test]
fn evidence_export_head_unreadable_exit6() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let ds = fx.dataset("app");
    let rel = snapshot_rel(ds.current().unwrap());
    ds.write(&rel, b"not json");
    let out = fx.tmp("h.tar");
    let r = fx.export(
        &W,
        &[
            "--dataset",
            "default/app",
            "--output",
            out.to_str().unwrap(),
        ],
    );
    assert_eq!(r.code, Some(6), "{}", r.stderr);
    assert!(r.stderr.contains(&rel), "{}", r.stderr);
    assert!(!out.exists());
}

#[test]
fn evidence_export_scope_buckets() {
    let fx = Fixture::new();
    let t = hour_of(now_us());
    let mut w = fx.start(&[
        ("lifecycle.lake_watermark", Some("\"6h\"")),
        ("storage.delete_delay", Some("\"1h\"")),
        common::store::FLUSH_1S,
    ]);
    for k in [5, 4, 3] {
        let b = t - k * HOUR;
        let recs: Vec<String> = (0..10)
            .map(|i| format!(r#"{{"_timestamp":{},"seq":{i}}}"#, b + 1 + i))
            .collect();
        common::api::ingest_ok(&w, "app", &format!("[{}]", recs.join(",")));
    }
    fx.dataset("app").wait_records(30, Duration::from_secs(60));
    stop(&mut w);
    let (from, to) = (iso(t - 4 * HOUR), iso(t - 3 * HOUR));
    let (p, _) = fx.export_app(&W, "e.tar", &["--from", &from, "--to", &to], 0);
    let (_, files) = untar(&p);
    let label = common::store::bucket_label(t - 4 * HOUR);
    let prefix = format!(
        "data/{}/{}/{}/{}/",
        &label[0..4],
        &label[4..6],
        &label[6..8],
        &label[8..10]
    );
    let hot: Vec<Value> = files_lines(&files)
        .into_iter()
        .filter(|l| l["tier"] == "hot")
        .collect();
    assert!(!hot.is_empty());
    assert!(
        hot.iter()
            .all(|l| l["path"].as_str().unwrap().starts_with(&prefix)),
        "{hot:?}"
    );
    assert_eq!(
        hot.iter()
            .map(|l| l["record_count"].as_u64().unwrap())
            .sum::<u64>(),
        10
    );
    let pieces: Vec<&String> = files
        .keys()
        .filter(|k| k.starts_with("objects/default/app/manifest/buckets/"))
        .collect();
    assert!(
        pieces.iter().all(|k| k.contains(&format!("/{label}-"))),
        "{pieces:?}"
    );
    let head = fx.dataset("app").head();
    let h = head["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|b| b["bucket"] == label.as_str())
        .unwrap()["manifest"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(
        pieces.contains(&&format!("objects/default/app/{h}")),
        "{pieces:?}"
    );
}

/// UTC 微秒 → `YYYY-MM-DDTHH:00:00Z`。
fn iso(us: i64) -> String {
    let l = common::store::bucket_label(us);
    format!(
        "{}-{}-{}T{}:00:00Z",
        &l[0..4],
        &l[4..6],
        &l[6..8],
        &l[8..10]
    )
}

#[test]
fn evidence_export_tar_members() {
    let fx = Fixture::new();
    laked_and_hot(&fx);
    let (p, _) = fx.export_app(&W, "e.tar", &[], 0);
    let list = Command::new("tar").arg("-tvf").arg(&p).output().unwrap();
    assert!(list.status.success());
    let text = String::from_utf8(list.stdout).unwrap();
    let mut names = BTreeSet::new();
    for line in text.lines() {
        assert!(line.starts_with('-'), "不是普通文件：{line}");
        names.insert(line.split_whitespace().last().unwrap().to_owned());
    }
    for n in [
        "evidence.json",
        "receipt.json",
        "files.jsonl",
        "check.json",
        "SHA256SUMS",
    ] {
        assert!(names.remove(n), "缺 {n}：{text}");
    }
    assert!(!names.is_empty());
    assert!(names.iter().all(|n| n.starts_with("objects/")), "{names:?}");
    // 逐个读头：magic、version、typeflag
    let bytes = std::fs::read(&p).unwrap();
    assert_ne!(&bytes[..2], &[0x1f, 0x8b]);
    let mut pos = 0;
    while bytes[pos..pos + 512].iter().any(|&b| b != 0) {
        let h = &bytes[pos..pos + 512];
        assert_eq!(&h[257..263], b"ustar\0");
        assert_eq!(&h[263..265], b"00");
        assert!(h[156] != b'L' && h[156] != b'K');
        let size = std::str::from_utf8(&h[124..135]).unwrap();
        let size = usize::from_str_radix(size.trim_matches(|c| c == '\0' || c == ' '), 8).unwrap();
        pos += 512 + size.div_ceil(512) * 512;
    }
}

#[test]
fn evidence_export_evidence_json() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let (p, _) = fx.export_app(
        &W,
        "e.tar",
        &[
            "--from",
            "2026-10-01T00:00:00Z",
            "--to",
            "2026-10-02T00:00:00Z",
        ],
        0,
    );
    let (_, files) = untar(&p);
    let e = member_json(&files, "evidence.json");
    let keys: Vec<&String> = e.as_object().unwrap().keys().collect();
    assert_eq!(
        keys,
        [
            "evidence_format_version",
            "dataset",
            "from",
            "to",
            "exported_at",
            "amberlog_version",
            "receipt",
            "iceberg_snapshots",
            "retirement"
        ]
    );
    assert_eq!(e["evidence_format_version"], 1);
    assert_eq!(e["from"], 1_790_812_800_000_000_i64);
    assert_eq!(e["to"], 1_790_899_200_000_000_i64);
    assert_eq!(e["receipt"], member_json(&files, "receipt.json"));
    let version = common::amberlog(fx.root())
        .arg("--version")
        .output()
        .unwrap();
    let version = String::from_utf8(version.stdout).unwrap();
    assert!(
        version
            .split_whitespace()
            .any(|w| Some(w) == e["amberlog_version"].as_str()),
        "{version} vs {}",
        e["amberlog_version"]
    );
}

#[test]
fn evidence_export_receipt_fields() {
    let fx = Fixture::new();
    laked_and_hot(&fx);
    let (p, _) = fx.export_app(&W, "e.tar", &[], 0);
    let (_, files) = untar(&p);
    let r = member_json(&files, "receipt.json");
    let keys: Vec<&String> = r.as_object().unwrap().keys().collect();
    assert_eq!(
        keys,
        [
            "evidence_format_version",
            "dataset",
            "exported_at",
            "amberlog_version",
            "snapshot",
            "retired_segment",
            "cold_deletion_segment",
            "cold"
        ]
    );
    let path = r["snapshot"]["path"].as_str().unwrap();
    let name = path
        .strip_prefix("manifest/")
        .unwrap()
        .strip_suffix(".json")
        .unwrap();
    assert!(
        name.len() == 20 && name.bytes().all(|b| b.is_ascii_digit()),
        "{path}"
    );
    let d = r["snapshot"]["content_digest"].as_str().unwrap();
    let hex = d.strip_prefix("sha256:").unwrap();
    assert!(
        hex.len() == 64
            && hex
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    );
}

#[test]
fn evidence_export_receipt_stdout() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let (p, r) = fx.export_app(&W, "e.tar", &[], 0);
    let (_, files) = untar(&p);
    assert_eq!(r.stdout.last(), Some(&b'\n'));
    assert_eq!(r.stdout.iter().filter(|&&b| b == b'\n').count(), 1);
    assert_eq!(
        &r.stdout[..r.stdout.len() - 1],
        files["receipt.json"].as_slice()
    );
}

#[test]
fn evidence_export_files_jsonl() {
    let fx = Fixture::new();
    laked_and_hot(&fx);
    let (p, _) = fx.export_app(&W, "e.tar", &[], 0);
    let (_, files) = untar(&p);
    let lines = files_lines(&files);
    assert!(!lines.is_empty());
    let members = [
        "tier",
        "path",
        "state",
        "content_digest",
        "record_count",
        "size_bytes",
        "min_timestamp",
        "max_timestamp",
        "added_in",
        "source",
        "deletion",
    ];
    for l in &lines {
        let keys: Vec<&String> = l.as_object().unwrap().keys().collect();
        assert_eq!(keys, members);
    }
    let order: Vec<(u8, Vec<u8>)> = lines
        .iter()
        .map(|l| {
            (
                u8::from(l["tier"] == "cold"),
                l["path"].as_str().unwrap().as_bytes().to_vec(),
            )
        })
        .collect();
    let mut sorted = order.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(order, sorted);
}

#[test]
fn evidence_export_files_hot_live() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let (p, _) = fx.export_app(&W, "e.tar", &[], 0);
    let (_, files) = untar(&p);
    let ds = fx.store_dir().join("amberlog");
    let live: Vec<Value> = files_lines(&files)
        .into_iter()
        .filter(|l| l["tier"] == "hot" && l["state"] == "live")
        .collect();
    assert!(!live.is_empty());
    for l in live {
        let m: Value =
            serde_json::from_slice(&std::fs::read(ds.join(l["source"].as_str().unwrap())).unwrap())
                .unwrap();
        let f = m["files"]
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["path"] == l["path"])
            .unwrap();
        for k in [
            "content_digest",
            "record_count",
            "size_bytes",
            "min_timestamp",
            "max_timestamp",
            "added_in",
        ] {
            assert_eq!(l[k], f[k], "{k}");
        }
        assert_eq!(l["deletion"], Value::Null);
    }
}

#[test]
fn evidence_export_objects_verbatim() {
    let fx = Fixture::new();
    laked_and_hot(&fx);
    let (p, r) = fx.export_app(&W, "e.tar", &[], 0);
    let receipt = r.json();
    let (_, files) = untar(&p);
    let base = fx.store_dir().join("amberlog");
    let mut n = 0;
    for (name, bytes) in &files {
        if let Some(rel) = name.strip_prefix("objects/") {
            assert_eq!(&std::fs::read(base.join(rel)).unwrap(), bytes, "{rel}");
            n += 1;
        }
    }
    assert!(n > 0);
    let ds = "objects/default/app/";
    assert!(files.contains_key(&format!(
        "{ds}{}",
        receipt["snapshot"]["path"].as_str().unwrap()
    )));
    let head = fx.dataset("app").head();
    for seg in ["retired_segment", "cold_deletion_segment"] {
        for s in fx.dataset("app").segments(&head[seg]) {
            let id = s["segment_id"].as_u64().unwrap();
            let dir = if seg == "retired_segment" {
                "retired"
            } else {
                "cold-deletions"
            };
            assert!(files.contains_key(&format!("{ds}manifest/{dir}/{id:020}.json")));
        }
    }
    let v = receipt["cold"]["metadata_version"].as_u64().unwrap();
    assert!(files.contains_key(&format!(
        "objects/_iceberg/default/app/metadata/v{v}.metadata.json"
    )));
    // 所涉每个桶 manifest 的头片，以及自头片沿 `previous` 与 `ledger_previous` 可达的每个清单片
    let app = fx.dataset("app");
    let mut heads: Vec<String> = head["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["manifest"].as_str().unwrap().to_owned())
        .collect();
    for s in app.segments(&head["retired_segment"]) {
        heads.extend(
            s["buckets"]
                .as_array()
                .unwrap()
                .iter()
                .map(|b| b["manifest"].as_str().unwrap().to_owned()),
        );
    }
    for h in heads {
        for p in app.pieces(&h) {
            assert!(files.contains_key(&format!("{ds}{p}")), "缺清单片 {p}");
        }
    }
}

#[test]
fn evidence_export_check_json_ok() {
    let fx = Fixture::new();
    laked_and_hot(&fx);
    let (p, _) = fx.export_app(&W, "e.tar", &[], 0);
    let (_, files) = untar(&p);
    let c = member_json(&files, "check.json");
    let keys: Vec<&String> = c.as_object().unwrap().keys().collect();
    assert_eq!(keys, ["result", "problems", "transitions", "notes"]);
    assert_eq!(c["result"], "ok", "{c}");
    assert_eq!(c["problems"], json!([]));
}

#[test]
fn evidence_export_sha256sums() {
    let fx = Fixture::new();
    laked_and_hot(&fx);
    let (p, _) = fx.export_app(&W, "e.tar", &[], 0);
    let (dir, files) = untar(&p);
    let st = Command::new("sha256sum")
        .args(["-c", "SHA256SUMS"])
        .current_dir(&dir)
        .output()
        .unwrap();
    assert!(
        st.status.success(),
        "{}",
        String::from_utf8_lossy(&st.stdout)
    );
    let sums = String::from_utf8(files["SHA256SUMS"].clone()).unwrap();
    let names: Vec<&str> = sums
        .lines()
        .map(|l| l.split_once("  ").unwrap().1)
        .collect();
    assert_eq!(names.len(), files.len() - 1);
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(names, sorted);
}

#[test]
fn evidence_export_bucket_manifest_tampered() {
    let fx = Fixture::new();
    let mut w = fx.start(&W);
    fx.hot_data(&w, 25);
    fx.hot_data(&w, 25);
    stop(&mut w);
    let ds = fx.dataset("app");
    let rel = common::evidence::bucket_manifest_paths(&ds)[0].clone();
    // 头片 M 末尾追加一个空格（仍为合法 JSON）
    let mut bytes = ds.read(&rel);
    bytes.push(b' ');
    ds.write(&rel, &bytes);
    let (p, r) = fx.export_app(&W, "e.tar", &[], 5);
    r.json();
    let (_, files) = untar(&p);
    let c = member_json(&files, "check.json");
    assert_eq!(c["result"], "problems_found");
    let object = format!("default/app/{rel}");
    assert!(
        objects_of(&c, "problems", "digest_mismatch").contains(&object),
        "{c}"
    );
    assert!(
        !objects_of(&c, "problems", "unparseable").contains(&object),
        "{c}"
    );
    assert_eq!(files[&format!("objects/{object}")], ds.read(&rel));
}

#[test]
fn evidence_export_chain_piece_tampered() {
    let fx = Fixture::new();
    let mut w = fx.start(&W);
    fx.hot_data(&w, 25);
    fx.hot_data(&w, 25);
    stop(&mut w);
    let ds = fx.dataset("app");
    let head = common::evidence::bucket_manifest_paths(&ds)
        .into_iter()
        .find(|h| ds.json(h)["kind"] == "delta")
        .expect("有头片为增量片的桶 manifest");
    let q = ds.json(&head)["previous"]["path"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut bytes = ds.read(&q);
    bytes.push(b' ');
    ds.write(&q, &bytes);
    let (p, _) = fx.export_app(&W, "e.tar", &[], 5);
    assert!(p.exists());
    let (_, files) = untar(&p);
    let c = member_json(&files, "check.json");
    assert!(
        objects_of(&c, "problems", "digest_mismatch").contains(&format!("default/app/{q}")),
        "{c}"
    );
    assert!(files.contains_key(&format!("objects/default/app/{head}")));
    assert!(files.contains_key(&format!("objects/default/app/{q}")));
}

#[test]
fn evidence_export_live_size_mismatch() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let ds = fx.dataset("app");
    let f = ds.file_entries(&ds.head())[0]["path"]
        .as_str()
        .unwrap()
        .to_owned();
    common::store::rewrite_parquet_uncompressed(&ds.path(&f));
    let (p, _) = fx.export_app(&W, "e.tar", &[], 5);
    let (_, files) = untar(&p);
    let c = member_json(&files, "check.json");
    assert!(
        objects_of(&c, "problems", "size_mismatch").contains(&format!("default/app/{f}")),
        "{c}"
    );
}

#[test]
fn evidence_export_problems_continue() {
    let fx = Fixture::new();
    let t = hour_of(now_us());
    let mut w = fx.start(&W);
    for b in [t - HOUR, t] {
        let recs: Vec<String> = (0..10)
            .map(|i| format!(r#"{{"_timestamp":{},"seq":{i}}}"#, b + 1 + i))
            .collect();
        common::api::ingest_ok(&w, "app", &format!("[{}]", recs.join(",")));
    }
    fx.dataset("app").wait_records(20, Duration::from_secs(60));
    stop(&mut w);
    let ds = fx.dataset("app");
    let head = ds.head();
    let b1 = ds
        .bucket_manifest(&head, &common::store::bucket_label(t - HOUR))
        .unwrap();
    let b2 = ds
        .bucket_manifest(&head, &common::store::bucket_label(t))
        .unwrap();
    let rel1 = head["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|b| b["bucket"] == common::store::bucket_label(t - HOUR).as_str())
        .unwrap()["manifest"]
        .as_str()
        .unwrap()
        .to_owned();
    let _ = b1;
    ds.write(&rel1, b"not json");
    let (p, _) = fx.export_app(&W, "e.tar", &[], 5);
    let (_, files) = untar(&p);
    let c = member_json(&files, "check.json");
    assert!(
        objects_of(&c, "problems", "unparseable").contains(&format!("default/app/{rel1}")),
        "{c}"
    );
    let lines = files_lines(&files);
    for f in b2["files"].as_array().unwrap() {
        assert!(
            lines
                .iter()
                .any(|l| l["path"] == f["path"] && l["state"] == "live")
        );
    }
    assert!(!files.contains_key(&format!("objects/default/app/{rel1}")));
}

#[test]
fn evidence_export_exit5_keeps_package() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let ds = fx.dataset("app");
    let rel = common::evidence::bucket_manifest_paths(&ds)[0].clone();
    ds.write(&rel, b"not json");
    let (p, _) = fx.export_app(&W, "e.tar", &[], 5);
    assert!(p.exists());
    let (_, files) = untar(&p);
    assert_eq!(
        member_json(&files, "check.json")["result"],
        "problems_found"
    );
}

#[test]
fn evidence_export_exit0() {
    let fx = Fixture::new();
    laked_and_hot(&fx);
    let (p, _) = fx.export_app(&W, "e.tar", &[], 0);
    let (_, files) = untar(&p);
    assert_eq!(member_json(&files, "check.json")["result"], "ok");
}

#[test]
fn evidence_export_no_retirement() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let (p, _) = fx.export_app(&W, "e.tar", &[], 0);
    let (_, files) = untar(&p);
    assert_eq!(
        member_json(&files, "evidence.json")["retirement"],
        Value::Null
    );
}

#[test]
fn evidence_export_retirement_unreadable() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let rec = fx.store_dir().join("amberlog/_system/retirement.json");
    std::fs::create_dir_all(rec.parent().unwrap()).unwrap();
    std::fs::write(&rec, b"not json").unwrap();
    let (p, _) = fx.export_app(&W, "e.tar", &[], 0);
    let (_, files) = untar(&p);
    assert_eq!(
        member_json(&files, "evidence.json")["retirement"],
        Value::Null
    );
    let c = member_json(&files, "check.json");
    assert_eq!(
        objects_of(&c, "notes", "retirement_unreadable").len(),
        1,
        "{c}"
    );
    assert_eq!(c["problems"], json!([]));
    assert_eq!(files["objects/_system/retirement.json"], b"not json");
}

#[test]
fn evidence_export_after_retirement() {
    let fx = Fixture::new();
    fx.retired();
    let record = std::fs::read(fx.retirement_record()).unwrap();
    let (p, _) = fx.export_app(&W, "e.tar", &[], 0);
    let (_, files) = untar(&p);
    assert_eq!(
        member_json(&files, "evidence.json")["retirement"],
        serde_json::from_slice::<Value>(&record).unwrap()
    );
    assert!(files.contains_key("objects/_system/retirement.json"));
    assert_eq!(member_json(&files, "check.json")["notes"], json!([]));
}

#[test]
fn evidence_export_retirement_mismatch_note() {
    let fx = Fixture::new();
    fx.retired();
    let rec = fx.retirement_record();
    let mut v: Value = serde_json::from_slice(&std::fs::read(&rec).unwrap()).unwrap();
    let app = v["datasets"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|d| d["dataset"] == "default/app")
        .unwrap();
    let id = app["snapshot"]["snapshot_id"].as_u64().unwrap();
    app["snapshot"]["snapshot_id"] = json!(id - 1);
    std::fs::write(&rec, serde_json::to_vec(&v).unwrap()).unwrap();
    let (p, _) = fx.export_app(&W, "e.tar", &[], 0);
    let (_, files) = untar(&p);
    let c = member_json(&files, "check.json");
    assert_eq!(
        objects_of(&c, "notes", "retirement_mismatch").len(),
        1,
        "{c}"
    );
    assert_eq!(c["problems"], json!([]));
}

#[test]
fn evidence_export_signal_exit6() {
    let fx = Fixture::new();
    laked_and_hot(&fx);
    let out = fx.tmp("s.tar");
    let (mut e, _) = fx.export_gated(&out, "evidence.after_partial_write", &[]);
    let dir = out.parent().unwrap();
    let temps = names_with(dir, "s.tar");
    assert_eq!(temps.len(), 1, "{temps:?}");
    assert!(std::fs::metadata(dir.join(&temps[0])).unwrap().len() > 0);
    e.signal("TERM");
    assert_eq!(
        e.wait_exit(Duration::from_secs(30)),
        Some(Some(6)),
        "{:#?}",
        e.stderr_lines()
    );
    assert!(e.stdout_lines().is_empty());
    assert!(!out.exists());
    assert!(names_with(dir, "s.tar").is_empty());
}

#[test]
fn evidence_export_rerun_after_sigkill() {
    let fx = Fixture::new();
    laked_and_hot(&fx);
    let out = fx.tmp("k.tar");
    let (mut e, _) = fx.export_gated(&out, "evidence.after_partial_write", &[]);
    e.sigkill();
    let dir = out.parent().unwrap();
    assert!(!out.exists());
    let temps = names_with(dir, "k.tar");
    assert_eq!(temps.len(), 1, "{temps:?}");
    let x = dir.join(&temps[0]);
    let before = std::fs::read(&x).unwrap();
    let r = fx.export(
        &W,
        &[
            "--dataset",
            "default/app",
            "--output",
            out.to_str().unwrap(),
        ],
    );
    assert_eq!(std::fs::read(&x).unwrap(), before);
    assert_eq!(r.code, Some(0), "{}", r.stderr);
    let (d, _) = untar(&out);
    let st = Command::new("sha256sum")
        .args(["-c", "SHA256SUMS"])
        .current_dir(&d)
        .status()
        .unwrap();
    assert!(st.success());
}

#[test]
fn evidence_export_rename_target_taken() {
    let fx = Fixture::new();
    laked_and_hot(&fx);
    let out = fx.tmp("t.tar");
    let (mut e, control) = fx.export_gated(&out, "evidence.after_partial_write", &[]);
    std::fs::write(&out, "keep").unwrap();
    std::fs::write(&control, "go").unwrap();
    assert_eq!(
        e.wait_exit(Duration::from_secs(60)),
        Some(Some(2)),
        "{:#?}",
        e.stderr_lines()
    );
    assert!(e.stderr_lines().join("\n").contains(out.to_str().unwrap()));
    assert!(e.stdout_lines().is_empty());
    assert_eq!(std::fs::read_to_string(&out).unwrap(), "keep");
    assert_eq!(names_with(out.parent().unwrap(), "t.tar"), ["t.tar"]);
}

#[test]
fn evidence_export_unavailable_exit6() {
    let fx = Fixture::new();
    laked_and_hot(&fx);
    let out = fx.tmp("u.tar");
    let faults = fx.root().join("fault-store");
    std::fs::write(&faults, "").unwrap();
    let (mut e, control) = fx.export_gated(
        &out,
        "evidence.after_pin",
        &[("TESTBED_FAULT_STORE", faults.display().to_string())],
    );
    std::fs::write(&faults, "* * 503\n").unwrap();
    std::fs::write(&control, "go").unwrap();
    assert_eq!(
        e.wait_exit(Duration::from_secs(300)),
        Some(Some(6)),
        "{:#?}",
        e.stderr_lines()
    );
    assert!(!e.stderr_lines().is_empty());
    assert!(!out.exists());
    assert!(e.stdout_lines().is_empty());
}

#[test]
fn evidence_export_no_partial() {
    let fx = Fixture::new();
    laked_and_hot(&fx);
    let out = fx.tmp("np.tar");
    let faults = fx.root().join("fault-store");
    std::fs::write(&faults, "").unwrap();
    let (mut e, control) = fx.export_gated(
        &out,
        "evidence.after_partial_write",
        &[("TESTBED_FAULT_STORE", faults.display().to_string())],
    );
    std::fs::write(&faults, "* * 503\n").unwrap();
    std::fs::write(&control, "go").unwrap();
    assert_eq!(
        e.wait_exit(Duration::from_secs(300)),
        Some(Some(6)),
        "{:#?}",
        e.stderr_lines()
    );
    assert!(!out.exists());
    assert!(names_with(out.parent().unwrap(), "np.tar").is_empty());
}

#[test]
fn evidence_export_write_fail_exit1() {
    let fx = Fixture::new();
    laked_and_hot(&fx);
    // 小容量文件系统：以 RLIMIT_FSIZE 把本进程能写的文件大小限在 4 KiB、忽略 SIGXFSZ，写临时文件即失败（EFBIG）；
    // 只改执行方式，不改断言
    let small = fx.tmp("small");
    std::fs::create_dir_all(&small).unwrap();
    let out = small.join("e.tar");
    let mut cmd = fx.evidence_cmd(
        &W,
        "export",
        &[
            "--dataset",
            "default/app",
            "--output",
            out.to_str().unwrap(),
        ],
    );
    common::limit_file_size(&mut cmd, 4096);
    let r: common::evidence::Run = cmd.output().unwrap().into();
    assert_eq!(r.code, Some(1), "{}", r.stderr);
    assert!(r.stderr.contains(small.to_str().unwrap()), "{}", r.stderr);
    assert!(!out.exists());
    assert!(r.stdout.is_empty());
}

// ---------------------------------------------------------------------------
// 入湖、TTL 与外部冷层提交的场景
// ---------------------------------------------------------------------------

use common::evidence::{ExternalIceberg, cold_changes, laked_cold_file, ttl_removed, wait_until};
use common::lake::{COLD_TTL, COLD_TTL_OFFSET_S};

#[test]
fn evidence_export_pinned_vanish_exit6() {
    let fx = Fixture::new();
    let w = fx.start(&w_with(&[&RECYCLE, &[MIN_ORPHAN_1S]]));
    fx.hot_data(&w, 10);
    let ds = fx.dataset("app");
    let out = fx.tmp("e.tar");
    let (mut e, control) = fx.export_gated(&out, "evidence.after_pin", &[]);
    // 钉住的是此刻的快照（W 此后没有摄取，不会提交）；取它所引用的一个桶 manifest，向同一小时桶摄取使它被换掉
    let pinned = ds.head();
    let m = pinned["buckets"][0]["manifest"]
        .as_str()
        .unwrap()
        .to_owned();
    let ts = ds.file_entries(&pinned)[0]["min_timestamp"]
        .as_i64()
        .unwrap();
    commit_until(
        &fx,
        &w,
        ts,
        "钉住的桶 manifest 被快照回收与巡检删除",
        || !ds.exists(&m),
    );
    std::fs::write(&control, "go").unwrap();
    assert_eq!(
        e.wait_exit(Duration::from_secs(300)),
        Some(Some(6)),
        "{:#?}",
        e.stderr_lines()
    );
    let stderr = e.stderr_lines().join("\n");
    assert!(stderr.contains("请重跑"), "{stderr}");
    assert!(stderr.contains(&m), "{stderr}");
    assert!(!out.exists());
    assert!(e.stdout_lines().is_empty());
}

#[test]
fn evidence_export_pin_ignores_later_commits() {
    let fx = Fixture::new();
    let w = fx.start(&W);
    fx.hot_data(&w, 50);
    let out = fx.tmp("e.tar");
    let (mut e, control) = fx.export_gated(&out, "evidence.after_pin", &[]);
    fx.hot_data(&w, 10);
    std::fs::write(&control, "go").unwrap();
    assert_eq!(
        e.wait_exit(Duration::from_secs(60)),
        Some(Some(0)),
        "{:#?}",
        e.stderr_lines()
    );
    let receipt: Value = serde_json::from_str(&e.stdout_lines()[0]).unwrap();
    assert!(
        receipt["snapshot"]["snapshot_id"].as_u64().unwrap() < fx.dataset("app").current().unwrap()
    );
    let (_, files) = untar(&out);
    let sum: u64 = files_lines(&files)
        .iter()
        .filter(|l| l["tier"] == "hot")
        .map(|l| l["record_count"].as_u64().unwrap())
        .sum();
    assert_eq!(sum, 50);
}

#[test]
fn evidence_export_pin_cold_version() {
    let fx = Fixture::new();
    let mut w = fx.start(&W);
    fx.laked(&w);
    stop(&mut w);
    let cold = fx.cold("app");
    let n = cold.version().unwrap();
    let (_, r) = fx.export_app(&W, "e.tar", &[], 0);
    let rel = format!("metadata/v{n}.metadata.json");
    assert_eq!(
        r.json()["cold"],
        json!({"metadata_version": n, "path": rel, "content_digest": sha256(&cold.read(&rel))})
    );
}

#[test]
fn evidence_export_scope_retired_bucket() {
    let fx = Fixture::new();
    let (mut w, retired) = fx.retire_bucket(&W, 10);
    stop(&mut w);
    let (from, to) = (iso(retired.bucket), iso(retired.bucket + HOUR));
    let (p, _) = fx.export_app(&W, "e.tar", &["--from", &from, "--to", &to], 0);
    let (_, files) = untar(&p);
    let label = &retired.label;
    let dir = format!(
        "data/{}/{}/{}/{}/",
        &label[0..4],
        &label[4..6],
        &label[6..8],
        &label[8..10]
    );
    let source = format!("default/app/{}", retired.manifest);
    let lines: Vec<Value> = files_lines(&files)
        .into_iter()
        .filter(|l| {
            l["tier"] == "hot"
                && l["state"] == "deleted"
                && l["deletion"]["reason"] == "lake"
                && l["path"].as_str().unwrap().starts_with(&dir)
        })
        .collect();
    assert!(!lines.is_empty(), "{:?}", files_lines(&files));
    assert!(lines.iter().all(|l| l["source"] == source.as_str()));
    assert!(files.contains_key(&format!("objects/{source}")));
}

#[test]
fn evidence_export_scope_cold() {
    let fx = Fixture::new();
    let (mut w, _) = ttl_removed(&fx, &[]);
    let b = hour_of(now_us()) - 2 * HOUR;
    let recs: Vec<String> = (0..10)
        .map(|i| format!(r#"{{"_timestamp":{},"seq":{i}}}"#, b + 1 + i))
        .collect();
    common::api::ingest_ok(&w, "app", &format!("[{}]", recs.join(",")));
    let m = fx.dataset("app").wait_handovers(
        &common::store::bucket_label(b),
        1,
        common::evidence::LAKE_LIMIT,
    );
    stop(&mut w);
    let (p, _) = fx.export_app(&W, "e.tar", &[], 0);
    let (_, files) = untar(&p);
    let lines = files_lines(&files);
    assert!(lines.iter().any(|l| l["tier"] == "cold"
        && l["state"] == "deleted"
        && l["deletion"]["reason"] == "ttl"
        && l["deletion"]["cutoff"] != Value::Null));
    let h = &m["handovers"][0];
    let f = &h["files"][0];
    let live = lines
        .iter()
        .find(|l| l["tier"] == "cold" && l["state"] == "live" && l["path"] == f["path"])
        .expect("冷层存活行");
    assert_eq!(live["content_digest"], f["content_digest"]);
    assert_eq!(live["added_in"], h["snapshot_id"]);
}

#[test]
fn evidence_export_scope_cold_range() {
    let fx = Fixture::new();
    let t = hour_of(now_us());
    let mut w = fx.start(&W);
    fx.laked_at(&w, t - 4 * HOUR, 10);
    fx.laked_at(&w, t - 3 * HOUR, 10);
    stop(&mut w);
    let (from, to) = (t - 4 * HOUR, t - 3 * HOUR);
    let (p, _) = fx.export_app(&W, "e.tar", &["--from", &iso(from), "--to", &iso(to)], 0);
    let (_, files) = untar(&p);
    let cold: Vec<Value> = files_lines(&files)
        .into_iter()
        .filter(|l| l["tier"] == "cold")
        .collect();
    assert!(!cold.is_empty());
    for l in &cold {
        assert!(l["min_timestamp"].as_i64().unwrap() >= from, "{l}");
        assert!(l["max_timestamp"].as_i64().unwrap() < to, "{l}");
    }
    assert_eq!(
        cold.iter()
            .map(|l| l["record_count"].as_u64().unwrap())
            .sum::<u64>(),
        10
    );
}

#[test]
fn evidence_export_handover_window() {
    let fx = Fixture::new();
    let (w, gate) = fx.start_gated(&W, "lake.after_cold_commit");
    let b = hour_of(now_us()) - 3 * HOUR;
    let recs: Vec<String> = (0..10)
        .map(|i| format!(r#"{{"_timestamp":{},"seq":{i}}}"#, b + 1 + i))
        .collect();
    common::api::ingest_ok(&w, "app", &format!("[{}]", recs.join(",")));
    gate.wait_held(&w, Duration::from_secs(180));
    let (p, _) = fx.export_app(&W, "e.tar", &[], 0);
    let (_, files) = untar(&p);
    let c = member_json(&files, "check.json");
    let window = objects_of(&c, "transitions", "handover_window");
    assert!(!window.is_empty(), "{c}");
    let lines = files_lines(&files);
    for o in &window {
        let rel = o.strip_prefix("_iceberg/default/app/").unwrap();
        assert!(
            !lines
                .iter()
                .any(|l| l["tier"] == "cold" && l["path"] == rel)
        );
    }
    let dir = {
        let l = common::store::bucket_label(b);
        format!("data/{}/{}/{}/{}/", &l[0..4], &l[4..6], &l[6..8], &l[8..10])
    };
    assert!(lines.iter().any(|l| l["tier"] == "hot"
        && l["state"] == "live"
        && l["path"].as_str().unwrap().starts_with(&dir)));
    gate.release();
}

#[test]
fn evidence_export_cold_segment_pending() {
    let fx = Fixture::new();
    let control = fx.root().join("gate-ttl");
    std::fs::write(&control, "hold").unwrap();
    let (w, g) = ttl_removed(
        &fx,
        &[
            ("TESTBED_FAULT_GATE", "ttl.after_cold_commit".to_owned()),
            ("TESTBED_FAULT_GATE_FILE", control.display().to_string()),
        ],
    );
    assert!(
        common::wait_file_content(&control, "held", Duration::from_secs(180)),
        "TTL 未停在暂停点：{:#?}",
        w.stderr_lines()
    );
    let (p, _) = fx.export_app(&W, "e.tar", &[], 0);
    let (_, files) = untar(&p);
    let c = member_json(&files, "check.json");
    assert!(
        objects_of(&c, "transitions", "cold_segment_pending")
            .contains(&format!("_iceberg/default/app/{g}")),
        "{c}"
    );
    assert!(
        !files_lines(&files)
            .iter()
            .any(|l| l["tier"] == "cold" && l["path"] == g.as_str())
    );
    std::fs::write(&control, "go").unwrap();
}

#[test]
fn evidence_export_iceberg_snapshot_map() {
    let fx = Fixture::new();
    let (mut w, _) = ttl_removed(&fx, &[]);
    stop(&mut w);
    let (p, _) = fx.export_app(&W, "e.tar", &[], 0);
    let (_, files) = untar(&p);
    let e = member_json(&files, "evidence.json");
    let removed_in = cold_changes(&fx)
        .iter()
        .find(|d| d["reason"] == "ttl")
        .unwrap()["cold_removed_in"]
        .clone();
    let el = e["iceberg_snapshots"]
        .as_array()
        .unwrap()
        .iter()
        .find(|x| x["iceberg_snapshot_id"] == removed_in)
        .unwrap_or_else(|| panic!("{e}"))
        .clone();
    assert_eq!(el["operation"], "ttl");
    let n = el["metadata_version"].as_u64().unwrap();
    let cold = fx.cold("app");
    let has = |k: u64| {
        common::lake::ColdDir::snapshots(&cold.metadata(k))
            .iter()
            .any(|s| s["snapshot-id"] == removed_in)
    };
    assert!(has(n));
    if n >= 2 {
        assert!(!has(n - 1));
    }
}

#[test]
fn evidence_export_files_lake_handover() {
    let fx = Fixture::new();
    let mut w = fx.start(&W);
    fx.laked(&w);
    stop(&mut w);
    let (p, _) = fx.export_app(&W, "e.tar", &[], 0);
    let (_, files) = untar(&p);
    let lines = files_lines(&files);
    let lake: Vec<&Value> = lines
        .iter()
        .filter(|l| l["deletion"]["reason"] == "lake")
        .collect();
    assert!(!lake.is_empty());
    for l in lake {
        let hf = l["deletion"]["handover_files"].as_array().unwrap();
        assert!(!hf.is_empty());
        for k in ["record_count", "size_bytes", "min_timestamp"] {
            assert_eq!(l[k], Value::Null);
        }
        for f in hf {
            assert!(lines.iter().any(|c| c["tier"] == "cold" && c["path"] == *f));
        }
    }
}

#[test]
fn evidence_export_files_cold() {
    let fx = Fixture::new();
    let mut w = fx.start(&W);
    fx.laked(&w);
    stop(&mut w);
    let (p, _) = fx.export_app(&W, "e.tar", &[], 0);
    let (_, files) = untar(&p);
    let base = fx.store_dir().join("amberlog");
    let cold: Vec<Value> = files_lines(&files)
        .into_iter()
        .filter(|l| l["tier"] == "cold" && l["state"] == "live")
        .collect();
    assert!(!cold.is_empty());
    for l in cold {
        let m: Value = serde_json::from_slice(
            &std::fs::read(base.join(l["source"].as_str().unwrap())).unwrap(),
        )
        .unwrap();
        let (h, f) = m["handovers"]
            .as_array()
            .unwrap()
            .iter()
            .find_map(|h| {
                h["files"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|f| f["path"] == l["path"])
                    .map(|f| (h.clone(), f.clone()))
            })
            .expect("交接记录中有该文件");
        for k in [
            "content_digest",
            "record_count",
            "size_bytes",
            "min_timestamp",
            "max_timestamp",
        ] {
            assert_eq!(l[k], f[k], "{k}");
        }
        assert_eq!(l["added_in"], h["snapshot_id"]);
    }
}

/// 两段冷层变更段：两个小时桶先后被 TTL 移出（以两档时钟偏移重启）。
#[test]
fn evidence_export_segment_chain_broken() {
    let fx = Fixture::new();
    let start = now_us() - 5 * HOUR - 50 * 60_000_000;
    let mut s = fx.start(&COLD_TTL);
    for (k, b) in [start, start + HOUR].into_iter().enumerate() {
        let recs: Vec<String> = (0..5)
            .map(|i| {
                format!(
                    r#"{{"_timestamp":{},"seq":{}}}"#,
                    b + i,
                    k * 10 + i as usize
                )
            })
            .collect();
        common::api::ingest_ok(&s, "app", &format!("[{}]", recs.join(",")));
        fx.dataset("app").wait_handovers(
            &common::store::bucket_label(b),
            1,
            common::evidence::LAKE_LIMIT,
        );
    }
    stop(&mut s);
    for offset in [COLD_TTL_OFFSET_S, COLD_TTL_OFFSET_S + 3600] {
        let mut s = fx.start_offset(&COLD_TTL, offset, &[]);
        let want = if offset == COLD_TTL_OFFSET_S { 1 } else { 2 };
        wait_until(Duration::from_secs(180), "冷层变更段", || {
            let head = fx.dataset("app").head();
            fx.dataset("app")
                .segments(&head["cold_deletion_segment"])
                .len()
                >= want
        });
        stop(&mut s);
    }
    let ds = fx.dataset("app");
    let head = ds.head();
    let newest = ds.json(head["cold_deletion_segment"]["path"].as_str().unwrap());
    let s_rel = newest["previous"]["path"].as_str().unwrap().to_owned();
    ds.remove(&s_rel);
    let (p, _) = fx.export_app(&W, "e.tar", &[], 5);
    let (_, files) = untar(&p);
    let c = member_json(&files, "check.json");
    assert!(
        objects_of(&c, "problems", "missing").contains(&format!("default/app/{s_rel}")),
        "{c}"
    );
}

#[test]
fn evidence_export_live_missing_untraced() {
    let fx = Fixture::new();
    let g = laked_cold_file(&fx);
    std::fs::remove_file(fx.cold("app").path(&g)).unwrap();
    let (p, _) = fx.export_app(&W, "e.tar", &[], 5);
    let (_, files) = untar(&p);
    let c = member_json(&files, "check.json");
    assert!(
        objects_of(&c, "problems", "missing").contains(&format!("_iceberg/default/app/{g}")),
        "{c}"
    );
}

#[test]
fn evidence_export_left_live_during_export() {
    let fx = Fixture::new();
    let b = hour_of(now_us()) - 3 * HOUR;
    // 先以较大的入湖水位写入 B，使它在导出钉住链头之后才入湖
    let late = [
        ("lifecycle.lake_watermark", Some("\"6h\"")),
        ("storage.delete_delay", Some("\"1s\"")),
        common::store::FLUSH_1S,
    ];
    let mut w = fx.start(&late);
    let recs: Vec<String> = (0..10)
        .map(|i| format!(r#"{{"_timestamp":{},"seq":{i}}}"#, b + 1 + i))
        .collect();
    common::api::ingest_ok(&w, "app", &format!("[{}]", recs.join(",")));
    fx.dataset("app").wait_records(10, Duration::from_secs(60));
    stop(&mut w);
    let label = common::store::bucket_label(b);
    let hot: Vec<String> = fx.dataset("app").bucket_now(&label).unwrap()["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["path"].as_str().unwrap().to_owned())
        .collect();
    let out = fx.tmp("e.tar");
    let (mut e, control) = fx.export_gated(&out, "evidence.after_pin", &[]);
    let mut w = fx.start(&[
        ("lifecycle.lake_watermark", Some("\"1h\"")),
        ("storage.delete_delay", Some("\"1s\"")),
        common::store::FLUSH_1S,
    ]);
    let ds = fx.dataset("app");
    wait_until(
        Duration::from_secs(300),
        "B 入湖并物理删除热层文件",
        || hot.iter().all(|f| !ds.exists(f)),
    );
    stop(&mut w);
    std::fs::write(&control, "go").unwrap();
    assert_eq!(
        e.wait_exit(Duration::from_secs(60)),
        Some(Some(0)),
        "{:#?}",
        e.stderr_lines()
    );
    let (_, files) = untar(&out);
    let c = member_json(&files, "check.json");
    assert_eq!(c["problems"], json!([]), "{c}");
    let left = objects_of(&c, "transitions", "left_live");
    let lines = files_lines(&files);
    for f in &hot {
        assert!(left.contains(&format!("default/app/{f}")), "{c}");
        assert!(
            lines
                .iter()
                .any(|l| l["path"] == f.as_str() && l["state"] == "live")
        );
    }
}

#[test]
fn evidence_export_cold_without_record() {
    let fx = Fixture::new();
    let g = laked_cold_file(&fx);
    let x = ExternalIceberg::new(&fx, "app").append_copy(&g);
    let (p, _) = fx.export_app(&W, "e.tar", &[], 5);
    let (_, files) = untar(&p);
    let c = member_json(&files, "check.json");
    assert!(
        objects_of(&c, "problems", "no_write_record")
            .contains(&format!("_iceberg/default/app/{x}")),
        "{c}"
    );
    let cold = fx.cold("app");
    let current = cold.current();
    let sid = current["current-snapshot-id"].clone();
    let e = member_json(&files, "evidence.json");
    let el = e["iceberg_snapshots"]
        .as_array()
        .unwrap()
        .iter()
        .find(|x| x["iceberg_snapshot_id"] == sid)
        .unwrap_or_else(|| panic!("{e}"))
        .clone();
    assert_eq!(el["operation"], Value::Null);
}

#[test]
fn evidence_export_untraced_cold_removal() {
    let fx = Fixture::new();
    let g = laked_cold_file(&fx);
    ExternalIceberg::new(&fx, "app").remove(&[&g]);
    std::fs::remove_file(fx.cold("app").path(&g)).unwrap();
    let (p, _) = fx.export_app(&W, "e.tar", &[], 5);
    let (_, files) = untar(&p);
    let c = member_json(&files, "check.json");
    assert!(
        objects_of(&c, "problems", "untraced_removal")
            .contains(&format!("_iceberg/default/app/{g}")),
        "{c}"
    );
}

#[test]
fn evidence_export_lake_without_handover() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let ds = fx.dataset("app");
    let id = ds.current().unwrap();
    let mut snap = ds.snapshot(id);
    let bref = snap["buckets"][0].clone();
    let rel = bref["manifest"].as_str().unwrap().to_owned();
    // 存活文件 F 取自还原所得；头片为全量片时另从其 `files` 中删去 F 的条目（增量片的还原按 `deletions` 去掉它）
    let f = ds.restore(&rel)["files"][0].clone();
    let mut m = ds.json(&rel);
    if m["kind"] == "full" {
        m["files"]
            .as_array_mut()
            .unwrap()
            .retain(|x| x["path"] != f["path"]);
    }
    m["deletions"].as_array_mut().unwrap().push(json!({
        "path": f["path"],
        "content_digest": f["content_digest"],
        "reason": "lake",
        "added_in": f["added_in"],
        "max_timestamp": f["max_timestamp"],
        "cutoff": null,
        "deleted_at": now_us(),
        "removed_in": id,
    }));
    ds.write_json(&rel, &m);
    snap["buckets"][0]["content_digest"] = json!(sha256(&ds.read(&rel)));
    ds.write_json(&snapshot_rel(id), &snap);
    let (p, _) = fx.export_app(&W, "e.tar", &[], 5);
    let (_, files) = untar(&p);
    let c = member_json(&files, "check.json");
    assert!(
        objects_of(&c, "problems", "lake_without_handover")
            .contains(&format!("default/app/{}", f["path"].as_str().unwrap())),
        "{c}"
    );
}

#[test]
fn evidence_export_live_after_removal() {
    let fx = Fixture::new();
    let (mut w, g) = ttl_removed(&fx, &[]);
    stop(&mut w);
    assert!(fx.cold("app").exists(&g), "G 尚未物理删除");
    ExternalIceberg::new(&fx, "app").append_existing(&g);
    let (p, _) = fx.export_app(&W, "e.tar", &[], 5);
    let (_, files) = untar(&p);
    let c = member_json(&files, "check.json");
    assert!(
        objects_of(&c, "problems", "live_after_removal")
            .contains(&format!("_iceberg/default/app/{g}")),
        "{c}"
    );
    let rows: Vec<Value> = files_lines(&files)
        .into_iter()
        .filter(|l| l["tier"] == "cold" && l["path"] == g.as_str())
        .collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["state"], "deleted");
}

#[test]
fn evidence_export_no_content_read() {
    let fx = Fixture::new();
    let g = laked_cold_file(&fx);
    let path = fx.cold("app").path(&g);
    let mut bytes = std::fs::read(&path).unwrap();
    // 同大小、不同内容：改动中间的一个字节
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xff;
    std::fs::write(&path, &bytes).unwrap();
    let log = fx.root().join("store.log");
    let mut cmd = fx.evidence_cmd(
        &W,
        "export",
        &[
            "--dataset",
            "default/app",
            "--output",
            fx.tmp("e.tar").to_str().unwrap(),
        ],
    );
    cmd.env("TESTBED_FAULT_STORE_LOG", &log);
    let r: common::evidence::Run = cmd.output().unwrap().into();
    assert_eq!(r.code, Some(0), "{}", r.stderr);
    let (_, files) = untar(&fx.tmp("e.tar"));
    assert_eq!(member_json(&files, "check.json")["problems"], json!([]));
    let key = format!("_iceberg/default/app/{g}");
    let lines = std::fs::read_to_string(&log).unwrap();
    let ops: Vec<&str> = lines
        .lines()
        .filter(|l| l.split_once(' ').is_some_and(|(_, k)| k == key))
        .map(|l| l.split_once(' ').unwrap().0)
        .collect();
    assert!(ops.contains(&"head"), "{ops:?}");
    assert!(!ops.contains(&"get"), "{ops:?}");
}

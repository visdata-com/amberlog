//! REQ-AMB-STO-EVIDENCE-VERIFY-V2 的系统测试：以子进程运行写入实例
//! `amberlog serve`、`amberlog evidence export` 与 `amberlog evidence verify`，对象存储为 `file://` 临时目录，测试直接读、改、
//! 删其中的对象构造篡改。场景里的 `/tmp/<名>` 一律落在每个测试自己的临时目录之下。

mod common;

use std::path::{Path, PathBuf};
use std::time::Duration;

use common::api::{ingest_ok, now_us};
use common::evidence::{
    ExternalIceberg, MIN_ORPHAN_1S, RECYCLE, W, commit_until, edit_json, edit_lines,
    laked_cold_file, member_json, receipt_snapshot_id, repack, report_objects, ttl_removed, untar,
    w_with, wait_until,
};
use common::store::{Ds, FLUSH_1S, HOUR, bucket_label, hour_of, sha256, snapshot_rel, stop};
use common::{Fixture, Kv};
use serde_json::{Value, json};

/// 「stream `app` 有热层数据，W 已停止」。
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

/// 导出 `default/app` 得到证据包 P 与回执文件 R。
fn export_pr(fx: &Fixture, name: &str, want: i32) -> (PathBuf, PathBuf) {
    let (p, r) = fx.export_app(&W, &format!("{name}.tar"), &[], want);
    let receipt = fx.tmp(&format!("{name}.receipt"));
    std::fs::write(&receipt, &r.stdout).unwrap();
    (p, receipt)
}

fn s(p: &Path) -> &str {
    p.to_str().unwrap()
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

/// 报告数组中各元素的 `kind`。
fn kinds(report: &Value, array: &str) -> Vec<String> {
    report[array]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["kind"].as_str().unwrap().to_owned())
        .collect()
}

/// 当前快照中桶 `label` 的桶 manifest 路径（相对数据集前缀）。
fn manifest_rel(ds: &Ds, label: &str) -> String {
    ds.head()["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|b| b["bucket"] == label)
        .unwrap()["manifest"]
        .as_str()
        .unwrap()
        .to_owned()
}

/// 重写快照 `from` 起到当前快照：每个先由 `edit` 改，再按父快照的新字节重算 `parent_digest`（编号不变）。
fn rewrite_snapshots(ds: &Ds, from: u64, mut edit: impl FnMut(&mut Value)) {
    let cur = ds.current().unwrap();
    let mut parent: Option<String> = None;
    for id in from..=cur {
        let mut v = ds.snapshot(id);
        edit(&mut v);
        if let Some(d) = &parent {
            v["parent_digest"] = json!(d);
        }
        ds.write_json(&snapshot_rel(id), &v);
        parent = Some(sha256(&ds.read(&snapshot_rel(id))));
    }
}

/// 改写桶 manifest `rel`，并据此重写引用它的快照及其后的整段快照链（摘要自洽，编号不变）。
fn rewrite_manifest_and_chain(ds: &Ds, rel: &str, edit: impl FnOnce(&mut Value)) {
    let first = (1..=ds.current().unwrap())
        .find(|id| {
            ds.exists(&snapshot_rel(*id))
                && ds.snapshot(*id)["buckets"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|b| b["manifest"] == rel)
        })
        .expect("有快照引用该桶 manifest");
    let mut m = ds.json(rel);
    edit(&mut m);
    ds.write_json(rel, &m);
    let digest = sha256(&ds.read(rel));
    rewrite_snapshots(ds, first, |s| {
        for b in s["buckets"].as_array_mut().unwrap() {
            if b["manifest"] == rel {
                b["content_digest"] = json!(digest);
            }
        }
    });
}

/// 改写增量链上的清单片 `chain` 的最后一片（`edit`），再自它起逐片重算重写经 `previous` 引用它的各片（`chain` 自头片起
/// 依次排列），并据此重写引用其中任一片的快照及其后的整段快照链（摘要自洽，编号不变）。
fn rewrite_pieces_and_chain(ds: &Ds, chain: &[String], edit: impl FnOnce(&mut Value)) {
    let mut digests: std::collections::BTreeMap<String, String> = Default::default();
    let (last, newer) = chain.split_last().unwrap();
    let mut m = ds.json(last);
    edit(&mut m);
    ds.write_json(last, &m);
    digests.insert(last.clone(), sha256(&ds.read(last)));
    let mut previous = last.clone();
    for rel in newer.iter().rev() {
        let mut p = ds.json(rel);
        p["previous"]["content_digest"] = json!(digests[&previous]);
        ds.write_json(rel, &p);
        digests.insert(rel.clone(), sha256(&ds.read(rel)));
        previous = rel.clone();
    }
    let first = (1..=ds.current().unwrap())
        .find(|id| {
            ds.exists(&snapshot_rel(*id))
                && ds.snapshot(*id)["buckets"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|b| digests.contains_key(b["manifest"].as_str().unwrap()))
        })
        .expect("有快照引用这些清单片");
    rewrite_snapshots(ds, first, |s| {
        for b in s["buckets"].as_array_mut().unwrap() {
            if let Some(d) = digests.get(b["manifest"].as_str().unwrap()) {
                b["content_digest"] = json!(d);
            }
        }
    });
}

// ---------------------------------------------------------------------------
// A · 子命令与参数
// ---------------------------------------------------------------------------

#[test]
fn evidence_verify_read_only_creds() {
    let fx = Fixture::new();
    laked_and_hot(&fx);
    let (p, _) = export_pr(&fx, "p", 0);
    // 只读凭据：本进程对 `<storage.prefix>/` 之下的写入与删除一律被拒（403），读取与列举照常
    let faults = fx.root().join("readonly-rules");
    std::fs::write(&faults, "write,delete * 403\n").unwrap();
    let before = store_digests(&fx);
    let pd = sha256(&std::fs::read(&p).unwrap());
    let mut cmd = fx.evidence_cmd(&W, "verify", &["--evidence", s(&p)]);
    cmd.env("TESTBED_FAULT_STORE", &faults);
    let r: common::evidence::Run = cmd.output().unwrap().into();
    assert_eq!(r.code, Some(0), "{}\n{}", r.stderr, r.stdout_text());
    assert_eq!(r.json()["result"], "pass");
    assert_eq!(store_digests(&fx), before);
    assert_eq!(sha256(&std::fs::read(&p).unwrap()), pd);
}

#[test]
fn evidence_verify_no_wal_dir() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let (p, _) = export_pr(&fx, "p", 0);
    let d = fx.root().join("no-such-wal");
    let wal = format!("\"{}\"", d.display());
    let mut conf: Vec<Kv> = W.to_vec();
    conf.push(("wal.dir", Some(&wal)));
    let r = fx.verify_ok(&conf, &p, &[], 0);
    assert_eq!(r["result"], "pass");
    assert!(!d.exists());
}

#[test]
fn evidence_verify_args_usage() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let (p, _) = export_pr(&fx, "p", 0);
    for args in [vec![], vec!["--evidence", s(&p), "--strict"]] {
        let r = fx.verify(&W, &args);
        assert_eq!(r.code, Some(2), "{args:?}");
        assert!(!r.stderr.is_empty());
        assert!(r.stdout.is_empty());
    }
}

#[test]
fn evidence_verify_file_missing() {
    let fx = Fixture::new();
    let nosuch = fx.tmp("nosuch.tar");
    let r = fx.verify(&W, &["--evidence", s(&nosuch)]);
    assert_eq!(r.code, Some(2));
    assert!(
        r.stderr.contains("--evidence") && r.stderr.contains(s(&nosuch)),
        "{}",
        r.stderr
    );
}

#[test]
fn evidence_verify_not_a_package() {
    let fx = Fixture::new();
    let dir = fx.tmp("x");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("a.txt"), "a").unwrap();
    let x = fx.tmp("x.tar");
    let st = std::process::Command::new("tar")
        .arg("-cf")
        .arg(&x)
        .arg("-C")
        .arg(&dir)
        .arg("a.txt")
        .status()
        .unwrap();
    assert!(st.success());
    let r = fx.verify(&W, &["--evidence", s(&x)]);
    assert_eq!(r.code, Some(2));
    assert!(r.stderr.contains("--evidence"), "{}", r.stderr);
}

#[test]
fn evidence_verify_previous_kind() {
    let fx = Fixture::new();
    let mut w = fx.start(&W);
    fx.hot_data(&w, 20);
    let (p1, r1) = export_pr(&fx, "p1", 0);
    fx.hot_data(&w, 10);
    stop(&mut w);
    let (p2, _) = export_pr(&fx, "p2", 0);
    let junk = fx.tmp("junk");
    std::fs::write(&junk, "hello").unwrap();
    for (prev, kind) in [(&r1, "receipt"), (&p1, "package")] {
        let rep = fx.verify_ok(&W, &p2, &["--previous", s(prev)], 0);
        let a: Vec<&Value> = rep["anchors"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|a| a["source"] == "previous")
            .collect();
        assert_eq!(a.len(), 1);
        assert_eq!(a[0]["kind"], kind);
    }
    let r = fx.verify(&W, &["--evidence", s(&p2), "--previous", s(&junk)]);
    assert_eq!(r.code, Some(2));
    assert!(r.stderr.contains("--previous"), "{}", r.stderr);
}

#[test]
fn evidence_verify_dataset_mismatch() {
    let fx = Fixture::new();
    let mut w = fx.start(&W);
    fx.hot_data(&w, 10);
    ingest_ok(&w, "web", &format!(r#"[{{"_timestamp":{}}}]"#, now_us()));
    fx.dataset("web").wait_records(1, Duration::from_secs(60));
    stop(&mut w);
    let (p, _) = export_pr(&fx, "p", 0);
    let rw = fx.tmp("rw.tar");
    let r = fx.export(&W, &["--dataset", "default/web", "--output", s(&rw)]);
    assert_eq!(r.code, Some(0), "{}", r.stderr);
    let receipt = fx.tmp("rw.receipt");
    std::fs::write(&receipt, &r.stdout).unwrap();
    let r = fx.verify(&W, &["--evidence", s(&p), "--previous", s(&receipt)]);
    assert_eq!(r.code, Some(2));
    assert!(
        r.stderr.contains("default/app") && r.stderr.contains("default/web"),
        "{}",
        r.stderr
    );
}

#[test]
fn evidence_verify_retention_arg() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let (p, _) = export_pr(&fx, "p", 0);
    for v in ["3", "0h", "1.5h"] {
        let r = fx.verify(&W, &["--evidence", s(&p), "--retention", v]);
        assert_eq!(r.code, Some(2), "{v}");
        assert!(r.stderr.contains("--retention"), "{v}: {}", r.stderr);
    }
}

#[test]
fn evidence_verify_args_before_probe() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let (p, _) = export_pr(&fx, "p", 0);
    std::fs::rename(fx.store_dir(), fx.root().join("store-gone")).unwrap();
    let r = fx.verify(&W, &["--evidence", s(&fx.tmp("nosuch.tar"))]);
    assert_eq!(r.code, Some(2), "{}", r.stderr);
    let r = fx.verify(&W, &["--evidence", s(&p)]);
    assert_eq!(r.code, Some(3), "{}", r.stderr);
}

#[test]
fn evidence_verify_format_version_higher() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let (p, _) = export_pr(&fx, "p", 0);
    let p2 = repack(&p, "p2.tar", true, |d| {
        edit_json(d, "evidence.json", |v| {
            v["evidence_format_version"] = json!(2)
        });
    });
    let rep = fx.verify_ok(&W, &p2, &[], 6);
    assert_eq!(rep["result"], "unprovable");
    assert!(
        kinds(&rep, "unprovable").contains(&"format_unsupported".to_owned()),
        "{rep}"
    );
    assert_eq!(rep["evidence_exported_at"], Value::Null);
    assert_eq!(rep["current"], Value::Null);
    assert_eq!(
        rep["checked"],
        json!({"data_files": 0, "digests_recomputed": 0})
    );
}

#[test]
fn evidence_verify_package_copy_format_v2() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let (p, r) = export_pr(&fx, "p", 0);
    let receipt: Value = serde_json::from_slice(&std::fs::read(&r).unwrap()).unwrap();
    let member = format!(
        "objects/default/app/{}",
        receipt["snapshot"]["path"].as_str().unwrap()
    );
    let p2 = repack(&p, "p2.tar", true, |d| {
        edit_json(d, &member, |v| v["format_version"] = json!(2));
    });
    let rep = fx.verify_ok(&W, &p2, &[], 6);
    assert!(
        report_objects(&rep, "unprovable", "format_unsupported")
            .contains(&format!("--evidence:{member}")),
        "{rep}"
    );
    assert_eq!(
        rep["checked"],
        json!({"data_files": 0, "digests_recomputed": 0})
    );
}

#[test]
fn evidence_verify_current_format_v2() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let (p, _) = export_pr(&fx, "p", 0);
    // 较早版本写出的第 2 版当前快照
    let old2 = fx.dataset("old2");
    old2.write(
        &snapshot_rel(1),
        json!({
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
        })
        .to_string()
        .as_bytes(),
    );
    old2.write("manifest/_current", br#"{"snapshot_id": 1}"#);
    let p2 = repack(&p, "p2.tar", true, |d| {
        edit_json(d, "evidence.json", |v| {
            v["dataset"] = json!("default/old2");
            v["receipt"]["dataset"] = json!("default/old2");
        });
        edit_json(d, "receipt.json", |v| v["dataset"] = json!("default/old2"));
    });
    let rep = fx.verify_ok(&W, &p2, &[], 6);
    assert!(
        report_objects(&rep, "unprovable", "format_unsupported")
            .contains(&format!("default/old2/{}", snapshot_rel(1))),
        "{rep}"
    );
}

// ---------------------------------------------------------------------------
// B · 证据包自身
// ---------------------------------------------------------------------------

#[test]
fn evidence_verify_package_sums_mismatch() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let (p, _) = export_pr(&fx, "p", 0);
    let p2 = repack(&p, "p2.tar", false, |d| {
        let mut first = true;
        edit_lines(d, |l| {
            if std::mem::take(&mut first) {
                l["content_digest"] = json!(format!("sha256:{}", "0".repeat(64)));
            }
        });
    });
    let rep = fx.verify_ok(&W, &p2, &[], 5);
    assert!(
        report_objects(&rep, "problems", "package_digest_mismatch")
            .contains(&"--evidence:files.jsonl".to_owned()),
        "{rep}"
    );
}

#[test]
fn evidence_verify_package_inconsistent() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let (p, _) = export_pr(&fx, "p", 0);
    let p2 = repack(&p, "p2.tar", true, |d| {
        edit_json(d, "receipt.json", |v| {
            v["exported_at"] = json!(v["exported_at"].as_i64().unwrap() + 1);
        });
    });
    let rep = fx.verify_ok(&W, &p2, &[], 5);
    assert!(
        kinds(&rep, "problems").contains(&"package_inconsistent".to_owned()),
        "{rep}"
    );
}

// ---------------------------------------------------------------------------
// C · 链的延长
// ---------------------------------------------------------------------------

#[test]
fn evidence_verify_current_snapshot_broken() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let (p, _) = export_pr(&fx, "p", 0);
    let ds = fx.dataset("app");
    let rel = snapshot_rel(ds.current().unwrap());
    ds.write(&rel, b"not json");
    let rep = fx.verify_ok(&W, &p, &[], 5);
    assert_eq!(rep["current"], Value::Null);
    assert!(
        report_objects(&rep, "problems", "chain_broken").contains(&format!("default/app/{rel}")),
        "{rep}"
    );
}

#[test]
fn evidence_verify_dataset_prefix_deleted() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let (p, _) = export_pr(&fx, "p", 0);
    std::fs::remove_dir_all(fx.store_dir().join("amberlog/default/app")).unwrap();
    let rep = fx.verify_ok(&W, &p, &[], 5);
    assert!(
        report_objects(&rep, "problems", "missing").contains(&"default/app/manifest/".to_owned()),
        "{rep}"
    );
}

#[test]
fn evidence_verify_hint_unusable() {
    let fx = Fixture::new();
    let mut w = fx.start(&W);
    fx.hot_data(&w, 25);
    fx.hot_data(&w, 25);
    stop(&mut w);
    let (p, _) = export_pr(&fx, "p", 0);
    let ds = fx.dataset("app");
    assert!(ds.current().unwrap() >= 2);
    ds.write("manifest/_current", b"not json");
    ds.remove(&snapshot_rel(1));
    let rep = fx.verify_ok(&W, &p, &[], 5);
    assert_eq!(rep["current"], Value::Null);
    assert!(
        report_objects(&rep, "problems", "chain_broken")
            .contains(&"default/app/manifest/_current".to_owned()),
        "{rep}"
    );
}

#[test]
fn evidence_verify_current_format_higher() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let (p, _) = export_pr(&fx, "p", 0);
    let ds = fx.dataset("app");
    let next = ds.current().unwrap() + 1;
    ds.write_json(
        &snapshot_rel(next),
        &json!({"format_version": 5, "snapshot_id": next}),
    );
    let rep = fx.verify_ok(&W, &p, &[], 6);
    assert!(
        report_objects(&rep, "unprovable", "format_unsupported")
            .contains(&format!("default/app/{}", snapshot_rel(next))),
        "{rep}"
    );
}

#[test]
fn evidence_verify_pass_while_serving() {
    let fx = Fixture::new();
    let w = fx.start(&W);
    fx.laked(&w);
    fx.hot_data(&w, 50);
    let (p, r) = export_pr(&fx, "p", 0);
    fx.hot_data(&w, 20);
    let rep = fx.verify_ok(&W, &p, &[], 0);
    assert_eq!(rep["result"], "pass");
    let receipt: Value = serde_json::from_slice(&std::fs::read(r).unwrap()).unwrap();
    assert!(
        rep["current"]["snapshot"]["snapshot_id"].as_u64()
            > receipt["snapshot"]["snapshot_id"].as_u64()
    );
}

#[test]
fn evidence_verify_chain_rewrite_needs_receipt() {
    let fx = Fixture::new();
    let label = laked_and_hot(&fx);
    let (_, r1) = export_pr(&fx, "p1", 0);
    let ds = fx.dataset("app");
    let rel = manifest_rel(&ds, &label);
    // 入湖使桶变空：头片是合拢全量片，交接记录全在它的 `handovers` 里
    assert_eq!(ds.json(&rel)["kind"], "full");
    let g = ds.json(&rel)["handovers"][0]["files"][0]["path"]
        .as_str()
        .unwrap()
        .to_owned();
    // 把冷层数据文件 G 换成另一个合法文件
    let gpath = fx.cold("app").path(&g);
    common::lake::alter_parquet_rows(&gpath);
    let bytes = std::fs::read(&gpath).unwrap();
    let (digest, size) = (sha256(&bytes), bytes.len());
    // 据此重写冷层元数据文件的摘要属性、桶 manifest 的交接记录与其后的整段快照链
    let cold = fx.cold("app");
    for k in 1..=cold.version().unwrap() {
        let rel_v = format!("metadata/v{k}.metadata.json");
        let mut m = cold.metadata(k);
        for snap in m["snapshots"].as_array_mut().unwrap() {
            if let Some(fd) = snap["summary"]["amberlog.file_digests"].as_str() {
                let mut map: serde_json::Map<String, Value> = serde_json::from_str(fd).unwrap();
                if map.contains_key(&g) {
                    map.insert(g.clone(), json!(digest));
                    snap["summary"]["amberlog.file_digests"] =
                        json!(serde_json::to_string(&map).unwrap());
                }
            }
        }
        std::fs::write(cold.path(&rel_v), serde_json::to_vec(&m).unwrap()).unwrap();
    }
    rewrite_manifest_and_chain(&ds, &rel, |m| {
        for h in m["handovers"].as_array_mut().unwrap() {
            for f in h["files"].as_array_mut().unwrap() {
                if f["path"] == g.as_str() {
                    f["content_digest"] = json!(digest);
                    f["size_bytes"] = json!(size);
                }
            }
        }
    });
    let (p2, _) = export_pr(&fx, "p2", 0);
    let rep = fx.verify_ok(&W, &p2, &[], 0);
    assert_eq!(rep["result"], "pass", "{rep}");
    let rep = fx.verify_ok(&W, &p2, &["--previous", s(&r1)], 5);
    let receipt: Value = serde_json::from_slice(&std::fs::read(&r1).unwrap()).unwrap();
    let from = receipt["snapshot"]["snapshot_id"].as_u64().unwrap();
    let broken = report_objects(&rep, "problems", "chain_broken");
    assert!(
        broken.iter().any(|o| {
            o.strip_prefix("default/app/")
                .and_then(amberlog_storage::layout::parse_snapshot_path)
                .is_some_and(|id| id.0 >= from)
        }),
        "{rep}"
    );
}

/// 回执文件 `r` 所记快照的编号。
fn receipt_id(r: &Path) -> u64 {
    receipt_snapshot_id(&serde_json::from_slice(&std::fs::read(r).unwrap()).unwrap())
}

#[test]
fn evidence_verify_snapshot_anchor_recycled() {
    let fx = Fixture::new();
    let mut w = fx.start(&w_with(&[&RECYCLE]));
    fx.hot_data(&w, 10);
    let (_, r1) = export_pr(&fx, "p1", 0);
    let anchor = receipt_id(&r1);
    let ds = fx.dataset("app");
    commit_until(
        &fx,
        &w,
        now_us(),
        "R1 所记快照被快照回收删除",
        || !ds.exists(&snapshot_rel(anchor)),
    );
    stop(&mut w);
    let (p2, _) = export_pr(&fx, "p2", 0);
    let rep = fx.verify_ok(&W, &p2, &["--previous", s(&r1)], 6);
    assert_eq!(rep["result"], "unprovable", "{rep}");
    assert!(
        kinds(&rep, "unprovable").contains(&"snapshot_anchor_recycled".to_owned()),
        "{rep}"
    );
    assert_eq!(rep["problems"], json!([]), "{rep}");
}

#[test]
fn evidence_verify_snapshot_gap() {
    let fx = Fixture::new();
    let mut w = fx.start(&W);
    fx.hot_data(&w, 20);
    let (_, r1) = export_pr(&fx, "p1", 0);
    let k = fx.dataset("app").current().unwrap();
    fx.hot_data(&w, 5);
    fx.hot_data(&w, 5);
    stop(&mut w);
    let ds = fx.dataset("app");
    assert!(ds.current().unwrap() >= k + 2);
    ds.remove(&snapshot_rel(k + 1));
    let (p2, _) = export_pr(&fx, "p2", 0);
    let rep = fx.verify_ok(&W, &p2, &["--previous", s(&r1)], 5);
    assert!(
        report_objects(&rep, "problems", "chain_broken")
            .contains(&format!("default/app/{}", snapshot_rel(k + 1))),
        "{rep}"
    );
}

#[test]
fn evidence_verify_segment_altered() {
    let fx = Fixture::new();
    let (mut w, _) = fx.retire_bucket(&W, 10);
    stop(&mut w);
    let (p, _) = export_pr(&fx, "p", 0);
    let ds = fx.dataset("app");
    let seg = ds.head()["retired_segment"]["path"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut v = ds.json(&seg);
    v["buckets"].as_array_mut().unwrap().pop();
    ds.write_json(&seg, &v);
    let rep = fx.verify_ok(&W, &p, &[], 5);
    assert!(
        report_objects(&rep, "problems", "chain_broken").contains(&format!("default/app/{seg}")),
        "{rep}"
    );
}

#[test]
fn evidence_verify_cold_version_altered() {
    let fx = Fixture::new();
    let _ = laked_cold_file(&fx);
    let (p, r) = export_pr(&fx, "p", 0);
    let receipt: Value = serde_json::from_slice(&std::fs::read(r).unwrap()).unwrap();
    let m = receipt["cold"]["metadata_version"].as_u64().unwrap();
    let cold = fx.cold("app");
    let rel = format!("metadata/v{m}.metadata.json");
    let mut meta = cold.metadata(m);
    let ts = meta["snapshots"][0]["timestamp-ms"].as_i64().unwrap();
    meta["snapshots"][0]["timestamp-ms"] = json!(ts + 1);
    std::fs::write(cold.path(&rel), serde_json::to_vec(&meta).unwrap()).unwrap();
    let rep = fx.verify_ok(&W, &p, &[], 5);
    assert!(
        report_objects(&rep, "problems", "chain_broken")
            .contains(&format!("_iceberg/default/app/{rel}")),
        "{rep}"
    );
}

#[test]
fn evidence_verify_bucket_moved() {
    let fx = Fixture::new();
    let _ = laked_cold_file(&fx);
    let (p, _) = export_pr(&fx, "p", 0);
    let other = fx.root().join("other-bucket");
    std::fs::create_dir_all(&other).unwrap();
    let st = std::process::Command::new("cp")
        .arg("-a")
        .arg(fx.store_dir().join("amberlog"))
        .arg(&other)
        .status()
        .unwrap();
    assert!(st.success());
    let url = format!("\"file://{}\"", other.display());
    let mut conf: Vec<Kv> = W.to_vec();
    conf.push(("object_store.url", Some(&url)));
    let rep = fx.verify_ok(&conf, &p, &[], 0);
    assert!(
        kinds(&rep, "notes").contains(&"table_location_differs".to_owned()),
        "{rep}"
    );
    assert_eq!(rep["problems"], json!([]));
}

#[test]
fn evidence_verify_untraced_cold_commit() {
    let fx = Fixture::new();
    let g = laked_cold_file(&fx);
    let (p, r) = export_pr(&fx, "p", 0);
    let receipt: Value = serde_json::from_slice(&std::fs::read(r).unwrap()).unwrap();
    let m = receipt["cold"]["metadata_version"].as_u64().unwrap();
    ExternalIceberg::new(&fx, "app").remove(&[&g]);
    std::fs::remove_file(fx.cold("app").path(&g)).unwrap();
    let rep = fx.verify_ok(&W, &p, &[], 5);
    assert!(
        report_objects(&rep, "problems", "untraced_cold_commit").contains(&format!(
            "_iceberg/default/app/metadata/v{}.metadata.json",
            m + 1
        )),
        "{rep}"
    );
}

#[test]
fn evidence_verify_cold_commits_ok() {
    let fx = Fixture::new();
    let _ = fx.lake_expiring();
    let (p, _) = export_pr(&fx, "p", 0);
    let s0 = fx.start_offset(
        &common::lake::COLD_TTL,
        common::lake::COLD_TTL_OFFSET_S,
        &[],
    );
    let mut w = s0;
    let b = hour_of(now_us()) - 2 * HOUR;
    ingest_ok(&w, "app", &format!(r#"[{{"_timestamp":{}}}]"#, b + 1));
    fx.dataset("app")
        .wait_handovers(&bucket_label(b), 1, common::evidence::LAKE_LIMIT);
    wait_until(Duration::from_secs(180), "TTL 冷层变更段", || {
        common::evidence::cold_changes(&fx)
            .iter()
            .any(|d| d["reason"] == "ttl")
    });
    stop(&mut w);
    let rep = fx.verify_ok(&W, &p, &[], 0);
    assert!(
        !kinds(&rep, "problems").contains(&"untraced_cold_commit".to_owned()),
        "{rep}"
    );
}

// ---------------------------------------------------------------------------
// D · 元数据副本与文件级延长
// ---------------------------------------------------------------------------

#[test]
fn evidence_verify_bucket_manifest_altered() {
    let fx = Fixture::new();
    let mut w = fx.start(&W);
    fx.hot_data(&w, 25);
    fx.hot_data(&w, 25);
    stop(&mut w);
    let (p, _) = export_pr(&fx, "p", 0);
    let ds = fx.dataset("app");
    let rel = common::evidence::bucket_manifest_paths(&ds)[0].clone();
    append_space(&ds, &rel);
    let rep = fx.verify_ok(&W, &p, &[], 5);
    assert!(
        report_objects(&rep, "problems", "digest_mismatch").contains(&format!("default/app/{rel}")),
        "{rep}"
    );
}

/// 在对象 `rel` 的文件末尾追加一个空格（仍为合法 JSON）。
fn append_space(ds: &Ds, rel: &str) {
    let mut bytes = ds.read(rel);
    bytes.push(b' ');
    ds.write(rel, &bytes);
}

#[test]
fn evidence_verify_chain_piece_altered() {
    let fx = Fixture::new();
    let mut w = fx.start(&W);
    fx.hot_data(&w, 25);
    fx.hot_data(&w, 25);
    stop(&mut w);
    let (p, _) = export_pr(&fx, "p", 0);
    let ds = fx.dataset("app");
    let head = common::evidence::bucket_manifest_paths(&ds)
        .into_iter()
        .find(|h| ds.json(h)["kind"] == "delta")
        .expect("有头片为增量片的桶 manifest");
    let q = ds.json(&head)["previous"]["path"]
        .as_str()
        .unwrap()
        .to_owned();
    append_space(&ds, &q);
    let rep = fx.verify_ok(&W, &p, &[], 5);
    assert!(
        report_objects(&rep, "problems", "digest_mismatch").contains(&format!("default/app/{q}")),
        "{rep}"
    );
}

#[test]
fn evidence_verify_copy_recycled() {
    let fx = Fixture::new();
    let mut w = fx.start(&w_with(&[&RECYCLE, &[MIN_ORPHAN_1S]]));
    fx.hot_data(&w, 10);
    let (p, _) = export_pr(&fx, "p", 0);
    let ds = fx.dataset("app");
    let head = ds.head();
    let m = head["buckets"][0]["manifest"].as_str().unwrap().to_owned();
    let ts = ds.file_entries(&head)[0]["min_timestamp"].as_i64().unwrap();
    commit_until(
        &fx,
        &w,
        ts,
        "P 所含的桶 manifest 被巡检删除",
        || !ds.exists(&m),
    );
    stop(&mut w);
    // 场景只断言报告：P 的回执所记快照此时也已回收，退出码不在本场景核对
    let rep = fx.verify(&W, &["--evidence", s(&p)]).json();
    assert!(
        report_objects(&rep, "notes", "copy_recycled").contains(&format!("default/app/{m}")),
        "{rep}"
    );
    assert_eq!(rep["problems"], json!([]), "{rep}");
}

#[test]
fn evidence_verify_archived_manifest_deleted() {
    let fx = Fixture::new();
    let (mut w, retired) = fx.retire_bucket(&W, 10);
    stop(&mut w);
    let (p, _) = export_pr(&fx, "p", 0);
    fx.dataset("app").remove(&retired.manifest);
    let rep = fx.verify_ok(&W, &p, &[], 5);
    let object = format!("default/app/{}", retired.manifest);
    assert!(
        report_objects(&rep, "problems", "missing").contains(&object),
        "{rep}"
    );
    assert!(
        !report_objects(&rep, "notes", "copy_recycled").contains(&object),
        "{rep}"
    );
}

#[test]
fn evidence_verify_ledger_manifest_altered() {
    let fx = Fixture::new();
    let mut w = fx.start(&W);
    fx.hot_data(&w, 10);
    let (p1, _) = export_pr(&fx, "p1", 0);
    let ds = fx.dataset("app");
    let f = ds.file_entries(&ds.head())[0].clone();
    let label = bucket_label(f["min_timestamp"].as_i64().unwrap());
    fx.hot_data(&w, 10);
    stop(&mut w);
    let rel = manifest_rel(&ds, &label);
    // B 的新头片 M′：全量片时另从其 `files` 中删去 F 的条目（增量片的还原按 `deletions` 去掉它）
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
        "removed_in": ds.current().unwrap(),
    }));
    ds.write_json(&rel, &m);
    ds.remove(f["path"].as_str().unwrap());
    let rep = fx.verify_ok(&W, &p1, &[], 5);
    assert!(
        report_objects(&rep, "problems", "digest_mismatch").contains(&format!("default/app/{rel}")),
        "{rep}"
    );
}

#[test]
fn evidence_verify_ledger_manifest_missing() {
    let fx = Fixture::new();
    let t = hour_of(now_us());
    let mut w = fx.start(&[("lifecycle.lake_watermark", Some("\"6h\"")), FLUSH_1S]);
    for k in [4, 3] {
        ingest_ok(
            &w,
            "app",
            &format!(r#"[{{"_timestamp":{}}}]"#, t - k * HOUR + 1),
        );
    }
    fx.dataset("app").wait_records(2, Duration::from_secs(60));
    stop(&mut w);
    let (from, to) = (iso(t - 4 * HOUR), iso(t - 3 * HOUR));
    let (p, _) = fx.export_app(&W, "p.tar", &["--from", &from, "--to", &to], 0);
    let ds = fx.dataset("app");
    let rel = manifest_rel(&ds, &bucket_label(t - 3 * HOUR));
    ds.remove(&rel);
    let rep = fx.verify_ok(&W, &p, &[], 5);
    assert!(
        report_objects(&rep, "problems", "missing").contains(&format!("default/app/{rel}")),
        "{rep}"
    );
}

#[test]
fn evidence_verify_ledger_head_advanced() {
    let fx = Fixture::new();
    let w = fx.start(&w_with(&[&RECYCLE, &[MIN_ORPHAN_1S]]));
    fx.hot_data(&w, 10);
    let (p, _) = export_pr(&fx, "p", 0);
    let (mut v, control) = fx.evidence_gated(
        "verify",
        &["--evidence", s(&p)],
        "verify.after_current_heads",
        &[],
    );
    // 查验确定的此刻快照即停住时的当前快照（W 此后只在下面摄取）
    let ds = fx.dataset("app");
    let head = ds.head();
    let m = head["buckets"][0]["manifest"].as_str().unwrap().to_owned();
    let ts = ds.file_entries(&head)[0]["min_timestamp"].as_i64().unwrap();
    commit_until(
        &fx,
        &w,
        ts,
        "此刻快照所引用的桶 manifest 被删除",
        || !ds.exists(&m),
    );
    std::fs::write(&control, "go").unwrap();
    assert_eq!(
        v.wait_exit(Duration::from_secs(300)),
        Some(Some(6)),
        "{:#?}",
        v.stderr_lines()
    );
    let rep: Value = serde_json::from_str(&v.stdout_lines()[0]).unwrap();
    assert!(
        rep["unprovable"].as_array().unwrap().iter().any(|e| {
            e["kind"] == "unavailable" && e["detail"].as_str().unwrap().contains("请重跑")
        }),
        "{rep}"
    );
}

/// UTC 微秒 → `YYYY-MM-DDTHH:00:00Z`。
fn iso(us: i64) -> String {
    let l = bucket_label(us);
    format!(
        "{}-{}-{}T{}:00:00Z",
        &l[0..4],
        &l[4..6],
        &l[6..8],
        &l[8..10]
    )
}

#[test]
fn evidence_verify_export_problems_note() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let ds = fx.dataset("app");
    let rel = common::evidence::bucket_manifest_paths(&ds)[0].clone();
    append_space(&ds, &rel);
    let (p, _) = export_pr(&fx, "p", 5);
    let r = fx.verify(&W, &["--evidence", s(&p)]);
    let rep = r.json();
    assert!(
        report_objects(&rep, "notes", "export_reported_problems")
            .contains(&"--evidence:check.json".to_owned()),
        "{rep}"
    );
}

#[test]
fn evidence_verify_file_level_extension() {
    let fx = Fixture::new();
    // P1 导出时数据还在热层：先以较大的入湖水位写入此刻前第 3 小时的桶
    let hot = late_bucket(&fx);
    let (p1, r1) = export_pr(&fx, "p1", 0);
    let anchor = receipt_id(&r1);
    let delay: [Kv; 1] = [("storage.delete_delay", Some("\"1s\""))];
    let mut w = fx.start(&w_with(&[&RECYCLE, &delay]));
    let ds = fx.dataset("app");
    wait_until(
        Duration::from_secs(300),
        "全部入湖并物理删除热层文件",
        || hot.iter().all(|f| !ds.exists(f)),
    );
    commit_until(
        &fx,
        &w,
        now_us(),
        "P1 的回执所记快照被快照回收删除",
        || !ds.exists(&snapshot_rel(anchor)),
    );
    stop(&mut w);
    let (p2, _) = export_pr(&fx, "p2", 0);
    let rep = fx.verify_ok(&W, &p2, &["--previous", s(&p1)], 6);
    assert_eq!(rep["problems"], json!([]), "{rep}");
    assert_eq!(
        kinds(&rep, "unprovable"),
        ["snapshot_anchor_recycled"],
        "{rep}"
    );
    let vanished = report_objects(&rep, "problems", "vanished");
    for f in &hot {
        assert!(!vanished.contains(&format!("default/app/{f}")), "{rep}");
    }
}

#[test]
fn evidence_verify_vanished() {
    let fx = Fixture::new();
    hot_stopped(&fx);
    let (p, _) = export_pr(&fx, "p", 0);
    let ds = fx.dataset("app");
    let rel = common::evidence::bucket_manifest_paths(&ds)[0].clone();
    let f = ds.restore(&rel)["files"][0]["path"]
        .as_str()
        .unwrap()
        .to_owned();
    // 记有 F 的文件条目的那一片：沿增量链找到列出它的清单片（全量片的 `files` 或增量片的 `files_added`）
    let chain = ds.pieces(&rel);
    let listed = |p: &Value| {
        let key = if p["kind"] == "delta" {
            "files_added"
        } else {
            "files"
        };
        p[key]
            .as_array()
            .unwrap()
            .iter()
            .any(|x| x["path"] == f.as_str())
    };
    let k = chain
        .iter()
        .position(|c| listed(&ds.json(c)))
        .expect("有清单片列出 F");
    rewrite_pieces_and_chain(&ds, &chain[..=k], |m| {
        for key in ["files", "files_added"] {
            if let Some(a) = m[key].as_array_mut() {
                a.retain(|x| x["path"] != f.as_str());
            }
        }
    });
    let rep = fx.verify_ok(&W, &p, &[], 5);
    assert!(
        report_objects(&rep, "problems", "vanished").contains(&format!("default/app/{f}")),
        "{rep}"
    );
}

#[test]
fn evidence_verify_cold_segment_pending() {
    let fx = Fixture::new();
    let _ = fx.lake_expiring();
    let (p, _) = export_pr(&fx, "p", 0);
    let control = fx.root().join("gate-ttl");
    std::fs::write(&control, "hold").unwrap();
    let w = fx.start_offset(
        &common::lake::COLD_TTL,
        common::lake::COLD_TTL_OFFSET_S,
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
    let rep = fx.verify_ok(&W, &p, &[], 0);
    assert!(
        kinds(&rep, "transitions").contains(&"cold_segment_pending".to_owned()),
        "{rep}"
    );
    std::fs::write(&control, "go").unwrap();
}

#[test]
fn evidence_verify_handover_missing() {
    let fx = Fixture::new();
    let mut w = fx.start(&[
        ("lifecycle.lake_watermark", Some("\"1h\"")),
        ("storage.delete_delay", Some("\"1s\"")),
        FLUSH_1S,
    ]);
    let label = fx.laked(&w);
    let ds = fx.dataset("app");
    let inputs: Vec<String> = ds.bucket_now(&label).unwrap()["deletions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["path"].as_str().unwrap().to_owned())
        .collect();
    wait_until(Duration::from_secs(300), "热层文件物理删除", || {
        inputs.iter().all(|f| !ds.exists(f))
    });
    stop(&mut w);
    let g = ds.bucket_now(&label).unwrap()["handovers"][0]["files"][0]["path"]
        .as_str()
        .unwrap()
        .to_owned();
    let (p, _) = export_pr(&fx, "p", 0);
    ExternalIceberg::new(&fx, "app").remove(&[&g]);
    std::fs::remove_file(fx.cold("app").path(&g)).unwrap();
    let rep = fx.verify_ok(&W, &p, &[], 5);
    assert!(
        report_objects(&rep, "problems", "handover_missing")
            .contains(&format!("_iceberg/default/app/{g}")),
        "{rep}"
    );
}

// ---------------------------------------------------------------------------
// E · 数据文件与冷层摘要
// ---------------------------------------------------------------------------

#[test]
fn evidence_verify_cold_file_replaced() {
    let fx = Fixture::new();
    let g = laked_cold_file(&fx);
    let (p, _) = export_pr(&fx, "p", 0);
    common::lake::alter_parquet_rows(&fx.cold("app").path(&g));
    let rep = fx.verify_ok(&W, &p, &[], 5);
    let o = format!("_iceberg/default/app/{g}");
    assert!(
        report_objects(&rep, "problems", "digest_mismatch").contains(&o)
            || report_objects(&rep, "problems", "size_mismatch").contains(&o),
        "{rep}"
    );
}

#[test]
fn evidence_verify_cold_file_deleted() {
    let fx = Fixture::new();
    let g = laked_cold_file(&fx);
    let (p, _) = export_pr(&fx, "p", 0);
    std::fs::remove_file(fx.cold("app").path(&g)).unwrap();
    let rep = fx.verify_ok(&W, &p, &[], 5);
    assert!(
        report_objects(&rep, "problems", "missing").contains(&format!("_iceberg/default/app/{g}")),
        "{rep}"
    );
}

/// 以较大的入湖水位向此刻前第 3 小时的桶 B 写入并停止；返回 B 的热层数据文件。
fn late_bucket(fx: &Fixture) -> Vec<String> {
    let b = hour_of(now_us()) - 3 * HOUR;
    let mut w = fx.start(&[
        ("lifecycle.lake_watermark", Some("\"6h\"")),
        ("storage.delete_delay", Some("\"1s\"")),
        FLUSH_1S,
    ]);
    ingest_ok(
        &w,
        "app",
        &format!(r#"[{{"_timestamp":{}}},{{"_timestamp":{}}}]"#, b + 1, b + 2),
    );
    fx.dataset("app").wait_records(2, Duration::from_secs(60));
    stop(&mut w);
    fx.dataset("app").bucket_now(&bucket_label(b)).unwrap()["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["path"].as_str().unwrap().to_owned())
        .collect()
}

/// 以入湖水位 `1h`、`storage.delete_delay` 为 `1s` 启动 W，等 `hot` 已物理删除后停止。
fn lake_and_purge(fx: &Fixture, hot: &[String]) {
    let mut w = fx.start(&[
        ("lifecycle.lake_watermark", Some("\"1h\"")),
        ("storage.delete_delay", Some("\"1s\"")),
        FLUSH_1S,
    ]);
    let ds = fx.dataset("app");
    wait_until(
        Duration::from_secs(300),
        "B 入湖并物理删除热层文件",
        || hot.iter().all(|f| !ds.exists(f)),
    );
    stop(&mut w);
}

#[test]
fn evidence_verify_left_live() {
    let fx = Fixture::new();
    let hot = late_bucket(&fx);
    let (p, _) = export_pr(&fx, "p", 0);
    lake_and_purge(&fx, &hot);
    let rep = fx.verify_ok(&W, &p, &[], 0);
    let left = report_objects(&rep, "transitions", "left_live");
    for f in &hot {
        assert!(left.contains(&format!("default/app/{f}")), "{rep}");
    }
}

#[test]
fn evidence_verify_left_live_concurrent() {
    let fx = Fixture::new();
    let hot = late_bucket(&fx);
    let (p, _) = export_pr(&fx, "p", 0);
    let (mut v, control) = fx.evidence_gated(
        "verify",
        &["--evidence", s(&p)],
        "verify.before_data_digest",
        &[],
    );
    lake_and_purge(&fx, &hot);
    std::fs::write(&control, "go").unwrap();
    assert_eq!(
        v.wait_exit(Duration::from_secs(120)),
        Some(Some(0)),
        "{:#?}",
        v.stderr_lines()
    );
    let rep: Value = serde_json::from_str(&v.stdout_lines()[0]).unwrap();
    let left = report_objects(&rep, "transitions", "left_live");
    for f in &hot {
        assert!(left.contains(&format!("default/app/{f}")), "{rep}");
    }
    assert!(
        !kinds(&rep, "problems").contains(&"missing".to_owned()),
        "{rep}"
    );
}

#[test]
fn evidence_verify_data_unreadable_continue() {
    let fx = Fixture::new();
    let label = laked_and_hot(&fx);
    let (p, _) = export_pr(&fx, "p", 0);
    let (_, files) = untar(&p);
    let live = common::evidence::files_lines(&files)
        .iter()
        .filter(|l| l["state"] == "live")
        .count() as u64;
    assert!(live >= 2);
    let g = fx.dataset("app").bucket_now(&label).unwrap()["handovers"][0]["files"][0]["path"]
        .as_str()
        .unwrap()
        .to_owned();
    let faults = fx.root().join("faults");
    std::fs::write(&faults, format!("get _iceberg/default/app/{g} 403\n")).unwrap();
    let mut cmd = fx.evidence_cmd(&W, "verify", &["--evidence", s(&p)]);
    cmd.env("TESTBED_FAULT_STORE", &faults);
    let r: common::evidence::Run = cmd.output().unwrap().into();
    assert_eq!(r.code, Some(6), "{}\n{}", r.stderr, r.stdout_text());
    let rep = r.json();
    assert_eq!(
        report_objects(&rep, "unprovable", "data_unreadable"),
        [format!("_iceberg/default/app/{g}")],
        "{rep}"
    );
    assert_eq!(rep["checked"]["digests_recomputed"], live - 1);
    assert_eq!(rep["problems"], json!([]));
}

#[test]
fn evidence_verify_traced_still_present() {
    let fx = Fixture::new();
    let _ = laked_cold_file(&fx);
    let (p, _) = export_pr(&fx, "p", 0);
    let rep = fx.verify_ok(&W, &p, &[], 0);
    assert!(
        kinds(&rep, "transitions").contains(&"traced_still_present".to_owned()),
        "{rep}"
    );
}

#[test]
fn evidence_verify_cold_record_ok() {
    let fx = Fixture::new();
    let _ = laked_cold_file(&fx);
    let (p, _) = export_pr(&fx, "p", 0);
    let rep = fx.verify_ok(&W, &p, &[], 0);
    assert!(
        !kinds(&rep, "problems").contains(&"cold_record_mismatch".to_owned()),
        "{rep}"
    );
}

#[test]
fn evidence_verify_cold_record_mismatch() {
    let fx = Fixture::new();
    let g = laked_cold_file(&fx);
    let (p, _) = export_pr(&fx, "p", 0);
    let (_, files) = untar(&p);
    let e = member_json(&files, "evidence.json");
    let cold = fx.cold("app");
    let mut changed = false;
    for el in e["iceberg_snapshots"].as_array().unwrap() {
        let Some(k) = el["metadata_version"].as_u64() else {
            continue;
        };
        let mut m = cold.metadata(k);
        for snap in m["snapshots"].as_array_mut().unwrap() {
            if snap["snapshot-id"] != el["iceberg_snapshot_id"] {
                continue;
            }
            if let Some(fd) = snap["summary"]["amberlog.file_digests"].as_str() {
                let mut map: serde_json::Map<String, Value> = serde_json::from_str(fd).unwrap();
                if map.contains_key(&g) {
                    map.insert(g.clone(), json!(format!("sha256:{}", "1".repeat(64))));
                    snap["summary"]["amberlog.file_digests"] =
                        json!(serde_json::to_string(&map).unwrap());
                    changed = true;
                }
            }
        }
        std::fs::write(
            cold.path(&format!("metadata/v{k}.metadata.json")),
            serde_json::to_vec(&m).unwrap(),
        )
        .unwrap();
    }
    assert!(changed);
    let rep = fx.verify_ok(&W, &p, &[], 5);
    assert!(
        report_objects(&rep, "problems", "cold_record_mismatch")
            .contains(&format!("_iceberg/default/app/{g}")),
        "{rep}"
    );
}

#[test]
fn evidence_verify_ttl_cutoff_ok() {
    let fx = Fixture::new();
    let (mut w, _) = ttl_removed(&fx, &[]);
    stop(&mut w);
    let (p, _) = export_pr(&fx, "p", 0);
    let rep = fx.verify_ok(&W, &p, &[], 0);
    assert!(
        !kinds(&rep, "problems").contains(&"ttl_cutoff_mismatch".to_owned()),
        "{rep}"
    );
}

// ---------------------------------------------------------------------------
// F · 没有提前删除
// ---------------------------------------------------------------------------

/// 「冷层合并已把 D 日 01 时与 02 时的两个冷层数据文件合成一个、冷层变更段已写，W 已停止」（D 为此刻所在 UTC 日的前两日；
/// W 以 `lifecycle.seal_grace` 为 `1s`、`lifecycle.lake_watermark` 为 `2s`、`cold_compaction.check_interval` 为 `10s`
/// 运行，每个小时桶摄取 10 条）。返回 D 日起点与记有合并的冷层变更段的路径（相对数据集前缀）。
fn merged(fx: &Fixture) -> (i64, String) {
    const DAY: i64 = 24 * HOUR;
    let d = now_us().div_euclid(DAY) * DAY - 2 * DAY;
    let mut w = fx.start(&[
        ("lifecycle.seal_grace", Some("\"1s\"")),
        ("lifecycle.lake_watermark", Some("\"2s\"")),
        ("cold_compaction.check_interval", Some("\"10s\"")),
        FLUSH_1S,
    ]);
    for h in [1, 2] {
        let recs: Vec<String> = (0..10)
            .map(|i| {
                format!(
                    r#"{{"_timestamp":{},"seq":{}}}"#,
                    d + h * HOUR + i * 1_000_000,
                    h * 100 + i
                )
            })
            .collect();
        ingest_ok(&w, "app", &format!("[{}]", recs.join(",")));
    }
    let ds = fx.dataset("app");
    let merge_segment = || -> Option<String> {
        ds.current()?;
        let mut r = ds.head()["cold_deletion_segment"].clone();
        while let Some(path) = r["path"].as_str().map(str::to_owned) {
            let seg = ds.json(&path);
            if seg["additions"].as_array().is_some_and(|a| !a.is_empty()) {
                return Some(path);
            }
            r = seg["previous"].clone();
        }
        None
    };
    wait_until(
        Duration::from_secs(300),
        "冷层合并的冷层变更段",
        || merge_segment().is_some(),
    );
    stop(&mut w);
    (d, merge_segment().unwrap())
}

#[test]
fn evidence_verify_count_conserve() {
    let fx = Fixture::new();
    let _ = merged(&fx);
    let (p, _) = export_pr(&fx, "p", 0);
    let rep = fx.verify_ok(&W, &p, &[], 0);
    assert!(
        !kinds(&rep, "problems").contains(&"count_mismatch".to_owned()),
        "{rep}"
    );
}

#[test]
fn evidence_verify_count_not_checked() {
    let fx = Fixture::new();
    let (d, segment) = merged(&fx);
    let (from, to) = (iso(d + 2 * HOUR), iso(d + 3 * HOUR));
    let (p, _) = fx.export_app(&W, "p.tar", &["--from", &from, "--to", &to], 0);
    let rep = fx.verify(&W, &["--evidence", s(&p)]).json();
    assert!(
        report_objects(&rep, "notes", "count_not_checked")
            .contains(&format!("default/app/{segment}")),
        "{rep}"
    );
    assert!(
        !kinds(&rep, "problems").contains(&"count_mismatch".to_owned()),
        "{rep}"
    );
}

#[test]
fn evidence_verify_ttl_order() {
    let fx = Fixture::new();
    let (mut w, _) = ttl_removed(&fx, &[]);
    stop(&mut w);
    let (p, _) = export_pr(&fx, "p", 0);
    let p2 = repack(&p, "p2.tar", true, |d| {
        edit_lines(d, |l| {
            if l["deletion"]["reason"] == "ttl" {
                l["max_timestamp"] = l["deletion"]["cutoff"].clone();
            }
        });
    });
    let rep = fx.verify_ok(&W, &p2, &[], 5);
    assert!(
        kinds(&rep, "problems").contains(&"early_deletion".to_owned()),
        "{rep}"
    );
}

#[test]
fn evidence_verify_early_deletion_retention() {
    let fx = Fixture::new();
    let conf = |period: &'static str| -> Vec<Kv<'static>> {
        vec![
            ("lifecycle.lake_watermark", Some("\"30m\"")),
            ("retention.period", Some(period)),
            ("retention.check_interval", Some("\"10s\"")),
            FLUSH_1S,
        ]
    };
    let mut w = fx.start(&conf("\"3h\""));
    let label = fx.laked_at(&w, hour_of(now_us()) - 2 * HOUR, 10);
    stop(&mut w);
    let g = fx.dataset("app").bucket_now(&label).unwrap()["handovers"][0]["files"][0]["path"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut w = fx.start(&conf("\"1h\""));
    wait_until(Duration::from_secs(180), "TTL 移出 G", || {
        fx.dataset("app").current().is_some()
            && common::evidence::cold_changes(&fx)
                .iter()
                .any(|d| d["path"] == g.as_str())
    });
    stop(&mut w);
    let mut w = fx.start(&conf("\"3h\""));
    stop(&mut w);
    let (p, _) = export_pr(&fx, "p", 0);
    let rep = fx.verify_ok(&W, &p, &["--retention", "3h"], 5);
    assert!(
        report_objects(&rep, "problems", "early_deletion")
            .contains(&format!("_iceberg/default/app/{g}")),
        "{rep}"
    );
    assert_eq!(rep["retention_checked"], true);
    let r = fx.verify(&W, &["--evidence", s(&p)]);
    let rep = r.json();
    assert_eq!(rep["retention_checked"], false);
    assert!(
        !kinds(&rep, "problems").contains(&"early_deletion".to_owned()),
        "{rep}"
    );
}

// ---------------------------------------------------------------------------
// H · 报告、中止与退出码
// ---------------------------------------------------------------------------

#[test]
fn evidence_verify_report_members() {
    let fx = Fixture::new();
    laked_and_hot(&fx);
    let (p, r) = export_pr(&fx, "p", 0);
    let out = fx.verify(&W, &["--evidence", s(&p), "--previous", s(&r)]);
    assert_eq!(out.code, Some(0), "{}", out.stderr);
    let rep = out.json();
    let keys: Vec<&String> = rep.as_object().unwrap().keys().collect();
    assert_eq!(
        keys,
        [
            "result",
            "dataset",
            "verified_at",
            "evidence_exported_at",
            "anchors",
            "current",
            "retention_checked",
            "checked",
            "problems",
            "unprovable",
            "transitions",
            "notes"
        ]
    );
    let anchors = rep["anchors"].as_array().unwrap();
    assert_eq!(anchors.len(), 2);
    assert!(
        anchors
            .iter()
            .any(|a| a["source"] == "evidence" && a["kind"] == "package")
    );
    assert!(
        anchors
            .iter()
            .any(|a| a["source"] == "previous" && a["kind"] == "receipt")
    );
    let (_, files) = untar(&p);
    let lines = common::evidence::files_lines(&files);
    assert_eq!(rep["checked"]["data_files"], lines.len() as u64);
    assert_eq!(
        rep["checked"]["digests_recomputed"],
        lines.iter().filter(|l| l["state"] == "live").count() as u64
    );
}

#[test]
fn evidence_verify_retired_manifest_altered() {
    let fx = Fixture::new();
    let (mut w, retired) = fx.retire_bucket(&W, 10);
    stop(&mut w);
    let (p, _) = export_pr(&fx, "p", 0);
    let ds = fx.dataset("app");
    let mut m = ds.json(&retired.manifest);
    m["deletions"].as_array_mut().unwrap().pop();
    ds.write_json(&retired.manifest, &m);
    let rep = fx.verify_ok(&W, &p, &[], 5);
    let closed = [
        "package_digest_mismatch",
        "package_inconsistent",
        "chain_not_extended",
        "chain_broken",
        "missing",
        "digest_mismatch",
        "size_mismatch",
        "vanished",
        "trace_altered",
        "handover_missing",
        "untraced_cold_commit",
        "cold_record_mismatch",
        "ttl_cutoff_mismatch",
        "count_mismatch",
        "early_deletion",
    ];
    for k in kinds(&rep, "problems") {
        assert!(closed.contains(&k.as_str()), "{k}");
    }
    assert!(
        report_objects(&rep, "problems", "digest_mismatch")
            .contains(&format!("default/app/{}", retired.manifest)),
        "{rep}"
    );
}

#[test]
fn evidence_verify_handover_window() {
    let fx = Fixture::new();
    let (w, gate) = fx.start_gated(&W, "lake.after_cold_commit");
    let b = hour_of(now_us()) - 3 * HOUR;
    ingest_ok(&w, "app", &format!(r#"[{{"_timestamp":{}}}]"#, b + 1));
    fx.dataset("app").wait_records(1, Duration::from_secs(60));
    let (p, _) = export_pr(&fx, "p", 0);
    gate.wait_held(&w, Duration::from_secs(180));
    let rep = fx.verify_ok(&W, &p, &[], 0);
    assert!(
        kinds(&rep, "transitions").contains(&"handover_window".to_owned()),
        "{rep}"
    );
    gate.release();
}

#[test]
fn evidence_verify_unavailable_exit6() {
    let fx = Fixture::new();
    let _ = laked_cold_file(&fx);
    let (p, _) = export_pr(&fx, "p", 0);
    let faults = fx.root().join("faults");
    std::fs::write(&faults, "").unwrap();
    let (mut v, control) = fx.evidence_gated(
        "verify",
        &["--evidence", s(&p)],
        "verify.after_current_heads",
        &[("TESTBED_FAULT_STORE", faults.display().to_string())],
    );
    std::fs::write(&faults, "* * 503\n").unwrap();
    std::fs::write(&control, "go").unwrap();
    assert_eq!(
        v.wait_exit(Duration::from_secs(300)),
        Some(Some(6)),
        "{:#?}",
        v.stderr_lines()
    );
    let rep: Value = serde_json::from_str(&v.stdout_lines()[0]).unwrap();
    assert_eq!(rep["result"], "unprovable");
    assert!(
        kinds(&rep, "unprovable").contains(&"unavailable".to_owned()),
        "{rep}"
    );
}

#[test]
fn evidence_verify_inconsistent_beats_unprovable() {
    let fx = Fixture::new();
    let mut w = fx.start(&w_with(&[&RECYCLE]));
    let label = fx.laked(&w);
    let (_, r1) = export_pr(&fx, "r1", 0);
    let anchor = receipt_id(&r1);
    let ds = fx.dataset("app");
    commit_until(
        &fx,
        &w,
        now_us(),
        "R1 所记快照被快照回收删除",
        || !ds.exists(&snapshot_rel(anchor)),
    );
    stop(&mut w);
    let g = ds.bucket_now(&label).unwrap()["handovers"][0]["files"][0]["path"]
        .as_str()
        .unwrap()
        .to_owned();
    std::fs::remove_file(fx.cold("app").path(&g)).unwrap();
    // 导出时 G 已不存在，导出本身报告问题（退出码 5）；查验仍以此包为依据
    let (p2, _) = export_pr(&fx, "p2", 5);
    let rep = fx.verify_ok(&W, &p2, &["--previous", s(&r1)], 5);
    assert_eq!(rep["result"], "inconsistent", "{rep}");
    assert!(
        kinds(&rep, "unprovable").contains(&"snapshot_anchor_recycled".to_owned()),
        "{rep}"
    );
    let gp = format!("_iceberg/default/app/{g}");
    assert!(
        rep["problems"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["object"] == gp.as_str()),
        "{rep}"
    );
}

#[test]
fn evidence_verify_signal_exit6() {
    let fx = Fixture::new();
    let _ = laked_cold_file(&fx);
    let (p, _) = export_pr(&fx, "p", 0);
    let (mut v, _) = fx.evidence_gated(
        "verify",
        &["--evidence", s(&p)],
        "verify.before_data_digest",
        &[],
    );
    v.signal("INT");
    assert_eq!(
        v.wait_exit(Duration::from_secs(30)),
        Some(Some(6)),
        "{:#?}",
        v.stderr_lines()
    );
    assert!(v.stdout_lines().is_empty());
}

// ---------------------------------------------------------------------------
// 退役记录只比对、只提示
// ---------------------------------------------------------------------------

/// 「stream `app` 已退役完成；导出全时段得到 P」，再对退役记录做 `edit`（收到它的文件路径）。返回 P。
fn retired_then(fx: &Fixture, edit: impl FnOnce(&Path)) -> PathBuf {
    fx.retired();
    let (p, _) = export_pr(fx, "p", 0);
    edit(&fx.retirement_record());
    p
}

#[test]
fn evidence_verify_after_retirement() {
    let fx = Fixture::new();
    let p = retired_then(&fx, |_| {});
    let rep = fx.verify_ok(&W, &p, &[], 0);
    assert_eq!(rep["result"], "pass", "{rep}");
    assert!(
        report_objects(&rep, "notes", "retirement_mismatch").is_empty(),
        "{rep}"
    );
}

#[test]
fn evidence_verify_retirement_mismatch_note() {
    let fx = Fixture::new();
    let p = retired_then(&fx, |rec| {
        let mut v: Value = serde_json::from_slice(&std::fs::read(rec).unwrap()).unwrap();
        v["datasets"]
            .as_array_mut()
            .unwrap()
            .retain(|d| d["dataset"] != "default/app");
        std::fs::write(rec, serde_json::to_vec(&v).unwrap()).unwrap();
    });
    let rep = fx.verify_ok(&W, &p, &[], 0);
    assert_eq!(
        report_objects(&rep, "notes", "retirement_mismatch").len(),
        1,
        "{rep}"
    );
}

#[test]
fn evidence_verify_retirement_unreadable_note() {
    let fx = Fixture::new();
    let p = retired_then(&fx, |rec| std::fs::write(rec, b"not json").unwrap());
    let rep = fx.verify_ok(&W, &p, &[], 0);
    assert_eq!(
        report_objects(&rep, "notes", "retirement_unreadable").len(),
        1,
        "{rep}"
    );
}

#[test]
fn evidence_verify_retirement_deleted_note() {
    let fx = Fixture::new();
    let p = retired_then(&fx, |rec| std::fs::remove_file(rec).unwrap());
    let rep = fx.verify_ok(&W, &p, &[], 0);
    assert_eq!(
        report_objects(&rep, "notes", "retirement_mismatch"),
        ["_system/retirement.json"],
        "{rep}"
    );
}

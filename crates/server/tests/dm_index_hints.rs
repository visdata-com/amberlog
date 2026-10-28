//! REQ-AMB-DM-INDEX-HINTS 的系统测试：以子进程运行 `amberlog`，
//! 经退出码、stderr 与 HTTP 观察；对象存储用 `file://` 临时目录。

mod common;

use std::path::Path;
use std::process::{Command, Output};
use std::time::Duration;

use common::api::{FLUSH_1H, ingest_ok, search, search_resp};
use common::store::stop;
use common::{Fixture, Kv, Server};
use serde_json::Value;

/// 以 `overrides` 写成的配置运行 `amberlog serve` 直到退出，返回退出码与 stderr。
fn run_to_exit(fx: &Fixture, overrides: &[Kv], env: &[(&str, &str)]) -> (i32, String) {
    let mut cmd: Command = fx.serve(overrides);
    for (k, v) in env {
        cmd.env(k, v);
    }
    let Output { status, stderr, .. } = cmd.output().expect("启动 amberlog");
    (
        status.code().expect("进程应正常退出而不是被信号终止"),
        String::from_utf8_lossy(&stderr).into_owned(),
    )
}

fn assert_exit2(fx: &Fixture, overrides: &[Kv], env: &[(&str, &str)], stderr_has: &[&str]) {
    let (code, stderr) = run_to_exit(fx, overrides, env);
    assert_eq!(code, 2, "stderr:\n{stderr}");
    for s in stderr_has {
        assert!(stderr.contains(s), "stderr 应含 {s:?}:\n{stderr}");
    }
}

/// 目录下（递归）是否有文件的内容含 `needle`。
fn any_file_contains(dir: &Path, needle: &[u8]) -> bool {
    std::fs::read_dir(dir).unwrap().any(|e| {
        let path = e.unwrap().path();
        if path.is_dir() {
            any_file_contains(&path, needle)
        } else {
            let bytes = std::fs::read(&path).unwrap();
            bytes.windows(needle.len()).any(|w| w == needle)
        }
    })
}

/// 系统表 `table` 的全部行，去掉随接收时刻变化的列（`_reconciliation` 的窗口起止；`_ledger` 的 `time_min`、
/// `time_max` 与随时间戳取值变化的压缩后 `size_bytes`），按行排序。
fn system_rows(s: &Server, table: &str) -> Vec<String> {
    let mut rows: Vec<String> = search(s, &format!("SELECT * FROM {table}"))
        .into_iter()
        .map(|mut row: Value| {
            let o = row.as_object_mut().unwrap();
            for col in [
                "window_start",
                "window_end",
                "time_min",
                "time_max",
                "size_bytes",
            ] {
                o.remove(col);
            }
            row.to_string()
        })
        .collect();
    rows.sort();
    rows
}

#[test]
fn index_hints_valid_no_effect() {
    let x_fx = Fixture::new();
    let y_fx = Fixture::new();
    let hints: Kv = (
        "index_hints",
        Some(r#"{ "default/app" = ["trace_id"], "default/later" = [] }"#),
    );
    let x_config = [FLUSH_1H, hints];
    let y_config = [FLUSH_1H];
    let mut x = x_fx.start(&x_config);
    let mut y = y_fx.start(&y_config);
    let mut bodies = Vec::new();
    for s in [&x, &y] {
        assert_eq!(s.wait_readyz(200, Duration::from_secs(10)).status, 200);
        ingest_ok(
            s,
            "app",
            r#"[{"trace_id":"t1","n":1},{"trace_id":"t2","n":2}]"#,
        );
        let r = search_resp(s, "SELECT trace_id, n FROM app ORDER BY n");
        assert_eq!(r.status, 200, "{}", r.text());
        bodies.push(r.json());
    }
    for field in ["hits", "total", "scan_files", "scan_records"] {
        assert_eq!(bodies[0][field], bodies[1][field], "{field}");
    }
    assert_eq!(bodies[0]["hits"].as_array().unwrap().len(), 2);

    // 优雅停机使两者都完成快照提交（`flush.interval` 为 1h，运行中不提交），确认快照与数据文件确已写出，
    // 「对象存储里没有对象含 `index_hints`」与系统表比对才不是空检查
    stop(&mut x);
    stop(&mut y);
    for fx in [&x_fx, &y_fx] {
        let ds = fx.dataset("app");
        assert!(ds.current().is_some(), "停机后没有快照");
        assert_eq!(ds.head()["summary"]["record_count"], 2);
        assert!(!ds.keys_under("data/").is_empty());
    }
    let x = x_fx.start(&x_config);
    let y = y_fx.start(&y_config);
    for table in ["_ledger", "_reconciliation"] {
        let (rx, ry) = (system_rows(&x, table), system_rows(&y, table));
        assert!(!rx.is_empty(), "{table} 为空");
        assert_eq!(rx, ry, "{table}");
    }
    let mut x = x;
    stop(&mut x);
    assert!(!any_file_contains(&x_fx.store_dir(), b"index_hints"));
}

#[test]
fn index_hints_bad_dataset_exit2() {
    let fx = Fixture::new();
    assert_exit2(
        &fx,
        &[(
            "index_hints",
            Some(r#"{ "default/Bad-Name" = ["trace_id"] }"#),
        )],
        &[],
        &["index_hints", "default/Bad-Name"],
    );
}

#[test]
fn index_hints_bad_value_exit2() {
    let fx = Fixture::new();
    for value in [
        r#"{ "default/app" = "trace_id" }"#,
        r#"{ "default/app" = ["trace_id", "trace_id"] }"#,
        r#"{ "default/app" = [""] }"#,
    ] {
        assert_exit2(&fx, &[("index_hints", Some(value))], &[], &["index_hints"]);
    }
}

#[test]
fn index_hints_env_exit2() {
    let fx = Fixture::new();
    let (code, stderr) = run_to_exit(&fx, &[], &[("AMBERLOG_INDEX_HINTS", "x")]);
    assert_eq!(code, 2, "stderr:\n{stderr}");
    assert!(
        stderr.contains("AMBERLOG_INDEX_HINTS") || stderr.contains("index_hints"),
        "{stderr}"
    );
}

#[test]
fn index_hints_bad_dataset_forms_exit2() {
    let fx = Fixture::new();
    for (value, key) in [
        (r#"{ "other/app" = ["x"] }"#, "other/app"),
        (r#"{ "app" = ["x"] }"#, "\"app\""),
        (r#"{ "default/app/x" = ["x"] }"#, "default/app/x"),
    ] {
        assert_exit2(
            &fx,
            &[("index_hints", Some(value))],
            &[],
            &["index_hints", key],
        );
    }
}

#[test]
fn index_hints_not_table_exit2() {
    let fx = Fixture::new();
    assert_exit2(
        &fx,
        &[("index_hints", Some(r#"["trace_id"]"#))],
        &[],
        &["index_hints"],
    );
}

#[test]
fn index_hints_env_prefix_exit2() {
    let fx = Fixture::new();
    let (code, stderr) = run_to_exit(
        &fx,
        &[],
        &[("AMBERLOG_INDEX_HINTS_DEFAULT_APP", "trace_id")],
    );
    assert_eq!(code, 2, "stderr:\n{stderr}");
}

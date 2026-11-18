//! REQ-AMB-ARCH-CONSTRAINTS-V6 的测试：单一可执行文件由 `cargo metadata`
//! 检查；查询层无状态由两个以同一 `object_store.url`、不同的空 `wal.dir` 先后启动的 `amberlog` 进程检查。
//! 治理面字段由一个实例真实写出的快照 `summary` 与计数对象检查。
//! V6 的发版平台由 ADR-0059 发版编译环境编出的两个发版二进制检查（路径由环境变量给出）；`file://` 后端的适用范围
//! 由对外安装说明的审查验收（人工审核场景，本文件只做其中能机械判定的部分）。

mod common;

use std::time::Duration;

use common::api::{FAR_END_US, counter_bytes, ingest_ok, sent, sent_element, settle_minute};
use common::store::{H10, HOUR};
use common::{BEARER, Fixture, Server};
use serde_json::{Value, json};

#[test]
fn arch_single_executable() {
    let out = std::process::Command::new(env!("CARGO"))
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("执行 cargo metadata");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let meta: Value = serde_json::from_slice(&out.stdout).unwrap();
    let bins: Vec<String> = meta["packages"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|p| p["targets"].as_array().unwrap().iter())
        .filter(|t| t["kind"].as_array().unwrap().iter().any(|k| k == "bin"))
        .map(|t| t["name"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(bins, ["amberlog"]);
}

/// 以 `(start, end)` 为时间范围、`size` 为 10000 执行 `sql`，断言 200，返回 `hits`。
fn hits(s: &Server, sql: &str, (start, end): (i64, i64)) -> Vec<Value> {
    let body = json!({"query": {"sql": sql, "start_time": start, "end_time": end, "size": 10000}});
    let r = s.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        body.to_string().as_bytes(),
    );
    assert_eq!(r.status, 200, "{sql}：{}", r.text());
    r.json()["hits"].as_array().unwrap().clone()
}

/// SIGTERM 并等进程以 0 退出（停机时 flush 并提交）。
fn stop(mut s: Server) {
    s.signal("TERM");
    assert_eq!(s.wait_exit(Duration::from_secs(60)), Some(Some(0)));
}

/// 以 `f` 的对象存储与一个新的空 `wal.dir` 启动另一个进程。
fn start_fresh(f: &Fixture) -> Server {
    start_fresh_with(f, &[])
}

/// 同 [`start_fresh`]，另加配置 `extra`。
fn start_fresh_with(f: &Fixture, extra: &[common::Kv]) -> Server {
    let wal = f.root().join("wal-fresh");
    assert!(!wal.exists());
    let value = format!("\"{}\"", wal.display());
    let mut all = vec![("wal.dir", Some(value.as_str()))];
    all.extend_from_slice(extra);
    f.start(&all)
}

#[test]
fn arch_fresh_process_reads_cold() {
    const WATERMARK: common::Kv = ("lifecycle.lake_watermark", Some("\"1h\""));
    let f = Fixture::new();
    let a = f.start(&[("flush.interval", Some("\"1s\"")), WATERMARK]);
    let now = common::api::now_us();
    let h = now - now.rem_euclid(HOUR) - 3 * HOUR;
    let records: Vec<Value> = (0..100)
        .map(|seq| json!({"_timestamp": h + 1 + seq * 1_000_000, "seq": seq}))
        .collect();
    ingest_ok(&a, "app", &Value::Array(records).to_string());
    // 当前快照中该桶已不含存活数据文件：数据已全部入湖
    let label = common::store::bucket_label(h);
    let m = f
        .dataset("app")
        .wait_handovers(&label, 1, Duration::from_secs(150));
    assert_eq!(m["files"], json!([]), "{m}");
    stop(a);

    let b = start_fresh_with(&f, &[WATERMARK]);
    let got = hits(&b, "SELECT seq FROM app", (h, h + HOUR));
    let seqs: std::collections::BTreeSet<i64> =
        got.iter().map(|x| x["seq"].as_i64().unwrap()).collect();
    assert_eq!(got.len(), 100);
    assert_eq!(seqs.len(), 100);
}

#[test]
fn arch_readonly_subcommand_coexists() {
    const WATERMARK: common::Kv = ("lifecycle.lake_watermark", Some("\"1h\""));
    let f = Fixture::new();
    let w = f.start(&[("flush.interval", Some("\"1s\"")), WATERMARK]);
    let now = common::api::now_us();
    let h = now - now.rem_euclid(HOUR) - 3 * HOUR;
    let records: Vec<Value> = (0..100)
        .map(|seq| json!({"_timestamp": h + 1 + seq * 1_000_000, "seq": seq}))
        .collect();
    ingest_ok(&w, "app", &Value::Array(records).to_string());
    let v1 = f.cold("app").path("metadata/v1.metadata.json");
    assert!(
        common::wait_file(&v1, Duration::from_secs(600)),
        "{:#?}",
        w.stderr_lines()
    );

    // C：与 W 相同的 object_store.url、storage.prefix 与 auth.token
    let c = Server::start(f.catalog(&[]));
    let fresh: Vec<Value> = (100..150)
        .map(|seq| json!({"_timestamp": common::api::now_us(), "seq": seq}))
        .collect();
    ingest_ok(&w, "app", &Value::Array(fresh).to_string());

    assert_eq!(c.wait_readyz(200, Duration::from_secs(60)).status, 200);
    let table = c.get_auth("/iceberg/v1/amberlog/namespaces/default/tables/app");
    assert_eq!(table.status, 200, "{}", table.text());
    let range = (h, common::api::now_us() + HOUR);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let got = loop {
        let got = hits(&w, "SELECT seq FROM app", range);
        if got.len() >= 150 || std::time::Instant::now() >= deadline {
            break got;
        }
        std::thread::sleep(Duration::from_millis(250));
    };
    let seqs: std::collections::BTreeSet<i64> =
        got.iter().map(|x| x["seq"].as_i64().unwrap()).collect();
    assert_eq!(got.len(), 150);
    assert_eq!(seqs.len(), 150);
    let metrics = w.get_auth("/metrics").text();
    assert!(
        !metrics
            .lines()
            .any(|l| l.starts_with("amberlog_dataset_disabled") && l.contains("stream=\"app\"")),
        "{metrics}"
    );
}

#[test]
fn arch_evidence_subcommands_coexist() {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    let f = Fixture::new();
    let w = f.start(&[("flush.interval", Some("\"1s\""))]);
    let records: Vec<Value> = (0..100).map(|seq| json!({"seq": seq})).collect();
    ingest_ok(&w, "app", &Value::Array(records).to_string());
    f.dataset("app").wait_records(100, Duration::from_secs(30));

    // 两个子命令运行期间每秒摄取 1 条、同时轮询 /readyz；先等到第一条摄取完成再起子命令，保证期间至少摄取一次
    let done = AtomicBool::new(false);
    let ok = AtomicU64::new(0);
    let package = f.tmp("e.tar");
    let (export, verify, failures) = std::thread::scope(|s| {
        let feeder = s.spawn(|| {
            let mut failures = Vec::new();
            let mut seq = 100;
            while !done.load(Ordering::SeqCst) {
                let r = common::api::ingest(&w, "app", &json!([{"seq": seq}]).to_string());
                seq += 1;
                if r.status == 200 {
                    ok.fetch_add(1, Ordering::SeqCst);
                } else {
                    failures.push(format!("ingest {}：{}", r.status, r.text()));
                }
                let ready = w.get("/readyz");
                if ready.status != 200 {
                    failures.push(format!("readyz {}", ready.status));
                }
                std::thread::sleep(Duration::from_secs(1));
            }
            failures
        });
        while ok.load(Ordering::SeqCst) == 0 && !feeder.is_finished() {
            std::thread::sleep(Duration::from_millis(50));
        }
        let export = f.export(
            &[],
            &[
                "--dataset",
                "default/app",
                "--output",
                package.to_str().unwrap(),
            ],
        );
        let verify = f.verify(&[], &["--evidence", package.to_str().unwrap()]);
        done.store(true, Ordering::SeqCst);
        (export, verify, feeder.join().unwrap())
    });
    assert_eq!(export.code, Some(0), "{}", export.stderr);
    assert_eq!(verify.code, Some(0), "{}", verify.stderr);
    assert!(failures.is_empty(), "{failures:#?}");

    let want = 100 + ok.load(Ordering::SeqCst) as usize;
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let got = loop {
        let got = hits(&w, "SELECT seq FROM app", (0, FAR_END_US));
        if got.len() >= want || std::time::Instant::now() >= deadline {
            break got;
        }
        std::thread::sleep(Duration::from_millis(250));
    };
    let seqs: std::collections::BTreeSet<i64> =
        got.iter().map(|x| x["seq"].as_i64().unwrap()).collect();
    assert_eq!(got.len(), want);
    assert_eq!(seqs.len(), want);
}

#[test]
fn arch_fresh_process_reads_all() {
    let f = Fixture::new();
    let a = f.start(&[]);
    let records: Vec<Value> = (0..500).map(|seq| json!({"seq": seq})).collect();
    ingest_ok(&a, "app", &Value::Array(records).to_string());
    stop(a);
    let snapshot = f.dataset("app").wait_records(500, Duration::from_secs(5));
    assert_eq!(snapshot["summary"]["record_count"], 500);

    let b = start_fresh(&f);
    assert_eq!(hits(&b, "SELECT seq FROM app", (0, FAR_END_US)).len(), 500);
}

#[test]
fn arch_fresh_process_same_results() {
    let f = Fixture::new();
    let a = f.start(&[("flush.interval", Some("\"1s\""))]);
    for (stream, salt) in [("sa", 0), ("sb", 100)] {
        let records: Vec<Value> = (0..30i64)
            .map(|n| {
                json!({
                    "_timestamp": H10 + (n % 3) * HOUR + n * 1_000_000 + salt,
                    "i": n % 7,
                    "f": n as f64 * 0.5,
                    "b": n % 2 == 0,
                    "s": format!("{}{n:02}", if n % 4 == 0 { "x" } else { "y" }),
                })
            })
            .collect();
        ingest_ok(&a, stream, &Value::Array(records).to_string());
    }
    std::thread::sleep(Duration::from_secs(5));
    let all = (0, FAR_END_US);
    let queries: [(&str, (i64, i64)); 5] = [
        ("SELECT * FROM sa", all),
        ("SELECT * FROM sb ORDER BY s", all),
        ("SELECT i, f FROM sa WHERE i > 3 AND b = TRUE", all),
        ("SELECT s FROM sb WHERE s LIKE 'x%' LIMIT 5", all),
        ("SELECT _timestamp FROM sa", (H10 + HOUR, H10 + 2 * HOUR)),
    ];
    let before: Vec<Vec<Value>> = queries.iter().map(|(q, r)| hits(&a, q, *r)).collect();
    stop(a);

    let b = start_fresh(&f);
    for ((q, r), want) in queries.iter().zip(&before) {
        assert!(!want.is_empty(), "{q}");
        assert_eq!(&hits(&b, q, *r), want, "{q}");
    }
}

/// 以 `flush.interval` 为 `1s` 运行的实例向 `gv` 摄取记录、提交一个合法的 `_sent` 元素后以 SIGTERM 停止，
/// 返回 `gv` 的 `_current` 所指快照的 `summary` 与本小时计数对象。
fn governance_records() -> (Value, Value) {
    let f = Fixture::new();
    let s = f.start(&[("flush.interval", Some("\"1s\""))]);
    let w = settle_minute(Duration::from_secs(10), true);
    ingest_ok(&s, "gv", r#"[{"a":1},{"a":2}]"#);
    let r = sent(&s, "gv", &format!("[{}]", sent_element("src", w, 2)));
    assert_eq!(r.status, 200, "{}", r.text());
    stop(s);
    let ds = f.dataset("gv");
    let summary = ds.snapshot(ds.current().expect("已提交"))["summary"].clone();
    let counters: Value =
        serde_json::from_slice(&counter_bytes(&f, "gv", w).expect("本小时计数对象已写出")).unwrap();
    (summary, counters)
}

/// 递归列出全部字段名；计数对象 `sent` 中的键是来源名（取值），不计。
fn field_names(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::Object(m) => {
            for (k, x) in m {
                out.push(k.clone());
                if k != "sent" {
                    field_names(x, out);
                }
            }
        }
        Value::Array(a) => a.iter().for_each(|x| field_names(x, out)),
        _ => {}
    }
}

/// 两条记录顶层的 `dataset` 与全部字段名都满足块的约束。
fn assert_governance_fields(summary: &Value, counters: &Value) {
    for (what, record) in [("summary", summary), ("计数对象", counters)] {
        assert_eq!(record["dataset"], "default/gv", "{what}：{record}");
        let mut names = Vec::new();
        field_names(record, &mut names);
        for name in &names {
            for banned in ["_timestamp", "log", "message"] {
                assert!(
                    !name.contains(banned),
                    "{what} 的字段 {name:?} 含 {banned:?}"
                );
            }
        }
    }
}

#[test]
fn arch_governance_fields_mechanical() {
    let (summary, counters) = governance_records();
    assert_governance_fields(&summary, &counters);
    let span: std::collections::BTreeSet<&str> = summary
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .filter(|k| k.contains("time") || k.contains("min") || k.contains("max"))
        .collect();
    assert_eq!(
        span,
        std::collections::BTreeSet::from(["time_min", "time_max"]),
        "{summary}"
    );
}

/// 人工审核场景：输出真实写出的两条治理面记录的字段清单供人工逐字段核对，机械可查的部分同时断言。
#[test]
fn arch_governance_field_review() {
    let (summary, counters) = governance_records();
    for (what, record) in [
        ("snapshot summary", &summary),
        ("counters object", &counters),
    ] {
        let mut names = Vec::new();
        field_names(record, &mut names);
        names.sort();
        names.dedup();
        eprintln!("{what} 字段：{names:?}\n{what} 原文：{record}");
    }
    // 第 2 版快照的 `summary.cold`（REQ-AMB-ARCH-CONSTRAINTS-V2 场景前提）：字段恰为两层同名的合计与首末时间
    let mut cold: Vec<&str> = summary["cold"]
        .as_object()
        .expect("summary 含 cold 对象")
        .keys()
        .map(String::as_str)
        .collect();
    cold.sort();
    assert_eq!(cold, ["record_count", "size_bytes", "time_max", "time_min"]);
    assert_governance_fields(&summary, &counters);
}

/// 以凭据 `bearer` 查 `sql`（时间范围覆盖全部测试数据、`size` 为 10000），返回应答。
fn search_with(s: &Server, bearer: &str, sql: &str) -> common::Resp {
    common::authz::search_as(s, bearer, sql)
}

#[test]
fn arch_fresh_process_same_principal_results() {
    let f = Fixture::new();
    let q = common::authz::ServiceToken::generate();
    let entries = [common::authz::token_entry(
        "q",
        &q,
        r#"[{ role = "querier", streams = ["app_*"] }]"#,
    )];
    let a = common::authz::start(&f, &entries, &[]);
    let records =
        |n: i64| Value::Array((0..n).map(|seq| json!({"seq": seq})).collect()).to_string();
    ingest_ok(&a, "app_a", &records(300));
    ingest_ok(&a, "hr", &records(200));
    let seqs = |s: &Server| -> Vec<Value> {
        let r = search_with(s, &q.bearer(), "SELECT seq FROM app_a");
        assert_eq!(r.status, 200, "{}", r.text());
        let mut v: Vec<Value> = r.json()["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| h["seq"].clone())
            .collect();
        v.sort_by_key(|x| x.as_i64());
        v
    };
    let before = seqs(&a);
    assert_eq!(before.len(), 300);
    search_with(&a, &q.bearer(), "SELECT seq FROM hr").assert_envelope(400, "stream_not_found");
    stop(a);
    f.dataset("app_a").wait_records(300, Duration::from_secs(5));
    f.dataset("hr").wait_records(200, Duration::from_secs(5));

    let wal = f.root().join("wal-fresh");
    assert!(!wal.exists());
    let value = format!("\"{}\"", wal.display());
    let b = common::authz::start(&f, &entries, &[("wal.dir", Some(value.as_str()))]);
    assert_eq!(seqs(&b), before);
    search_with(&b, &q.bearer(), "SELECT seq FROM hr").assert_envelope(400, "stream_not_found");
    let r = search_with(&b, &q.bearer(), "SELECT dataset FROM _ledger");
    assert_eq!(r.status, 200, "{}", r.text());
    let datasets: Vec<Value> = r.json()["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["dataset"].clone())
        .collect();
    assert_eq!(datasets, [json!("default/app_a")]);
}

// ---------- REQ-AMB-ARCH-CONSTRAINTS-V6：发版平台 ----------

/// 两个发版二进制的路径来自这两个环境变量，各自应有的 ELF 机器类型（`readelf -h` 的 `Machine` 一栏）。
const RELEASE_BINS: [(&str, &str); 2] = [
    (
        "TESTBED_RELEASE_BIN_X86_64",
        "Advanced Micro Devices X86-64",
    ),
    ("TESTBED_RELEASE_BIN_AARCH64", "AArch64"),
];

/// 发版平台的 glibc 下限（REQ-AMB-ARCH-CONSTRAINTS-V6，ADR-0059 第 1 条）。
const MAX_GLIBC: [u32; 2] = [2, 28];

/// `readelf <args> <path>` 的 stdout；失败即测试失败。
fn readelf(args: &[&str], path: &str) -> String {
    let out = std::process::Command::new("readelf")
        .args(args)
        .arg(path)
        .output()
        .expect("执行 readelf（binutils）");
    assert!(
        out.status.success(),
        "readelf {args:?} {path}：{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("readelf 的输出是 UTF-8")
}

/// 读法与发版脚本 `build-release.sh` 相同：`readelf -W -V` 版本需求节里 `Name: GLIBC_…` 的各个版本，按各段数值逐段比较。
/// 拿不到二进制（环境变量没有设置）不判通过：本测试只在发版编译环境编出两个二进制的作业里运行，见合约「已定决策」。
#[test]
fn arch_release_glibc_baseline() {
    for (var, machine) in RELEASE_BINS {
        let path = std::env::var(var).unwrap_or_else(|_| {
            panic!("{var} 未设置：本测试要读 ADR-0059 发版编译环境编出的发版二进制，不能在没有二进制时判通过")
        });
        let header = readelf(&["-h"], &path);
        let got = header
            .lines()
            .find_map(|l| l.trim().strip_prefix("Machine:"))
            .map(str::trim);
        assert_eq!(got, Some(machine), "{path} 的 ELF 机器类型");
        let versions = readelf(&["-W", "-V"], &path);
        let names: Vec<&str> = versions
            .lines()
            .filter_map(|l| l.split_once("Name: ").map(|(_, rest)| rest))
            .filter_map(|rest| rest.split_whitespace().next())
            .filter(|name| name.starts_with("GLIBC_"))
            .collect();
        assert!(
            !names.is_empty(),
            "{path} 读不出任何 GLIBC_ 版本（不是动态链接 glibc 的产物）：\n{versions}"
        );
        let mut highest: Option<Vec<u32>> = None;
        for name in names {
            // GLIBC_PRIVATE 与 GLIBC_ABI_* 一类不是 GLIBC_x.y 形式的版本，同样判失败
            let segments: Vec<u32> = name["GLIBC_".len()..]
                .split('.')
                .map(|s| s.parse().ok())
                .collect::<Option<Vec<u32>>>()
                .unwrap_or_else(|| panic!("{path} 要求 {name}，不是 GLIBC_x.y 或 GLIBC_x.y.z"));
            assert!(
                segments.len() >= 2,
                "{path} 要求 {name}，不是 GLIBC_x.y 或 GLIBC_x.y.z"
            );
            assert!(
                segments.as_slice() <= MAX_GLIBC.as_slice(),
                "{path} 要求 {name}，高于 GLIBC_2.28"
            );
            if highest.as_ref().is_none_or(|h| segments > *h) {
                highest = Some(segments);
            }
        }
        let highest: Vec<String> = highest.unwrap().iter().map(u32::to_string).collect();
        eprintln!("{path}：{machine}，最高要求 GLIBC_{}", highest.join("."));
    }
}

// ---------- REQ-AMB-ARCH-CONSTRAINTS-V6：`file://` 后端的适用范围 ----------

/// 对外安装说明所在目录：内部仓库在 `publish/github/docs/`，导出到发布仓库后在 `docs/`。
fn install_docs_dir() -> std::path::PathBuf {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    ["publish/github/docs", "docs"]
        .iter()
        .map(|d| root.join(d))
        .find(|d| d.join("install.md").is_file())
        .expect("找不到对外安装说明 install.md")
}

/// 讲 `file://` 的两处：「部署前要准备的」表里对象存储一行，与「本地磁盘」一节（到下一个二级标题为止）。
fn file_backend_passages(text: &str, row: &str, section: &str) -> (String, String) {
    let row = text
        .lines()
        .find(|l| l.starts_with(&format!("| {row} |")))
        .unwrap_or_else(|| panic!("找不到表格行 {row}"))
        .to_owned();
    let start = text
        .find(&format!("\n{section}\n"))
        .unwrap_or_else(|| panic!("找不到小节 {section}"));
    let rest = &text[start + 1..];
    let end = rest.find("\n## ").map_or(rest.len(), |i| i + 1);
    (row, rest[..end].to_owned())
}

/// 人工审核场景「安装说明写明 `file://` 后端的适用范围」的绑定测试：把两份安装说明里讲 `file://` 的段落打到 stderr
/// 供架构师审查，并断言其中能机械判定的部分（各要点的说法出现）。测试通过不代替审核，审核记录在合约里。
#[test]
fn arch_file_backend_scope_documented() {
    let dir = install_docs_dir();
    let cases: [(&str, &str, &str, [&str; 7]); 2] = [
        (
            "install.md",
            "Object storage",
            "### 6.8 Local disk",
            [
                "file://",
                "local file system",
                "block-storage volume",
                "trial or a single-node deployment",
                "network file system",
                "outside what Amberlog guarantees",
                "we recommend `s3://`",
            ],
        ),
        (
            "install.zh-CN.md",
            "对象存储",
            "### 6.8 本地磁盘",
            [
                "file://",
                "本地文件系统",
                "块设备卷",
                "试用与单机部署",
                "网络文件系统",
                "不在 Amberlog 对 `file://` 所作保证的范围之内",
                "推荐用 `s3://`",
            ],
        ),
    ];
    for (file, row, section, points) in cases {
        let text = std::fs::read_to_string(dir.join(file)).unwrap();
        let (row_text, section_text) = file_backend_passages(&text, row, section);
        eprintln!(
            "===== {file}：表格行 =====\n{row_text}\n===== {file}：{section} =====\n{section_text}"
        );
        assert!(
            row_text.contains("file://"),
            "{file} 的表格行没有提到 file://"
        );
        for p in points {
            assert!(section_text.contains(p), "{file} {section} 没有写到 {p:?}");
        }
    }
}

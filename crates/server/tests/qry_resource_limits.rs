//! 查询资源上限（REQ-AMB-QRY-RESOURCE-LIMITS-V2）：执行名额、准入、
//! 单查询内存上限、执行结束之后的内存、不波及摄取与就绪、两个指标、告警评估占用执行名额。
//!
//! 「已提交数据」以 `flush.interval` 为 `1h` 的实例摄取后优雅停机写出（停机时 flush 并提交），再以场景的配置重启。
//! 停住查询用可放行暂停点 `query.after_admission`（或 `query.after_input`）的 `hold-all` 模式：每个走到该步骤的查询都停住，
//! 控制文件改为 `go` 才一并继续；停住了几个从 `/metrics` 的 `amberlog_search_running` 读，或从控制文件旁的 `.held` 读。

mod common;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use common::alerting::{Clock, RuleToml, rules};
use common::api;
use common::webhook::{Receiver, Reply};
use common::{BEARER, Fixture, Kv, Resp, Server, gov, http};
use serde_json::{Value, json};

/// 场景约定的查询时间范围上界（2100-01-01T00:00:00Z，UTC 微秒）。
const END: i64 = 4_102_444_800_000_000;

/// 等待被测进程到达某一状态的时限。
const WAIT: Duration = Duration::from_secs(30);

/// `_search` 请求体：`start_time` 为 0、`end_time` 为 [`END`]，`size` 缺省时不写。
fn body(sql: &str, size: Option<u64>) -> String {
    let mut q = json!({"sql": sql, "start_time": 0, "end_time": END});
    if let Some(n) = size {
        q["size"] = n.into();
    }
    json!({ "query": q }).to_string()
}

fn search_at(addr: SocketAddr, body: &str) -> Resp {
    http(
        addr,
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        body.as_bytes(),
    )
}

fn search(server: &Server, sql: &str) -> Resp {
    search_at(server.addr, &body(sql, None))
}

fn search_sized(server: &Server, sql: &str, size: u64) -> Resp {
    search_at(server.addr, &body(sql, Some(size)))
}

/// 在另一线程提交 `sql`，返回其应答的句柄。
fn spawn_search(server: &Server, sql: &str, size: Option<u64>) -> JoinHandle<Resp> {
    let addr = server.addr;
    let b = body(sql, size);
    std::thread::spawn(move || search_at(addr, &b))
}

fn reason(r: &Resp) -> String {
    r.json()["reason"].as_str().unwrap_or_default().to_owned()
}

#[track_caller]
fn assert_reason(r: &Resp, status: u16, want: &str) {
    assert_eq!(r.status, status, "{}", r.text());
    assert_eq!(reason(r), want, "{}", r.text());
}

/// `/metrics`（认证区内，带有效凭据）中某条样本的取值（`series` 为指标名加标签，与样本行逐字相同）；不存在的序列按 0 计。
fn metric(server: &Server, series: &str) -> f64 {
    let r = server.get_auth("/metrics");
    assert_eq!(r.status, 200);
    r.text()
        .lines()
        .find_map(|l| {
            l.strip_prefix(series)
                .and_then(|rest| rest.strip_prefix(' '))
                .map(|v| v.trim().parse::<f64>().unwrap())
        })
        .unwrap_or(0.0)
}

fn running(server: &Server) -> f64 {
    metric(server, "amberlog_search_running")
}

fn rejected(server: &Server, reason: &str) -> f64 {
    metric(
        server,
        &format!("amberlog_search_rejected_total{{reason=\"{reason}\"}}"),
    )
}

/// 等到 `amberlog_search_running` 等于 `n`。
#[track_caller]
fn wait_running(server: &Server, n: f64) {
    let deadline = Instant::now() + WAIT;
    loop {
        let now = running(server);
        if now == n {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "amberlog_search_running 停在 {now}，未到 {n}：{:#?}",
            server.stderr_lines()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// 一个步骤的 `hold-all` 闸门：一开始就停住每个走到该步骤的调用，[`Gate::release`] 后一并放行。
struct Gate {
    step: &'static str,
    file: PathBuf,
}

impl Gate {
    fn new(fx: &Fixture, step: &'static str) -> Gate {
        let file = fx.root().join(format!("gate-{step}"));
        std::fs::write(&file, "hold-all").unwrap();
        Gate { step, file }
    }

    fn release(&self) {
        std::fs::write(&self.file, "go").unwrap();
    }

    /// 此刻停在该步骤的调用数（`hold-all` 写在控制文件旁的 `<控制文件>.held`；尚未写出为 0）。
    fn held(&self) -> usize {
        let mut held = self.file.clone().into_os_string();
        held.push(".held");
        std::fs::read_to_string(held)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0)
    }

    fn env(&self) -> (String, String) {
        (
            "TESTBED_FAULT_GATES".into(),
            format!("{}={}", self.step, self.file.display()),
        )
    }
}

/// 以配置 `overrides` 与环境变量 `env` 启动实例。
fn start(fx: &Fixture, overrides: &[Kv], env: &[(String, String)]) -> Server {
    let mut cmd = fx.serve(overrides);
    for (k, v) in env {
        cmd.env(k, v);
    }
    Server::start(cmd)
}

/// stream `app` 已有已提交数据（三条记录）。
fn seed_small(fx: &Fixture) {
    gov::committed(fx, &[("app", r#"[{"k":"x1"},{"k":"x2"},{"k":"x3"}]"#)]);
}

/// stream `stream` 已提交 `n` 条记录，字段 `a` 取 `0..n` 这 `n` 个互不相同的整数。
fn seed_distinct(fx: &Fixture, stream: &str, n: i64) {
    let records: Vec<Value> = (0..n).map(|i| json!({"a": i})).collect();
    let body = Value::Array(records).to_string();
    gov::committed(fx, &[(stream, &body)]);
}

/// 停住一个 `SELECT * FROM app` 于 `query.after_admission`，等到它占用执行名额。
fn hold_one(server: &Server) -> JoinHandle<Resp> {
    let q1 = spawn_search(server, "SELECT * FROM app", None);
    wait_running(server, 1.0);
    q1
}

const ONE_SLOT: Kv = ("query.max_concurrency", Some("1"));
const ONE_BYTE: Kv = ("query.max_memory_bytes", Some("1"));

// ---- 两个上限与适用范围 ----

#[test]
fn resource_limits_system_table_limited() {
    let fx = Fixture::new();
    seed_small(&fx);
    let gate = Gate::new(&fx, "query.after_admission");
    let server = start(&fx, &[ONE_SLOT], &[gate.env()]);
    let q1 = hold_one(&server);
    assert_reason(
        &search(&server, "SELECT dataset FROM _ledger"),
        429,
        "too_many_queries",
    );
    gate.release();
    assert_eq!(q1.join().unwrap().status, 200);
    drop(server);

    let fx = Fixture::new();
    gov::committed(&fx, &[("app", r#"[{"k":"a"}]"#), ("web", r#"[{"k":"w"}]"#)]);
    let server = start(&fx, &[ONE_BYTE], &[]);
    assert_reason(
        &search(
            &server,
            "SELECT status, count(*) AS n FROM _ledger GROUP BY status",
        ),
        400,
        "query_memory_exceeded",
    );
}

#[test]
fn resource_limits_default_concurrency() {
    let fx = Fixture::new();
    seed_small(&fx);
    let gate = Gate::new(&fx, "query.after_admission");
    let server = start(&fx, &[], &[gate.env()]);
    let first: Vec<_> = (0..4)
        .map(|_| spawn_search(&server, "SELECT * FROM app", None))
        .collect();
    wait_running(&server, 4.0);
    assert_reason(
        &search(&server, "SELECT * FROM app"),
        429,
        "too_many_queries",
    );
    gate.release();
    for q in first {
        let r = q.join().unwrap();
        assert_eq!(r.status, 200, "{}", r.text());
    }
}

#[test]
fn resource_limits_slot_released() {
    let fx = Fixture::new();
    seed_small(&fx);
    let gate = Gate::new(&fx, "query.after_admission");
    let server = start(&fx, &[ONE_SLOT], &[gate.env()]);
    let q1 = hold_one(&server);
    gate.release();
    assert_eq!(q1.join().unwrap().status, 200);
    let q2 = search(&server, "SELECT * FROM app");
    assert_eq!(q2.status, 200, "{}", q2.text());
    assert_eq!(running(&server), 0.0);
}

#[test]
fn resource_limits_memory_per_query() {
    const Q: &str = "SELECT a FROM app ORDER BY a";
    const RECORDS: i64 = 1_000_000;
    const CONCURRENT: usize = 8;
    let fx = Fixture::new();
    // 分批摄取（每批不超过请求体上限），`a` 取 0..RECORDS 这些互不相同的整数
    let bodies: Vec<String> = (0..5)
        .map(|k| {
            let chunk = RECORDS / 5;
            let records: Vec<Value> = (k * chunk..(k + 1) * chunk)
                .map(|i| json!({"a": i}))
                .collect();
            Value::Array(records).to_string()
        })
        .collect();
    let batches: Vec<(&str, &str)> = bodies.iter().map(|b| ("app", b.as_str())).collect();
    gov::committed(&fx, &batches);
    let q = || {
        json!({"query": {"sql": Q, "start_time": 0, "end_time": END, "from": 999_990, "size": 10}})
            .to_string()
    };

    // X：2 的整数次幂中、使实例对 Q 依次单独作答 SINGLE_TRIES 次都回 200 的最小上限（逐次加倍求得）。Q 的计量用量由
    // 数据量决定（前 K 行排序要留住全部 100 万行），同一并行度下浮动不到 7%，X 因而不小于单个 Q 的峰值。
    const SINGLE_TRIES: usize = 10;
    let mut x: u64 = 1 << 20;
    loop {
        let limit = x.to_string();
        let server = start(&fx, &[("query.max_memory_bytes", Some(&limit))], &[]);
        let mut passed = true;
        for _ in 0..SINGLE_TRIES {
            let r = search_at(server.addr, &q());
            match r.status {
                200 => {}
                400 if reason(&r) == "query_memory_exceeded" => {
                    passed = false;
                    break;
                }
                _ => panic!("上限 {x}：{}", r.text()),
            }
        }
        drop(server);
        if passed {
            break;
        }
        x *= 2;
        assert!(x <= 1 << 32, "上限加到 4 GiB 仍越限");
    }

    // 8 个 Q 都停在 `query.after_input`（数据源已交出全部输入，排序已收下的数据仍计在各自的内存池里）或已作答之后才放行。
    // 每查询一份池时 2X 不小于单个峰值的 2 倍，8 个都回 200；全进程共用一个池时 8 个停住时合计不少于约 6.4 倍单个峰值，
    // 而 2X 小于 4 倍单个峰值，必有 Q 以 400 作答。
    let gate = Gate::new(&fx, "query.after_input");
    let limit = (2 * x).to_string();
    let concurrency = CONCURRENT.to_string();
    let server = start(
        &fx,
        &[
            ("query.max_memory_bytes", Some(&limit)),
            ("query.max_concurrency", Some(&concurrency)),
        ],
        &[gate.env()],
    );
    let qs: Vec<_> = (0..CONCURRENT)
        .map(|_| {
            let (addr, b) = (server.addr, q());
            std::thread::spawn(move || search_at(addr, &b))
        })
        .collect();
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let answered = qs.iter().filter(|h| h.is_finished()).count();
        if gate.held() + answered == CONCURRENT {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "停住 {}、已作答 {answered}，未到 {CONCURRENT}：{:#?}",
            gate.held(),
            server.stderr_lines()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    gate.release();
    for q in qs {
        let r = q.join().unwrap();
        assert_eq!(r.status, 200, "X = {x}：{}", r.text());
    }
}

#[test]
fn resource_limits_sort_group_metered() {
    let queries = [
        "SELECT a FROM app ORDER BY a",
        "SELECT a, count(*) AS n FROM app GROUP BY a",
    ];
    let fx = Fixture::new();
    seed_distinct(&fx, "app", 1000);
    let server = start(&fx, &[ONE_BYTE], &[]);
    for q in queries {
        assert_reason(
            &search_sized(&server, q, 1000),
            400,
            "query_memory_exceeded",
        );
    }
    drop(server);

    let fx = Fixture::new();
    seed_distinct(&fx, "app", 1000);
    let server = start(&fx, &[], &[]);
    for q in queries {
        let r = search_sized(&server, q, 1000);
        assert_eq!(r.status, 200, "{q}：{}", r.text());
    }
}

// ---- 准入 ----

#[test]
fn resource_limits_admission_after_validation() {
    let fx = Fixture::new();
    seed_small(&fx);
    let gate = Gate::new(&fx, "query.after_admission");
    let server = start(&fx, &[ONE_SLOT], &[gate.env()]);
    let q1 = hold_one(&server);

    assert_reason(&search(&server, "SELEC * FROM app"), 400, "invalid_sql");
    assert_eq!(running(&server), 1.0);
    assert_reason(
        &search(&server, "SELECT * FROM no_such"),
        400,
        "stream_not_found",
    );
    assert_eq!(running(&server), 1.0);
    let bad_range =
        json!({"query": {"sql": "SELECT * FROM app", "start_time": 3000, "end_time": 1000}});
    assert_reason(
        &search_at(server.addr, &bad_range.to_string()),
        400,
        "invalid_time_range",
    );
    assert_eq!(running(&server), 1.0);
    assert_reason(
        &search(&server, "SELECT * FROM app"),
        429,
        "too_many_queries",
    );
    assert_eq!(running(&server), 1.0);

    gate.release();
    assert_eq!(q1.join().unwrap().status, 200);
}

#[test]
fn resource_limits_reject_immediately() {
    let fx = Fixture::new();
    seed_small(&fx);
    let gate = Gate::new(&fx, "query.after_admission");
    let server = start(&fx, &[("query.max_concurrency", Some("2"))], &[gate.env()]);
    let first: Vec<_> = (0..2)
        .map(|_| spawn_search(&server, "SELECT * FROM app", None))
        .collect();
    wait_running(&server, 2.0);

    let started = Instant::now();
    let r = search(&server, "SELECT * FROM app");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    assert_reason(&r, 429, "too_many_queries");
    assert!(first.iter().all(|q| !q.is_finished()), "先提交的查询已作答");

    gate.release();
    for q in first {
        assert_eq!(q.join().unwrap().status, 200);
    }
}

#[test]
fn resource_limits_catalog_exempt() {
    let fx = Fixture::new();
    seed_small(&fx);
    let gate = Gate::new(&fx, "query.after_admission");
    let server = start(&fx, &[ONE_SLOT], &[gate.env()]);
    let q1 = hold_one(&server);

    let r = server.get_auth("/iceberg/v1/config");
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(running(&server), 1.0);

    gate.release();
    assert_eq!(q1.join().unwrap().status, 200);
}

#[test]
fn resource_limits_shutdown_before_admission() {
    let fx = Fixture::new();
    seed_small(&fx);
    let gate = Gate::new(&fx, "query.after_admission");
    let mut server = start(&fx, &[ONE_SLOT], &[gate.env()]);
    let q1 = hold_one(&server);

    server.signal("TERM");
    // 等进程进入停机：停机开始后 `/readyz` 回 503
    server.wait_readyz(503, WAIT);
    assert_reason(&search(&server, "SELECT * FROM app"), 503, "unavailable");

    gate.release();
    let r = q1.join().unwrap();
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(server.wait_exit(WAIT), Some(Some(0)));
}

// ---- 执行中的内存上限 ----

#[test]
fn resource_limits_memory_exceeded() {
    let fx = Fixture::new();
    seed_distinct(&fx, "app", 1000);
    let server = start(&fx, &[ONE_BYTE], &[]);
    let r = search_sized(&server, "SELECT a, count(*) AS n FROM app GROUP BY a", 1000);
    assert_eq!(r.status, 400, "{}", r.text());
    let v = r.json();
    assert_eq!(v["code"], 400);
    assert_eq!(v["reason"], "query_memory_exceeded");
}

#[test]
fn resource_limits_no_partial_result() {
    let fx = Fixture::new();
    seed_distinct(&fx, "app", 1000);
    let server = start(&fx, &[ONE_BYTE], &[]);
    let r = search(&server, "SELECT a FROM app ORDER BY a LIMIT 5");
    assert_reason(&r, 400, "query_memory_exceeded");
    assert!(r.json().get("hits").is_none(), "{}", r.text());
}

/// 目录 `dir` 下全部文件（递归）的相对路径与长度。
fn listing(dir: &std::path::Path) -> Vec<(PathBuf, u64)> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let e = e.unwrap();
            let meta = e.metadata().unwrap();
            if meta.is_dir() {
                stack.push(e.path());
            } else {
                out.push((
                    e.path().strip_prefix(dir).unwrap().to_path_buf(),
                    meta.len(),
                ));
            }
        }
    }
    out.sort();
    out
}

/// `wal.dir` 的文件清单，不含就绪求值当场检查 `wal.dir` 可写（REQ-AMB-OBS-HEALTH-METRICS-V5）时新建又随即删除的
/// 探测文件 `.amberlog-write-probe-*`：后台每个探测周期求值一次，可能恰好落在列举的时刻；它不是查询的中间数据。
fn wal_listing(fx: &Fixture) -> Vec<(PathBuf, u64)> {
    let mut all = listing(&fx.wal_dir());
    all.retain(|(p, _)| !p.to_string_lossy().starts_with(".amberlog-write-probe-"));
    all
}

#[test]
fn resource_limits_no_spill() {
    let fx = Fixture::new();
    seed_distinct(&fx, "app", 1000);
    let tmp = fx.root().join("tmpdir");
    std::fs::create_dir(&tmp).unwrap();
    let server = start(
        &fx,
        &[ONE_BYTE],
        &[("TMPDIR".into(), tmp.display().to_string())],
    );
    let wal_before = wal_listing(&fx);
    let r = search_sized(&server, "SELECT a FROM app ORDER BY a", 1000);
    assert_reason(&r, 400, "query_memory_exceeded");
    assert!(listing(&tmp).is_empty(), "{:?}", listing(&tmp));
    assert_eq!(wal_listing(&fx), wal_before);
}

// ---- 执行结束之后的内存 ----

/// 被测进程 `/proc/<pid>/status` 中 `RssAnon` 的字节数。
fn rss_anon(server: &Server) -> u64 {
    let status = std::fs::read_to_string(format!("/proc/{}/status", server.pid())).unwrap();
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

/// 每条不同的 100 字节字符串（xorshift 生成的小写字母，不可压缩到可忽略）。
fn random_msg(state: &mut u64) -> String {
    (0..100)
        .map(|_| {
            *state ^= *state << 13;
            *state ^= *state >> 7;
            *state ^= *state << 17;
            (b'a' + (*state % 26) as u8) as char
        })
        .collect()
}

/// 被测进程是测试启动的 `amberlog serve` 子进程，读它的 `/proc/<pid>/status`（只为 Linux 编写）。
#[test]
fn resource_limits_rss_no_growth() {
    const RECORDS: i64 = 1_000_000;
    const Q: &str = "SELECT a, count(*) AS n, max(msg) AS m FROM app GROUP BY a";
    let fx = Fixture::new();
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let server = seed_one_hour(&fx, &SEAL_FAST, RECORDS, &mut |_| random_msg(&mut state));

    // 此后不再发其他请求（不轮询 /metrics 或 /readyz），只依次提交 20 次 Q
    let mut rss = Vec::new();
    for i in 0..20 {
        let r = search_sized(&server, Q, 1000);
        assert_eq!(r.status, 200, "第 {} 次：{}", i + 1, r.text());
        std::thread::sleep(Duration::from_secs(5));
        rss.push(rss_anon(&server));
    }
    let (r1, r20) = (rss[0], rss[19]);
    assert!(
        r20 <= r1 + 536_870_912,
        "R1 = {r1}，R20 = {r20}；各次 RssAnon（MiB）：{:?}",
        rss.iter().map(|b| b >> 20).collect::<Vec<_>>()
    );
}

/// 摄取 `records` 条记录到 `app`（`_timestamp` 在此刻 3 小时之前的那一小时里均匀铺开，`a` 取 1000 个整数，`msg` 由
/// `msg(i)` 给出），返回以配置 `overrides` 运行、已把这一小时并成一个数据文件的实例。
///
/// 摄取时以 `lifecycle.seal_grace` 为 `24h`、其余为 `overrides` 启动，等记录都可检索后以 SIGTERM 退出，再以 `overrides`
/// 重启：边摄取边封口时，中途的一次合并若已吃掉大部分数据，它与后到的文件构不成可合并的一组
/// （REQ-AMB-STO-SEAL-COMPACTION-V4），桶停在 2 个文件。
fn seed_one_hour(
    fx: &Fixture,
    overrides: &[Kv],
    records: i64,
    msg: &mut dyn FnMut(i64) -> String,
) -> Server {
    const BATCH: i64 = 50_000;
    let mut server = start(fx, &unsealed(overrides), &[]);
    let hour = (api::now_us() / api::HOUR_US - 3) * api::HOUR_US;
    for chunk in 0..records / BATCH {
        let rows: Vec<Value> = (chunk * BATCH..(chunk + 1) * BATCH)
            .map(|i| {
                json!({
                    "_timestamp": hour + i * (api::HOUR_US / records),
                    "a": i % 1000,
                    "msg": msg(i),
                })
            })
            .collect();
        api::ingest_ok(&server, "app", &Value::Array(rows).to_string());
    }
    let (r, all) = api::poll(
        Duration::from_secs(120),
        || search(&server, "SELECT count(*) AS n FROM app"),
        |r| r.status == 200 && r.json()["hits"][0]["n"] == records,
    );
    assert!(all, "记录未在 2 分钟内都可检索：{}", r.text());
    common::store::stop(&mut server);

    let server = start(fx, overrides, &[]);
    let (r, merged) = api::poll(
        Duration::from_secs(300),
        || search(&server, "SELECT count(*) AS n FROM app"),
        |r| r.status == 200 && r.json()["scan_files"] == 1,
    );
    assert!(merged, "封口合并未在 5 分钟内完成：{}", r.text());
    assert_eq!(r.json()["hits"][0]["n"], records, "{}", r.text());
    server
}

/// `overrides` 中的 `lifecycle.seal_grace` 换成 `24h`：摄取期间不封口、不合并（见 [`seed_one_hour`]）。
fn unsealed<'a>(overrides: &[Kv<'a>]) -> Vec<Kv<'a>> {
    let mut kv: Vec<Kv> = overrides
        .iter()
        .filter(|(k, _)| *k != "lifecycle.seal_grace")
        .copied()
        .collect();
    kv.push(("lifecycle.seal_grace", Some("\"24h\"")));
    kv
}

/// 合并查询所用的配置：封口快、这一小时并成一个文件。
const SEAL_FAST: [Kv; 3] = [
    ("lifecycle.seal_grace", Some("\"1s\"")),
    ("flush.interval", Some("\"1s\"")),
    ("compaction.target_file_bytes", Some("1073741824")),
];

/// 不对应块场景的回归测试（METERED-SCOPE 的实现手段）：扫描的输入逐批交给执行，不先整份读进内存，不计量的占用不随被扫
/// 数据量增长。100 万条、`msg` 各 400 字节（可压缩：数据文件远小于解码后的大小）；`match_all` 要解码 `msg` 列，整份
/// 读进内存时仅这一列就约 400 MB。执行期间每 20 毫秒读一次被测进程的 `RssAnon`，峰值比执行前多出的不得超过 300 MiB
/// （留给一个数据文件的原始字节、执行中的几批与各并行分区的缓冲）。
#[test]
fn scan_memory_does_not_grow_with_scanned_bytes() {
    const RECORDS: i64 = 1_000_000;
    const BOUND: u64 = 300 << 20;
    let fx = Fixture::new();
    let pattern = "abcdefgh".repeat(48);
    let server = seed_one_hour(&fx, &SEAL_FAST, RECORDS, &mut |i| {
        format!("{i:016}{pattern}")
    });

    // 执行名额空闲 1 秒后空闲内存已归还
    std::thread::sleep(Duration::from_secs(3));
    let before = rss_anon(&server);
    let q = spawn_search(
        &server,
        "SELECT count(*) AS n FROM app WHERE match_all('no-such-text')",
        None,
    );
    let mut peak = before;
    while !q.is_finished() {
        peak = peak.max(rss_anon(&server));
        std::thread::sleep(Duration::from_millis(20));
    }
    let r = q.join().unwrap();
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["hits"][0]["n"], 0, "{}", r.text());
    assert_eq!(r.json()["scan_records"], RECORDS, "{}", r.text());
    assert!(
        peak <= before + BOUND,
        "执行前 RssAnon {} MiB，执行中峰值 {} MiB",
        before >> 20,
        peak >> 20
    );
}

/// 不对应块场景的回归测试（RSS-NO-GROWTH 的条款前提只要求「上一次作答之后才提交」，不要求间隔）：数据与查询同场景
/// 「依次重复的查询不使常驻内存增长」，但每次作答之后立即读 `RssAnon`、立即提交下一次，名额从不空闲满 1 秒。
#[test]
fn rss_no_growth_back_to_back() {
    const RECORDS: i64 = 1_000_000;
    const Q: &str = "SELECT a, count(*) AS n, max(msg) AS m FROM app GROUP BY a";
    let fx = Fixture::new();
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let server = seed_one_hour(&fx, &SEAL_FAST, RECORDS, &mut |_| random_msg(&mut state));

    let mut rss = Vec::new();
    for i in 0..20 {
        let r = search_sized(&server, Q, 1000);
        assert_eq!(r.status, 200, "第 {} 次：{}", i + 1, r.text());
        rss.push(rss_anon(&server));
    }
    let (r1, r20) = (rss[0], rss[19]);
    assert!(
        r20 <= r1 + 536_870_912,
        "R1 = {r1}，R20 = {r20}；各次 RssAnon（MiB）：{:?}",
        rss.iter().map(|b| b >> 20).collect::<Vec<_>>()
    );
}

/// 不对应块场景的回归测试（METERED-SCOPE 的实现手段：各并行分区按需取批，中间没有队列）：命中多的「取最新 100 条」，
/// 计量用量只取决于并行分区数与批的大小，不随扫描的文件数增长。8 个小时、每小时一个数据文件，每小时 12.5 万条宽行，
/// 三分之一命中。X 为最近 4 小时的查询依次单独作答 3 次都回 200 的最小上限（2 的整数次幂，逐次加倍求得），全部 8 小时
/// 的同一查询在 2X 下 3 次都须回 200。用 4 小时而不用 1 小时求 X：各分区的前 K 行排序暂存的是过滤后凑满一批的行，
/// 每个分区要收到好几批输入才凑得满，1 小时的输入分到各分区还凑不满一批，计量峰值远低于上界（32 核机器实测 1 小时的 X 为
/// 4 MiB，8 小时约 36 MiB）。4 小时约 61 批输入，8 个分区时每个分区约 7～8 批、已凑满；核数少于 8 时分区更少，每个
/// 分区分到的批更多，更早凑满。引擎把单个输入分区轮转分发给各并行分区时，分发队列没有容量上限，下游较慢的分区积压的
/// 批次计入内存池，扫描量越大积压越多（32 核机器上 8 个文件约为 4 个文件的 6 倍以上）。
#[test]
fn limit_query_metered_memory_does_not_grow_with_scanned_files() {
    const HOURS: i64 = 8;
    const PER_HOUR: i64 = 125_000;
    const BATCH: i64 = 25_000;
    const TRIES: usize = 3;
    const Q: &str = "SELECT * FROM app WHERE match_all('error') LIMIT 100";
    let fx = Fixture::new();
    // 摄取期间不封口，摄取完以 SIGTERM 退出后再以 `SEAL_FAST` 重启（理由见 [`seed_one_hour`]）
    let mut server = start(&fx, &unsealed(&SEAL_FAST), &[]);
    let top = (api::now_us() / api::HOUR_US - 3) * api::HOUR_US;
    let pattern = "abcdefgh".repeat(48);
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    for h in 0..HOURS {
        let hour = top - (HOURS - 1 - h) * api::HOUR_US;
        for chunk in 0..PER_HOUR / BATCH {
            let rows: Vec<Value> = (chunk * BATCH..(chunk + 1) * BATCH)
                .map(|i| {
                    let mut r = json!({
                        "_timestamp": hour + i * (api::HOUR_US / PER_HOUR),
                        "level": if i % 3 == 0 { "error" } else { "info" },
                        "msg": format!("{i:016}{pattern}"),
                        "trace_id": random_msg(&mut state)[..32].to_string(),
                        "svc": format!("svc-{}", i % 17),
                    });
                    if i % 20 == 0 {
                        r["stack"] = json!(format!("at frame {i}\n").repeat(120));
                    }
                    r
                })
                .collect();
            api::ingest_ok(&server, "app", &Value::Array(rows).to_string());
        }
    }
    let (r, all) = api::poll(
        Duration::from_secs(120),
        || search(&server, "SELECT count(*) AS n FROM app"),
        |r| r.status == 200 && r.json()["hits"][0]["n"] == HOURS * PER_HOUR,
    );
    assert!(all, "记录未在 2 分钟内都可检索：{}", r.text());
    common::store::stop(&mut server);
    let server = start(&fx, &SEAL_FAST, &[]);
    let (r, merged) = api::poll(
        Duration::from_secs(600),
        || search(&server, "SELECT count(*) AS n FROM app"),
        |r| r.status == 200 && r.json()["scan_files"] == HOURS,
    );
    assert!(merged, "封口合并未在 10 分钟内完成：{}", r.text());
    drop(server);

    let q =
        |start: i64| json!({"query": {"sql": Q, "start_time": start, "end_time": END}}).to_string();
    let (last_hours, all_hours) = (
        q(top - 3 * api::HOUR_US),
        q(top - (HOURS - 1) * api::HOUR_US),
    );
    let answers = |limit: u64, body: &str| -> Result<(), String> {
        let limit = limit.to_string();
        let mut kv: Vec<Kv> = SEAL_FAST.to_vec();
        kv.push(("query.max_memory_bytes", Some(&limit)));
        let server = start(&fx, &kv, &[]);
        for _ in 0..TRIES {
            let r = search_at(server.addr, body);
            match r.status {
                200 => assert_eq!(r.json()["total"], 100, "{}", r.text()),
                400 if reason(&r) == "query_memory_exceeded" => return Err(r.text()),
                _ => panic!("上限 {limit}：{}", r.text()),
            }
        }
        Ok(())
    };
    let mut x: u64 = 4 << 20;
    while answers(x, &last_hours).is_err() {
        x *= 2;
        assert!(x <= 1 << 30, "上限加到 1 GiB 仍越限");
    }
    if let Err(e) = answers(2 * x, &all_hours) {
        panic!("最近 4 小时的 X = {} MiB，8 小时在 2X 下越限：{e}", x >> 20);
    }
}

// ---- 不波及摄取、就绪与其他端点 ----

#[test]
fn resource_limits_readyz_unchanged() {
    let fx = Fixture::new();
    seed_distinct(&fx, "app", 1000);
    let gate = Gate::new(&fx, "query.after_admission");
    let server = start(&fx, &[ONE_SLOT, ONE_BYTE], &[gate.env()]);
    assert_eq!(server.wait_readyz(200, WAIT).status, 200);
    let q1 = spawn_search(&server, "SELECT a FROM app ORDER BY a", None);
    wait_running(&server, 1.0);

    let ready = |server: &Server| {
        assert_eq!(server.get("/readyz").status, 200);
        assert_eq!(metric(server, "amberlog_ready"), 1.0);
    };
    assert_reason(
        &search(&server, "SELECT * FROM app"),
        429,
        "too_many_queries",
    );
    ready(&server);
    gate.release();
    assert_reason(&q1.join().unwrap(), 400, "query_memory_exceeded");
    ready(&server);
}

#[test]
fn resource_limits_ingest_unaffected() {
    let fx = Fixture::new();
    seed_small(&fx);
    let gate = Gate::new(&fx, "query.after_admission");
    let server = start(&fx, &[ONE_SLOT], &[gate.env()]);
    let q1 = hold_one(&server);
    assert_reason(
        &search(&server, "SELECT * FROM app"),
        429,
        "too_many_queries",
    );

    let r = api::ingest(&server, "app", r#"[{"k":"busy"}]"#);
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["status"][0]["successful"], 1, "{}", r.text());
    let w = api::minute_of(api::now_us());
    let r = api::sent(
        &server,
        "app",
        &format!("[{}]", api::sent_element("src", w, 1)),
    );
    assert_eq!(r.status, 200, "{}", r.text());

    gate.release();
    assert_eq!(q1.join().unwrap().status, 200);
    let (hits, ok) = api::poll(
        Duration::from_secs(10),
        || search(&server, "SELECT k FROM app WHERE k = 'busy'"),
        |r| r.status == 200 && r.json()["hits"].as_array().is_some_and(|h| h.len() == 1),
    );
    assert!(ok, "{}", hits.text());
}

#[test]
fn resource_limits_other_endpoints_unaffected() {
    let fx = Fixture::new();
    seed_small(&fx);
    let gate = Gate::new(&fx, "query.after_admission");
    let server = start(&fx, &[ONE_SLOT], &[gate.env()]);
    let q1 = hold_one(&server);
    // `/metrics` 在认证区内，带有效凭据；其余三个不要求凭据
    for path in ["/healthz", "/readyz", "/metrics", "/ui"] {
        let r = server.get_auth(path);
        assert_eq!(r.status, 200, "{path}：{}", r.text());
    }
    assert_eq!(running(&server), 1.0);
    gate.release();
    assert_eq!(q1.join().unwrap().status, 200);
}

// ---- 指标 ----

#[test]
fn resource_limits_rejected_metric() {
    let fx = Fixture::new();
    seed_distinct(&fx, "app", 1000);
    let gate = Gate::new(&fx, "query.after_admission");
    let server = start(&fx, &[ONE_SLOT, ONE_BYTE], &[gate.env()]);
    let busy0 = rejected(&server, "too_many_queries");
    let mem0 = rejected(&server, "query_memory_exceeded");

    let q1 = spawn_search(&server, "SELECT a FROM app ORDER BY a", None);
    wait_running(&server, 1.0);
    assert_reason(
        &search(&server, "SELECT * FROM app"),
        429,
        "too_many_queries",
    );
    assert_reason(&search(&server, "SELEC * FROM app"), 400, "invalid_sql");
    gate.release();
    assert_reason(&q1.join().unwrap(), 400, "query_memory_exceeded");

    assert_eq!(rejected(&server, "too_many_queries"), busy0 + 1.0);
    assert_eq!(rejected(&server, "query_memory_exceeded"), mem0 + 1.0);
}

#[test]
fn resource_limits_running_metric() {
    let fx = Fixture::new();
    seed_small(&fx);
    let gate = Gate::new(&fx, "query.after_admission");
    let server = start(&fx, &[("query.max_concurrency", Some("3"))], &[gate.env()]);
    let qs: Vec<_> = (0..2)
        .map(|_| spawn_search(&server, "SELECT * FROM app", None))
        .collect();
    std::thread::sleep(Duration::from_secs(5));
    assert_eq!(running(&server), 2.0);
    gate.release();
    for q in qs {
        assert_eq!(q.join().unwrap().status, 200);
    }
    assert_eq!(running(&server), 0.0);
}

// ---- 告警评估 ----

/// 以 `clock` 的偏移、一个执行名额与一条投递到 `rx` 的规则 `r1`（`interval` 为 `1m`、`window` 为 `7d`、
/// `sql` 为 `SELECT * FROM app`、`condition` 为 `{ rows = "> 0" }`）启动实例。
fn start_alerting(fx: &Fixture, clock: Clock, rx: &Receiver, env: &[(String, String)]) -> Server {
    let r = rules(&[RuleToml::new("r1", &rx.url("/hook"))]);
    let mut cmd = fx.serve(&[ONE_SLOT, ("alerts.rules", Some(&r))]);
    clock.apply(&mut cmd);
    for (k, v) in env {
        cmd.env(k, v);
    }
    Server::start(cmd)
}

/// 被测进程的时钟落在某整分之前 20 秒：留出启动的用时，评估在约 20 秒后到期（不开 `fault-injection` 时等真实的整分）。
fn alert_clock() -> Clock {
    Clock::before_minute(Duration::from_secs(20))
}

/// 评估到期并走到暂停点所需的时限：至多一个周期，加启动与余量。
const TICK_WAIT: Duration = Duration::from_secs(90);

#[test]
fn resource_limits_alert_evaluation_holds_slot() {
    let fx = Fixture::new();
    seed_small(&fx);
    let rx = Receiver::http();
    let gate = Gate::new(&fx, "alert.after_admission");
    let server = start_alerting(&fx, alert_clock(), &rx, &[gate.env()]);
    let deadline = Instant::now() + TICK_WAIT;
    while gate.held() == 0 {
        assert!(
            Instant::now() < deadline,
            "{TICK_WAIT:?} 内没有评估停在 alert.after_admission：{:#?}",
            server.stderr_lines()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_reason(
        &search(&server, "SELECT * FROM app"),
        429,
        "too_many_queries",
    );
    assert_eq!(running(&server), 1.0);
    gate.release();
}

#[test]
fn resource_limits_slot_free_during_delivery() {
    let fx = Fixture::new();
    seed_small(&fx);
    let rx = Receiver::http();
    rx.always(Reply::status(503).delay(Duration::from_secs(9)).into());
    let server = start_alerting(&fx, alert_clock(), &rx, &[]);
    let first = rx.wait_for(1, TICK_WAIT)[0].clone();
    let sent = api::now_us();
    let r = search(&server, "SELECT * FROM app");
    let after = Duration::from_micros((sent - first.at_us()).max(0) as u64);
    assert!(
        after < Duration::from_secs(2),
        "收到第一个请求之后 {after:?} 才提交"
    );
    assert_eq!(r.status, 200, "{}", r.text());
}

// ---- 其他块中与查询资源上限共用搭建的场景 ----

/// REQ-AMB-QRY-SEARCH-ENDPOINT-V7 场景「越限在报告顺序中的位置」：系统表的 `unknown_column` 与 stream 不存在在准入之前，
/// stream 查询的 `unknown_column` 在准入之后。
#[test]
fn search_resource_limit_report_order() {
    let fx = Fixture::new();
    seed_small(&fx);
    let gate = Gate::new(&fx, "query.after_admission");
    let server = start(&fx, &[ONE_SLOT], &[gate.env()]);
    let q1 = hold_one(&server);
    assert_reason(
        &search(&server, "SELECT typo FROM _ledger"),
        400,
        "unknown_column",
    );
    assert_reason(
        &search(&server, "SELECT * FROM no_such"),
        400,
        "stream_not_found",
    );
    assert_reason(
        &search(&server, "SELECT typo FROM app"),
        429,
        "too_many_queries",
    );
    assert_reason(
        &search(&server, "SELECT dataset FROM _ledger"),
        429,
        "too_many_queries",
    );
    gate.release();
    assert_eq!(q1.join().unwrap().status, 200);
}

/// REQ-AMB-ERR-ENVELOPE-V6 场景「并发越限以 429 错误信封作答」。
#[test]
fn error_too_many_queries_429_envelope() {
    let fx = Fixture::new();
    seed_small(&fx);
    let gate = Gate::new(&fx, "query.after_admission");
    let server = start(&fx, &[ONE_SLOT], &[gate.env()]);
    let q1 = hold_one(&server);
    let r = http(
        server.addr,
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER), ("x-request-id", "q-2")],
        body("SELECT * FROM app", None).as_bytes(),
    );
    assert_eq!(r.status, 429, "{}", r.text());
    let v = r.json();
    assert_eq!(v["code"], 429);
    assert_eq!(v["reason"], "too_many_queries");
    assert_eq!(r.header("x-request-id"), Some("q-2"));
    gate.release();
    assert_eq!(q1.join().unwrap().status, 200);
}

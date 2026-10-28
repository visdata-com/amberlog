//! 优雅退役（REQ-AMB-LIFE-RETIREMENT）场景的共用搭建，按块的场景约定：
//!
//! - 「退役前的实例」：以 `lifecycle.seal_grace` 为 `24h`、`lifecycle.lake_watermark` 为 `200h`、`flush.interval` 为 `1s`、
//!   `storage.delete_delay` 为 `1h` 运行的 `amberlog serve`（[`Fixture::before`]）；客户端经 `_json` 向 stream 摄取带唯一
//!   整数列 `seq` 的记录（[`Seq`]）。
//! - 「运行 `retire`」：以相同的 `object_store.url`、`storage.prefix` 与 `wal.dir`，`storage.delete_delay` 为 `1s`、其余配置
//!   不变运行 `amberlog retire --config <该配置文件>` 并等其退出（[`Fixture::retire`]、[`Fixture::run_retire`]）。
//! - 「报告」：`retire` 写到 stdout 的那一行 JSON（[`Ran::report`]）。
//! - 「直读」：不启动任何 Amberlog 进程，用 iceberg-rust 自带的读取器 `StaticTable` 按 `version-hint.text` 定位当前版本读
//!   `<storage.prefix>/_iceberg/default/<stream>/` 下的冷层表（[`direct_seqs`]）。

use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

use arrow::array::{Array, Int64Array};
use arrow::record_batch::RecordBatch;
use serde_json::Value;

use super::api::{ingest_ok, now_us};
use super::store::HOUR;
use super::{Fixture, Kv, Server, amberlog};

/// 「退役前的实例」的配置。
pub const BEFORE: &[Kv<'static>] = &[
    ("lifecycle.seal_grace", Some("\"24h\"")),
    ("lifecycle.lake_watermark", Some("\"200h\"")),
    ("flush.interval", Some("\"1s\"")),
    ("storage.delete_delay", Some("\"1h\"")),
];

/// 等 `retire` 退出的上限。
pub const RETIRE_WAIT: Duration = Duration::from_secs(300);

/// 带唯一整数列 `seq` 的记录：每次摄取接着上一次的 `seq` 编号。
#[derive(Default)]
pub struct Seq(pub i64);

impl Seq {
    /// 以一个请求向 `stream` 摄取 `_timestamp` 依次为 `ts` 的记录，断言全部受理（HTTP 200）。返回这些记录的 `seq`。
    pub fn ingest(&mut self, server: &Server, stream: &str, ts: &[i64]) -> Vec<i64> {
        let first = self.0;
        let records: Vec<String> = ts
            .iter()
            .enumerate()
            .map(|(i, t)| format!(r#"{{"_timestamp":{t},"seq":{}}}"#, first + i as i64))
            .collect();
        let r = ingest_ok(server, stream, &format!("[{}]", records.join(",")));
        assert_eq!(r["status"][0]["successful"], ts.len(), "{r}");
        self.0 += ts.len() as i64;
        (first..self.0).collect()
    }
}

/// 落在此刻前第 `n` 小时所在小时桶内的 `count` 个时间戳（自该小时起点后 1 分钟起、每 1 毫秒一个）。
pub fn in_hour_ago(n: i64, count: usize) -> Vec<i64> {
    let start = now_us().div_euclid(HOUR) * HOUR - n * HOUR + 60_000_000;
    (0..count as i64).map(|i| start + i * 1000).collect()
}

/// 此刻的 `count` 个时间戳（每 1 毫秒一个）。
pub fn now_ts(count: usize) -> Vec<i64> {
    let start = now_us() - 1_000_000;
    (0..count as i64).map(|i| start + i * 1000).collect()
}

/// 一次 `retire` 运行的结果。
pub struct Ran {
    pub code: Option<i32>,
    pub stdout: Vec<String>,
    pub stderr: Vec<String>,
}

impl Ran {
    /// stdout 恰为一行 JSON 对象时的报告。
    pub fn report(&self) -> Value {
        assert_eq!(
            self.stdout.len(),
            1,
            "stdout 应恰为一行：{:#?}\n{:#?}",
            self.stdout,
            self.stderr
        );
        serde_json::from_str(&self.stdout[0]).expect("报告是 JSON")
    }

    /// stderr 中 `event` 为 `event` 的 JSON 日志。
    pub fn events(&self, event: &str) -> Vec<Value> {
        self.stderr
            .iter()
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .filter(|v| v["event"] == event)
            .collect()
    }
}

impl Fixture {
    /// 以「退役前的实例」的配置加 `extra` 启动 `amberlog serve`，等到 `ready`。
    pub fn before(&self, extra: &[Kv]) -> Server {
        Server::start(self.serve(&merged(BEFORE, extra)))
    }

    /// `amberlog serve` 的命令（「退役前的实例」的配置加 `extra`），不等 `ready`。
    pub fn before_cmd(&self, extra: &[Kv]) -> Command {
        self.serve(&merged(BEFORE, extra))
    }

    /// 「运行 `retire`」所用的配置文件：「退役前的实例」的配置，`storage.delete_delay` 为 `1s`，再加 `extra`。
    pub fn retire_config(&self, extra: &[Kv]) -> PathBuf {
        let mut kv = merged(BEFORE, &[("storage.delete_delay", Some("\"1s\""))]);
        kv = merged(&kv, extra);
        let p = self.root().join("retire.toml");
        std::fs::write(&p, self.toml(&kv)).unwrap();
        p
    }

    /// `amberlog retire --config <配置>`。
    pub fn retire(&self, extra: &[Kv]) -> Command {
        let path = self.retire_config(extra);
        let mut c = amberlog(self.root());
        c.arg("retire").arg("--config").arg(path);
        c
    }

    /// 运行 `retire` 并等其退出（至多 [`RETIRE_WAIT`]）。
    pub fn run_retire(&self, extra: &[Kv]) -> Ran {
        run(self.retire(extra))
    }
}

/// 运行命令并等其退出（至多 [`RETIRE_WAIT`]），收集 stdout 与 stderr。
pub fn run(cmd: Command) -> Ran {
    let mut p = Server::spawn(cmd);
    finish(&mut p, RETIRE_WAIT)
}

/// 等已启动的进程退出（至多 `limit`）。
pub fn finish(p: &mut Server, limit: Duration) -> Ran {
    let code = p.wait_exit(limit);
    assert!(
        code.is_some(),
        "{} 秒内未退出：{:#?}",
        limit.as_secs(),
        p.stderr_lines()
    );
    // stdout、stderr 由收集线程读到管道关闭为止
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut stdout = p.stdout_lines();
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
        let again = p.stdout_lines();
        if again == stdout {
            break;
        }
        stdout = again;
    }
    Ran {
        code: code.flatten(),
        stdout,
        stderr: p.stderr_lines(),
    }
}

fn merged<'a>(base: &[Kv<'a>], extra: &[Kv<'a>]) -> Vec<Kv<'a>> {
    let mut out: Vec<Kv<'a>> = base
        .iter()
        .filter(|(k, _)| !extra.iter().any(|(e, _)| e == k))
        .copied()
        .collect();
    out.extend_from_slice(extra);
    out
}

/// 直读 stream 的冷层表：`by_hint` 为真时按 `version-hint.text` 定位当前版本，否则取 `metadata/` 下编号最大的
/// `v<N>.metadata.json`；返回全表的 `seq`（按读出的先后）。尚无冷层表为空。
pub fn direct_seqs(f: &Fixture, stream: &str, by_hint: bool) -> Vec<i64> {
    use futures::TryStreamExt;
    use iceberg::TableIdent;
    use iceberg::io::FileIO;
    use iceberg::table::StaticTable;

    let cold = f.cold(stream);
    let Some(latest) = cold.version() else {
        return Vec::new();
    };
    let n = if by_hint {
        String::from_utf8(cold.read("metadata/version-hint.text"))
            .unwrap()
            .trim()
            .parse::<u64>()
            .unwrap()
    } else {
        latest
    };
    let location = cold.metadata(n)["location"].as_str().unwrap().to_owned();
    let metadata = format!("{location}/metadata/v{n}.metadata.json");
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let batches: Vec<RecordBatch> = rt.block_on(async {
        let table = StaticTable::from_metadata_file(
            &metadata,
            TableIdent::from_strs(["default", stream]).unwrap(),
            FileIO::new_with_fs(),
        )
        .await
        .unwrap();
        table
            .scan()
            .select_all()
            .build()
            .unwrap()
            .to_arrow()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap()
    });
    let mut out = Vec::new();
    for b in &batches {
        let col = b
            .column_by_name("seq")
            .expect("冷层表有 seq 列")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("seq 为 Int64");
        out.extend(
            (0..col.len())
                .filter(|i| col.is_valid(*i))
                .map(|i| col.value(i)),
        );
    }
    out
}

/// 一个场景的注入与暂停点控制（跨进程共用）：对象存储注入的控制文件、请求记录文件、每个暂停点一个控制文件、崩溃点的标记文件。
pub struct Ctl {
    root: PathBuf,
}

impl Ctl {
    pub fn new(f: &Fixture) -> Ctl {
        let root = f.root().join("retire-ctl");
        std::fs::create_dir_all(root.join("gates")).unwrap();
        std::fs::write(root.join("store"), "").unwrap();
        std::fs::write(root.join("store.log"), "").unwrap();
        Ctl { root }
    }

    /// 对象存储注入规则（每行一条，见 `amberlog_storage::fault::store`）；运行中改写即生效。
    pub fn faults(&self, rules: &str) {
        std::fs::write(self.root.join("store"), rules).unwrap();
    }

    /// 请求记录（`<操作> <相对根前缀的对象键>`）。
    pub fn requests(&self) -> Vec<String> {
        std::fs::read_to_string(self.root.join("store.log"))
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    fn gate_file(&self, step: &str) -> PathBuf {
        self.root.join("gates").join(step)
    }

    /// 启用暂停点 `step`：下一个走到它的调用停住。
    pub fn hold(&self, step: &str) {
        std::fs::write(self.gate_file(step), "hold").unwrap();
    }

    pub fn held(&self, step: &str) -> bool {
        std::fs::read_to_string(self.gate_file(step)).is_ok_and(|s| s.trim() == "held")
    }

    pub fn wait_held(&self, step: &str, p: &Server, limit: Duration) {
        let deadline = Instant::now() + limit;
        while !self.held(step) {
            assert!(
                Instant::now() < deadline,
                "{} 秒内没有走到暂停点 {step}：{:#?}",
                limit.as_secs(),
                p.stderr_lines()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    pub fn release(&self, step: &str) {
        std::fs::write(self.gate_file(step), "go").unwrap();
    }

    fn marker(&self) -> PathBuf {
        self.root.join("pause-marker")
    }

    /// 等进程走到崩溃点（`TESTBED_FAULT_PAUSE`）后以 SIGKILL 强杀。
    pub fn kill_at_pause(&self, p: &mut Server, limit: Duration) {
        let deadline = Instant::now() + limit;
        while !self.marker().exists() {
            assert!(
                Instant::now() < deadline,
                "{} 秒内没有走到崩溃点：{:#?}",
                limit.as_secs(),
                p.stderr_lines()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        p.sigkill();
        let _ = std::fs::remove_file(self.marker());
    }

    /// 给命令装上对象存储注入与请求记录、暂停点 `gates`（各自的控制文件写为 `hold` 的才停）与崩溃点 `pause`。
    pub fn apply(&self, mut cmd: Command, gates: &[&str], pause: Option<&str>) -> Command {
        cmd.env("TESTBED_FAULT_STORE", self.root.join("store"))
            .env("TESTBED_FAULT_STORE_LOG", self.root.join("store.log"));
        if !gates.is_empty() {
            let gates: Vec<String> = gates
                .iter()
                .map(|g| format!("{g}={}", self.gate_file(g).display()))
                .collect();
            cmd.env("TESTBED_FAULT_GATES", gates.join(";"));
        }
        if let Some(step) = pause {
            let _ = std::fs::remove_file(self.marker());
            cmd.env("TESTBED_FAULT_PAUSE", step)
                .env("TESTBED_FAULT_MARKER", self.marker());
        }
        cmd
    }
}

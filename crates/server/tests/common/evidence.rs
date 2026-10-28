//! 证据导出与查验的系统测试共用（REQ-AMB-STO-EVIDENCE-EXPORT、REQ-AMB-STO-EVIDENCE-VERIFY 的场景约定）：写入实例 W 的配置、
//! 「stream `app` 已入湖」「stream `app` 有热层数据」的搭建、以子进程运行 `amberlog evidence export` 与解开证据包。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::Duration;

use serde_json::Value;

use super::api::{ingest_ok, now_us};
use super::store::{Ds, FLUSH_1S, HOUR, bucket_label, hour_of};
use super::{Fixture, Kv, Server, amberlog};

/// 场景约定的 W：`lifecycle.lake_watermark` 为 `1h`、`storage.delete_delay` 为 `1h`（另以 `flush.interval` 为 `1s`
/// 使摄取很快提交进快照）；`storage.prefix` 取缺省的 `amberlog`。
pub const W: [Kv<'static>; 3] = [
    ("lifecycle.lake_watermark", Some("\"1h\"")),
    ("storage.delete_delay", Some("\"1h\"")),
    FLUSH_1S,
];

/// 「stream `app` 已入湖」的等待上限。
pub const LAKE_LIMIT: Duration = Duration::from_secs(600);

/// 一次子命令运行的结果。
pub struct Run {
    pub code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: String,
}

impl Run {
    pub fn stdout_text(&self) -> String {
        String::from_utf8_lossy(&self.stdout).into_owned()
    }

    /// stdout 恰为一行 JSON 时解析之。
    pub fn json(&self) -> Value {
        let text = self.stdout_text();
        assert!(
            text.ends_with('\n') && text.matches('\n').count() == 1,
            "stdout 不是恰好一行：{text:?}\nstderr：{}",
            self.stderr
        );
        serde_json::from_str(text.trim_end()).unwrap()
    }
}

impl From<Output> for Run {
    fn from(o: Output) -> Run {
        Run {
            code: o.status.code(),
            stdout: o.stdout,
            stderr: String::from_utf8_lossy(&o.stderr).into_owned(),
        }
    }
}

impl Fixture {
    /// `amberlog evidence <action> --config <与 W 同一份配置> <args>`（尚未运行）。
    pub fn evidence_cmd(&self, overrides: &[Kv], action: &str, args: &[&str]) -> Command {
        let cfg = self.root().join("w.toml");
        std::fs::write(&cfg, self.toml(overrides)).unwrap();
        let mut c = amberlog(self.root());
        c.arg("evidence")
            .arg(action)
            .arg("--config")
            .arg(cfg)
            .args(args);
        c
    }

    /// 运行一次导出直到退出。
    pub fn export(&self, overrides: &[Kv], args: &[&str]) -> Run {
        self.evidence_cmd(overrides, "export", args)
            .stdin(Stdio::null())
            .output()
            .expect("运行 amberlog evidence export")
            .into()
    }

    /// 本测试临时目录下的路径（场景里的 `/tmp/<名>` 一律落在这里）。
    pub fn tmp(&self, name: &str) -> PathBuf {
        let dir = self.root().join("tmp");
        std::fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    /// 全时段导出 `default/app` 到 `name`，断言退出码为 `want`，返回证据包路径与运行结果。
    pub fn export_app(
        &self,
        overrides: &[Kv],
        name: &str,
        extra: &[&str],
        want: i32,
    ) -> (PathBuf, Run) {
        let out = self.tmp(name);
        let mut args = vec![
            "--dataset",
            "default/app",
            "--output",
            out.to_str().unwrap(),
        ];
        args.extend_from_slice(extra);
        let r = self.export(overrides, &args);
        assert_eq!(
            r.code,
            Some(want),
            "stderr：{}\nstdout：{}",
            r.stderr,
            r.stdout_text()
        );
        (out, r)
    }

    /// 「stream `app` 有热层数据」：W 已向 `app` 摄取 `_timestamp` 为此刻的 `n` 条记录并已提交进快照。
    pub fn hot_data(&self, w: &Server, n: u64) {
        let now = now_us();
        let recs: Vec<String> = (0..n)
            .map(|i| format!(r#"{{"_timestamp":{},"seq":{i}}}"#, now + i as i64))
            .collect();
        let before = self.dataset("app").current().map_or(0, |id| {
            self.dataset("app").snapshot(id)["summary"]["record_count"]
                .as_u64()
                .unwrap()
        });
        ingest_ok(w, "app", &format!("[{}]", recs.join(",")));
        self.dataset("app")
            .wait_records(before + n, Duration::from_secs(60));
    }

    /// 「stream `app` 已按 REQ-AMB-LIFE-RETIREMENT 退役完成」：W 向 `app` 摄取 50 条此刻的记录后停止，再以同一对象存储与
    /// `wal.dir` 运行 `amberlog retire`（[`Fixture::run_retire`]）并等其以退出码 0 退出。
    pub fn retired(&self) {
        let mut w = self.start(&W);
        self.hot_data(&w, 50);
        super::store::stop(&mut w);
        let r = self.run_retire(&[]);
        assert_eq!(r.code, Some(0), "{:#?}", r.stderr);
    }

    /// 对象存储中的退役记录 `amberlog/_system/retirement.json`（文件路径）。
    pub fn retirement_record(&self) -> PathBuf {
        self.store_dir().join("amberlog/_system/retirement.json")
    }

    /// 「stream `app` 已入湖」：W 已向 `app` 摄取 `_timestamp` 落在此刻前第 3 小时的 100 条记录，并等到该桶的交接完成、
    /// 热层当前快照中该桶不再含存活数据文件（至多 10 分钟）。返回该桶的 `YYYYMMDDHH`。
    pub fn laked(&self, w: &Server) -> String {
        self.laked_at(w, hour_of(now_us()) - 3 * HOUR, 100)
    }

    /// 向 `app` 摄取 `_timestamp` 自 `bucket` 起的 `n` 条记录并等它入湖。
    pub fn laked_at(&self, w: &Server, bucket: i64, n: u64) -> String {
        let recs: Vec<String> = (0..n)
            .map(|i| {
                format!(
                    r#"{{"_timestamp":{},"seq":{i}}}"#,
                    bucket + 1 + i as i64 * 1000
                )
            })
            .collect();
        ingest_ok(w, "app", &format!("[{}]", recs.join(",")));
        let label = bucket_label(bucket);
        let ds = self.dataset("app");
        let m = ds.wait_handovers(&label, 1, LAKE_LIMIT);
        assert!(
            m["files"].as_array().unwrap().is_empty(),
            "入湖后桶 {label} 仍有存活文件：{m}"
        );
        label
    }
}

/// 用 `tar -xf` 把证据包解到一个新的空目录，返回该目录下各普通文件（相对路径 → 内容）。
pub fn untar(package: &Path) -> (PathBuf, BTreeMap<String, Vec<u8>>) {
    let dir = package.with_extension("x");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let st = Command::new("tar")
        .arg("-xf")
        .arg(package)
        .arg("-C")
        .arg(&dir)
        .status()
        .unwrap();
    assert!(st.success(), "tar -xf 失败");
    let mut files = BTreeMap::new();
    for rel in super::lake::keys(&dir) {
        files.insert(rel.clone(), std::fs::read(dir.join(&rel)).unwrap());
    }
    (dir, files)
}

/// 解开的证据包中某个 JSON 成员。
pub fn member_json(files: &BTreeMap<String, Vec<u8>>, name: &str) -> Value {
    serde_json::from_slice(
        files
            .get(name)
            .unwrap_or_else(|| panic!("证据包没有 {name}")),
    )
    .unwrap()
}

/// `files.jsonl` 的各行。
pub fn files_lines(files: &BTreeMap<String, Vec<u8>>) -> Vec<Value> {
    String::from_utf8(files["files.jsonl"].clone())
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

/// `check.json` 某数组中 `kind` 为 `kind` 的元素的 `object`。
pub fn objects_of(check: &Value, array: &str, kind: &str) -> Vec<String> {
    check[array]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["kind"] == kind)
        .map(|e| e["object"].as_str().unwrap().to_owned())
        .collect()
}

/// 当前快照所引用的各桶 manifest 的相对路径（相对数据集前缀）。
pub fn bucket_manifest_paths(ds: &Ds) -> Vec<String> {
    ds.head()["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["manifest"].as_str().unwrap().to_owned())
        .collect()
}

/// 不经 Amberlog 写入实例、直接对冷层表做 Iceberg 提交（场景里的「外部用一个 Iceberg 客户端」）：用 `amberlog-storage` 的
/// 冷层提交代码在测试进程里写出新的清单、清单列表与 `v<N+1>.metadata.json`，摘要不带 `amberlog.*` 属性，不写冷层变更段、
/// 不写交接记录。
pub struct ExternalIceberg {
    store: amberlog_storage::store::Store,
    layout: amberlog_storage::cold::layout::ColdLayout,
    dataset: amberlog_common::dataset::Dataset,
}

impl ExternalIceberg {
    pub fn new(fx: &Fixture, stream: &str) -> ExternalIceberg {
        use amberlog_storage::store::{Location, S3Options, Store};
        let location = Location::File {
            dir: fx.store_dir(),
        };
        let store = Store::open(&location, &S3Options::default(), "amberlog").unwrap();
        let layout = amberlog_storage::cold::layout::ColdLayout::new(&location, store.root.clone());
        ExternalIceberg {
            store,
            layout,
            dataset: amberlog_common::dataset::Dataset::new("default", stream),
        }
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    fn commit(&self, change: amberlog_storage::cold::commit::Change) -> u64 {
        use amberlog_storage::cold::commit::{Committer, Progress};
        let committer = Committer::new(
            self.store.clone(),
            self.layout.clone(),
            std::sync::Arc::new(amberlog_storage::gate::DatasetGates::new()),
            8,
        );
        Self::rt().block_on(async {
            let _guard = committer.lock(&self.dataset).await;
            let mut progress = Progress::new(&self.dataset, committer.inflight());
            committer
                .commit(&self.dataset, &change, &mut progress)
                .await
                .unwrap()
                .version
        })
    }

    /// 提交一次删除，使冷层数据文件 `rels`（相对冷层表位置）离开冷层存活文件集。返回新版本号。
    pub fn remove(&self, rels: &[&str]) -> u64 {
        self.commit(amberlog_storage::cold::commit::Change::Delete {
            paths: rels.iter().map(|r| (*r).to_owned()).collect(),
            summary: Default::default(),
        })
    }

    /// 把冷层表目录下已存在的 Parquet 文件 `rel` 作为一个数据文件追加进冷层存活文件集。返回新版本号。
    pub fn append_existing(&self, rel: &str) -> u64 {
        use amberlog_storage::cold::{datafile, layout};
        let bytes = Self::rt()
            .block_on(
                self.store
                    .read(&self.layout.object_path(&self.dataset, rel)),
            )
            .unwrap()
            .expect("文件存在")
            .bytes;
        let rt = Self::rt();
        let (n, _) = rt
            .block_on(amberlog_storage::cold::current::version(
                &self.store,
                &self.layout,
                &self.dataset,
            ))
            .unwrap();
        let meta = rt
            .block_on(amberlog_storage::cold::read::metadata(
                &self.store,
                &self.layout,
                &self.dataset,
                n,
            ))
            .unwrap();
        let day = layout::parse_data_file_path(rel)
            .map(|d| {
                let days = |y: i64, m: i64, d: i64| {
                    // 自 1970-01-01 起的天数（公历）
                    let y = if m <= 2 { y - 1 } else { y };
                    let era = y.div_euclid(400);
                    let yoe = y - era * 400;
                    let mp = (m + 9) % 12;
                    let doy = (153 * mp + 2) / 5 + d - 1;
                    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
                    era * 146_097 + doe - 719_468
                };
                days(
                    d[0..4].parse().unwrap(),
                    d[5..7].parse().unwrap(),
                    d[8..10].parse().unwrap(),
                ) as i32
            })
            .expect("冷层数据文件路径");
        let data_file = datafile::data_file_from_footer(
            self.layout.object_uri(&self.dataset, rel),
            &bytes,
            meta.current_schema(),
            day,
        )
        .unwrap();
        let schema = self.hot_schema();
        self.commit(amberlog_storage::cold::commit::Change::Append {
            schema,
            files: vec![data_file],
            summary: Default::default(),
        })
    }

    /// 把冷层数据文件 `rel` 复制为同一分区日下的新文件，追加进冷层存活文件集；返回新文件的相对路径。
    pub fn append_copy(&self, rel: &str) -> String {
        let day = rel
            .strip_prefix("data/_timestamp_day=")
            .and_then(|r| r.split_once('/'))
            .map(|(d, _)| d.to_owned())
            .expect("冷层数据文件路径");
        let new = format!(
            "data/_timestamp_day={day}/{}.parquet",
            amberlog_storage::layout::new_object_id()
        );
        let rt = Self::rt();
        let bytes = rt
            .block_on(
                self.store
                    .read(&self.layout.object_path(&self.dataset, rel)),
            )
            .unwrap()
            .expect("文件存在")
            .bytes;
        rt.block_on(
            self.store
                .put(&self.layout.object_path(&self.dataset, &new), bytes),
        )
        .unwrap();
        self.append_existing(&new);
        new
    }

    /// 热层当前快照的 schema。
    fn hot_schema(&self) -> Vec<amberlog_storage::schema::SchemaColumn> {
        use amberlog_storage::format::{SnapshotContent, decode_snapshot};
        let rt = Self::rt();
        let mut id = 1;
        while rt
            .block_on(self.store.exists(&self.store.object_path(
                &self.dataset,
                &amberlog_storage::layout::snapshot_path(amberlog_storage::chain::SnapshotId(
                    id + 1,
                )),
            )))
            .unwrap()
        {
            id += 1;
        }
        let sid = amberlog_storage::chain::SnapshotId(id);
        let bytes = rt
            .block_on(
                self.store.read(
                    &self
                        .store
                        .object_path(&self.dataset, &amberlog_storage::layout::snapshot_path(sid)),
                ),
            )
            .unwrap()
            .unwrap()
            .bytes;
        match decode_snapshot(&bytes, &self.dataset, sid).unwrap() {
            SnapshotContent::Supported(s) => s.schema,
            SnapshotContent::Unsupported { .. } => panic!("快照格式版本"),
        }
    }
}

impl Fixture {
    /// 运行一次查验 `amberlog evidence verify --config <a.toml> <args>`（`a.toml` 与 W 同一份配置加 `overrides`）。
    pub fn verify(&self, overrides: &[Kv], args: &[&str]) -> Run {
        self.evidence_cmd(overrides, "verify", args)
            .stdin(Stdio::null())
            .output()
            .expect("运行 amberlog evidence verify")
            .into()
    }

    /// 以 `--evidence <p>` 加 `extra` 查验，断言退出码为 `want`，返回报告。
    pub fn verify_ok(&self, overrides: &[Kv], p: &Path, extra: &[&str], want: i32) -> Value {
        let mut args = vec!["--evidence", p.to_str().unwrap()];
        args.extend_from_slice(extra);
        let r = self.verify(overrides, &args);
        assert_eq!(
            r.code,
            Some(want),
            "stderr：{}\nstdout：{}",
            r.stderr,
            r.stdout_text()
        );
        r.json()
    }
}

/// 报告某数组中 `kind` 为 `kind` 的元素的 `object`（同 [`objects_of`]）。
pub fn report_objects(report: &Value, array: &str, kind: &str) -> Vec<String> {
    objects_of(report, array, kind)
}

/// 场景约定的「重新打包」：解开证据包，改动成员（`edit` 收到解开的目录），可选地重写 `SHA256SUMS`，再以
/// `tar --format=pax -cf <新文件> -C <该目录> <各成员名>` 打成新的 tar（成员名不带 `./`）。返回新文件。
pub fn repack(package: &Path, name: &str, rewrite_sums: bool, edit: impl FnOnce(&Path)) -> PathBuf {
    let (dir, files) = untar(package);
    edit(&dir);
    if rewrite_sums {
        let mut sums = String::new();
        for rel in files.keys().filter(|k| k.as_str() != "SHA256SUMS") {
            let bytes = std::fs::read(dir.join(rel)).unwrap();
            let d = super::store::sha256(&bytes);
            sums.push_str(&format!("{}  {rel}\n", d.strip_prefix("sha256:").unwrap()));
        }
        std::fs::write(dir.join("SHA256SUMS"), sums).unwrap();
    }
    let out = package.with_file_name(name);
    let st = Command::new("tar")
        .arg("--format=pax")
        .arg("-cf")
        .arg(&out)
        .arg("-C")
        .arg(&dir)
        .args(files.keys())
        .status()
        .unwrap();
    assert!(st.success());
    out
}

/// 改写解开目录中的一个 JSON 成员。
pub fn edit_json(dir: &Path, member: &str, f: impl FnOnce(&mut Value)) {
    let p = dir.join(member);
    let mut v: Value = serde_json::from_slice(&std::fs::read(&p).unwrap()).unwrap();
    f(&mut v);
    std::fs::write(&p, serde_json::to_vec(&v).unwrap()).unwrap();
}

/// 改写解开目录中 `files.jsonl` 的各行。
pub fn edit_lines(dir: &Path, mut f: impl FnMut(&mut Value)) {
    let p = dir.join("files.jsonl");
    let mut out = String::new();
    for l in std::fs::read_to_string(&p).unwrap().lines() {
        let mut v: Value = serde_json::from_str(l).unwrap();
        f(&mut v);
        out.push_str(&serde_json::to_string(&v).unwrap());
        out.push('\n');
    }
    std::fs::write(&p, out).unwrap();
}

/// 冷层变更段链上的全部 `deletions`（stream `app`）。
pub fn cold_changes(fx: &Fixture) -> Vec<Value> {
    let ds = fx.dataset("app");
    let head = ds.head();
    ds.segments(&head["cold_deletion_segment"])
        .into_iter()
        .flat_map(|s| s["deletions"].as_array().unwrap().clone())
        .collect()
}

/// 等 `pred` 成立（至多 `limit`）。
pub fn wait_until(limit: Duration, what: &str, mut pred: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + limit;
    while !pred() {
        assert!(
            std::time::Instant::now() < deadline,
            "{} 秒内未见：{what}",
            limit.as_secs()
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// 以 `COLD_TTL` 把此刻前 5 小时 50 分起的 10 条记录入湖后，以时钟偏移重启，等它们所在的冷层数据文件被 TTL 移出并
/// 写出冷层变更段（`envs` 含暂停点 `TESTBED_FAULT_GATE` 时不等）。返回运行中的 W 与被移出的冷层数据文件 G（相对冷层表位置）。
pub fn ttl_removed(fx: &Fixture, envs: &[(&str, String)]) -> (Server, String) {
    use super::lake::{COLD_TTL, COLD_TTL_OFFSET_S};
    let handed = fx.lake_expiring();
    let g = handed.keys().next().unwrap().clone();
    let s = fx.start_offset(&COLD_TTL, COLD_TTL_OFFSET_S, envs);
    if envs.iter().all(|(k, _)| *k != "TESTBED_FAULT_GATE") {
        wait_until(Duration::from_secs(180), "TTL 冷层变更段", || {
            fx.dataset("app").current().is_some()
                && cold_changes(fx)
                    .iter()
                    .any(|d| d["reason"] == "ttl" && d["path"] == g.as_str())
        });
    }
    (s, g)
}

/// 「stream `app` 已入湖，W 已停止」，返回冷层存活文件集中的一个冷层数据文件 G（相对冷层表位置）。
pub fn laked_cold_file(fx: &Fixture) -> String {
    let mut w = fx.start(&W);
    let label = fx.laked(&w);
    super::store::stop(&mut w);
    let m = fx.dataset("app").bucket_now(&label).unwrap();
    m["handovers"][0]["files"][0]["path"]
        .as_str()
        .unwrap()
        .to_owned()
}

impl Fixture {
    /// 以暂停点 `step` 运行 `amberlog evidence <action> <args>`（`TESTBED_FAULT_GATE`），等它停住；返回进程与控制文件。
    pub fn evidence_gated(
        &self,
        action: &str,
        args: &[&str],
        step: &str,
        envs: &[(&str, String)],
    ) -> (Server, PathBuf) {
        let control = self.root().join(format!("gate-{step}"));
        std::fs::write(&control, "hold").unwrap();
        let mut cmd = self.evidence_cmd(&W, action, args);
        cmd.env("TESTBED_FAULT_GATE", step)
            .env("TESTBED_FAULT_GATE_FILE", &control);
        for (k, v) in envs {
            cmd.env(k, v);
        }
        let s = Server::spawn(cmd);
        assert!(
            super::wait_file_content(&control, "held", Duration::from_secs(120)),
            "{action} 未停在 {step}：{:#?}",
            s.stderr_lines()
        );
        (s, control)
    }

    /// 以暂停点 `step` 导出 `default/app` 到 `out`，等它停住。
    pub fn export_gated(
        &self,
        out: &Path,
        step: &str,
        envs: &[(&str, String)],
    ) -> (Server, PathBuf) {
        self.evidence_gated(
            "export",
            &[
                "--dataset",
                "default/app",
                "--output",
                out.to_str().unwrap(),
            ],
            step,
            envs,
        )
    }
}

/// 场景约定的快照回收（REQ-AMB-STO-SNAPSHOT-GC）：`hot_retention.snapshots` 为 `2`、`hot_retention.age` 为 `1s`、
/// `hot_fsck.interval` 为 `1s`。
pub const RECYCLE: [Kv<'static>; 3] = [
    ("hot_retention.snapshots", Some("2")),
    ("hot_retention.age", Some("\"1s\"")),
    ("hot_fsck.interval", Some("\"1s\"")),
];

/// 巡检删除不再被引用的桶 manifest 的最小年龄取 `1s`（与 [`RECYCLE`] 合用）。
pub const MIN_ORPHAN_1S: Kv<'static> = ("hot_fsck.min_orphan_age", Some("\"1s\""));

/// [`W`] 之上依次加 `more` 的各组配置（后出现的同名键覆盖先出现的）。
pub fn w_with(more: &[&[Kv<'static>]]) -> Vec<Kv<'static>> {
    let mut kv = W.to_vec();
    for m in more {
        kv.extend_from_slice(m);
    }
    kv
}

/// 「W 再提交多个快照，直到 …」：反复向 `app` 摄取 1 条 `_timestamp` 为 `ts` 的记录并等它提交出新快照，直到 `done` 成立
/// （至多 5 分钟：使一份头片离开可达范围要再提交 64 次以上，REQ-AMB-STO-HOT-LAYOUT-V8）。
pub fn commit_until(fx: &Fixture, w: &Server, ts: i64, what: &str, mut done: impl FnMut() -> bool) {
    let ds = fx.dataset("app");
    let deadline = std::time::Instant::now() + Duration::from_secs(300);
    while !done() {
        assert!(
            std::time::Instant::now() < deadline,
            "300 秒内未见：{what}：{:#?}",
            w.stderr_lines()
        );
        let before = ds.current();
        ingest_ok(w, "app", &format!(r#"[{{"_timestamp":{ts}}}]"#));
        wait_until(Duration::from_secs(60), "新快照", || {
            ds.current() > before
        });
        std::thread::sleep(Duration::from_millis(300));
    }
}

/// 回执（stdout 的一行 JSON，或证据包的 `receipt.json`）所记快照的编号。
pub fn receipt_snapshot_id(receipt: &Value) -> u64 {
    receipt["snapshot"]["snapshot_id"].as_u64().unwrap()
}

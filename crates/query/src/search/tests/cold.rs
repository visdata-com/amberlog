//! 跨层查询的冷侧（REQ-AMB-QRY-CROSS-TIER）在 query 这一层的测试：冷层读取用本文件的 [`SeqCold`]（可按次序给出不同的
//! 冷层版本内容，并记下每次调用），热层沿用上层的替身。块场景由 `crates/server/tests/qry_cross_tier.rs` 的系统测试验证。

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Mutex;

use amberlog_storage::BoxFuture;
use amberlog_storage::chain::{BucketManifest, ChainReader};
use amberlog_storage::cold::{
    ColdBatches, ColdDataFile, ColdOperation, ColdReadError, ColdReader, ColdScope, ColdSnapshot,
    ColdState, ColdTable,
};
use amberlog_storage::layout::HourBucket;
use arrow::datatypes::SchemaRef;

use super::*;

/// 1970-01-01T00 这个小时桶。
const B0: &str = "1970010100";
/// 冷层 `lake` 提交 7 所记的 H。
const LAKE_H: u64 = 5;

/// 次序可控的冷层读取：`version` 恒为预设值；`table_at` 依次取出预设的内容，剩最后一份时一直给它；`read_file` 按路径。
#[derive(Default)]
struct SeqCold {
    version: Mutex<Option<Result<Option<u64>, ColdReadError>>>,
    tables: Mutex<VecDeque<Result<Arc<ColdTable>, ColdReadError>>>,
    files: Mutex<HashMap<String, Result<Vec<RecordBatch>, ColdReadError>>>,
    /// 调用记录：`version`、`table_at <scope>`、`file <path>`。
    calls: Mutex<Vec<String>>,
    /// `read_file` 收到的 target。
    targets: Mutex<Vec<SchemaRef>>,
}

impl SeqCold {
    fn with_table(table: ColdTable) -> Arc<SeqCold> {
        let c = SeqCold::default();
        *c.version.lock().unwrap() = Some(Ok(Some(table.metadata_version)));
        c.tables.lock().unwrap().push_back(Ok(Arc::new(table)));
        Arc::new(c)
    }

    fn then_table(&self, table: Result<ColdTable, ColdReadError>) {
        self.tables.lock().unwrap().push_back(table.map(Arc::new));
    }

    fn put(&self, path: &str, r: Result<Vec<RecordBatch>, ColdReadError>) {
        self.files.lock().unwrap().insert(path.into(), r);
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    fn files_read(&self) -> Vec<String> {
        self.calls()
            .into_iter()
            .filter_map(|c| c.strip_prefix("file ").map(str::to_owned))
            .collect()
    }
}

impl ColdReader for SeqCold {
    fn table<'a>(
        &'a self,
        _dataset: &'a Dataset,
        _scope: ColdScope,
    ) -> BoxFuture<'a, Result<ColdState, ColdReadError>> {
        unreachable!("跨层查询分两步读：version 与 table_at")
    }

    fn version<'a>(
        &'a self,
        _dataset: &'a Dataset,
    ) -> BoxFuture<'a, Result<Option<u64>, ColdReadError>> {
        self.calls.lock().unwrap().push("version".into());
        let v = self.version.lock().unwrap().clone().unwrap_or(Ok(None));
        Box::pin(std::future::ready(v))
    }

    fn table_at<'a>(
        &'a self,
        _dataset: &'a Dataset,
        _version: u64,
        scope: ColdScope,
    ) -> BoxFuture<'a, Result<Arc<ColdTable>, ColdReadError>> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("table_at {scope:?}"));
        let mut tables = self.tables.lock().unwrap();
        let t = if tables.len() > 1 {
            tables.pop_front().unwrap()
        } else {
            tables.front().cloned().expect("预设了冷层表")
        };
        Box::pin(std::future::ready(t))
    }

    fn snapshot_files<'a>(
        &'a self,
        _dataset: &'a Dataset,
        _table: &'a ColdTable,
        _snapshot_id: i64,
    ) -> BoxFuture<'a, Result<Vec<ColdDataFile>, ColdReadError>> {
        unreachable!("查询不用")
    }

    fn referenced<'a>(
        &'a self,
        _dataset: &'a Dataset,
        _table: &'a ColdTable,
    ) -> BoxFuture<'a, Result<std::collections::HashSet<String>, ColdReadError>> {
        unreachable!("查询不用")
    }

    fn snapshot_references<'a>(
        &'a self,
        _dataset: &'a Dataset,
        _table: &'a ColdTable,
        _snapshot_id: i64,
    ) -> BoxFuture<'a, Result<std::collections::HashSet<String>, ColdReadError>> {
        unreachable!("查询不用")
    }

    fn file_bytes<'a>(
        &'a self,
        _dataset: &'a Dataset,
        _path: &'a str,
    ) -> BoxFuture<'a, Result<bytes::Bytes, ColdReadError>> {
        unreachable!("查询不用")
    }

    fn read_file<'a>(
        &'a self,
        _dataset: &'a Dataset,
        _table: &'a ColdTable,
        file: &'a ColdDataFile,
        target: SchemaRef,
        _time: std::ops::Range<i64>,
    ) -> BoxFuture<'a, Result<ColdBatches, ColdReadError>> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("file {}", file.path));
        self.targets.lock().unwrap().push(target);
        let r = self
            .files
            .lock()
            .unwrap()
            .get(&file.path)
            .cloned()
            .unwrap_or_else(|| Err(ColdReadError::NotFound(file.path.clone())))
            .map(|batches| Box::new(batches.into_iter().map(Ok)) as ColdBatches);
        Box::pin(std::future::ready(r))
    }
}

/// 第一次 `current` 给出 `first`，其后照 `inner`（模拟查询途中链尾前进）。
struct FirstCurrent {
    inner: Arc<FakeChain>,
    first: Mutex<Option<SnapshotId>>,
}

impl ChainReader for FirstCurrent {
    fn current<'a>(
        &'a self,
        dataset: &'a Dataset,
    ) -> BoxFuture<'a, Result<Option<SnapshotId>, ChainError>> {
        match self.first.lock().unwrap().take() {
            Some(id) => Box::pin(std::future::ready(Ok(Some(id)))),
            None => self.inner.current(dataset),
        }
    }

    fn snapshot<'a>(
        &'a self,
        dataset: &'a Dataset,
        id: SnapshotId,
    ) -> BoxFuture<'a, Result<Arc<Snapshot>, ChainError>> {
        self.inner.snapshot(dataset, id)
    }

    fn bucket_manifest<'a>(
        &'a self,
        dataset: &'a Dataset,
        snapshot: &'a Snapshot,
        bucket: &'a BucketRef,
    ) -> BoxFuture<'a, Result<Arc<BucketManifest>, ChainError>> {
        self.inner.bucket_manifest(dataset, snapshot, bucket)
    }
}

fn cold_file(path: &str, added_by: i64, min: i64, max: i64, records: u64) -> ColdDataFile {
    ColdDataFile {
        path: path.into(),
        added_snapshot_id: added_by,
        record_count: records,
        size_bytes: 500 + records,
        min_timestamp: min,
        max_timestamp: max,
        day: (min / 86_400_000_000) as i32,
    }
}

fn lake(id: i64, bucket: &str, h: u64) -> ColdSnapshot {
    ColdSnapshot {
        snapshot_id: id,
        parent_snapshot_id: None,
        timestamp_ms: 0,
        operation: ColdOperation::Lake {
            bucket: HourBucket::parse(bucket).unwrap(),
            hot_snapshot_id: SnapshotId(h),
            file_digests: BTreeMap::new(),
        },
    }
}

fn table(snapshots: Vec<ColdSnapshot>, files: Vec<ColdDataFile>) -> ColdTable {
    ColdTable {
        location: "file:///x/amberlog/_iceberg/default/app".into(),
        metadata_version: 3,
        current_snapshot_id: snapshots.last().map(|s| s.snapshot_id),
        fields: Vec::new(),
        snapshots,
        files,
    }
}

impl Fx {
    /// stream `app`（列 `a` Int64）：热层快照 `id` 只含桶 B0，桶里是 `data/h.parquet`（`_timestamp` 10、20，`added_in`
    /// 为 `added_in`）；`summary.cold.record_count` 为 `cold_records`。
    fn hot_b0(&mut self, id: u64, added_in: u64, cold_records: u64) {
        if !self.columns.contains_key("app") {
            self.stream("app", &[("a", I)]);
        }
        let rows = vec![(10, json!({"a": 1})), (20, json!({"a": 2}))];
        self.files
            .put("data/h.parquet", vec![batch(&self.columns["app"], &rows)]);
        let mut f = file_entry("data/h.parquet", 10, 20, 2);
        f.added_in = SnapshotId(added_in);
        let bucket = self.bucket("app", B0, vec![f]);
        let cold = ColdSummary {
            record_count: cold_records,
            ..ColdSummary::EMPTY
        };
        self.put_snapshot_with("app", id, vec![bucket], cold);
    }

    /// 冷层文件 `path` 的内容：`_timestamp` 依次为 `ts`，`a` 为 100 起。
    fn cold_rows(&self, ts: &[i64]) -> Vec<RecordBatch> {
        let rows: Vec<(i64, Value)> = ts
            .iter()
            .enumerate()
            .map(|(i, t)| (*t, json!({"a": 100 + i as i64})))
            .collect();
        vec![batch(&self.columns["app"], &rows)]
    }

    fn use_cold(&mut self, cold: Arc<SeqCold>) {
        self.engine.cold = cold;
    }
}

fn a_values(v: &Value) -> Vec<i64> {
    let mut a: Vec<i64> = hits(v).iter().map(|h| h["a"].as_i64().unwrap()).collect();
    a.sort();
    a
}

/// UNION、COLD-PART、SCAN-*：交接已完成（热层只剩 `added_in` 大于 H 的迟到文件），两层一并作答、一并计数。
#[tokio::test]
async fn hot_and_cold_parts_union_and_scan_counts() {
    let mut fx = Fx::new();
    fx.hot_b0(9, LAKE_H + 1, 3);
    let cold = SeqCold::with_table(table(
        vec![lake(7, B0, LAKE_H)],
        vec![cold_file(
            "data/_timestamp_day=1970-01-01/c.parquet",
            7,
            30,
            50,
            3,
        )],
    ));
    cold.put(
        "data/_timestamp_day=1970-01-01/c.parquet",
        Ok(fx.cold_rows(&[30, 40, 50])),
    );
    fx.use_cold(cold.clone());
    let r = fx.ok("SELECT a FROM app").await;
    assert_eq!(a_values(&r), [1, 2, 100, 101, 102]);
    assert_eq!(r["scan_files"], 2);
    assert_eq!(r["scan_bytes"], 1002 + 503);
    assert_eq!(r["scan_records"], 5);
    // 先确定冷层当前版本，再按查询范围所含的 UTC 日读清单
    assert_eq!(
        cold.calls()[..2],
        [
            "version".to_string(),
            "table_at Days(0..=47481)".to_string()
        ]
    );
    // 按字段编号读：target 为 stream 当前 schema，各列带字段编号
    let target = cold.targets.lock().unwrap()[0].clone();
    assert_eq!(
        target
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.metadata()["PARQUET:field_id"].as_str()))
            .collect::<Vec<_>>(),
        [("_timestamp", "1"), ("a", "2")]
    );
}

/// WINDOW：热层当前快照中桶 B0 仍有 `added_in` 不大于 H 的文件，`lake` 提交 7 处于交接窗口，其新增的冷层文件不读。
#[tokio::test]
async fn files_added_by_a_commit_in_the_handover_window_are_not_read() {
    let mut fx = Fx::new();
    fx.hot_b0(9, LAKE_H, 2);
    let cold = SeqCold::with_table(table(
        vec![lake(7, B0, LAKE_H)],
        vec![cold_file(
            "data/_timestamp_day=1970-01-01/c.parquet",
            7,
            10,
            20,
            2,
        )],
    ));
    fx.use_cold(cold.clone());
    let r = fx.ok("SELECT a FROM app").await;
    assert_eq!(a_values(&r), [1, 2]);
    assert_eq!(r["scan_files"], 1);
    assert!(cold.files_read().is_empty());
}

/// WINDOW：同一桶第二次交接的窗口内，第一次入湖新增的冷层文件照常作答。
#[tokio::test]
async fn earlier_lake_commit_of_the_same_bucket_still_answers() {
    let mut fx = Fx::new();
    // 第一次入湖（H=2）之后的迟到文件 added_in 为 5，第二次入湖以 H=5 写了冷层提交 8、尚未交接
    fx.hot_b0(9, 5, 3);
    let cold = SeqCold::with_table(table(
        vec![lake(7, B0, 2), lake(8, B0, 5)],
        vec![
            cold_file("data/_timestamp_day=1970-01-01/first.parquet", 7, 30, 50, 3),
            cold_file(
                "data/_timestamp_day=1970-01-01/second.parquet",
                8,
                10,
                20,
                2,
            ),
        ],
    ));
    cold.put(
        "data/_timestamp_day=1970-01-01/first.parquet",
        Ok(fx.cold_rows(&[30, 40, 50])),
    );
    fx.use_cold(cold.clone());
    let r = fx.ok("SELECT a FROM app").await;
    assert_eq!(a_values(&r), [1, 2, 100, 101, 102]);
    assert_eq!(
        cold.files_read(),
        ["data/_timestamp_day=1970-01-01/first.parquet"]
    );
}

/// PRUNE-TIME 与 SCAN-METADATA：与查询范围无交集的冷层文件不读；只读了冷层元数据时扫描量为 0。
#[tokio::test]
async fn cold_files_outside_the_range_are_not_scanned() {
    let mut fx = Fx::new();
    fx.hot_b0(9, LAKE_H + 1, 3);
    let day2 = 2 * 86_400_000_000;
    let cold = SeqCold::with_table(table(
        vec![lake(7, B0, LAKE_H), lake(8, "1970010300", 1)],
        vec![
            cold_file("data/_timestamp_day=1970-01-01/c.parquet", 7, 30, 50, 3),
            cold_file(
                "data/_timestamp_day=1970-01-03/d.parquet",
                8,
                day2,
                day2 + 9,
                4,
            ),
        ],
    ));
    cold.put(
        "data/_timestamp_day=1970-01-03/d.parquet",
        Ok(fx.cold_rows(&[day2, day2 + 9])),
    );
    fx.use_cold(cold.clone());
    let q = |s, e| json!({"query": {"sql": "SELECT a FROM app", "start_time": s, "end_time": e}});
    let r = fx.run(q(day2, day2 + 10)).await.unwrap();
    assert_eq!(r["scan_files"], 1);
    assert_eq!(r["scan_records"], 4);
    assert_eq!(
        cold.files_read(),
        ["data/_timestamp_day=1970-01-03/d.parquet"]
    );
    // 第 10 天：两层都没有相交的文件
    let r = fx
        .run(q(10 * 86_400_000_000, 11 * 86_400_000_000))
        .await
        .unwrap();
    assert_eq!(
        (
            r["scan_files"].clone(),
            r["scan_bytes"].clone(),
            r["scan_records"].clone()
        ),
        (json!(0), json!(0), json!(0))
    );
    assert!(cold.calls().contains(&"table_at Days(10..=10)".to_string()));
}

/// NO-TABLE：尚无冷层表时冷层部分为空集；当前快照记有冷层记录时是冷层表整体消失，回 500 并停用。
#[tokio::test]
async fn missing_cold_table_is_empty_unless_the_snapshot_records_cold_data() {
    let mut fx = Fx::new();
    fx.hot_b0(9, 1, 0);
    let r = fx.ok("SELECT a FROM app").await;
    assert_eq!(r["total"], 2);

    let mut fx = Fx::new();
    fx.hot_b0(9, 1, 100);
    assert_eq!(
        fx.sql("SELECT a FROM app").await.unwrap_err(),
        Reason::Internal
    );
    assert_eq!(fx.gates.state(&ds("app")), DatasetState::ManifestUnreadable);
}

/// 报告顺序（REQ-AMB-QRY-SEARCH-ENDPOINT-V6）：确定冷层当前版本的失败先于 `unknown_column`，读元数据与清单的失败在其后；
/// UNAVAILABLE 不停用。
#[tokio::test]
async fn cold_failures_take_their_places_in_the_report_order() {
    let mut fx = Fx::new();
    fx.hot_b0(9, LAKE_H + 1, 3);
    let cold = SeqCold::default();
    *cold.version.lock().unwrap() = Some(Err(ColdReadError::Unavailable("403".into())));
    fx.use_cold(Arc::new(cold));
    assert_eq!(
        fx.sql("SELECT nope FROM app").await.unwrap_err(),
        Reason::Unavailable
    );

    let mut fx = Fx::new();
    fx.hot_b0(9, LAKE_H + 1, 3);
    let cold = SeqCold::default();
    *cold.version.lock().unwrap() = Some(Ok(Some(3)));
    cold.then_table(Err(ColdReadError::Unavailable("403".into())));
    fx.use_cold(Arc::new(cold));
    assert_eq!(
        fx.sql("SELECT nope FROM app").await.unwrap_err(),
        Reason::UnknownColumn
    );
    assert_eq!(
        fx.sql("SELECT a FROM app").await.unwrap_err(),
        Reason::Unavailable
    );
    assert_eq!(fx.gates.state(&ds("app")), DatasetState::Normal);
}

/// CORRUPT 与 DISABLED：冷层元数据或清单无法解析回 500 并停用；停用之后的查询不再读任何冷层对象。
#[tokio::test]
async fn unparsable_cold_metadata_disables_and_later_queries_skip_the_cold_tier() {
    let mut fx = Fx::new();
    fx.hot_b0(9, LAKE_H + 1, 3);
    let cold = Arc::new(SeqCold::default());
    *cold.version.lock().unwrap() = Some(Ok(Some(3)));
    cold.then_table(Err(ColdReadError::Integrity("bad manifest".into())));
    fx.use_cold(cold.clone());
    assert_eq!(
        fx.sql("SELECT a FROM app").await.unwrap_err(),
        Reason::Internal
    );
    assert_eq!(fx.gates.state(&ds("app")), DatasetState::ManifestUnreadable);
    let before = cold.calls().len();
    assert_eq!(
        fx.sql("SELECT a FROM app").await.unwrap_err(),
        Reason::Internal
    );
    assert_eq!(cold.calls().len(), before, "停用的数据集不读冷层");
}

/// MOVED：冷层数据文件不存在、重判后已不在冷层存活文件集 → 503、不停用；仍在 → 500 并停用（CORRUPT）。
#[tokio::test]
async fn missing_cold_file_is_moved_only_if_no_longer_live() {
    let c = "data/_timestamp_day=1970-01-01/c.parquet";
    let with_c = || table(vec![lake(7, B0, LAKE_H)], vec![cold_file(c, 7, 30, 50, 3)]);

    let mut fx = Fx::new();
    fx.hot_b0(9, LAKE_H + 1, 3);
    let cold = SeqCold::with_table(with_c());
    cold.then_table(Ok(table(vec![lake(7, B0, LAKE_H)], Vec::new())));
    fx.use_cold(cold.clone());
    assert_eq!(
        fx.sql("SELECT a FROM app").await.unwrap_err(),
        Reason::Unavailable
    );
    assert_eq!(fx.gates.state(&ds("app")), DatasetState::Normal);
    // 重判只读该文件所在的 UTC 日
    assert_eq!(cold.calls().last().unwrap(), "table_at Days(0..=0)");

    let mut fx = Fx::new();
    fx.hot_b0(9, LAKE_H + 1, 3);
    fx.use_cold(SeqCold::with_table(with_c()));
    assert_eq!(
        fx.sql("SELECT a FROM app").await.unwrap_err(),
        Reason::Internal
    );
    assert_eq!(fx.gates.state(&ds("app")), DatasetState::ManifestUnreadable);
}

/// MOVED：热层数据文件不存在、重新确定的当前快照已不含它 → 503、不停用；仍含 → 500。
#[tokio::test]
async fn missing_hot_file_is_moved_only_if_no_longer_live() {
    let mut fx = Fx::new();
    fx.hot_b0(1, 1, 0);
    fx.files
        .fail("data/h.parquet", FileError::NotFound("gone".into()));
    // 链尾前进到快照 2：桶 B0 的新桶 manifest 已不含 data/h.parquet
    let late = file_entry("data/late.parquet", 30, 30, 1);
    let path = "manifest/buckets/1970010100-2.json";
    fx.chain.put_bucket_manifest(
        &ds("app"),
        path,
        BucketManifest {
            format_version: 1,
            bucket: B0.into(),
            files: vec![late.clone()],
            deletions: Vec::new(),
            handovers: Vec::new(),
        },
    );
    let other = BucketRef {
        manifest: path.into(),
        ..fx.bucket("app", "unused", vec![late])
    };
    let other = BucketRef {
        bucket: B0.into(),
        ..other
    };
    fx.put_snapshot("app", 2, vec![other]);
    fx.engine.chain = Arc::new(FirstCurrent {
        inner: fx.chain.clone(),
        first: Mutex::new(Some(SnapshotId(1))),
    });
    assert_eq!(
        fx.sql("SELECT a FROM app").await.unwrap_err(),
        Reason::Unavailable
    );
    assert_eq!(fx.gates.state(&ds("app")), DatasetState::Normal);

    let mut fx = Fx::new();
    fx.hot_b0(1, 1, 0);
    fx.files
        .fail("data/h.parquet", FileError::NotFound("gone".into()));
    assert_eq!(
        fx.sql("SELECT a FROM app").await.unwrap_err(),
        Reason::Internal
    );
}

/// MOVED 的重判在读取途中做：可见集的行已交给执行之后，热层数据文件不存在、已不在重新确定的当前快照中 → 503。
#[tokio::test]
async fn moved_file_after_earlier_input_was_delivered() {
    let mut fx = Fx::new();
    fx.hot_b0(1, 1, 0);
    fx.memtable_rows("app", &[(5, json!({"a": 0}))]);
    fx.files
        .fail("data/h.parquet", FileError::NotFound("gone".into()));
    // 链尾前进到快照 2：其中已没有桶 B0
    fx.put_snapshot("app", 2, Vec::new());
    fx.engine.chain = Arc::new(FirstCurrent {
        inner: fx.chain.clone(),
        first: Mutex::new(Some(SnapshotId(1))),
    });
    assert_eq!(
        fx.sql("SELECT count(*) AS n FROM app").await.unwrap_err(),
        Reason::Unavailable
    );
    assert_eq!(fx.gates.state(&ds("app")), DatasetState::Normal);
}

/// PER-QUERY：创世快照尚未提交成功、只以内存表作答时不确定冷层当前版本。
#[tokio::test]
async fn memtable_only_answers_do_not_touch_the_cold_tier() {
    let mut fx = Fx::new();
    fx.stream("app", &[("a", I)]);
    fx.memtable_rows("app", &[(1, json!({"a": 1}))]);
    let cold = Arc::new(SeqCold::default());
    fx.use_cold(cold.clone());
    assert_eq!(fx.ok("SELECT a FROM app").await["total"], 1);
    assert!(cold.calls().is_empty());
}

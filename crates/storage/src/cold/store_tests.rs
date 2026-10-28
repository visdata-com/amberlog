//! 冷层模块（[`super`]）在 `file://` 临时目录上的组合测试：建表、追加、删除、冲突换号、同号续做、找当前版本，
//! 并用 iceberg-rust 自己的读取器（不经本模块）按提示文件找当前版本读回整表。

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use amberlog_common::dataset::Dataset;
use amberlog_common::schema::{ColumnType, TIMESTAMP_COLUMN};
use arrow::array::{Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use iceberg::TableIdent;
use iceberg::io::FileIO;
use iceberg::spec::{FormatVersion, ManifestList, ManifestStatus, TableMetadata};
use iceberg::table::StaticTable;

use super::commit::{Change, Committer, OPERATION, Progress};
use super::current;
use super::datafile;
use super::error::ColdError;
use super::layout::{self, ColdLayout};
use super::manifests;
use crate::gate::{DatasetGates, DatasetState};
use crate::schema::{SchemaColumn, StreamColumns};
use crate::store::{Location, S3Options, Store};

/// 2026-08-22T10:00:00Z
const T: i64 = 1_787_392_800_000_000;

struct Fixture {
    dir: std::path::PathBuf,
    committer: Committer,
    dataset: Dataset,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn fixture(max_retries: u32) -> Fixture {
    let dir = std::env::temp_dir().join(format!(
        "amberlog-cold-{}-{}",
        std::process::id(),
        rand::random::<u64>()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let location = Location::File { dir: dir.clone() };
    let store = Store::open(&location, &S3Options::default(), "amberlog").unwrap();
    let layout = ColdLayout::new(&location, store.root.clone());
    Fixture {
        dir,
        committer: Committer::new(store, layout, Arc::new(DatasetGates::new()), max_retries),
        dataset: Dataset::new("default", "app"),
    }
}

fn col(name: &str, column_type: ColumnType, field_id: u32) -> SchemaColumn {
    SchemaColumn {
        name: name.into(),
        column_type,
        field_id,
    }
}

fn hot_schema() -> Vec<SchemaColumn> {
    vec![
        col(TIMESTAMP_COLUMN, ColumnType::Int64, 1),
        col("msg", ColumnType::Utf8, 2),
        col("n", ColumnType::Int64, 3),
    ]
}

fn lake_summary(bucket: &str) -> BTreeMap<String, String> {
    BTreeMap::from([
        (OPERATION.to_owned(), "lake".to_owned()),
        ("amberlog.bucket".to_owned(), bucket.to_owned()),
    ])
}

impl Fixture {
    /// 写出并上传一个含 `rows` 条记录（`_timestamp` 自 `start` 起）的冷层数据文件，做一次入湖式的冷层提交。
    async fn append(&self, schema: &[SchemaColumn], start: i64, rows: usize) -> (u64, String) {
        let c = &self.committer;
        let _guard = c.lock(&self.dataset).await;
        let cur = current::current(&c.store, &c.layout, &self.dataset)
            .await
            .unwrap();
        let prev = cur
            .metadata
            .as_ref()
            .map(|(_, m)| Arc::clone(m.current_schema()));
        let cold_schema = iceberg::spec::Schema::builder()
            .with_fields(super::names::evolve(prev.as_deref(), schema))
            .build()
            .unwrap();
        let hot = StreamColumns::from_columns(schema.to_vec());
        let fields = vec![
            hot.get(TIMESTAMP_COLUMN).unwrap().arrow_field(),
            hot.get("msg").unwrap().arrow_field(),
        ];
        let ts: Vec<i64> = (0..rows as i64).map(|i| start + i).collect();
        let msg: Vec<String> = (0..rows).map(|i| format!("m{i}")).collect();
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(fields)),
            vec![
                Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
                Arc::new(StringArray::from(msg)),
            ],
        )
        .unwrap();
        let file = datafile::write(
            &c.layout,
            &self.dataset,
            &cold_schema,
            &batch,
            &crate::bloom::BloomColumns::none(),
        )
        .unwrap();
        let mut progress = Progress::new(&self.dataset, c.inflight());
        progress
            .upload(&c.store, &c.layout, &file.path, file.bytes.clone())
            .await
            .unwrap();
        let change = Change::Append {
            schema: schema.to_vec(),
            files: vec![file.data_file.clone()],
            summary: lake_summary("2026082210"),
        };
        let done = c
            .commit(&self.dataset, &change, &mut progress)
            .await
            .unwrap();
        (done.version, file.path)
    }

    fn read(&self, rel: &str) -> Option<Vec<u8>> {
        std::fs::read(self.dir.join("amberlog/_iceberg/default/app").join(rel)).ok()
    }

    fn write(&self, rel: &str, bytes: &[u8]) {
        let p = self.dir.join("amberlog/_iceberg/default/app").join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, bytes).unwrap();
    }

    fn metadata(&self, n: u64) -> TableMetadata {
        serde_json::from_slice(&self.read(&layout::metadata_path(n)).unwrap()).unwrap()
    }

    /// 冷层当前快照中存活文件（相对表目录）与其清单条目快照号。
    async fn live(&self) -> Vec<(String, i64)> {
        let c = &self.committer;
        let cur = current::current(&c.store, &c.layout, &self.dataset)
            .await
            .unwrap();
        let (_, m) = cur.metadata.unwrap();
        let mut out = Vec::new();
        for f in manifests::manifest_list(
            &c.store,
            &c.layout,
            &self.dataset,
            m.current_snapshot().unwrap(),
        )
        .await
        .unwrap()
        {
            for e in manifests::entries(&c.store, &c.layout, &self.dataset, &f)
                .await
                .unwrap()
            {
                if e.status() != ManifestStatus::Deleted {
                    let rel = c.layout.rel_of_uri(&self.dataset, e.file_path()).unwrap();
                    out.push((rel.to_owned(), e.snapshot_id().unwrap()));
                }
            }
        }
        out.sort();
        out
    }
}

#[tokio::test]
async fn first_lake_commit_creates_v1_with_hot_field_ids() {
    let f = fixture(8);
    let (version, path) = f.append(&hot_schema(), T, 10).await;
    assert_eq!(version, 1);
    assert_eq!(f.read(layout::HINT).unwrap(), b"1");
    let m = f.metadata(1);
    assert_eq!(m.format_version(), FormatVersion::V2);
    assert_eq!(m.location(), f.committer.layout.table_location(&f.dataset));
    let ids: Vec<_> = m
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|x| (x.id, x.name.clone()))
        .collect();
    assert_eq!(
        ids,
        [(1, "_timestamp".into()), (2, "msg".into()), (3, "n".into())]
    );
    let spec = m.default_partition_spec();
    assert_eq!(spec.fields().len(), 1);
    assert_eq!(spec.fields()[0].name, "_timestamp_day");
    assert_eq!(spec.fields()[0].source_id, 1);
    assert!(m.properties().is_empty());
    let snap = m.current_snapshot().unwrap();
    assert_eq!(m.snapshots().count(), 1);
    assert_eq!(
        snap.summary()
            .additional_properties
            .get(OPERATION)
            .map(String::as_str),
        Some("lake")
    );
    assert!(path.starts_with("data/_timestamp_day=2026-08-22/"));
    assert_eq!(f.live().await, [(path, snap.snapshot_id())]);
    // 表目录之下只有元数据与数据两个子前缀，元数据之下只有元数据文件、Avro 与提示
    let mut names = Vec::new();
    for e in walk(&f.dir.join("amberlog/_iceberg")) {
        names.push(e);
    }
    assert!(
        names
            .iter()
            .all(|n| n.starts_with("default/app/metadata/") || n.starts_with("default/app/data/")),
        "{names:?}"
    );
    assert!(
        names
            .iter()
            .filter(|n| n.starts_with("default/app/metadata/"))
            .all(|n| n.ends_with(".metadata.json")
                || n.ends_with(".avro")
                || n.ends_with("version-hint.text"))
    );
    // 清单列表中每个清单的 content 为 0
    let list = ManifestList::parse_with_version(
        &f.read(
            f.committer
                .layout
                .rel_of_uri(&f.dataset, snap.manifest_list())
                .unwrap(),
        )
        .unwrap(),
        FormatVersion::V2,
    )
    .unwrap();
    assert!(
        list.entries()
            .iter()
            .all(|e| e.content == iceberg::spec::ManifestContentType::Data)
    );
}

fn walk(root: &std::path::Path) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let p = e.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else {
                out.push(p.strip_prefix(root).unwrap().to_string_lossy().into_owned());
            }
        }
    }
    out
}

#[tokio::test]
async fn later_commits_add_one_snapshot_and_grow_schema_in_place() {
    let f = fixture(8);
    f.append(
        &[
            col(TIMESTAMP_COLUMN, ColumnType::Int64, 1),
            col("msg", ColumnType::Utf8, 3),
        ],
        T,
        5,
    )
    .await;
    let v1 = f.metadata(1);
    let mut wider = vec![
        col(TIMESTAMP_COLUMN, ColumnType::Int64, 1),
        col("msg", ColumnType::Utf8, 3),
    ];
    wider.push(col("b", ColumnType::Boolean, 4));
    let (version, _) = f.append(&wider, T + 1_000_000, 5).await;
    assert_eq!(version, 2);
    let v2 = f.metadata(2);
    assert_eq!(v2.snapshots().count(), v1.snapshots().count() + 1);
    let s = v2.current_snapshot().unwrap();
    assert_eq!(
        s.parent_snapshot_id(),
        Some(v1.current_snapshot().unwrap().snapshot_id())
    );
    let names: Vec<_> = v2
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|x| x.name.clone())
        .collect();
    assert_eq!(names, ["_timestamp", "msg", "b"]);
    assert_eq!(f.read(layout::HINT).unwrap(), b"2");
    assert_eq!(f.live().await.len(), 2);
}

#[tokio::test]
async fn delete_marks_whole_files_and_keeps_added_snapshot_ids() {
    let f = fixture(8);
    let (_, a) = f.append(&hot_schema(), T, 3).await;
    let (_, b) = f.append(&hot_schema(), T + 10, 3).await;
    let before = f.live().await;
    let c = &f.committer;
    let _guard = c.lock(&f.dataset).await;
    let mut progress = Progress::new(&f.dataset, c.inflight());
    let change = Change::Delete {
        paths: HashSet::from([
            a.clone(),
            "data/_timestamp_day=2026-08-22/gone.parquet".into(),
        ]),
        summary: BTreeMap::from([
            (OPERATION.to_owned(), "ttl".to_owned()),
            ("amberlog.ttl_cutoff".to_owned(), "-5".to_owned()),
        ]),
    };
    let done = c.commit(&f.dataset, &change, &mut progress).await.unwrap();
    drop(_guard);
    assert_eq!(done.version, 3);
    assert_eq!(done.removed, std::slice::from_ref(&a));
    let live = f.live().await;
    // b 仍以入湖时的快照号存活（清单条目改为 EXISTING 后继承原快照号）
    assert_eq!(
        live,
        before
            .into_iter()
            .filter(|(p, _)| *p == b)
            .collect::<Vec<_>>()
    );
    let m = f.metadata(3);
    assert_eq!(
        m.current_snapshot().unwrap().summary().operation,
        iceberg::spec::Operation::Delete
    );
    // 再删同一个文件：一个都不存活
    let _guard = c.lock(&f.dataset).await;
    let mut progress = Progress::new(&f.dataset, c.inflight());
    let change = Change::Delete {
        paths: HashSet::from([a]),
        summary: BTreeMap::new(),
    };
    assert_eq!(
        c.commit(&f.dataset, &change, &mut progress)
            .await
            .unwrap_err(),
        ColdError::Stale
    );
}

#[tokio::test]
async fn standard_reader_reads_all_records_via_hint() {
    let f = fixture(8);
    f.append(&hot_schema(), T, 7).await;
    f.append(&hot_schema(), T + 100, 5).await;
    let hint = String::from_utf8(f.read(layout::HINT).unwrap()).unwrap();
    let location = format!(
        "{}/metadata/v{hint}.metadata.json",
        f.committer.layout.table_location(&f.dataset)
    );
    let table = StaticTable::from_metadata_file(
        &location,
        TableIdent::from_strs(["default", "app"]).unwrap(),
        FileIO::new_with_fs(),
    )
    .await
    .unwrap();
    let batches: Vec<RecordBatch> = table
        .scan()
        .select_all()
        .build()
        .unwrap()
        .to_arrow()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 12);
    // 文件不含 `n`，读出为空值
    let n = batches[0].column_by_name("n").unwrap();
    assert_eq!(n.null_count(), n.len());
    let _ = Int64Array::from(vec![0]);
}

#[tokio::test]
async fn conflict_regenerates_on_new_current_version() {
    let f = fixture(8);
    f.append(&hot_schema(), T, 3).await;
    let c = &f.committer;
    let _guard = c.lock(&f.dataset).await;
    // 本次提交先以 v1 为基础生成编号 2 的内容，此时别人写成了另一份 v2（同一快照、不同的 last-updated-ms）
    let mut progress = Progress::new(&f.dataset, c.inflight());
    let change = Change::Delete {
        paths: HashSet::new(),
        summary: BTreeMap::new(),
    };
    let cur = current::current(&c.store, &c.layout, &f.dataset)
        .await
        .unwrap();
    assert!(matches!(
        c.prepare_for_test(&f.dataset, &change, &cur).await,
        Err(ColdError::Stale)
    ));
    let mut foreign: serde_json::Value =
        serde_json::from_slice(&f.read(&layout::metadata_path(1)).unwrap()).unwrap();
    foreign["last-updated-ms"] =
        serde_json::json!(foreign["last-updated-ms"].as_i64().unwrap() + 1);
    let s = f.metadata(1).current_snapshot().unwrap().snapshot_id();
    let file = f.live().await[0].0.clone();
    let change = Change::Delete {
        paths: HashSet::from([file]),
        summary: BTreeMap::new(),
    };
    progress.set_prepared_for_test(c.prepare_for_test(&f.dataset, &change, &cur).await.unwrap());
    f.write(
        &layout::metadata_path(2),
        &serde_json::to_vec(&foreign).unwrap(),
    );
    let done = c.commit(&f.dataset, &change, &mut progress).await.unwrap();
    assert_eq!(done.version, 3);
    assert_eq!(
        f.metadata(3)
            .current_snapshot()
            .unwrap()
            .parent_snapshot_id(),
        Some(s)
    );
    assert_eq!(f.read(layout::HINT).unwrap(), b"3");
}

#[tokio::test]
async fn same_number_retry_reuses_bytes_and_does_not_commit_twice() {
    let f = fixture(8);
    let (_, file) = f.append(&hot_schema(), T, 3).await;
    let c = &f.committer;
    let _guard = c.lock(&f.dataset).await;
    let change = Change::Delete {
        paths: HashSet::from([file]),
        summary: BTreeMap::new(),
    };
    let cur = current::current(&c.store, &c.layout, &f.dataset)
        .await
        .unwrap();
    let mut progress = Progress::new(&f.dataset, c.inflight());
    let prepared = c.prepare_for_test(&f.dataset, &change, &cur).await.unwrap();
    // 上一轮的创建请求其实已写成（结果不明），本轮续做
    f.write(&layout::metadata_path(2), &prepared_bytes(&prepared));
    progress.set_prepared_for_test(prepared);
    let done = c.commit(&f.dataset, &change, &mut progress).await.unwrap();
    assert_eq!(done.version, 2);
    assert!(f.read(&layout::metadata_path(3)).is_none());
}

fn prepared_bytes(p: &super::commit::PreparedForTest) -> Vec<u8> {
    p.bytes().to_vec()
}

#[tokio::test]
async fn exhausted_retries_disable_dataset() {
    let f = fixture(0);
    let (_, file) = f.append(&hot_schema(), T, 3).await;
    let c = &f.committer;
    let _guard = c.lock(&f.dataset).await;
    let change = Change::Delete {
        paths: HashSet::from([file]),
        summary: BTreeMap::new(),
    };
    // 本次提交生成编号 2 的内容之后、创建之前，别人写成了另一份 v2
    let mut foreign: serde_json::Value =
        serde_json::from_slice(&f.read(&layout::metadata_path(1)).unwrap()).unwrap();
    foreign["last-updated-ms"] =
        serde_json::json!(foreign["last-updated-ms"].as_i64().unwrap() + 1);
    let target = f
        .dir
        .join("amberlog/_iceberg/default/app")
        .join(layout::metadata_path(2));
    c.before_create_for_test(move || {
        std::fs::write(target, serde_json::to_vec(&foreign).unwrap()).unwrap()
    });
    let mut progress = Progress::new(&f.dataset, c.inflight());
    assert_eq!(
        c.commit(&f.dataset, &change, &mut progress)
            .await
            .unwrap_err(),
        ColdError::Exhausted
    );
    assert_eq!(c.gates.state(&f.dataset), DatasetState::ManifestUnreadable);
    // 停用后不再写任何对象
    let mut progress = Progress::new(&f.dataset, c.inflight());
    assert!(matches!(
        c.commit(&f.dataset, &change, &mut progress).await,
        Err(ColdError::Disabled(DatasetState::ManifestUnreadable))
    ));
    assert!(f.read(&layout::metadata_path(3)).is_none());
}

#[tokio::test]
async fn create_conflict_retries_with_new_number() {
    let f = fixture(1);
    let (_, file) = f.append(&hot_schema(), T, 3).await;
    let s = f.metadata(1).current_snapshot().unwrap().snapshot_id();
    let c = &f.committer;
    let _guard = c.lock(&f.dataset).await;
    let change = Change::Delete {
        paths: HashSet::from([file]),
        summary: BTreeMap::new(),
    };
    let mut foreign: serde_json::Value =
        serde_json::from_slice(&f.read(&layout::metadata_path(1)).unwrap()).unwrap();
    foreign["last-updated-ms"] =
        serde_json::json!(foreign["last-updated-ms"].as_i64().unwrap() + 1);
    let target = f
        .dir
        .join("amberlog/_iceberg/default/app")
        .join(layout::metadata_path(2));
    c.before_create_for_test(move || {
        std::fs::write(target, serde_json::to_vec(&foreign).unwrap()).unwrap()
    });
    let mut progress = Progress::new(&f.dataset, c.inflight());
    let done = c.commit(&f.dataset, &change, &mut progress).await.unwrap();
    assert_eq!(done.version, 3);
    let m = f.metadata(3);
    assert_eq!(m.current_snapshot().unwrap().parent_snapshot_id(), Some(s));
    assert_eq!(m.snapshots().count(), 2);
}

#[tokio::test]
async fn current_version_from_missing_corrupt_lagging_and_ahead_hints() {
    let f = fixture(8);
    for i in 0..3 {
        f.append(&hot_schema(), T + i, 1).await;
    }
    let c = &f.committer;
    for hint in [Some(&b"1"[..]), Some(b"abc"), None] {
        let p = f
            .dir
            .join("amberlog/_iceberg/default/app")
            .join(layout::HINT);
        match hint {
            Some(h) => std::fs::write(&p, h).unwrap(),
            None => std::fs::remove_file(&p).unwrap(),
        }
        let cur = current::current(&c.store, &c.layout, &f.dataset)
            .await
            .unwrap();
        assert_eq!(cur.version, 3, "hint {hint:?}");
    }
    // 提示所指版本不存在：不回退，当前版本即为 9，读 v9 得到不存在（REQ-AMB-STO-COLD-TABLE-V2）
    let hint_path = f
        .dir
        .join("amberlog/_iceberg/default/app")
        .join(layout::HINT);
    std::fs::write(&hint_path, b"9").unwrap();
    assert_eq!(
        current::version(&c.store, &c.layout, &f.dataset)
            .await
            .unwrap(),
        (9, Some(9))
    );
    assert!(matches!(
        current::current(&c.store, &c.layout, &f.dataset).await,
        Err(ColdError::Broken(_))
    ));
    // 提示落后（1）时，下一次提交先补写提示再创建 v4，提交后提示为 4
    std::fs::write(
        f.dir
            .join("amberlog/_iceberg/default/app")
            .join(layout::HINT),
        b"1",
    )
    .unwrap();
    let (v, _) = f.append(&hot_schema(), T + 9, 1).await;
    assert_eq!(v, 4);
    assert_eq!(f.read(layout::HINT).unwrap(), b"4");
    // 尚无冷层表
    let none = current::current(&c.store, &c.layout, &Dataset::new("default", "web"))
        .await
        .unwrap();
    assert_eq!(none.version, 0);
}

#[tokio::test]
async fn missing_metadata_dir_is_unavailable() {
    let f = fixture(8);
    std::fs::remove_dir_all(&f.dir).unwrap();
    let c = &f.committer;
    assert!(matches!(
        current::current(&c.store, &c.layout, &f.dataset).await,
        Err(ColdError::Unavailable(_))
    ));
}

// ---------------------------------------------------------------------------
// 入湖重写与读取
// ---------------------------------------------------------------------------

/// 热层写法的 Parquet（经物化写出）：`ts` 与可选的 `msg`、`n` 列。
fn hot_input(ts: &[i64], msg: Option<Vec<&str>>, n: Option<Vec<i64>>) -> bytes::Bytes {
    let hot = StreamColumns::from_columns(hot_schema());
    let mut fields = vec![hot.get(TIMESTAMP_COLUMN).unwrap().arrow_field()];
    let mut arrays: Vec<arrow::array::ArrayRef> = vec![Arc::new(
        TimestampMicrosecondArray::from(ts.to_vec()).with_timezone("UTC"),
    )];
    if let Some(msg) = msg {
        fields.push(hot.get("msg").unwrap().arrow_field());
        arrays.push(Arc::new(StringArray::from(msg)));
    }
    if let Some(n) = n {
        fields.push(hot.get("n").unwrap().arrow_field());
        arrays.push(Arc::new(Int64Array::from(n)));
    }
    let batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), arrays).unwrap();
    let files = crate::flush::write_data_files(&hot, &[batch], &crate::bloom::BloomColumns::none())
        .unwrap();
    assert_eq!(files.len(), 1);
    files[0].bytes.clone()
}

impl Fixture {
    async fn lake(
        &self,
        inputs: &[bytes::Bytes],
        target: u64,
    ) -> (super::commit::Committed, Vec<super::lake::WrittenFile>) {
        let c = &self.committer;
        let _guard = c.lock(&self.dataset).await;
        let rewritten = super::lake::rewrite(
            c,
            &self.dataset,
            &hot_schema(),
            inputs,
            target,
            &crate::bloom::BloomColumns::none(),
        )
        .await
        .unwrap();
        let mut progress = Progress::new(&self.dataset, c.inflight());
        let done = super::lake::commit(
            c,
            &self.dataset,
            crate::layout::HourBucket::of(T),
            crate::chain::SnapshotId(7),
            &hot_schema(),
            &rewritten,
            &mut progress,
        )
        .await
        .unwrap();
        (done, rewritten.files())
    }
}

#[tokio::test]
async fn lake_rewrite_keeps_all_records_sorted_with_digests() {
    let f = fixture(8);
    let a = hot_input(&[T + 5, T + 1], Some(vec!["e", "a"]), None);
    let b = hot_input(&[T + 3], None, Some(vec![30]));
    let (done, files) = f.lake(&[a, b], 1 << 30).await;
    assert_eq!(files.len(), 1);
    let w = &files[0];
    assert_eq!(
        (w.min_timestamp, w.max_timestamp, w.record_count),
        (T + 1, T + 5, 3)
    );
    let bytes = f.read(&w.path).unwrap();
    assert_eq!(w.size_bytes, bytes.len() as u64);
    assert_eq!(w.digest, crate::chain::Digest::of(&bytes));

    let view = super::read::table(&f.committer.store, &f.committer.layout, &f.dataset, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(view.version, done.version);
    assert_eq!(view.current_snapshot_id, Some(done.snapshot_id));
    assert_eq!(
        view.snapshots[0].operation,
        super::read::Operation::Lake {
            bucket: crate::layout::HourBucket::of(T),
            hot_snapshot_id: crate::chain::SnapshotId(7),
            file_digests: BTreeMap::from([(w.path.clone(), w.digest)]),
        }
    );
    assert_eq!(view.files.len(), 1);
    let lf = &view.files[0];
    assert_eq!(
        (
            lf.path.as_str(),
            lf.added_snapshot_id,
            lf.record_count,
            lf.size_bytes,
            lf.min_timestamp,
            lf.max_timestamp
        ),
        (
            w.path.as_str(),
            done.snapshot_id,
            3,
            w.size_bytes,
            T + 1,
            T + 5
        )
    );
    assert_eq!(lf.day, layout::day_of(T));

    // 按字段编号读回，目标 schema 取热层写法（多一列文件里没有的也读作空值）
    let mut hot = hot_schema();
    hot.push(col("absent", ColumnType::Boolean, 9));
    let target = Arc::new(StreamColumns::from_columns(hot).arrow_schema());
    let batches = super::read::read_file(
        &f.committer.store,
        &f.committer.layout,
        &f.dataset,
        &w.path,
        target,
        i64::MIN..i64::MAX,
    )
    .await
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    let all = arrow::compute::concat_batches(&batches[0].schema(), &batches).unwrap();
    let ts = all
        .column(0)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert_eq!(ts.values().to_vec(), [T + 1, T + 3, T + 5]);
    let msg = all
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(msg.iter().collect::<Vec<_>>(), [Some("a"), None, Some("e")]);
    let n = all.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(n.iter().collect::<Vec<_>>(), [None, Some(30), None]);
    assert_eq!(all.column(3).null_count(), 3);
}

#[tokio::test]
async fn lake_rewrite_splits_by_target_size_in_timestamp_order() {
    let f = fixture(8);
    let ts: Vec<i64> = (0..6).rev().map(|i| T + i).collect();
    let input = hot_input(&ts, Some(vec!["x"; 6]), None);
    let (_, files) = f.lake(&[input], 1).await;
    // 目标大小小于任何文件：切到每个文件一条记录
    assert_eq!(files.len(), 6);
    let mut spans: Vec<_> = files
        .iter()
        .map(|w| (w.min_timestamp, w.max_timestamp, w.record_count))
        .collect();
    spans.sort();
    assert_eq!(spans, (0..6).map(|i| (T + i, T + i, 1)).collect::<Vec<_>>());
}

#[tokio::test]
async fn referenced_and_day_scope() {
    let f = fixture(8);
    let day = 86_400_000_000;
    f.lake(&[hot_input(&[T], Some(vec!["a"]), None)], 1 << 30)
        .await;
    f.lake(&[hot_input(&[T + 3 * day], Some(vec!["b"]), None)], 1 << 30)
        .await;
    let c = &f.committer;
    let d0 = layout::day_of(T);
    let only_first = super::read::table(&c.store, &c.layout, &f.dataset, Some(d0..=d0))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(only_first.files.len(), 1);
    assert_eq!(only_first.files[0].day, d0);
    let all = super::read::table(&c.store, &c.layout, &f.dataset, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(all.files.len(), 2);
    let referenced = super::read::referenced(&c.store, &c.layout, &f.dataset, all.version)
        .await
        .unwrap();
    // 引用集恰为表目录下除元数据文件与提示之外的全部对象
    let on_disk: HashSet<String> = walk(&f.dir.join("amberlog/_iceberg/default/app"))
        .into_iter()
        .filter(|p| !p.ends_with(".metadata.json") && !p.ends_with("version-hint.text"))
        .collect();
    assert_eq!(referenced, on_disk);
    // 每个快照的存活文件
    let first = all
        .snapshots
        .iter()
        .find(|s| s.parent_snapshot_id.is_none())
        .unwrap();
    let files = super::read::snapshot_files(
        &c.store,
        &c.layout,
        &f.dataset,
        all.version,
        first.snapshot_id,
    )
    .await
    .unwrap();
    assert_eq!(files.len(), 1);
}

#[tokio::test]
async fn broken_manifest_is_reported_not_disabled() {
    let f = fixture(8);
    f.lake(&[hot_input(&[T], Some(vec!["a"]), None)], 1 << 30)
        .await;
    let c = &f.committer;
    let m = f.metadata(1);
    let list = m.current_snapshot().unwrap().manifest_list().to_owned();
    let rel = c.layout.rel_of_uri(&f.dataset, &list).unwrap().to_owned();
    f.write(&rel, b"not avro");
    assert!(matches!(
        super::read::table(&c.store, &c.layout, &f.dataset, None).await,
        Err(ColdError::Broken(_))
    ));
    assert_eq!(c.gates.state(&f.dataset), DatasetState::Normal);
}

// ---------------------------------------------------------------------------
// 冷层孤儿
// ---------------------------------------------------------------------------

#[tokio::test]
async fn orphan_cleanup_keeps_referenced_inflight_and_young_objects() {
    let f = fixture(8);
    f.lake(&[hot_input(&[T], Some(vec!["a"]), None)], 1 << 30)
        .await;
    let c = &f.committer;
    // 崩溃留下的数据文件与清单、正在进行的提交已上传的文件
    let orphan = layout::new_data_file_path(layout::day_of(T));
    f.write(&orphan, b"x");
    f.write("metadata/zz-m0.avro", b"x");
    let inflight = layout::new_data_file_path(layout::day_of(T));
    let mut progress = Progress::new(&f.dataset, c.inflight());
    progress
        .upload(
            &c.store,
            &c.layout,
            &inflight,
            bytes::Bytes::from_static(b"y"),
        )
        .await
        .unwrap();
    let before: HashSet<String> = walk(&f.dir.join("amberlog/_iceberg/default/app"))
        .into_iter()
        .collect();

    // 都还不够旧：不删
    let young = super::orphan::clean(
        &c.store,
        &c.layout,
        c.inflight(),
        &f.dataset,
        std::time::Duration::from_secs(3600),
        || true,
    )
    .await
    .unwrap();
    assert!(super::orphan::rels(&young).is_empty());

    let done = super::orphan::clean(
        &c.store,
        &c.layout,
        c.inflight(),
        &f.dataset,
        std::time::Duration::ZERO,
        || true,
    )
    .await
    .unwrap();
    assert_eq!(
        super::orphan::rels(&done),
        HashSet::from([orphan.clone(), "metadata/zz-m0.avro".to_owned()])
    );
    let after: HashSet<String> = walk(&f.dir.join("amberlog/_iceberg/default/app"))
        .into_iter()
        .collect();
    assert_eq!(&before - &after, super::orphan::rels(&done));
    assert!(after.contains(&inflight));
    // 放弃这次提交后，它上传的对象成为孤儿
    drop(progress);
    let done = super::orphan::clean(
        &c.store,
        &c.layout,
        c.inflight(),
        &f.dataset,
        std::time::Duration::ZERO,
        || true,
    )
    .await
    .unwrap();
    assert_eq!(super::orphan::rels(&done), HashSet::from([inflight]));
}

#[tokio::test]
async fn orphan_cleanup_aborts_when_a_snapshot_manifest_list_is_missing() {
    let f = fixture(8);
    f.lake(&[hot_input(&[T], Some(vec!["a"]), None)], 1 << 30)
        .await;
    f.lake(&[hot_input(&[T + 1], Some(vec!["b"]), None)], 1 << 30)
        .await;
    let c = &f.committer;
    let orphan = layout::new_data_file_path(layout::day_of(T));
    f.write(&orphan, b"x");
    let m = f.metadata(2);
    let old = m
        .snapshots()
        .find(|s| Some(s.snapshot_id()) != m.current_snapshot_id())
        .unwrap();
    let rel = c
        .layout
        .rel_of_uri(&f.dataset, old.manifest_list())
        .unwrap()
        .to_owned();
    std::fs::remove_file(f.dir.join("amberlog/_iceberg/default/app").join(rel)).unwrap();
    assert!(matches!(
        super::orphan::clean(
            &c.store,
            &c.layout,
            c.inflight(),
            &f.dataset,
            std::time::Duration::ZERO,
            || true
        )
        .await,
        Err(ColdError::Missing(_))
    ));
    assert!(f.read(&orphan).is_some());
    assert_eq!(c.gates.state(&f.dataset), DatasetState::Normal);
}

// ---------------------------------------------------------------------------
// ColdReader / ColdWriter 的对象存储实现
// ---------------------------------------------------------------------------

fn store_cold(f: &Fixture) -> super::store::StoreCold {
    super::store::StoreCold::new(
        f.committer.store.clone(),
        Arc::clone(&f.committer.gates),
        8,
        1 << 30,
    )
}

#[tokio::test]
async fn writer_append_and_delete_through_traits() {
    use super::{
        ColdAppend, ColdDelete, ColdReader, ColdScope, ColdState, ColdWriteError, ColdWriter,
    };
    let f = fixture(8);
    let cold = store_cold(&f);
    assert_eq!(
        cold.table(&f.dataset, ColdScope::All).await.unwrap(),
        ColdState::NoTable
    );
    let draft = cold
        .append(
            &f.dataset,
            ColdAppend {
                bucket: crate::layout::HourBucket::of(T),
                hot_snapshot_id: crate::chain::SnapshotId(3),
                schema: hot_schema(),
                inputs: vec![hot_input(&[T + 2, T], Some(vec!["b", "a"]), None)],
                bloom: crate::bloom::BloomColumns::none(),
            },
        )
        .await
        .unwrap();
    assert_eq!(draft.metadata_version, 1);
    assert_eq!(
        draft.table_location,
        f.committer.layout.table_location(&f.dataset)
    );
    assert_eq!(draft.files.len(), 1);
    let ColdState::Table(table) = cold.table(&f.dataset, ColdScope::All).await.unwrap() else {
        panic!("应已建表");
    };
    assert_eq!(table.current_snapshot_id, Some(draft.iceberg_snapshot_id));
    assert_eq!(table.files.len(), 1);
    let file = &table.files[0];
    let entry = &draft.files[0];
    assert_eq!(
        (
            &file.path,
            file.record_count,
            file.size_bytes,
            file.min_timestamp,
            file.max_timestamp
        ),
        (
            &entry.path,
            entry.record_count,
            entry.size_bytes,
            entry.min_timestamp,
            entry.max_timestamp
        )
    );
    assert!(matches!(
        &table.snapshots[0].operation,
        super::ColdOperation::Lake { file_digests, .. } if file_digests.get(&entry.path) == Some(&entry.content_digest)
    ));
    let names: Vec<_> = table
        .fields
        .iter()
        .map(|x| (x.field_id, x.name.as_str(), x.column_type))
        .collect();
    assert_eq!(
        names,
        [
            (1, "_timestamp", ColumnType::Int64),
            (2, "msg", ColumnType::Utf8),
            (3, "n", ColumnType::Int64)
        ]
    );
    assert!(cold.in_flight(&f.dataset).is_empty());

    let deleted = cold
        .delete(
            &f.dataset,
            ColdDelete {
                cutoff: -7,
                paths: vec![entry.path.clone()],
            },
        )
        .await
        .unwrap();
    assert_eq!(deleted.metadata_version, 2);
    assert_eq!(deleted.removed, vec![entry.path.clone()]);
    let ColdState::Table(table) = cold.table(&f.dataset, ColdScope::All).await.unwrap() else {
        panic!();
    };
    assert!(table.files.is_empty());
    assert!(
        table
            .snapshots
            .iter()
            .any(|s| s.operation == super::ColdOperation::Ttl { cutoff: -7 })
    );
    // 删除前的快照下仍能列出该文件（TTL 补写冷层变更段时相减用）
    let before = cold
        .snapshot_files(&f.dataset, &table, draft.iceberg_snapshot_id)
        .await
        .unwrap();
    assert_eq!(before.len(), 1);
    assert_eq!(
        cold.delete(
            &f.dataset,
            ColdDelete {
                cutoff: -7,
                paths: vec![entry.path.clone()],
            },
        )
        .await
        .unwrap_err(),
        ColdWriteError::Stale
    );
}

#[tokio::test]
async fn writer_refuses_disabled_dataset_and_lake_broken_disables() {
    use super::{ColdAppend, ColdWriteError, ColdWriter};
    let f = fixture(8);
    let cold = store_cold(&f);
    f.lake(&[hot_input(&[T], Some(vec!["a"]), None)], 1 << 30)
        .await;
    // 当前版本的清单列表被换成非 Avro：入湖读到无法解析即停用
    let m = f.metadata(1);
    let rel = f
        .committer
        .layout
        .rel_of_uri(&f.dataset, m.current_snapshot().unwrap().manifest_list())
        .unwrap()
        .to_owned();
    f.write(&rel, b"not avro");
    let request = ColdAppend {
        bucket: crate::layout::HourBucket::of(T),
        hot_snapshot_id: crate::chain::SnapshotId(4),
        schema: hot_schema(),
        inputs: vec![hot_input(&[T + 1], Some(vec!["b"]), None)],
        bloom: crate::bloom::BloomColumns::none(),
    };
    assert_eq!(
        cold.append(&f.dataset, request.clone()).await.unwrap_err(),
        ColdWriteError::Disabled(DatasetState::ManifestUnreadable)
    );
    assert_eq!(
        f.committer.gates.state(&f.dataset),
        DatasetState::ManifestUnreadable
    );
    let listed = walk(&f.dir.join("amberlog/_iceberg/default/app")).len();
    assert_eq!(
        cold.append(&f.dataset, request).await.unwrap_err(),
        ColdWriteError::Disabled(DatasetState::ManifestUnreadable)
    );
    assert_eq!(
        walk(&f.dir.join("amberlog/_iceberg/default/app")).len(),
        listed
    );
}

#[tokio::test]
async fn lake_progress_survives_an_interleaved_ttl_delete() {
    // 入湖的提交结果不明（这里是创建 v2 之前对象存储不可访问）后、重发之前，TTL 先提交了一次：
    // 入湖重发同一请求时沿用已写出的数据文件，不再重写一份
    use super::{ColdAppend, ColdDelete, ColdWriteError, ColdWriter};
    let f = fixture(8);
    let cold = store_cold(&f);
    let (_, old) = f.append(&hot_schema(), T, 3).await;
    let table = f.dir.join("amberlog/_iceberg/default/app");
    let data_files = || -> HashSet<String> {
        walk(&table)
            .into_iter()
            .filter(|p| p.starts_with("data/"))
            .collect()
    };
    let before = data_files();
    // 把 `metadata/` 换成同名普通文件：其下的写入都失败
    let (meta, aside) = (table.join("metadata"), table.join("metadata.aside"));
    let (m, a) = (meta.clone(), aside.clone());
    cold.committer_for_test().before_create_for_test(move || {
        std::fs::rename(&m, &a).unwrap();
        std::fs::write(&m, b"").unwrap();
    });
    let request = ColdAppend {
        bucket: crate::layout::HourBucket::of(T),
        hot_snapshot_id: crate::chain::SnapshotId(5),
        schema: hot_schema(),
        inputs: vec![hot_input(&[T + 1], Some(vec!["b"]), None)],
        bloom: crate::bloom::BloomColumns::none(),
    };
    assert!(matches!(
        cold.append(&f.dataset, request.clone()).await.unwrap_err(),
        ColdWriteError::Unavailable(_)
    ));
    std::fs::remove_file(&meta).unwrap();
    std::fs::rename(&aside, &meta).unwrap();
    let uploaded: HashSet<String> = data_files().difference(&before).cloned().collect();
    assert_eq!(uploaded.len(), 1);

    let deleted = cold
        .delete(
            &f.dataset,
            ColdDelete {
                cutoff: T,
                paths: vec![old],
            },
        )
        .await
        .unwrap();
    assert_eq!(deleted.metadata_version, 2);

    let draft = cold.append(&f.dataset, request).await.unwrap();
    assert_eq!(draft.metadata_version, 3);
    let paths: HashSet<String> = draft.files.into_iter().map(|e| e.path).collect();
    assert_eq!(paths, uploaded);
    assert_eq!(data_files(), &before | &uploaded);
}

#[tokio::test]
async fn reader_splits_version_from_table_and_reports_missing_data_file() {
    // 跨层查询把「确定当前版本」与「读元数据与清单」分开报错，并把数据文件不存在与内容无法解析分开
    use super::{ColdReadError, ColdReader, ColdScope, ColdState};
    let f = fixture(8);
    let cold = store_cold(&f);
    assert_eq!(cold.version(&f.dataset).await.unwrap(), None);
    f.append(&hot_schema(), T, 3).await;
    let (_, file) = f.append(&hot_schema(), T + 10, 2).await;
    assert_eq!(cold.version(&f.dataset).await.unwrap(), Some(2));
    let ColdState::Table(whole) = cold.table(&f.dataset, ColdScope::All).await.unwrap() else {
        panic!("应已建表");
    };
    assert_eq!(
        cold.table_at(&f.dataset, 2, ColdScope::All).await.unwrap(),
        whole
    );
    let v1 = cold.table_at(&f.dataset, 1, ColdScope::All).await.unwrap();
    assert_eq!((v1.metadata_version, v1.files.len()), (1, 1));

    // 当前版本的清单列表损坏：确定当前版本照常，读表报内容无法解析
    let m = f.metadata(2);
    let list = f
        .committer
        .layout
        .rel_of_uri(&f.dataset, m.current_snapshot().unwrap().manifest_list())
        .unwrap()
        .to_owned();
    let saved = f.read(&list).unwrap();
    f.write(&list, b"not avro");
    assert_eq!(cold.version(&f.dataset).await.unwrap(), Some(2));
    assert!(matches!(
        cold.table_at(&f.dataset, 2, ColdScope::All).await,
        Err(ColdReadError::Integrity(_))
    ));
    f.write(&list, &saved);

    let target = Arc::new(StreamColumns::from_columns(hot_schema()).arrow_schema());
    let entry = whole.files.iter().find(|x| x.path == file).unwrap();
    let path = f.dir.join("amberlog/_iceberg/default/app").join(&file);
    let bytes = std::fs::read(&path).unwrap();
    std::fs::write(&path, b"not parquet").unwrap();
    assert!(matches!(
        cold.read_file(
            &f.dataset,
            &whole,
            entry,
            Arc::clone(&target),
            i64::MIN..i64::MAX
        )
        .await
        .map(|_| ()),
        Err(ColdReadError::Integrity(_))
    ));
    std::fs::remove_file(&path).unwrap();
    assert_eq!(
        cold.read_file(
            &f.dataset,
            &whole,
            entry,
            Arc::clone(&target),
            i64::MIN..i64::MAX
        )
        .await
        .map(|_| ())
        .unwrap_err(),
        ColdReadError::NotFound(file.clone())
    );
    std::fs::write(&path, bytes).unwrap();
    let batches = cold
        .read_file(&f.dataset, &whole, entry, target, i64::MIN..i64::MAX)
        .await
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
}

#[tokio::test]
async fn table_lists_snapshots_in_commit_order() {
    use super::{ColdReader, ColdScope, ColdState};
    let f = fixture(8);
    let cold = store_cold(&f);
    let mut committed = Vec::new();
    for i in 0..6 {
        let (v, _) = f.append(&hot_schema(), T + i * 10, 1).await;
        committed.push(f.metadata(v).current_snapshot().unwrap().snapshot_id());
    }
    for _ in 0..5 {
        let ColdState::Table(t) = cold.table(&f.dataset, ColdScope::All).await.unwrap() else {
            panic!("应已建表");
        };
        let ids: Vec<i64> = t.snapshots.iter().map(|s| s.snapshot_id).collect();
        assert_eq!(ids, committed);
    }
}

async fn lake_via_trait(cold: &super::store::StoreCold, f: &Fixture, ts: &[i64], h: u64) {
    use super::{ColdAppend, ColdWriter};
    cold.append(
        &f.dataset,
        ColdAppend {
            bucket: crate::layout::HourBucket::of(ts[0]),
            hot_snapshot_id: crate::chain::SnapshotId(h),
            schema: hot_schema(),
            inputs: vec![hot_input(ts, None, Some(ts.to_vec()))],
            bloom: crate::bloom::BloomColumns::none(),
        },
    )
    .await
    .unwrap();
}

fn metadata_json(f: &Fixture, n: u64) -> serde_json::Value {
    let path = f
        .dir
        .join("amberlog/_iceberg/default/app")
        .join(layout::metadata_path(n));
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

#[tokio::test]
async fn replace_merges_inputs_and_keeps_other_entries() {
    use super::{
        ColdOperation, ColdReader, ColdReplace, ColdScope, ColdState, ColdUpkeep, ColdWriteError,
        ColdWriter,
    };
    let f = fixture(8);
    let cold = store_cold(&f);
    const HOUR: i64 = 3_600_000_000;
    lake_via_trait(&cold, &f, &[T - 24 * HOUR], 1).await;
    lake_via_trait(&cold, &f, &[T + 5, T + 1], 2).await;
    lake_via_trait(&cold, &f, &[T + HOUR + 2, T + HOUR], 3).await;
    let ColdState::Table(before) = cold.table(&f.dataset, ColdScope::All).await.unwrap() else {
        panic!("应已建表");
    };
    let day = layout::day_of(T);
    let (inputs, others): (Vec<_>, Vec<_>) =
        before.files.iter().cloned().partition(|x| x.day == day);
    assert_eq!((inputs.len(), others.len()), (2, 1));
    let mut request = ColdReplace {
        day,
        inputs: Vec::new(),
        bloom: crate::bloom::BloomColumns::none(),
    };
    for x in &inputs {
        let bytes = cold.file_bytes(&f.dataset, &x.path).await.unwrap();
        request.inputs.push((x.path.clone(), bytes));
    }
    let done = cold.replace(&f.dataset, request.clone()).await.unwrap();
    assert_eq!(done.metadata_version, before.metadata_version + 1);
    assert_eq!(done.outputs.iter().map(|o| o.record_count).sum::<u64>(), 4);
    assert_eq!(
        (
            done.outputs[0].min_timestamp,
            done.outputs.last().unwrap().max_timestamp
        ),
        (T + 1, T + HOUR + 2)
    );

    let ColdState::Table(after) = cold.table(&f.dataset, ColdScope::All).await.unwrap() else {
        panic!();
    };
    // 其余条目保留原快照号；合并产出由合并快照新增
    let kept = after
        .files
        .iter()
        .find(|x| x.path == others[0].path)
        .unwrap();
    assert_eq!(kept.added_snapshot_id, others[0].added_snapshot_id);
    let mut live: Vec<_> = after.files.iter().map(|x| x.path.clone()).collect();
    live.sort();
    let mut want: Vec<_> = done
        .outputs
        .iter()
        .map(|o| o.path.clone())
        .chain([others[0].path.clone()])
        .collect();
    want.sort();
    assert_eq!(live, want);
    let snap = after
        .snapshots
        .iter()
        .find(|s| s.snapshot_id == done.iceberg_snapshot_id)
        .unwrap();
    let ColdOperation::Compaction { file_digests } = &snap.operation else {
        panic!("合并快照应读作 compaction：{:?}", snap.operation);
    };
    for o in &done.outputs {
        let bytes = cold.file_bytes(&f.dataset, &o.path).await.unwrap();
        assert_eq!(
            file_digests.get(&o.path),
            Some(&crate::chain::Digest::of(&bytes))
        );
        assert_eq!(o.content_digest, crate::chain::Digest::of(&bytes));
        assert!(
            o.path
                .starts_with(&format!("data/_timestamp_day={}/", layout::day_string(day)))
        );
    }
    let meta = metadata_json(&f, done.metadata_version);
    let snapshots = meta["snapshots"].as_array().unwrap();
    let s = snapshots
        .iter()
        .find(|s| s["snapshot-id"] == done.iceberg_snapshot_id)
        .unwrap();
    assert_eq!(s["summary"]["operation"], "replace");
    assert!(cold.in_flight(&f.dataset).is_empty());

    // 输入已不存活：放弃，不写提交
    assert_eq!(
        cold.replace(&f.dataset, request).await.unwrap_err(),
        ColdWriteError::Stale
    );
    assert_eq!(
        cold.version(&f.dataset).await.unwrap(),
        Some(done.metadata_version)
    );
}

/// 只选出不是当前快照的快照；`fail` 时以对象存储不可访问失败。
struct AllButCurrent {
    fail: bool,
}

impl super::ExpireSelect for AllButCurrent {
    fn select<'a>(
        &'a self,
        _dataset: &'a Dataset,
        table: &'a super::ColdTable,
    ) -> crate::BoxFuture<'a, Result<Vec<i64>, super::ColdReadError>> {
        let r = if self.fail {
            Err(super::ColdReadError::Unavailable("injected".into()))
        } else {
            Ok(table
                .snapshots
                .iter()
                .map(|s| s.snapshot_id)
                .filter(|id| Some(*id) != table.current_snapshot_id)
                .collect())
        };
        Box::pin(std::future::ready(r))
    }
}

#[tokio::test]
async fn expire_removes_selected_snapshots_and_truncates_snapshot_log() {
    use super::{ColdExpire, ColdReader, ColdScope, ColdState, ColdUpkeep, ColdWriteError};
    let f = fixture(8);
    let cold = store_cold(&f);
    for (i, ts) in [T, T + 1, T + 2].into_iter().enumerate() {
        lake_via_trait(&cold, &f, &[ts], i as u64 + 1).await;
    }
    let ColdState::Table(before) = cold.table(&f.dataset, ColdScope::All).await.unwrap() else {
        panic!();
    };
    assert_eq!(before.snapshots.len(), 3);
    let failing = ColdExpire {
        select: Arc::new(AllButCurrent { fail: true }),
    };
    assert!(matches!(
        cold.expire(&f.dataset, failing).await,
        Err(ColdWriteError::Unavailable(_))
    ));
    assert_eq!(f.committer.gates.state(&f.dataset), DatasetState::Normal);

    let expire = ColdExpire {
        select: Arc::new(AllButCurrent { fail: false }),
    };
    let done = cold
        .expire(&f.dataset, expire.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(done.metadata_version, 4);
    let mut want: Vec<i64> = before.snapshots[..2]
        .iter()
        .map(|s| s.snapshot_id)
        .collect();
    want.sort();
    assert_eq!(done.removed, want);
    let ColdState::Table(after) = cold.table(&f.dataset, ColdScope::All).await.unwrap() else {
        panic!();
    };
    assert_eq!(after.current_snapshot_id, before.current_snapshot_id);
    assert_eq!(after.snapshots.len(), 1);
    assert_eq!(after.files.len(), 3);
    let (old, new) = (metadata_json(&f, 3), metadata_json(&f, 4));
    for k in [
        "current-snapshot-id",
        "refs",
        "schemas",
        "partition-specs",
        "properties",
    ] {
        assert_eq!(old[k], new[k], "{k}");
    }
    let log = new["snapshot-log"].as_array().unwrap();
    assert_eq!(log.len(), 1);
    assert_eq!(log[0]["snapshot-id"], old["current-snapshot-id"]);
    assert_eq!(new["metadata-log"].as_array().unwrap().len(), 3);
    // 没有可移除的：不写
    assert_eq!(cold.expire(&f.dataset, expire).await.unwrap(), None);
    assert_eq!(cold.version(&f.dataset).await.unwrap(), Some(4));
}

/// `metadata-log` 由 iceberg-rust 的元数据构建器按缺省（表属性 `write.metadata.previous-versions-max` 不写）截到 100 条
/// （REQ-AMB-STO-COLD-TABLE-V2）；系统测试 `cold_table_metadata_log_capped` 的完整版要 17 分钟，这里快速核对上限本身。
#[tokio::test]
async fn metadata_log_capped_at_100() {
    let f = fixture(8);
    for i in 0..102 {
        f.append(&hot_schema(), T + i, 1).await;
    }
    let meta = metadata_json(&f, 102);
    let log = meta["metadata-log"].as_array().unwrap();
    assert_eq!(log.len(), 100);
    assert!(
        log.last().unwrap()["metadata-file"]
            .as_str()
            .unwrap()
            .ends_with("/metadata/v101.metadata.json")
    );
    assert!(
        meta["properties"]
            .get("write.metadata.previous-versions-max")
            .is_none()
    );
}

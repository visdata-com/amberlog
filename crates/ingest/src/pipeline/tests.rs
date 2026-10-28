//! 摄取管道的进程内测试：WAL 用真实的 [`FileWal`] 或可设为失败的替身，快照提交与可见集用替身。
//!
//! 与 REQ-AMB-ING-ACK-RESPONSE 各场景同一输入、同一断言，但不经 HTTP 与 `_search`；
//! 块场景本身由经 `_json` 端点的系统测试验收。

use std::sync::atomic::{AtomicU32, Ordering};

use amberlog_common::record::FieldValue;
use amberlog_common::schema::{ColumnType, SchemaLookup};
use amberlog_storage::testing::{FakeCommitter, FakeMemtable, FakeWal};
use amberlog_storage::wal::FileWal;

use super::*;

struct Harness {
    wal: Arc<FakeWal>,
    memtable: Arc<FakeMemtable>,
    committer: Arc<FakeCommitter>,
    registry: Arc<DatasetRegistry>,
    pipeline: Pipeline,
}

fn harness() -> Harness {
    let wal = Arc::new(FakeWal::new());
    let memtable = Arc::new(FakeMemtable::new());
    let committer = Arc::new(FakeCommitter::new());
    let registry = Arc::new(DatasetRegistry::new());
    let pipeline = Pipeline::new(
        wal.clone(),
        memtable.clone(),
        memtable.clone(),
        committer.clone(),
        registry.clone(),
        TimestampWindow::default(),
        HashMap::new(),
    );
    Harness {
        wal,
        memtable,
        committer,
        registry,
        pipeline,
    }
}

fn ds(stream: &str) -> Dataset {
    Dataset::new("default", stream)
}

const NOW_US: i64 = 1_790_000_000_000_000;

impl Harness {
    async fn ingest(&self, stream: &str, body: &str) -> Result<Status, Rejected> {
        self.pipeline
            .ingest(&ds(stream), body.as_bytes(), NOW_US)
            .await
    }

    fn records(&self, stream: &str) -> Vec<Record> {
        self.wal
            .entries()
            .into_iter()
            .filter_map(|(_, e)| match e {
                WalEntry::Records { dataset, records } if dataset == ds(stream) => Some(records),
                _ => None,
            })
            .flatten()
            .collect()
    }
}

fn reasons(status: &Status) -> Vec<(usize, &'static str)> {
    status
        .errors
        .iter()
        .map(|(i, r)| (*i, r.as_str()))
        .collect()
}

#[tokio::test]
async fn partial_failure_is_reported_per_record() {
    let h = harness();
    let status = h
        .ingest("app", r#"[{"a":1},"not-an-object",{"a":3}]"#)
        .await
        .unwrap();
    assert_eq!((status.successful, status.failed), (2, 1));
    assert_eq!(reasons(&status), [(1, "not_an_object")]);
    assert_eq!(h.records("app").len(), 2);
}

#[tokio::test]
async fn ok_body_has_the_fields_types_and_order_of_the_block() {
    let h = harness();
    h.ingest("sh", r#"{"a":1}"#).await.unwrap();
    let status = h
        .ingest("sh", r#"[{"_x":1},{"a":2},5,{"a":1e400},{"b":true}]"#)
        .await
        .unwrap();
    let body = status.ok_body();
    assert_eq!(body["code"], json!(200));
    assert!(body["code"].is_u64());
    let entries = body["status"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    let e = &entries[0];
    assert_eq!(e["name"], "sh");
    assert!(e["successful"].is_u64() && e["failed"].is_u64());
    assert_eq!(
        (e["successful"].as_u64(), e["failed"].as_u64()),
        (Some(2), Some(3))
    );
    let errors = e["errors"].as_array().unwrap();
    assert_eq!(errors.len(), 3);
    let got: Vec<(u64, &str)> = errors
        .iter()
        .map(|x| (x["index"].as_u64().unwrap(), x["reason"].as_str().unwrap()))
        .collect();
    assert_eq!(
        got,
        [
            (0, "reserved_field"),
            (2, "not_an_object"),
            (3, "malformed_record")
        ]
    );
    assert!(errors.iter().all(|x| x["message"].is_string()));
}

#[tokio::test]
async fn all_rejected_batch_is_400_with_details_status() {
    let h = harness();
    let rejected = h.ingest("app", r#"["x","y"]"#).await.unwrap_err();
    let body = serde_json::to_value(rejected.error_body()).unwrap();
    assert_eq!(body["code"], 400);
    assert_eq!(body["reason"], "all_records_rejected");
    let s = &body["details"]["status"][0];
    assert_eq!(
        (s["successful"].as_u64(), s["failed"].as_u64()),
        (Some(0), Some(2))
    );
    let idx: Vec<u64> = s["errors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["index"].as_u64().unwrap())
        .collect();
    assert_eq!(idx, [0, 1]);
    assert!(h.wal.entries().is_empty(), "全拒的批次不写 WAL");
}

#[tokio::test]
async fn all_rejected_status_keeps_every_constraint() {
    let h = harness();
    h.ingest("ar", r#"{"a":1}"#).await.unwrap();
    let Err(Rejected::AllRecordsRejected(status)) =
        h.ingest("ar", r#"[{"a":-1e400},{"-bad":1},7]"#).await
    else {
        panic!("应为全部拒绝");
    };
    let body =
        serde_json::to_value(Rejected::AllRecordsRejected(status.clone()).error_body()).unwrap();
    let entries = body["details"]["status"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["name"], "ar");
    assert_eq!((status.successful, status.failed), (0, 3));
    assert_eq!(
        reasons(&status),
        [
            (0, "malformed_record"),
            (1, "invalid_field_name"),
            (2, "not_an_object")
        ]
    );
}

#[tokio::test]
async fn empty_batch_is_200_not_all_rejected() {
    let h = harness();
    let status = h.ingest("app", "[]").await.unwrap();
    assert_eq!((status.successful, status.failed), (0, 0));
    assert!(h.memtable.schema(&ds("app")).is_some(), "空批次也建 stream");
    assert!(status.errors.is_empty());
}

#[tokio::test]
async fn invalid_json_is_rejected_before_anything_else() {
    let h = harness();
    assert_eq!(
        h.ingest("app", "[1,").await.unwrap_err(),
        Rejected::InvalidJson
    );
    assert!(h.committer.ensured().is_empty(), "不因非法请求建 stream");
    assert!(h.memtable.schema(&ds("app")).is_none());
    assert!(h.wal.entries().is_empty());
}

#[tokio::test]
async fn wal_failure_is_503_and_nothing_reaches_the_memtable() {
    let h = harness();
    h.wal.set_failing(true);
    let rejected = h.ingest("app", r#"[{"a":1}]"#).await.unwrap_err();
    assert!(matches!(rejected, Rejected::Unavailable(_)));
    let body = serde_json::to_value(rejected.error_body()).unwrap();
    assert_eq!(
        (body["code"].as_u64(), body["reason"].as_str()),
        (Some(503), Some("unavailable"))
    );
    assert!(h.memtable.written().is_empty());
}

#[tokio::test]
async fn columns_of_a_503_batch_do_not_enter_the_schema() {
    let h = harness();
    h.ingest("nc", r#"{"a":1}"#).await.unwrap();
    h.wal.set_failing(true);
    assert!(matches!(
        h.ingest("nc", r#"[{"newcol":"s"}]"#).await.unwrap_err(),
        Rejected::Unavailable(_)
    ));
    h.wal.set_failing(false);
    let status = h.ingest("nc", r#"[{"newcol":1}]"#).await.unwrap();
    assert_eq!(status.successful, 1);
    let schema = h.memtable.schema(&ds("nc")).unwrap();
    assert_eq!(schema.column_type("newcol"), Some(ColumnType::Int64));
}

#[tokio::test]
async fn retry_after_503_is_recorded_once() {
    let h = harness();
    h.ingest("rt", r#"{"k":"base"}"#).await.unwrap();
    h.wal.set_failing(true);
    let batch = r#"[{"k":"r1"},{"k":"r2"}]"#;
    assert!(h.ingest("rt", batch).await.is_err());
    h.wal.set_failing(false);
    h.ingest("rt", batch).await.unwrap();
    let ks: Vec<FieldValue> = h
        .records("rt")
        .into_iter()
        .map(|r| r.fields[0].1.clone())
        .collect();
    assert_eq!(ks, ["base", "r1", "r2"].map(|k| FieldValue::Utf8(k.into())));
}

#[tokio::test]
async fn all_rejected_batch_for_a_new_stream_needs_its_existence_persisted() {
    let h = harness();
    h.committer.set_pending(&ds("ax"), true);
    h.wal.set_failing(true);
    assert!(matches!(
        h.ingest("ax", r#"[{"_x":1}]"#).await.unwrap_err(),
        Rejected::Unavailable(_)
    ));
    assert_eq!(
        h.registry.persistence(&ds("ax")),
        None,
        "存在没能持久化就不登记 LocalWal"
    );
    // WAL 可写时：存在随本批写入，应答照常为 400
    h.wal.set_failing(false);
    assert!(matches!(
        h.ingest("ax", r#"[{"_x":1}]"#).await,
        Err(Rejected::AllRecordsRejected(_))
    ));
    assert_eq!(
        h.wal
            .entries()
            .into_iter()
            .map(|(_, e)| e)
            .collect::<Vec<_>>(),
        [WalEntry::DatasetExists { dataset: ds("ax") }]
    );
}

#[tokio::test]
async fn empty_batch_for_a_pending_stream_persists_its_existence() {
    let h = harness();
    h.committer.set_pending(&ds("ne"), true);
    h.wal.set_failing(true);
    assert!(matches!(
        h.ingest("ne", "[]").await.unwrap_err(),
        Rejected::Unavailable(_)
    ));
    h.wal.set_failing(false);
    assert!(h.ingest("ne", "[]").await.is_ok());
    assert_eq!(h.wal.entries().len(), 1);
}

#[tokio::test]
async fn baseline_is_logged_with_the_first_accepted_batch_only() {
    let wal = Arc::new(FakeWal::new());
    let memtable = Arc::new(FakeMemtable::new());
    let pipeline = Pipeline::new(
        wal.clone(),
        memtable.clone(),
        memtable.clone(),
        Arc::new(FakeCommitter::new()),
        Arc::new(DatasetRegistry::new()),
        TimestampWindow::default(),
        HashMap::from([(ds("app"), Some(SnapshotId(7)))]),
    );
    pipeline
        .ingest(&ds("app"), b"[5]", NOW_US)
        .await
        .unwrap_err();
    pipeline
        .ingest(&ds("app"), br#"{"a":1}"#, NOW_US)
        .await
        .unwrap();
    pipeline
        .ingest(&ds("app"), br#"{"a":2}"#, NOW_US)
        .await
        .unwrap();
    let kinds: Vec<WalEntry> = wal.entries().into_iter().map(|(_, e)| e).collect();
    assert_eq!(kinds.len(), 3);
    assert_eq!(
        kinds[0],
        WalEntry::Baseline {
            dataset: ds("app"),
            snapshot_id: Some(SnapshotId(7))
        }
    );
    assert!(matches!(kinds[1], WalEntry::Records { .. }));
    assert!(matches!(kinds[2], WalEntry::Records { .. }));
    // 内存表按 Records 条目的位置写入
    let written: Vec<u64> = memtable.written().iter().map(|w| w.position.0).collect();
    assert_eq!(written, [2, 3]);
}

/// 200 之后进程被强杀：批次已在 WAL 中，以同一目录重放即可恢复。
#[tokio::test]
async fn acknowledged_batch_is_in_the_wal_on_reopen() {
    static N: AtomicU32 = AtomicU32::new(0);
    let dir = std::env::temp_dir().join(format!(
        "amberlog-ingest-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let wal = Arc::new(FileWal::open(&dir));
    wal.replay(&mut |_, _| {}).unwrap();
    let memtable = Arc::new(FakeMemtable::new());
    let pipeline = Pipeline::new(
        wal,
        memtable.clone(),
        memtable,
        Arc::new(FakeCommitter::new()),
        Arc::new(DatasetRegistry::new()),
        TimestampWindow::default(),
        HashMap::new(),
    );
    pipeline
        .ingest(&ds("app"), br#"[{"k":"survive"}]"#, NOW_US)
        .await
        .unwrap();

    let reopened = FileWal::open(&dir);
    let mut found = Vec::new();
    reopened
        .replay(&mut |_, e| {
            if let WalEntry::Records { records, .. } = e {
                found.extend(records);
            }
        })
        .unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].fields[0].1, FieldValue::Utf8("survive".into()));
    let _ = std::fs::remove_dir_all(&dir);
}

/// REQ-AMB-ING-JSON-ENDPOINT 的场景「合法 JSON 的字节级判定」：`_search` 与 `_sent` 共用的
/// `amberlog_common::json::parse_body`，以及 `_json` 请求体经批次化入口的整体判定。
#[test]
fn ingest_json_validity_rules() {
    let nested = |n: usize| format!("{}1{}", "[".repeat(n), "]".repeat(n)).into_bytes();
    // (请求体字节, `_search` 与 `_sent` 合法, `_json` 整体合法)
    let cases: Vec<(Vec<u8>, bool, bool)> = vec![
        (
            [b"\xEF\xBB\xBF".as_slice(), br#"[{"a":1}]"#].concat(),
            false,
            false,
        ),
        (
            [br#"[{"a":""#.as_slice(), b"\xFF", br#""}]"#].concat(),
            false,
            true,
        ),
        (
            [br#"[{"a":1}"#.as_slice(), b"\xFF", b"]"].concat(),
            false,
            false,
        ),
        (br#"[{"a":"\ud800"}]"#.to_vec(), false, true),
        (br#"[{"a":"\udc00x"}]"#.to_vec(), false, true),
        (br#"[{"a":1e400}]"#.to_vec(), false, true),
        (br#"[{"a":-1e400}]"#.to_vec(), false, true),
        (br#"[{"a":1.7976931348623159e308}]"#.to_vec(), false, true),
        (br#"[{"a":1.7976931348623158e308}]"#.to_vec(), true, true),
        (br#"[{"a":1e308}]"#.to_vec(), true, true),
        ("[{\"a\":\"\u{1F600}\"}]".as_bytes().to_vec(), true, true),
        (
            br#"[{"a":123456789012345678901234567890}]"#.to_vec(),
            true,
            true,
        ),
        (nested(128), true, true),
        (nested(129), false, true),
        (br#"[{"a":1},{"a":"#.to_vec(), false, false),
    ];
    for (bytes, search_valid, json_valid) in cases {
        let shown = String::from_utf8_lossy(&bytes).into_owned();
        assert_eq!(
            amberlog_common::json::parse_body(&bytes).is_ok(),
            search_valid,
            "parse_body: {shown}"
        );
        assert_eq!(
            parse_batch(&bytes).is_some(),
            json_valid,
            "parse_batch: {shown}"
        );
    }
}

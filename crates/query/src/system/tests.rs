//! 系统表在 query 这一层的测试：快照链、闸门、计数对象与桶状态一律用替身，不经 HTTP、不读对象存储。
//! 块场景的系统测试（启动 `amberlog` 二进制）在 `amberlog-server` 的 `gov_ledger.rs`、`gov_reconciliation.rs` 与 `qry_search.rs`。

use std::collections::BTreeMap;
use std::sync::Arc;

use amberlog_common::dataset::Dataset;
use amberlog_common::error::Reason;
use amberlog_storage::chain::{
    BucketRef, ChainError, ColdSummary, Digest, Snapshot, SnapshotId, Summary,
};
use amberlog_storage::gate::{DatasetGates, DatasetState};
use amberlog_storage::registry::{DatasetRegistry, Persistence};
use amberlog_storage::testing::{FakeChain, FakeMemtable};
use serde_json::{Value, json};

use super::*;
use crate::request::Limits;
use crate::search::Engine;
use crate::sql;
use crate::testing::{FakeAlertHistory, FakeBucketStates, FakeCounters, FakeFiles};

const END: i64 = 4_102_444_800_000_000;
const MINUTE: i64 = 60_000_000;

fn ds(stream: &str) -> Dataset {
    Dataset::new("default", stream)
}

struct Fx {
    chain: Arc<FakeChain>,
    gates: Arc<DatasetGates>,
    registry: Arc<DatasetRegistry>,
    counters: Arc<FakeCounters>,
    buckets: Arc<FakeBucketStates>,
    alerts: Arc<FakeAlertHistory>,
    retention: Option<Retention>,
}

impl Fx {
    fn new() -> Fx {
        let (_, registry, counters, buckets) = crate::testing::governance();
        Fx {
            chain: Arc::new(FakeChain::new()),
            gates: Arc::new(DatasetGates::new()),
            registry,
            counters,
            buckets,
            alerts: Arc::new(FakeAlertHistory::new()),
            retention: None,
        }
    }

    fn engine(&self) -> Engine {
        Engine {
            memtable: Arc::new(FakeMemtable::new()),
            chain: self.chain.clone(),
            gates: self.gates.clone(),
            files: Arc::new(FakeFiles::new()),
            cold: Arc::new(amberlog_storage::testing::FakeColdReader::new()),
            limits: Limits::default(),
            admission: crate::testing::admission(),
            governance: Governance {
                registry: self.registry.clone(),
                counters: self.counters.clone(),
                buckets: self.buckets.clone(),
                alerts: self.alerts.clone(),
                retention: self.retention.clone(),
                backfill: Arc::new(crate::testing::FakeBackfill::new()),
                grace_us: crate::testing::GRACE_US,
            },
        }
    }

    /// 存在、创世快照尚未成为 `_current` 的数据集。
    fn genesis(&self, stream: &str) {
        self.registry.record(&ds(stream), Persistence::LocalWal);
        self.chain.set_current(&ds(stream), Ok(None));
    }

    /// 存在、`_current` 指向一个快照的数据集；`buckets` 为（桶标签，条数）。
    fn committed(&self, stream: &str, buckets: &[(&str, u64)], time: (i64, i64)) {
        self.committed_with(stream, buckets, Some(time), ColdSummary::EMPTY);
    }

    /// 同 [`Fx::committed`]，另给热层 `summary` 的首末时间（`None` 为热层没有数据）与 `summary.cold`。
    fn committed_with(
        &self,
        stream: &str,
        buckets: &[(&str, u64)],
        hot_time: Option<(i64, i64)>,
        cold: ColdSummary,
    ) {
        let time = hot_time.unwrap_or((0, 0));
        self.registry.record(&ds(stream), Persistence::Stored);
        let buckets: Vec<BucketRef> = buckets
            .iter()
            .map(|(label, n)| BucketRef {
                bucket: (*label).into(),
                manifest: format!("manifest/buckets/{label}-x.json"),
                content_digest: Digest([0; 32]),
                record_count: *n,
                size_bytes: n * 10,
                time_min: Some(time.0),
                time_max: Some(time.1),
            })
            .collect();
        let summary = Summary {
            dataset: format!("default/{stream}"),
            record_count: buckets.iter().map(|b| b.record_count).sum(),
            size_bytes: buckets.iter().map(|b| b.size_bytes).sum(),
            time_min: hot_time.map(|t| t.0),
            time_max: hot_time.map(|t| t.1),
            cold,
        };
        self.chain.put_snapshot(
            &ds(stream),
            Snapshot {
                format_version: 1,
                snapshot_id: SnapshotId(3),
                parent_snapshot_id: Some(SnapshotId(2)),
                parent_digest: None,
                created_at: 0,
                schema: Vec::new(),
                buckets,
                summary,
                retired_segment: None,
                cold_deletion_segment: None,
            },
        );
        self.chain.set_current(&ds(stream), Ok(Some(SnapshotId(3))));
    }

    async fn run(&self, sql: &str, range: (i64, i64)) -> Result<Value, Reason> {
        let body = json!({"query": {"sql": sql, "start_time": range.0, "end_time": range.1}});
        self.engine()
            .search("default", body.to_string().as_bytes())
            .await
            .map(|r| r.to_json())
            .map_err(|e| e.reason)
    }

    async fn hits(&self, sql: &str) -> Vec<Value> {
        match self.run(sql, (0, END)).await {
            Ok(v) => v["hits"].as_array().unwrap().clone(),
            Err(r) => panic!("{sql}: {r:?}"),
        }
    }
}

fn window(start: i64, received: u64, durable: u64, sent: &[(&str, u64)]) -> CounterWindow {
    CounterWindow {
        window_start: start,
        window_end: start + MINUTE,
        received,
        durable,
        sent: sent
            .iter()
            .map(|(s, n)| ((*s).to_owned(), *n))
            .collect::<BTreeMap<_, _>>(),
        durable_ts: 0,
        durable_ts_by_source: BTreeMap::new(),
        sent_ts: BTreeMap::new(),
    }
}

// ---------------------------------------------------------------------------
// 接入 `_search`：列集合、报告顺序、扫描统计
// ---------------------------------------------------------------------------

#[tokio::test]
async fn system_table_names_and_unknown_column() {
    let fx = Fx::new();
    fx.committed("app", &[("2026082210", 5)], (1, 2));
    let body = fx
        .run("SELECT dataset, record_count FROM _ledger", (0, 1))
        .await;
    let body = body.unwrap();
    assert_eq!(
        body["hits"],
        json!([{"dataset": "default/app", "record_count": 5}])
    );
    for k in ["scan_records", "scan_files", "scan_bytes"] {
        assert_eq!(body[k], 0, "{k}");
    }
    // 大小写：未加引号的名称按小写解析，加引号的照原样（不是系统表名，也不合 stream 命名）
    assert!(fx.run("SELECT dataset FROM _LEDGER", (0, 1)).await.is_ok());
    for (sql, want) in [
        ("SELECT * FROM _nosuch", Reason::StreamNotFound),
        ("SELECT * FROM \"_Ledger\"", Reason::StreamNotFound),
        ("SELECT _timestamp FROM _ledger", Reason::UnknownColumn),
        (
            "SELECT dataset FROM _reconciliation ORDER BY tier",
            Reason::UnknownColumn,
        ),
        // `GROUP BY` 合法，解析不到的名字回 `unknown_column`；子集外的调用仍先于它报告
        (
            "SELECT nosuch FROM _ledger GROUP BY nosuch",
            Reason::UnknownColumn,
        ),
        ("SELECT lower(nosuch) FROM _ledger", Reason::SqlUnsupported),
        (
            "SELECT dataset FROM _ledger WHERE dataset = 1",
            Reason::InvalidSql,
        ),
        (
            "SELECT nosuch FROM _ledger WHERE dataset = 1",
            Reason::UnknownColumn,
        ),
    ] {
        assert_eq!(fx.run(sql, (0, 1)).await.err(), Some(want), "{sql}");
    }
}

/// 读取错误排在 `unknown_column` 与类型检查之后：对象存储不可访问时，列名写错仍报 `unknown_column`。
#[tokio::test]
async fn read_errors_come_last() {
    let fx = Fx::new();
    fx.committed("app", &[], (1, 2));
    fx.counters
        .fail(&ds("app"), CounterError::Unavailable("down".into()));
    assert_eq!(
        fx.run("SELECT nosuch FROM _reconciliation", (0, 1))
            .await
            .err(),
        Some(Reason::UnknownColumn)
    );
    assert_eq!(
        fx.run("SELECT * FROM _reconciliation", (0, 1)).await.err(),
        Some(Reason::Unavailable)
    );
}

#[test]
fn named_datasets_from_top_level_and() {
    let named = |w: &str| {
        named_datasets(&sql::parse(&format!("SELECT dataset FROM _ledger WHERE {w}")).unwrap())
            .map(|s| s.into_iter().collect::<Vec<_>>())
    };
    assert_eq!(
        named("dataset = 'default/a'"),
        Some(vec!["default/a".into()])
    );
    assert_eq!(
        named("('default/b' = \"dataset\") AND (status = 'x' AND dataset = 'default/a')"),
        Some(vec!["default/a".into(), "default/b".into()])
    );
    assert_eq!(named("dataset = 'default/a' OR status = 'x'"), None);
    assert_eq!(named("NOT dataset = 'default/a'"), None);
    assert_eq!(named("dataset != 'default/a'"), None);
    assert_eq!(named("dataset IN ('default/a')"), None);
    assert_eq!(named("status = 'default/a'"), None);
    assert_eq!(
        named_datasets(&sql::parse("SELECT dataset FROM _ledger").unwrap()),
        None
    );
}

// ---------------------------------------------------------------------------
// _ledger
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ledger_rows_from_snapshot_and_counters() {
    let mut fx = Fx::new();
    fx.retention = Some(Retention {
        text: "30d".into(),
        micros: 30 * 86_400 * 1_000_000,
    });
    fx.committed("app", &[("2026082210", 40), ("2026082213", 60)], (100, 200));
    fx.buckets.set_pending("2026082210");
    fx.counters.put(
        &ds("app"),
        vec![
            window(0, 1, 1, &[("vector-1", 1)]),
            window(MINUTE, 1, 1, &[("fluentbit-2", 0), ("vector-1", 2)]),
        ],
    );
    fx.genesis("fresh");
    let hits = fx.hits("SELECT * FROM _ledger").await;
    assert_eq!(
        hits,
        [
            json!({
                "dataset": "default/app", "status": "active", "sources": "fluentbit-2,vector-1",
                "time_min": 100, "time_max": 200, "record_count": 100, "size_bytes": 1000,
                "tier": "hot_pending_lake", "pending_lake_record_count": 40,
                "retention": "30d", "expires_at": 100 + 2_592_000_000_000i64,
                "reconciliation": "three_way",
                // 只有按送达的上报：没有受判来源，两个窗口都判不可判定
                "windows_pending": 0, "windows_consistent": 0, "windows_missing": 0,
                "windows_duplicate": 0, "windows_silent": 0, "windows_undeterminable": 2, "silent_sources": 0,
            }),
            json!({
                "dataset": "default/fresh", "status": "active", "record_count": 0, "size_bytes": 0,
                "pending_lake_record_count": 0, "retention": "30d", "reconciliation": "two_way",
                "windows_pending": 0, "windows_consistent": 0, "windows_missing": 0,
                "windows_duplicate": 0, "windows_silent": 0, "windows_undeterminable": 0, "silent_sources": 0,
            }),
        ]
    );
    // 判定桶状态用的是收到请求的时刻，同一查询各行共用
    let seen = fx.buckets.seen();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0], seen[1]);
}

#[tokio::test]
async fn ledger_tier_and_expires_at_edges() {
    let mut fx = Fx::new();
    fx.retention = Some(Retention {
        text: "36500d".into(),
        micros: 36_500 * 86_400 * 1_000_000,
    });
    // 9950-01-01 加一百年越过 9999 年
    let t9950 = 251_824_464_000_000_000;
    fx.committed("far", &[("995001010", 1)], (t9950, t9950));
    fx.committed("empty", &[], (1, 1));
    let hits = fx
        .hits("SELECT dataset, tier, retention, expires_at FROM _ledger")
        .await;
    assert_eq!(
        hits,
        [
            json!({"dataset": "default/empty", "retention": "36500d", "expires_at": 1 + 36_500 * 86_400 * 1_000_000i64}),
            json!({"dataset": "default/far", "tier": "hot", "retention": "36500d"}),
        ]
    );
}

/// 条数、字节与首末时间跨两层，所在层按两层条数取值（REQ-AMB-GOV-LEDGER-V2）。
#[tokio::test]
async fn ledger_cross_tier_values() {
    let fx = Fx::new();
    let cold = |n: u64, t: (i64, i64)| ColdSummary {
        record_count: n,
        size_bytes: n * 7,
        time_min: Some(t.0),
        time_max: Some(t.1),
    };
    // 热层 60 条在 [500, 900]，冷层 40 条在 [100, 300]
    fx.committed_with(
        "app",
        &[("2026082213", 60)],
        Some((500, 900)),
        cold(40, (100, 300)),
    );
    // 只有冷层
    fx.committed_with("old", &[], None, cold(10, (100, 200)));
    // 冷层有数据而热层有待入湖的桶：两层都有即 hot_cold
    fx.committed_with(
        "mix",
        &[("2026082210", 5)],
        Some((50, 60)),
        cold(1, (70, 80)),
    );
    fx.buckets.set_pending("2026082210");
    let hits = fx
        .hits("SELECT dataset, time_min, time_max, record_count, size_bytes, tier, pending_lake_record_count FROM _ledger")
        .await;
    assert_eq!(
        hits,
        [
            json!({"dataset": "default/app", "time_min": 100, "time_max": 900, "record_count": 100,
                   "size_bytes": 600 + 280, "tier": "hot_cold", "pending_lake_record_count": 0}),
            json!({"dataset": "default/mix", "time_min": 50, "time_max": 80, "record_count": 6,
                   "size_bytes": 50 + 7, "tier": "hot_cold", "pending_lake_record_count": 5}),
            json!({"dataset": "default/old", "time_min": 100, "time_max": 200, "record_count": 10,
                   "size_bytes": 70, "tier": "cold", "pending_lake_record_count": 0}),
        ]
    );
}

#[tokio::test]
async fn ledger_disabled_datasets_and_unknown_counters() {
    let fx = Fx::new();
    // 此前已读到停用：不再读快照链
    fx.committed("a", &[("2026082210", 1)], (1, 2));
    fx.gates.enter(&ds("a"), DatasetState::ManifestUnreadable);
    // 本次读到停用
    fx.registry.record(&ds("b"), Persistence::Stored);
    fx.chain.set_current(
        &ds("b"),
        Err(ChainError::Disabled(DatasetState::IncompatibleFormat)),
    );
    // 计数对象无法解析：只让该行为 unknown
    fx.committed("c", &[("2026082210", 1)], (1, 2));
    fx.counters
        .fail(&ds("c"), CounterError::Unparseable("bad".into()));
    let hits = fx
        .hits("SELECT dataset, status, record_count, tier, sources, reconciliation FROM _ledger")
        .await;
    assert_eq!(
        hits,
        [
            json!({"dataset": "default/a", "status": "disabled_manifest_unreadable", "reconciliation": "two_way"}),
            json!({"dataset": "default/b", "status": "disabled_format_version", "reconciliation": "two_way"}),
            json!({"dataset": "default/c", "status": "active", "record_count": 1, "tier": "hot", "reconciliation": "unknown"}),
        ]
    );
}

#[tokio::test]
async fn ledger_unavailable_is_503() {
    let fx = Fx::new();
    fx.registry.record(&ds("a"), Persistence::Stored);
    fx.chain
        .set_current(&ds("a"), Err(ChainError::Unavailable("down".into())));
    assert_eq!(
        fx.run("SELECT * FROM _ledger", (0, 1)).await.err(),
        Some(Reason::Unavailable)
    );
    let fx = Fx::new();
    fx.committed("a", &[], (1, 2));
    fx.counters
        .fail(&ds("a"), CounterError::Unavailable("down".into()));
    assert_eq!(
        fx.run("SELECT * FROM _ledger", (0, 1)).await.err(),
        Some(Reason::Unavailable)
    );
}

/// 点名数据集时只读被点名且存在的数据集；排序：缺省 `dataset` 升序，`ORDER BY` 同键时按 `dataset` 升序，先于 `LIMIT`。
#[tokio::test]
async fn ledger_named_datasets_and_order() {
    let fx = Fx::new();
    for s in ["web", "app", "db"] {
        fx.committed(s, &[], (1, 2));
    }
    let names = |hits: Vec<Value>| -> Vec<String> {
        hits.iter()
            .map(|h| h["dataset"].as_str().unwrap().to_owned())
            .collect()
    };
    assert_eq!(
        names(fx.hits("SELECT dataset FROM _ledger").await),
        ["default/app", "default/db", "default/web"]
    );
    assert_eq!(
        names(fx.hits("SELECT dataset FROM _ledger ORDER BY status").await),
        ["default/app", "default/db", "default/web"]
    );
    assert_eq!(
        names(fx.hits("SELECT dataset FROM _ledger LIMIT 1").await),
        ["default/app"]
    );
    assert_eq!(
        names(
            fx.hits("SELECT dataset FROM _ledger ORDER BY dataset DESC")
                .await
        ),
        ["default/web", "default/db", "default/app"]
    );

    let before = fx.chain.reads();
    assert_eq!(
        names(
            fx.hits("SELECT dataset FROM _ledger WHERE dataset = 'default/db' AND dataset = 'default/ghost'")
                .await
        ),
        Vec::<String>::new()
    );
    assert_eq!(
        names(
            fx.hits("SELECT dataset FROM _ledger WHERE dataset = 'default/db'")
                .await
        ),
        ["default/db"]
    );
    // 两次查询各只读 db 的 `_current` 与快照
    assert_eq!(fx.chain.reads() - before, 4);
    assert!(
        fx.counters
            .reads()
            .iter()
            .rev()
            .take(2)
            .all(|(d, _, _)| *d == ds("db"))
    );
}

// ---------------------------------------------------------------------------
// _reconciliation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn reconciliation_rows_and_gaps() {
    let fx = Fx::new();
    fx.committed("app", &[], (1, 2));
    fx.counters.put(
        &ds("app"),
        vec![
            window(0, 1_000_000, 999_997, &[("vector-1", 1_000_000)]),
            window(MINUTE, 10, 10, &[]),
            window(
                2 * MINUTE,
                4,
                4,
                &[("vector-1", 3), ("Fluent", 0), ("agent-b", 2)],
            ),
        ],
    );
    let hits = fx.hits("SELECT * FROM _reconciliation").await;
    assert_eq!(
        hits,
        [
            json!({"dataset": "default/app", "window_start": 2 * MINUTE, "window_end": 3 * MINUTE,
                   "sent": 5, "sent_sources": "Fluent,agent-b,vector-1", "received": 4, "durable": 4,
                   "gap_sent_received": 1, "gap_received_durable": 0, "verdict": "undeterminable"}),
            json!({"dataset": "default/app", "window_start": MINUTE, "window_end": 2 * MINUTE,
                   "received": 10, "durable": 10, "gap_received_durable": 0, "verdict": "undeterminable"}),
            json!({"dataset": "default/app", "window_start": 0, "window_end": MINUTE,
                   "sent": 1_000_000, "sent_sources": "vector-1", "received": 1_000_000, "durable": 999_997,
                   "gap_sent_received": 0, "gap_received_durable": 3, "verdict": "undeterminable"}),
        ]
    );
    // 查询范围只取 window_start 落在其中的窗口
    let r = fx
        .run(
            "SELECT window_start FROM _reconciliation",
            (MINUTE, 2 * MINUTE),
        )
        .await
        .unwrap();
    assert_eq!(r["hits"], json!([{"window_start": MINUTE}]));
}

#[tokio::test]
async fn reconciliation_default_order_and_tie_break() {
    let fx = Fx::new();
    for s in ["web", "app"] {
        fx.committed(s, &[], (1, 2));
        fx.counters.put(
            &ds(s),
            vec![window(0, 1, 1, &[]), window(MINUTE, 1, 1, &[])],
        );
    }
    let pairs = |hits: Vec<Value>| -> Vec<(String, i64)> {
        hits.iter()
            .map(|h| {
                (
                    h["dataset"].as_str().unwrap().to_owned(),
                    h["window_start"].as_i64().unwrap(),
                )
            })
            .collect()
    };
    let p = |d: &str, w: i64| (format!("default/{d}"), w);
    assert_eq!(
        pairs(
            fx.hits("SELECT dataset, window_start FROM _reconciliation")
                .await
        ),
        [p("app", MINUTE), p("web", MINUTE), p("app", 0), p("web", 0)]
    );
    assert_eq!(
        pairs(
            fx.hits("SELECT dataset, window_start FROM _reconciliation ORDER BY dataset")
                .await
        ),
        [p("app", MINUTE), p("app", 0), p("web", MINUTE), p("web", 0)]
    );
}

#[tokio::test]
async fn reconciliation_read_failures() {
    let fx = Fx::new();
    fx.committed("app", &[], (1, 2));
    fx.committed("web", &[], (1, 2));
    fx.counters
        .fail(&ds("app"), CounterError::Unparseable("bad".into()));
    fx.counters.put(&ds("web"), vec![window(0, 1, 1, &[])]);
    assert_eq!(
        fx.run("SELECT * FROM _reconciliation", (0, END))
            .await
            .err(),
        Some(Reason::Internal)
    );
    assert_eq!(
        fx.run(
            "SELECT * FROM _reconciliation WHERE dataset = 'default/app'",
            (0, END)
        )
        .await
        .err(),
        Some(Reason::Internal)
    );
    assert!(
        fx.run(
            "SELECT * FROM _reconciliation WHERE dataset = 'default/web'",
            (0, END)
        )
        .await
        .is_ok()
    );
    // 停用的数据集照样出行
    fx.gates.enter(&ds("web"), DatasetState::ManifestUnreadable);
    assert_eq!(
        fx.hits("SELECT dataset FROM _reconciliation WHERE dataset = 'default/web'")
            .await,
        [json!({"dataset": "default/web"})]
    );

    // sent 之和超出 Int64
    fx.counters.put(
        &ds("web"),
        vec![window(0, 1, 1, &[("a", i64::MAX as u64), ("b", 1)])],
    );
    assert_eq!(
        fx.run(
            "SELECT received FROM _reconciliation WHERE dataset = 'default/web'",
            (0, END)
        )
        .await
        .err(),
        Some(Reason::Internal)
    );
}

// ---------------------------------------------------------------------------
// _alert_evaluations
// ---------------------------------------------------------------------------

fn evaluation(rule: &str, scheduled_at: i64, outcome: &str) -> EvaluationRow {
    let matched = matches!(outcome, "matched" | "not_matched");
    EvaluationRow {
        rule: rule.into(),
        scheduled_at,
        started_at: matched.then_some(scheduled_at + 1),
        finished_at: scheduled_at + 2,
        window_start: scheduled_at - 3 * MINUTE,
        window_end: scheduled_at,
        outcome: outcome.into(),
        reason: (!matched).then(|| "previous_running".into()),
        matched_rows: matched.then_some(2),
        truncated: matched.then_some(false),
        delivery: if matched {
            "delivered"
        } else {
            "not_attempted"
        }
        .into(),
        attempts: i64::from(matched),
        last_http_status: matched.then_some(200),
    }
}

#[tokio::test]
async fn alert_evaluations_columns_and_scan_stats() {
    let fx = Fx::new();
    fx.alerts
        .put("default", evaluation("r1", 10 * MINUTE, "matched"));
    fx.alerts
        .put("other", evaluation("r1", 10 * MINUTE, "matched"));
    let body = fx
        .run("SELECT * FROM _alert_evaluations LIMIT 1", (0, END))
        .await
        .unwrap();
    let hit = body["hits"][0].as_object().unwrap();
    let keys: Vec<&str> = hit.keys().map(String::as_str).collect();
    let mut want: Vec<&str> = alert_evaluations::COLUMNS
        .iter()
        .map(|(n, _)| *n)
        .filter(|n| *n != "reason")
        .collect();
    want.sort_unstable();
    let mut keys = keys;
    keys.sort_unstable();
    assert_eq!(keys, want);
    assert_eq!(body["hits"].as_array().unwrap().len(), 1);
    for k in ["scan_records", "scan_files", "scan_bytes"] {
        assert_eq!(body[k], 0, "{k}");
    }
    assert_eq!(
        fx.run("SELECT severity FROM _alert_evaluations", (0, END))
            .await
            .err(),
        Some(Reason::UnknownColumn)
    );
    // 读取所用的 org 与范围
    let reads = fx.alerts.reads();
    assert!(
        reads
            .iter()
            .all(|r| r.0 == "default" && (r.2, r.3) == (0, END))
    );
}

#[tokio::test]
async fn alert_evaluations_default_order_and_ties() {
    let fx = Fx::new();
    for (rule, t, outcome) in [
        ("b1", MINUTE, "matched"),
        ("a1", 2 * MINUTE, "skipped"),
        ("a1", MINUTE, "skipped"),
        ("b1", 2 * MINUTE, "matched"),
    ] {
        fx.alerts.put("default", evaluation(rule, t, outcome));
    }
    let order = |hits: Vec<Value>| -> Vec<(String, i64)> {
        hits.iter()
            .map(|h| {
                (
                    h["rule"].as_str().unwrap().to_owned(),
                    h["scheduled_at"].as_i64().unwrap(),
                )
            })
            .collect()
    };
    let m = |r: &str, n: i64| (r.to_owned(), n * MINUTE);
    assert_eq!(
        order(
            fx.hits("SELECT rule, scheduled_at FROM _alert_evaluations")
                .await
        ),
        [m("a1", 2), m("b1", 2), m("a1", 1), m("b1", 1)]
    );
    assert_eq!(
        order(
            fx.hits("SELECT rule, scheduled_at FROM _alert_evaluations ORDER BY outcome")
                .await
        ),
        [m("b1", 2), m("b1", 1), m("a1", 2), m("a1", 1)]
    );
}

#[tokio::test]
async fn alert_evaluations_named_rule_and_errors() {
    let fx = Fx::new();
    fx.alerts
        .put("default", evaluation("a1", MINUTE, "matched"));
    assert_eq!(
        fx.hits("SELECT * FROM _alert_evaluations WHERE (\"rule\" = 'a1') AND outcome = 'matched'")
            .await
            .len(),
        1
    );
    assert_eq!(
        fx.hits("SELECT * FROM _alert_evaluations WHERE 'no_such' = rule")
            .await,
        Vec::<Value>::new()
    );
    let named: Vec<Option<Vec<String>>> = fx
        .alerts
        .reads()
        .into_iter()
        .map(|r| r.1.map(|s| s.into_iter().collect()))
        .collect();
    assert_eq!(
        named,
        [
            Some(vec!["a1".to_owned()]),
            Some(vec!["no_such".to_owned()])
        ]
    );

    fx.alerts.fail(HistoryError::Unavailable("down".into()));
    assert_eq!(
        fx.run("SELECT nosuch FROM _alert_evaluations", (0, END))
            .await
            .err(),
        Some(Reason::UnknownColumn)
    );
    assert_eq!(
        fx.run("SELECT * FROM _alert_evaluations", (0, END))
            .await
            .err(),
        Some(Reason::Unavailable)
    );
    fx.alerts.fail(HistoryError::Unparseable("bad".into()));
    assert_eq!(
        fx.run("SELECT * FROM _alert_evaluations", (0, END))
            .await
            .err(),
        Some(Reason::Internal)
    );
}

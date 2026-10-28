use amberlog_common::record::FieldValue;
use amberlog_common::schema::TIMESTAMP_COLUMN;

use super::*;
use crate::chain::Digest;
use crate::schema::{SchemaColumn, StreamColumns};
use crate::testing::{FakeCommitter, FakeMemtable, FakeWal};

fn app() -> Dataset {
    Dataset::new("default", "app")
}

fn rec(fields: &[(&str, FieldValue)]) -> Record {
    Record {
        timestamp_us: 1,
        fields: fields
            .iter()
            .map(|(n, v)| ((*n).to_owned(), v.clone()))
            .collect(),
    }
}

fn seq(n: i64) -> Record {
    rec(&[("seq", FieldValue::Int64(n))])
}

fn records(rs: Vec<Record>) -> WalEntry {
    WalEntry::Records {
        dataset: app(),
        records: rs,
    }
}

fn intent(p: u64, b: u64) -> WalEntry {
    WalEntry::CommitIntent(CommitIntent {
        dataset: app(),
        snapshot_id: SnapshotId(p),
        through: WalPosition(b),
        through_partial: None,
        digest: Digest([p as u8; 32]),
        files: Vec::new(),
    })
}

fn baseline(p: Option<u64>) -> WalEntry {
    WalEntry::Baseline {
        dataset: app(),
        snapshot_id: p.map(SnapshotId),
    }
}

fn chain(current: Option<u64>) -> Vec<Baseline> {
    vec![Baseline {
        dataset: app(),
        snapshot_id: current.map(SnapshotId),
        columns: None,
    }]
}

fn numbered(entries: Vec<WalEntry>) -> Vec<(WalPosition, WalEntry)> {
    entries
        .into_iter()
        .enumerate()
        .map(|(i, e)| (WalPosition(i as u64 + 1), e))
        .collect()
}

struct World {
    wal: FakeWal,
    committer: FakeCommitter,
    memtable: FakeMemtable,
    gates: DatasetGates,
    registry: DatasetRegistry,
    max_columns: usize,
}

impl World {
    fn new() -> World {
        World {
            wal: FakeWal::new(),
            committer: FakeCommitter::new(),
            memtable: FakeMemtable::new(),
            gates: DatasetGates::new(),
            registry: DatasetRegistry::new(),
            max_columns: 1000,
        }
    }

    async fn run(&self, entries: Vec<(WalPosition, WalEntry)>, chain: &[Baseline]) -> Recovered {
        recover(
            entries,
            chain,
            Parts {
                wal: &self.wal,
                committer: &self.committer,
                memtable: &self.memtable,
                writer: &self.memtable,
                gates: &self.gates,
                registry: &self.registry,
                max_columns: self.max_columns,
            },
        )
        .await
    }

    /// 写回可见集的全部 `seq`，按写入顺序。
    fn seqs(&self) -> Vec<i64> {
        self.memtable
            .written()
            .iter()
            .flat_map(|w| w.records.iter())
            .filter_map(|r| {
                r.fields.iter().find_map(|(n, v)| match (n.as_str(), v) {
                    ("seq", FieldValue::Int64(s)) => Some(*s),
                    _ => None,
                })
            })
            .collect()
    }
}

#[tokio::test]
async fn records_without_intents_are_written_back_in_wal_order() {
    let w = World::new();
    let out = w
        .run(
            numbered(vec![
                baseline(None),
                records(vec![seq(0), seq(1)]),
                records(vec![seq(2)]),
            ]),
            &[],
        )
        .await;
    assert!(out.stale.is_empty());
    assert_eq!(w.seqs(), [0, 1, 2]);
    let positions: Vec<u64> = w.memtable.written().iter().map(|x| x.position.0).collect();
    assert_eq!(positions, [2, 3]);
    assert!(w.wal.entries().is_empty(), "不过时就不追加");
}

#[tokio::test]
async fn records_up_to_a_committed_intent_are_not_written_back() {
    let w = World::new();
    // 位置：1 基线，2 seq0，3 意图(2, b=2)，4 seq1，5 意图(3, b=4)，6 seq2
    let entries = numbered(vec![
        baseline(Some(1)),
        records(vec![seq(0)]),
        intent(2, 2),
        records(vec![seq(1)]),
        intent(3, 4),
        records(vec![seq(2)]),
    ]);
    w.committer
        .set_intent_outcome(&app(), SnapshotId(3), IntentOutcome::Committed);
    let out = w.run(entries, &chain(Some(3))).await;
    assert!(out.stale.is_empty(), "链上 3 号就是本目录写的");
    assert_eq!(w.seqs(), [2]);
    assert_eq!(w.committer.resolved().len(), 1, "只判明最后一条意图");
    assert_eq!(
        w.wal.releases(),
        [Release::Records {
            dataset: app(),
            through: WalPosition(4)
        }]
    );
}

#[tokio::test]
async fn an_absent_last_intent_leaves_its_records_to_be_committed_again() {
    let w = World::new();
    let entries = numbered(vec![
        records(vec![seq(0)]),
        intent(2, 1),
        records(vec![seq(1)]),
        intent(3, 3),
        intent(3, 3), // 同号重记：前一条未成
    ]);
    // 默认判明结果为 Absent
    let out = w.run(entries, &chain(Some(2))).await;
    assert!(out.stale.is_empty());
    assert_eq!(w.seqs(), [1]);
}

/// 链上 3 号由别的 wal.dir 写出，schema 为 `_timestamp`、`seq`（Int64）与 `host`（Utf8）。
fn chain_schema_with_host_utf8() -> StreamColumns {
    StreamColumns::from_columns(vec![
        SchemaColumn {
            name: TIMESTAMP_COLUMN.into(),
            column_type: ColumnType::Int64,
            field_id: 1,
        },
        SchemaColumn {
            name: "seq".into(),
            column_type: ColumnType::Int64,
            field_id: 2,
        },
        SchemaColumn {
            name: "host".into(),
            column_type: ColumnType::Utf8,
            field_id: 3,
        },
    ])
}

#[tokio::test]
async fn stale_wal_dir_recovers_records_into_shadow_columns() {
    let w = World::new();
    w.memtable
        .add_dataset(&app(), chain_schema_with_host_utf8());
    let entries = numbered(vec![
        baseline(Some(2)),
        records(vec![
            rec(&[
                ("seq", FieldValue::Int64(1)),
                ("host", FieldValue::Int64(123)),
            ]),
            seq(2),
        ]),
    ]);
    let out = w.run(entries.clone(), &chain(Some(3))).await;
    assert_eq!(
        out.stale,
        [StaleReport {
            dataset: app(),
            recovered: 2,
            discarded: 0,
            persisted: true
        }]
    );
    assert_eq!(w.seqs(), [1, 2]);
    // 追加：作废标记、当前快照号的基线、收回的记录（类型不同的字段改落影子列）；可见集按新位置写入
    let shadowed = rec(&[
        ("seq", FieldValue::Int64(1)),
        ("host__int64", FieldValue::Int64(123)),
    ]);
    let appended: Vec<WalEntry> = w.wal.entries().into_iter().map(|(_, e)| e).collect();
    assert_eq!(
        appended,
        [
            WalEntry::Discard {
                dataset: app(),
                through: WalPosition(2)
            },
            baseline(Some(3)),
            records(vec![shadowed, seq(2)]),
        ]
    );
    assert_eq!(w.memtable.written()[0].position, WalPosition(3));

    // 再次重启（链未变）：不再判过时，收回的恰好一份
    let again = World::new();
    let mut all = entries;
    all.extend(
        appended
            .into_iter()
            .enumerate()
            .map(|(i, e)| (WalPosition(i as u64 + 3), e)),
    );
    let out = again.run(all, &chain(Some(3))).await;
    assert!(out.stale.is_empty());
    assert_eq!(again.seqs(), [1, 2]);
}

#[tokio::test]
async fn stale_wal_dir_discards_records_over_the_column_cap() {
    let mut w = World::new();
    w.max_columns = 3;
    w.memtable
        .add_dataset(&app(), chain_schema_with_host_utf8());
    let entries = numbered(vec![
        baseline(Some(2)),
        records(vec![
            rec(&[("seq", FieldValue::Int64(1)), ("a", FieldValue::Int64(1))]),
            seq(2),
            rec(&[
                ("seq", FieldValue::Int64(3)),
                ("host", FieldValue::Utf8("x".into())),
            ]),
        ]),
    ]);
    let out = w.run(entries, &chain(Some(3))).await;
    assert_eq!(
        (out.stale[0].recovered, out.stale[0].discarded),
        (2, 1),
        "会登记新列 a 而超限的那条被丢弃"
    );
    assert_eq!(w.seqs(), [2, 3]);
}

/// 回归：重判后追加 WAL 失败、而内存上限又放不下收回的记录时，物化读到的必须是收回的记录本身，
/// 不能按原滞留区的位置从 WAL 读回另一条原始记录（REQ-AMB-LIFE-DURABILITY-V2 重判、REQ-AMB-ING-HOT-BACKPRESSURE 溢出）。
#[tokio::test]
async fn stale_rejudge_unpersisted_records_are_not_read_back_from_wal() {
    use crate::memtable::{Memtable, MemtableGenerations};

    let entries = vec![
        baseline(Some(2)),
        records(vec![rec(&[
            ("seq", FieldValue::Int64(1)),
            ("host", FieldValue::Int64(123)),
        ])]),
    ];
    let wal = FakeWal::with_entries(entries.clone());
    wal.set_failing(true);
    let memtable = Memtable::with_backlog_limit(1);
    memtable.register(&app(), chain_schema_with_host_utf8());
    let committer = FakeCommitter::new();
    let out = recover(
        numbered(entries),
        &chain(Some(3)),
        Parts {
            wal: &wal,
            committer: &committer,
            memtable: &memtable,
            writer: &memtable,
            gates: &DatasetGates::new(),
            registry: &DatasetRegistry::new(),
            max_columns: 1000,
        },
    )
    .await;
    assert_eq!((out.stale[0].recovered, out.stale[0].persisted), (1, false));

    // 物化所见：留在内存中的批次，加上按溢出位置从 WAL 读回的记录条目
    let frozen: Vec<_> = memtable.freeze(&app(), u64::MAX).into_iter().collect();
    let spilled: Vec<WalPosition> = frozen
        .iter()
        .flat_map(|g| g.spilled.iter().map(|s| s.position))
        .collect();
    let read_back = wal.read(spilled).await.unwrap();
    assert_eq!(read_back, [], "读回的不是收回的记录");
    let batches: Vec<_> = frozen.iter().flat_map(|g| g.batches.iter()).collect();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    assert!(
        batches[0]
            .schema()
            .column_with_name("host__int64")
            .is_some(),
        "{:?}",
        batches[0].schema()
    );
}

#[tokio::test]
async fn foreign_snapshot_under_our_intent_makes_the_records_stale() {
    let w = World::new();
    let entries = numbered(vec![baseline(Some(1)), records(vec![seq(7)]), intent(2, 2)]);
    w.committer
        .set_intent_outcome(&app(), SnapshotId(2), IntentOutcome::Foreign);
    let out = w.run(entries, &chain(Some(2))).await;
    assert_eq!(out.stale.len(), 1);
    assert_eq!((out.stale[0].recovered, out.stale[0].discarded), (1, 0));
    assert_eq!(w.seqs(), [7]);
}

#[tokio::test]
async fn up_to_date_wal_dir_is_not_stale() {
    let w = World::new();
    let entries = numbered(vec![baseline(Some(3)), records(vec![seq(1)])]);
    let out = w.run(entries, &chain(Some(3))).await;
    assert!(out.stale.is_empty());
    assert_eq!(w.seqs(), [1]);
}

#[tokio::test]
async fn disabled_dataset_is_not_rejudged() {
    let w = World::new();
    w.gates.enter(&app(), DatasetState::ManifestUnreadable);
    let entries = numbered(vec![baseline(Some(1)), records(vec![seq(1)])]);
    let out = w.run(entries, &chain(Some(5))).await;
    assert!(out.stale.is_empty(), "停用时不判过时、不报 wal_stale");
    assert_eq!(w.seqs(), [1]);
    assert!(w.wal.entries().is_empty());
}

#[tokio::test]
async fn records_covered_by_a_discard_marker_stay_discarded() {
    let w = World::new();
    let entries = numbered(vec![
        records(vec![seq(1)]),
        WalEntry::Discard {
            dataset: app(),
            through: WalPosition(1),
        },
        records(vec![seq(2)]),
    ]);
    w.run(entries, &[]).await;
    assert_eq!(w.seqs(), [2]);
}

#[tokio::test]
async fn existence_entries_trigger_genesis_recommit() {
    let w = World::new();
    let entries = numbered(vec![
        WalEntry::DatasetExists { dataset: app() },
        records(vec![seq(1)]),
    ]);
    w.run(entries, &[]).await;
    assert_eq!(w.committer.ensured(), [app()]);
    assert_eq!(w.seqs(), [1]);
    assert_eq!(w.registry.persistence(&app()), Some(Persistence::LocalWal));

    // 只有存在性条目的数据集同样进入进程内目录
    let w = World::new();
    let other = Dataset::new("default", "other");
    w.run(
        numbered(vec![WalEntry::DatasetExists {
            dataset: other.clone(),
        }]),
        &[],
    )
    .await;
    assert_eq!(w.memtable.schema(&other), Some(StreamColumns::genesis()));
}

/// 分段提交切在一个条目中间（意图记该条目已提交前 k 条，REQ-AMB-ING-HOT-BACKPRESSURE-V5）：水位落在第 k 条之后，
/// 该条目其余的记录以 `write_tail` 写回，回收通知只到它之前的位置。
#[tokio::test]
async fn partial_watermark_writes_back_the_rest_of_the_entry() {
    let w = World::new();
    let partial = |p: u64, b: u64, k: u32| {
        WalEntry::CommitIntent(CommitIntent {
            dataset: app(),
            snapshot_id: SnapshotId(p),
            through: WalPosition(b),
            through_partial: Some(k),
            digest: Digest([p as u8; 32]),
            files: Vec::new(),
        })
    };
    w.committer
        .set_intent_outcome(&app(), SnapshotId(3), IntentOutcome::Committed);
    let out = w
        .run(
            numbered(vec![
                baseline(Some(1)),
                records(vec![seq(0), seq(1)]),
                records(vec![seq(2), seq(3), seq(4)]),
                intent(2, 2),
                partial(3, 3, 2),
                records(vec![seq(5)]),
            ]),
            &chain(Some(3)),
        )
        .await;
    assert!(out.stale.is_empty());
    assert_eq!(w.seqs(), [4, 5]);
    let written: Vec<(u64, u32)> = w
        .memtable
        .written()
        .iter()
        .map(|x| (x.position.0, x.skip))
        .collect();
    assert_eq!(written, [(3, 2), (6, 0)]);
    assert_eq!(
        w.wal.releases(),
        [Release::Records {
            dataset: app(),
            through: WalPosition(2)
        }]
    );
}

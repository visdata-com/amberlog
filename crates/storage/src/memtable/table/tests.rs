use amberlog_common::schema::SchemaLookup;
use arrow::array::{Array, Int64Array, StringArray};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use super::*;

fn app() -> Dataset {
    Dataset::new("default", "app")
}

fn rec(ts: i64, fields: &[(&str, FieldValue)]) -> Record {
    Record {
        timestamp_us: ts,
        fields: fields
            .iter()
            .map(|(n, v)| ((*n).to_owned(), v.clone()))
            .collect(),
    }
}

fn int(v: i64) -> FieldValue {
    FieldValue::Int64(v)
}

fn text(v: &str) -> FieldValue {
    FieldValue::Utf8(v.into())
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

#[test]
fn register_loads_the_baseline_and_keeps_an_existing_schema() {
    let m = Memtable::new();
    assert!(m.schema(&app()).is_none(), "未登记即不在进程内目录");
    let baseline = StreamColumns::from_columns(vec![
        SchemaColumn {
            name: TIMESTAMP_COLUMN.into(),
            column_type: ColumnType::Int64,
            field_id: 1,
        },
        SchemaColumn {
            name: "host".into(),
            column_type: ColumnType::Utf8,
            field_id: 4,
        },
    ]);
    m.register(&app(), baseline.clone());
    m.register(&app(), StreamColumns::genesis());
    assert_eq!(m.schema(&app()), Some(baseline));
    assert!(m.candidates(&app()).participating(None).is_empty());
}

/// 新列按记录顺序、同一记录内按字段出现的顺序各取最大字段 id + 1（ADR-0022、REQ-AMB-STO-HOT-LAYOUT-V2）。
#[test]
fn new_columns_take_field_ids_in_order_of_first_appearance() {
    let m = Memtable::new();
    m.register(&app(), StreamColumns::genesis());
    m.write(
        &app(),
        WalPosition(1),
        vec![
            rec(1, &[("b", int(1)), ("a", text("x"))]),
            rec(2, &[("c", FieldValue::Boolean(true)), ("a", text("y"))]),
        ],
    );
    let s = m.schema(&app()).unwrap();
    let ids: Vec<(&str, u32)> = s
        .columns()
        .iter()
        .map(|c| (c.name.as_str(), c.field_id))
        .collect();
    assert_eq!(ids, [("_timestamp", 1), ("b", 2), ("a", 3), ("c", 4)]);
    assert_eq!(s.column_type("c"), Some(ColumnType::Boolean));
}

#[test]
fn written_records_are_visible_immediately_as_arrow_batches_with_field_ids() {
    let m = Memtable::new();
    m.register(&app(), StreamColumns::genesis());
    m.write(&app(), WalPosition(1), vec![rec(10, &[("k", text("a"))])]);
    m.write(
        &app(),
        WalPosition(2),
        vec![rec(20, &[("n", int(5))]), rec(30, &[("k", text("b"))])],
    );
    let batches = m.candidates(&app()).participating(None);
    assert_eq!(rows(&batches), 3);
    let second = &batches[1];
    let names: Vec<&str> = second
        .schema_ref()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(names, ["_timestamp", "k", "n"]);
    let k = second
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(k.is_null(0));
    assert_eq!(k.value(1), "b");
    let n = second
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(n.value(0), 5);
    assert_eq!(
        second.schema_ref().field(2).metadata()[PARQUET_FIELD_ID_META_KEY],
        "3"
    );
}

#[test]
fn freeze_hands_over_the_active_backlog_as_a_new_generation() {
    let m = Memtable::new();
    m.register(&app(), StreamColumns::genesis());
    assert!(m.freeze(&app(), 100).is_none(), "没有积压不冻结");
    m.write(&app(), WalPosition(3), vec![rec(1, &[("k", int(1))])]);
    m.write(&app(), WalPosition(7), vec![rec(2, &[("k", int(2))])]);
    let g1 = m.freeze(&app(), 100).unwrap();
    assert_eq!((g1.first, g1.last), (WalPosition(3), WalPosition(7)));
    assert_eq!((g1.records, g1.last_partial), (2, None));
    assert_eq!(rows(&g1.batches), 2);
    assert!(m.freeze(&app(), 100).is_none());

    m.write(&app(), WalPosition(9), vec![rec(3, &[("k", int(3))])]);
    let g2 = m.freeze(&app(), 100).unwrap();
    assert!(g2.id > g1.id);
    assert_eq!((g2.first, g2.last), (WalPosition(9), WalPosition(9)));
    // 冻结代在回执之前照常可查
    assert_eq!(rows(&m.candidates(&app()).participating(None)), 3);
}

/// 冻结按确认的先后取至多 `max_records` 条的前缀，必要时切开一个条目（REQ-AMB-ING-HOT-BACKPRESSURE-V5）。
#[test]
fn freeze_takes_a_prefix_of_at_most_max_records_in_ack_order() {
    let m = Memtable::new();
    m.register(&app(), StreamColumns::genesis());
    let batch = |from: i64, n: i64| -> Vec<Record> {
        (from..from + n)
            .map(|i| rec(i, &[("seq", int(i))]))
            .collect()
    };
    m.write(&app(), WalPosition(1), batch(0, 3));
    m.write(&app(), WalPosition(2), batch(3, 4));
    m.write(&app(), WalPosition(4), batch(7, 2));

    let seqs = |g: &FrozenGeneration| -> Vec<i64> {
        g.batches
            .iter()
            .flat_map(|b| {
                let col = b.column_by_name("seq").unwrap();
                let col = col.as_any().downcast_ref::<Int64Array>().unwrap();
                col.values().to_vec()
            })
            .collect()
    };
    let g1 = m.freeze(&app(), 5).unwrap();
    assert_eq!(seqs(&g1), [0, 1, 2, 3, 4]);
    assert_eq!(
        (g1.first, g1.last, g1.last_partial, g1.records),
        (WalPosition(1), WalPosition(2), Some(2), 5)
    );
    let g2 = m.freeze(&app(), 5).unwrap();
    assert_eq!(seqs(&g2), [5, 6, 7, 8]);
    assert_eq!(
        (g2.first, g2.last, g2.last_partial),
        (WalPosition(2), WalPosition(4), None)
    );
    assert!(m.freeze(&app(), 5).is_none());
    // 切开不改变候选视图：9 条各出现一次
    assert_eq!(rows(&m.candidates(&app()).participating(None)), 9);
}

/// 没有绑定过的冻结代可以退回活动代之首，下一次冻结按同样的先后重新取前缀。
#[test]
fn thaw_returns_the_generation_to_the_front_of_the_backlog() {
    let m = Memtable::new();
    m.register(&app(), StreamColumns::genesis());
    m.write(&app(), WalPosition(1), vec![rec(1, &[("k", int(1))])]);
    let g = m.freeze(&app(), 10).unwrap();
    m.write(&app(), WalPosition(2), vec![rec(2, &[("k", int(2))])]);
    m.thaw(&app(), g.id);
    let again = m.freeze(&app(), 10).unwrap();
    assert_eq!(
        (again.first, again.last, again.records),
        (WalPosition(1), WalPosition(2), 2)
    );
    assert!(again.id > g.id);
    assert_eq!(rows(&m.candidates(&app()).participating(None)), 2);
}

/// 不切开经 `write_resident` 写入的一批：放不下时留给下一次，为前缀的第一批时整批冻结。
#[test]
fn resident_batches_are_not_split() {
    let m = Memtable::new();
    m.register(&app(), StreamColumns::genesis());
    m.write(&app(), WalPosition(1), vec![rec(1, &[("k", int(1))])]);
    m.write_resident(
        &app(),
        WalPosition(1),
        (0..3).map(|i| rec(i, &[("k", int(i))])).collect(),
    );
    let g1 = m.freeze(&app(), 2).unwrap();
    assert_eq!(g1.records, 1);
    let g2 = m.freeze(&app(), 2).unwrap();
    assert_eq!((g2.records, g2.last_partial), (3, None));
}

/// 候选取定之后才绑定或回执（ADR-0021），按判定那一刻的绑定号与 Q 决定是否从内存出，不重不漏。
#[test]
fn bind_and_retire_follow_the_query_watermark() {
    let m = Memtable::new();
    m.register(&app(), StreamColumns::genesis());
    m.write(&app(), WalPosition(1), vec![rec(1, &[("k", int(1))])]);
    let g = m.freeze(&app(), 100).unwrap();
    m.write(&app(), WalPosition(2), vec![rec(2, &[("k", int(2))])]);

    let before = m.candidates(&app());
    m.bind(&app(), &[g.id], SnapshotId(5));
    // 查询读到 Q = 4：绑定号 5 > 4，冻结代仍从内存出
    assert_eq!(rows(&before.clone().participating(Some(SnapshotId(4)))), 2);
    // 读到 Q = 5（快照已写成而回执未到）：冻结代只从文件出
    assert_eq!(rows(&before.participating(Some(SnapshotId(5)))), 1);

    let held = m.candidates(&app());
    m.retire(&app(), &[g.id]);
    assert_eq!(
        rows(&held.participating(Some(SnapshotId(4)))),
        2,
        "回执前取的候选仍持有它"
    );
    assert_eq!(
        rows(&m.candidates(&app()).participating(Some(SnapshotId(5)))),
        1
    );
}

#[test]
fn write_without_register_uses_the_genesis_schema() {
    let m = Memtable::new();
    m.write(&app(), WalPosition(1), vec![rec(1, &[("k", int(1))])]);
    assert_eq!(m.schema(&app()).unwrap().get("k").unwrap().field_id, 2);
}

/// 内存中的积压有上限：放得下整批留在内存，放不下整批溢出（只记 WAL 位置、不进候选视图，列照常登记）；
/// 冻结时溢出字节达到上限即截断；回执归还额度（REQ-AMB-ING-HOT-BACKPRESSURE）。
#[test]
fn backlog_over_the_limit_spills_and_caps_the_frozen_prefix() {
    let one = vec![rec(1, &[("m", text(&"x".repeat(100)))])];
    let size = records_len(&app(), &one);
    let m = Memtable::with_backlog_limit(size * 2);
    m.register(&app(), StreamColumns::genesis());
    m.write(&app(), WalPosition(1), one.clone());
    m.write(&app(), WalPosition(2), one.clone());
    m.write(
        &app(),
        WalPosition(3),
        vec![rec(3, &[("n", int(1))]), one[0].clone()],
    );
    assert_eq!(
        rows(&m.candidates(&app()).participating(None)),
        2,
        "溢出记录不进候选视图"
    );
    assert!(
        m.schema(&app()).unwrap().get("n").is_some(),
        "溢出记录的列照常登记"
    );
    assert_eq!(m.backlog_memory_bytes(), size * 2);

    // 第 3 批溢出的字节未达上限：一并冻结
    let frozen = m.freeze(&app(), 100).unwrap();
    let spilled = |g: &FrozenGeneration| -> Vec<WalPosition> {
        g.spilled.iter().map(|s| s.position).collect()
    };
    assert_eq!(spilled(&frozen), [WalPosition(3)]);
    assert!(!frozen.spill_capped);
    assert_eq!(
        (frozen.first, frozen.last),
        (WalPosition(1), WalPosition(3))
    );

    // 额度已满：再写的三批都溢出，冻结时溢出字节达到上限即截断
    m.write(&app(), WalPosition(4), one.clone());
    m.write(&app(), WalPosition(5), one.clone());
    m.write(&app(), WalPosition(6), one.clone());
    let f2 = m.freeze(&app(), 100).unwrap();
    assert_eq!(spilled(&f2), [WalPosition(4), WalPosition(5)]);
    assert!(f2.spill_capped);
    let f3 = m.freeze(&app(), 100).unwrap();
    assert_eq!(spilled(&f3), [WalPosition(6)]);

    // 回执归还该代留在内存中的额度：此后放得下的批次又留在内存
    m.retire(&app(), &[frozen.id]);
    assert_eq!(m.backlog_memory_bytes(), 0);
    m.write(&app(), WalPosition(7), one.clone());
    assert_eq!(rows(&m.candidates(&app()).participating(None)), 1);
}

/// 一个条目切成两段时按记录数分摊字节，两段都回执后额度全部归还。
#[test]
fn split_pieces_share_the_bytes_and_release_them_all() {
    let m = Memtable::new();
    m.register(&app(), StreamColumns::genesis());
    let records: Vec<Record> = (0..7).map(|i| rec(i, &[("m", text("abc"))])).collect();
    let size = records_len(&app(), &records);
    m.write(&app(), WalPosition(1), records);
    assert_eq!(m.backlog_memory_bytes(), size);
    let g1 = m.freeze(&app(), 3).unwrap();
    m.retire(&app(), &[g1.id]);
    let left = m.backlog_memory_bytes();
    assert!(left > 0 && left < size);
    let g2 = m.freeze(&app(), 10).unwrap();
    assert_eq!(g2.records, 4);
    m.retire(&app(), &[g2.id]);
    assert_eq!(m.backlog_memory_bytes(), 0);
}

/// 重放写回的条目余下部分：溢出时记下自第 `skip` 条起读回。
#[test]
fn write_tail_records_the_skip_for_readback() {
    let one = vec![rec(1, &[("m", text(&"x".repeat(100)))])];
    let m = Memtable::with_backlog_limit(1);
    m.register(&app(), StreamColumns::genesis());
    m.write_tail(&app(), WalPosition(5), 3, one.clone());
    let g = m.freeze(&app(), 10).unwrap();
    assert_eq!(
        g.spilled,
        [SpilledRecords {
            position: WalPosition(5),
            skip: 3,
            count: 1
        }]
    );
    assert_eq!(g.last_partial, None);
}

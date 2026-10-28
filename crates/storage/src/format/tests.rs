use serde_json::json;

use super::*;

const M: &str = "manifest/buckets/2026082210-01k6aaaaaaaaaaaaaaaaaaaaaa.json";
const F: &str = "data/2026/08/22/10/01k6bbbbbbbbbbbbbbbbbbbbbb.parquet";
const C: &str = "data/_timestamp_day=2026-08-22/01k6dddddddddddddddddddddd.parquet";

fn app() -> Dataset {
    Dataset::new("default", "app")
}

fn cold_file() -> ColdFileEntry {
    ColdFileEntry {
        path: C.into(),
        min_timestamp: 1_787_393_000_000_000,
        max_timestamp: 1_787_393_100_000_000,
        record_count: 7,
        size_bytes: 4321,
        content_digest: Digest::of(b"cold"),
    }
}

fn manifest() -> Piece {
    Piece {
        format_version: 4,
        bucket: "2026082210".into(),
        link: PieceLink::Full {
            ledger_previous: None,
        },
        files: vec![FileEntry {
            path: F.into(),
            min_timestamp: 1_787_393_700_000_000,
            max_timestamp: 1_787_393_700_000_001,
            record_count: 2,
            size_bytes: 1234,
            content_digest: Digest::of(b"file"),
            added_in: SnapshotId(2),
            column_stats: None,
        }],
        deletions: Vec::new(),
        handovers: Vec::new(),
    }
}

/// 全量片 `p` 本身即一份桶 manifest（`ledger_previous` 为 null 时）。
fn restored(p: &Piece) -> crate::chain::BucketManifest {
    crate::chain::BucketManifest {
        format_version: p.format_version,
        bucket: p.bucket.clone(),
        files: p.files.clone(),
        deletions: p.deletions.clone(),
        handovers: p.handovers.clone(),
    }
}

fn snapshot(id: u64, parent: Option<u64>) -> Snapshot {
    let m = manifest();
    let buckets = vec![BucketRef::of(
        &restored(&m),
        M.into(),
        Digest::of(&encode_piece(&m)),
    )];
    Snapshot {
        format_version: 4,
        snapshot_id: SnapshotId(id),
        parent_snapshot_id: parent.map(SnapshotId),
        parent_digest: parent.map(|_| Digest::of(b"parent")),
        created_at: 1_787_393_800_000_000,
        schema: vec![
            SchemaColumn {
                name: "_timestamp".into(),
                column_type: ColumnType::Int64,
                field_id: 1,
            },
            SchemaColumn {
                name: "host".into(),
                column_type: ColumnType::Utf8,
                field_id: 2,
            },
        ],
        summary: Summary::of(&app(), &buckets, ColdSummary::of([&cold_file()])),
        buckets,
        retired_segment: Some(SegmentRef {
            path: "manifest/retired/00000000000000000003.json".into(),
            content_digest: Digest::of(b"retired"),
        }),
        cold_deletion_segment: None,
    }
}

fn decode(v: &Value, id: u64) -> Result<SnapshotContent, Unparseable> {
    decode_snapshot(&serde_json::to_vec(v).unwrap(), &app(), SnapshotId(id))
}

#[test]
fn snapshot_roundtrip_and_field_shapes() {
    let s = snapshot(2, Some(1));
    let bytes = encode_snapshot(&s);
    assert_eq!(
        decode_snapshot(&bytes, &app(), SnapshotId(2)),
        Ok(SnapshotContent::Supported(Box::new(s.clone())))
    );
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    // 字段顺序固定（Value 的键已排序，所以对原始字节核对）
    let text = String::from_utf8(bytes.clone()).unwrap();
    assert!(
        text.starts_with(
            r#"{"format_version":4,"snapshot_id":2,"parent_snapshot_id":1,"parent_digest":"sha256:"#
        ),
        "{text}"
    );
    let pos = |k: &str| text.find(&format!("\"{k}\":")).unwrap();
    assert!(pos("parent_digest") < pos("created_at") && pos("created_at") < pos("schema"));
    assert!(pos("schema") < pos("buckets") && pos("buckets") < pos("summary"));
    assert!(pos("summary") < pos("retired_segment"));
    assert!(pos("retired_segment") < pos("cold_deletion_segment"));
    assert_eq!(v["summary"]["dataset"], "default/app");
    assert_eq!(v["summary"]["record_count"], 2);
    assert_eq!(
        v["summary"]["cold"],
        json!({"record_count": 7, "size_bytes": 4321, "time_min": 1_787_393_000_000_000_i64, "time_max": 1_787_393_100_000_000_i64})
    );
    assert_eq!(
        v["retired_segment"]["path"],
        "manifest/retired/00000000000000000003.json"
    );
    assert_eq!(v["cold_deletion_segment"], Value::Null);
    assert_eq!(v["buckets"][0]["time_min"], 1_787_393_700_000_000_i64);
    assert_eq!(
        v["schema"][1],
        json!({"name":"host","type":"Utf8","field_id":2})
    );

    // 创世快照：parent、parent_digest、两种段为 null 也写出键，summary.cold 为空
    let genesis = Snapshot {
        buckets: Vec::new(),
        summary: Summary::of(&app(), &[], ColdSummary::EMPTY),
        parent_snapshot_id: None,
        parent_digest: None,
        retired_segment: None,
        schema: vec![s.schema[0].clone()],
        ..snapshot(1, None)
    };
    let v: Value = serde_json::from_slice(&encode_snapshot(&genesis)).unwrap();
    assert_eq!(v["parent_snapshot_id"], Value::Null);
    assert_eq!(v["parent_digest"], Value::Null);
    assert_eq!(v["retired_segment"], Value::Null);
    assert_eq!(v["summary"]["time_min"], Value::Null);
    assert_eq!(
        v["summary"]["cold"],
        json!({"record_count": 0, "size_bytes": 0, "time_min": null, "time_max": null})
    );
    assert_eq!(
        v["schema"],
        json!([{"name":"_timestamp","type":"Int64","field_id":1}])
    );
    assert_eq!(encode_current(SnapshotId(7)), br#"{"snapshot_id":7}"#);
}

#[test]
fn readers_ignore_unknown_fields() {
    let mut v: Value = serde_json::from_slice(&encode_snapshot(&snapshot(3, Some(2)))).unwrap();
    v["x_future"] = json!(1);
    v["buckets"][0]["x_future"] = json!({"a": [1]});
    v["summary"]["x_future"] = json!(null);
    v["summary"]["cold"]["x_future"] = json!(2);
    v["retired_segment"]["x_future"] = json!(3);
    assert!(matches!(decode(&v, 3), Ok(SnapshotContent::Supported(_))));
    assert_eq!(
        decode_current(br#"{"snapshot_id":4,"x_future":1}"#),
        Ok(SnapshotId(4))
    );
    let mut m = manifest();
    m.handovers.push(handover());
    let mut v: Value = serde_json::from_slice(&encode_piece(&m)).unwrap();
    v["x_future"] = json!(1);
    v["files"][0]["x_future"] = json!("y");
    // 附属加速文件的摘要是可选字段：不认识也不判为无法解析
    v["handovers"][0]["files"][0]["accel_digest"] = json!("sha256:00");
    let bytes = serde_json::to_vec(&v).unwrap();
    assert_eq!(decode_piece(&bytes, 4, "2026082210"), Ok(m));
}

/// 回归：桶 manifest 里 `column_stats` 的 Float64 写法与 `hits` 相同——零值写作 `0.0`（不写 `-0.0`），
/// 正指数不带 `+`（`1e300`）（REQ-AMB-QRY-COLUMN-PRUNING）；读取方照样读旧写法。
#[test]
fn column_stats_float_spelling() {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float64Array, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("z", DataType::Float64, true),
        Field::new("big", DataType::Float64, true),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(TimestampMicrosecondArray::from(vec![1, 2]).with_timezone("UTC")),
        Arc::new(Float64Array::from(vec![-0.0, -0.0])),
        Arc::new(Float64Array::from(vec![1e300, 1.5e16])),
    ];
    let batch = RecordBatch::try_new(schema, columns).unwrap();
    let mut m = manifest();
    m.files[0].column_stats = Some(crate::stats::column_stats(&batch));
    let text = String::from_utf8(encode_piece(&m)).unwrap();
    // 键的先后随 serde_json 的 `preserve_order` 特性而变，逐个取值核对
    assert!(
        text.contains(r#""min":0.0"#) && text.contains(r#""max":0.0"#),
        "{text}"
    );
    assert!(
        text.contains(r#""min":1.5e16"#) && text.contains(r#""max":1e300"#),
        "{text}"
    );
    assert!(!text.contains("-0.0") && !text.contains("e+"), "{text}");

    // 旧写法照样读
    let old = text
        .replace(r#""min":0.0"#, r#""min":-0.0"#)
        .replace("1e300", "1e+300");
    assert!(decode_piece(old.as_bytes(), 4, "2026082210").is_ok());
}

#[test]
fn format_version_classification() {
    let base: Value = serde_json::from_slice(&encode_snapshot(&snapshot(3, Some(2)))).unwrap();
    let with = |fv: Value| {
        let mut v = base.clone();
        v["format_version"] = fv;
        v
    };
    // 0、字符串、缺失：内容无法解析
    assert!(decode(&with(json!(0)), 3).is_err());
    assert!(decode(&with(json!("2")), 3).is_err());
    let mut missing = base.clone();
    missing.as_object_mut().unwrap().remove("format_version");
    assert!(decode(&missing, 3).is_err());
    assert!(decode(&with(json!("4")), 3).is_err());
    // 1、2（早期程序写出的）、3（留空，本仓不写出）与大于 4：格式版本不兼容，其余内容不再解析
    for fv in [1, 2, 3, 5] {
        assert_eq!(
            decode(
                &json!({"format_version": fv, "snapshot_id": 3, "anything": []}),
                3
            ),
            Ok(SnapshotContent::Unsupported { format_version: fv })
        );
    }
    // 清单片的版本不等于快照：内容无法解析
    let mut m: Value = serde_json::from_slice(&encode_piece(&manifest())).unwrap();
    for fv in [json!(1), json!(2), json!(3), json!(5), json!(0), json!("4")] {
        m["format_version"] = fv.clone();
        assert!(
            decode_piece(&serde_json::to_vec(&m).unwrap(), 4, "2026082210").is_err(),
            "{fv}"
        );
    }
}

#[test]
fn content_that_breaks_the_rules_is_unparseable() {
    let base: Value = serde_json::from_slice(&encode_snapshot(&snapshot(3, Some(2)))).unwrap();
    let broken = |f: &dyn Fn(&mut Value)| {
        let mut v = base.clone();
        f(&mut v);
        decode(&v, 3)
    };
    assert!(decode_snapshot(b"not json", &app(), SnapshotId(3)).is_err());
    assert!(decode_snapshot(b"{}", &app(), SnapshotId(3)).is_err());
    assert!(decode(&base, 4).is_err(), "编号与文件名不符");
    assert!(
        broken(&|v| v["snapshot_id"] = json!(3.0)).is_err(),
        "快照号须为整数"
    );
    assert!(broken(&|v| v["created_at"] = json!("1")).is_err());
    for key in [
        "parent_snapshot_id",
        "parent_digest",
        "retired_segment",
        "cold_deletion_segment",
    ] {
        assert!(
            broken(&|v| {
                v.as_object_mut().unwrap().remove(key);
            })
            .is_err(),
            "{key} 的键不得省略"
        );
    }
    assert!(
        broken(&|v| v["parent_digest"] = json!(null)).is_err(),
        "非创世快照的 parent_digest 不得为 null"
    );
    assert!(broken(&|v| v["parent_digest"] = json!("sha256:00")).is_err());
    assert!(broken(&|v| v["summary"]["dataset"] = json!("default/web")).is_err());
    assert!(
        broken(&|v| {
            v["summary"].as_object_mut().unwrap().remove("cold");
        })
        .is_err()
    );
    assert!(broken(&|v| v["summary"]["cold"]["time_min"] = json!(null)).is_err());
    assert!(
        broken(&|v| v["retired_segment"]["path"] = json!("manifest/../x.json")).is_err(),
        "段路径须合模板"
    );
    assert!(
        broken(
            &|v| v["cold_deletion_segment"] = json!({"path": "manifest/retired/00000000000000000001.json", "content_digest": Digest::of(b"x").to_string()})
        )
        .is_err(),
        "冷层变更段的引用须指向 cold-deletions/"
    );
    assert!(
        broken(&|v| v["schema"] = json!([])).is_err(),
        "缺 _timestamp"
    );
    assert!(broken(&|v| v["schema"][1]["type"] = json!("Int32")).is_err());
    assert!(broken(&|v| v["buckets"][0]["content_digest"] = json!("sha256:00")).is_err());
    assert!(broken(&|v| v["buckets"][0]["record_count"] = json!(-1)).is_err());
    assert!(broken(&|v| v["buckets"][0]["manifest"] = json!("../../web/manifest/x.json")).is_err());
    assert!(
        broken(&|v| v["buckets"][0]["bucket"] = json!("2026082211")).is_err(),
        "路径与桶不符"
    );
    assert!(decode_current(b"not json").is_err());
    assert!(decode_current(br#"{"snapshot_id":0}"#).is_err());
    assert!(decode_current(br#"{"snapshot_id":"1"}"#).is_err());

    let m = encode_piece(&manifest());
    assert!(
        decode_piece(&m, 4, "2026082211").is_err(),
        "bucket 与引用不符"
    );
    let mut v: Value = serde_json::from_slice(&m).unwrap();
    v["files"][0]["path"] = json!("data/2026/08/22/11/01k6bbbbbbbbbbbbbbbbbbbbbb.parquet");
    assert!(decode_piece(&serde_json::to_vec(&v).unwrap(), 4, "2026082210").is_err());
    for key in ["deletions", "handovers", "ledger_previous", "kind", "files"] {
        let mut v: Value = serde_json::from_slice(&m).unwrap();
        v.as_object_mut().unwrap().remove(key);
        assert!(
            decode_piece(&serde_json::to_vec(&v).unwrap(), 4, "2026082210").is_err(),
            "{key}"
        );
    }
    let mut v: Value = serde_json::from_slice(&m).unwrap();
    v["kind"] = json!("partial");
    assert!(decode_piece(&serde_json::to_vec(&v).unwrap(), 4, "2026082210").is_err());
    // 第 2 版整份写出的桶 manifest（没有 `kind`）在第 4 版快照下无法解析
    let mut v: Value = serde_json::from_slice(&m).unwrap();
    v.as_object_mut().unwrap().remove("kind");
    v.as_object_mut().unwrap().remove("ledger_previous");
    v["format_version"] = json!(2);
    assert!(decode_piece(&serde_json::to_vec(&v).unwrap(), 4, "2026082210").is_err());
}

fn delta() -> Piece {
    Piece {
        link: PieceLink::Delta {
            previous: PieceRef {
                path: M.into(),
                content_digest: Digest::of(b"previous"),
            },
        },
        ..manifest()
    }
}

#[test]
fn delta_and_full_pieces_roundtrip() {
    let d = delta();
    let bytes = encode_piece(&d);
    let text = String::from_utf8(bytes.clone()).unwrap();
    assert!(
        text.starts_with(
            r#"{"format_version":4,"bucket":"2026082210","kind":"delta","previous":{"path":"#
        ),
        "{text}"
    );
    let pos = |k: &str| text.find(&format!("\"{k}\":")).unwrap();
    assert!(pos("previous") < pos("files_added") && pos("files_added") < pos("deletions"));
    assert!(pos("deletions") < pos("handovers_added"));
    assert_eq!(decode_piece(&bytes, 4, "2026082210"), Ok(d.clone()));

    let mut f = manifest();
    f.link = PieceLink::Full {
        ledger_previous: Some(PieceRef {
            path: M.into(),
            content_digest: Digest::of(b"ledger"),
        }),
    };
    let bytes = encode_piece(&f);
    let text = String::from_utf8(bytes.clone()).unwrap();
    assert!(
        text.starts_with(r#"{"format_version":4,"bucket":"2026082210","kind":"full","files":["#),
        "{text}"
    );
    assert!(pos_of(&text, "handovers") < pos_of(&text, "ledger_previous"));
    assert_eq!(decode_piece(&bytes, 4, "2026082210"), Ok(f));
    let v: Value = serde_json::from_slice(&encode_piece(&manifest())).unwrap();
    assert_eq!(v["ledger_previous"], Value::Null);

    // 增量片：`previous` 不得为 null、须指向同一桶的清单片路径
    let base: Value = serde_json::from_slice(&encode_piece(&d)).unwrap();
    let broken = |f: &dyn Fn(&mut Value)| {
        let mut v = base.clone();
        f(&mut v);
        decode_piece(&serde_json::to_vec(&v).unwrap(), 4, "2026082210")
    };
    assert!(broken(&|v| v["previous"] = Value::Null).is_err());
    assert!(
        broken(&|v| v["previous"]["path"] =
            json!("manifest/buckets/2026082211-01k6aaaaaaaaaaaaaaaaaaaaaa.json"))
        .is_err()
    );
    assert!(broken(&|v| v["previous"]["content_digest"] = json!("sha256:00")).is_err());
    for key in ["files_added", "handovers_added", "deletions", "previous"] {
        assert!(
            broken(&|v| {
                v.as_object_mut().unwrap().remove(key);
            })
            .is_err(),
            "{key}"
        );
    }
    // 读取方忽略未定义的字段
    assert!(broken(&|v| v["x_future"] = json!(1)).is_ok());
}

fn pos_of(text: &str, k: &str) -> usize {
    text.find(&format!("\"{k}\":")).unwrap()
}

fn handover() -> Handover {
    Handover {
        table_location: "s3://logs/amberlog/_iceberg/default/app".into(),
        metadata_version: 3,
        iceberg_snapshot_id: 8_765_432_109_876_543_210_i64,
        snapshot_id: SnapshotId(5),
        files: vec![cold_file()],
    }
}

#[test]
fn deletions_and_handovers_roundtrip() {
    let mut m = manifest();
    m.deletions.push(Deletion {
        path: "data/2026/08/22/10/01k6cccccccccccccccccccccc.parquet".into(),
        content_digest: Digest::of(b"gone"),
        reason: DeletionReason::Orphan,
        added_in: None,
        max_timestamp: None,
        cutoff: None,
        deleted_at: 5,
        removed_in: SnapshotId(3),
    });
    m.deletions.push(Deletion {
        path: "data/2026/08/22/10/01k6eeeeeeeeeeeeeeeeeeeeee.parquet".into(),
        content_digest: Digest::of(b"aged"),
        reason: DeletionReason::Ttl,
        added_in: Some(SnapshotId(2)),
        max_timestamp: Some(10),
        cutoff: Some(11),
        deleted_at: 6,
        removed_in: SnapshotId(4),
    });
    m.deletions.push(Deletion {
        path: "data/2026/08/22/10/01k6ffffffffffffffffffffff.parquet".into(),
        content_digest: Digest::of(b"lake"),
        reason: DeletionReason::Lake,
        added_in: Some(SnapshotId(2)),
        max_timestamp: Some(9),
        cutoff: None,
        deleted_at: 7,
        removed_in: SnapshotId(5),
    });
    m.handovers.push(handover());
    let bytes = encode_piece(&m);
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["deletions"][0]["reason"], "orphan");
    assert_eq!(v["deletions"][0]["added_in"], Value::Null);
    assert_eq!(v["deletions"][0]["max_timestamp"], Value::Null);
    assert_eq!(v["deletions"][0]["cutoff"], Value::Null);
    assert_eq!(v["deletions"][1]["cutoff"], 11);
    assert_eq!(v["deletions"][2]["reason"], "lake");
    assert_eq!(
        v["handovers"][0]["iceberg_snapshot_id"],
        json!(8_765_432_109_876_543_210_i64)
    );
    assert_eq!(v["handovers"][0]["snapshot_id"], 5);
    assert_eq!(v["handovers"][0]["files"][0]["path"], C);
    assert_eq!(decode_piece(&bytes, 4, "2026082210"), Ok(m.clone()));

    // 键不得省略；冷层文件路径须相对表位置
    let mut broken = v.clone();
    broken["deletions"][0]
        .as_object_mut()
        .unwrap()
        .remove("cutoff");
    assert!(decode_piece(&serde_json::to_vec(&broken).unwrap(), 4, "2026082210").is_err());
    let mut broken = v.clone();
    broken["handovers"][0]["files"][0]["path"] = json!("/data/x.parquet");
    assert!(decode_piece(&serde_json::to_vec(&broken).unwrap(), 4, "2026082210").is_err());
    let mut broken = v;
    broken["deletions"][0]["reason"] = json!("expired");
    assert!(decode_piece(&serde_json::to_vec(&broken).unwrap(), 4, "2026082210").is_err());
}

#[test]
fn retired_segment_roundtrip() {
    let s = RetiredSegment {
        format_version: 2,
        segment_id: SegmentId(4),
        previous: Some(SegmentRef {
            path: "manifest/retired/00000000000000000002.json".into(),
            content_digest: Digest::of(b"prev"),
        }),
        created_at: 99,
        buckets: vec![RetiredBucket {
            bucket: "2026082210".into(),
            manifest: M.into(),
            content_digest: Digest::of(b"m"),
        }],
    };
    let bytes = encode_retired_segment(&s);
    let text = String::from_utf8(bytes.clone()).unwrap();
    assert!(
        text.starts_with(r#"{"format_version":2,"segment_id":4,"previous":{"path":"#),
        "{text}"
    );
    assert_eq!(decode_retired_segment(&bytes, SegmentId(4)), Ok(s.clone()));
    assert!(
        decode_retired_segment(&bytes, SegmentId(5)).is_err(),
        "编号与文件名不符"
    );
    let first = RetiredSegment {
        previous: None,
        segment_id: SegmentId(1),
        ..s
    };
    let v: Value = serde_json::from_slice(&encode_retired_segment(&first)).unwrap();
    assert_eq!(v["previous"], Value::Null);
    let mut broken = v.clone();
    broken["format_version"] = json!(3);
    assert!(decode_retired_segment(&serde_json::to_vec(&broken).unwrap(), SegmentId(1)).is_err());
    let mut broken = v.clone();
    broken["buckets"][0]["manifest"] =
        json!("manifest/buckets/2026082211-01k6aaaaaaaaaaaaaaaaaaaaaa.json");
    assert!(decode_retired_segment(&serde_json::to_vec(&broken).unwrap(), SegmentId(1)).is_err());
    let mut broken = v;
    broken.as_object_mut().unwrap().remove("previous");
    assert!(decode_retired_segment(&serde_json::to_vec(&broken).unwrap(), SegmentId(1)).is_err());
}

#[test]
fn cold_change_segment_roundtrip() {
    let s = ColdChangeSegment {
        format_version: 2,
        segment_id: SegmentId(1),
        previous: None,
        created_at: 50,
        deletions: vec![ColdDeletion {
            path: C.into(),
            content_digest: Digest::of(b"cold"),
            max_timestamp: 40,
            reason: ColdDeletionReason::Ttl,
            cutoff: Some(41),
            deleted_at: 50,
            removed_in: SnapshotId(9),
            cold_removed_in: -3,
        }],
        additions: Vec::new(),
    };
    let bytes = encode_cold_change_segment(&s);
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["additions"], json!([]));
    assert_eq!(v["deletions"][0]["reason"], "ttl");
    assert_eq!(v["deletions"][0]["cold_removed_in"], -3);
    assert_eq!(decode_cold_change_segment(&bytes, SegmentId(1)), Ok(s));
    let mut broken = v.clone();
    broken["deletions"][0]["reason"] = json!("lake");
    assert!(
        decode_cold_change_segment(&serde_json::to_vec(&broken).unwrap(), SegmentId(1)).is_err()
    );
    let mut broken = v;
    broken["previous"] = json!({"path": "manifest/retired/00000000000000000001.json", "content_digest": Digest::of(b"x").to_string()});
    assert!(
        decode_cold_change_segment(&serde_json::to_vec(&broken).unwrap(), SegmentId(1)).is_err()
    );
}

//! 快照、提示、清单片、留痕归档段与冷层变更段的 JSON 编解码（快照与清单片为第 4 版，REQ-AMB-STO-HOT-LAYOUT-V8
//! 「快照文件与提示」「桶 manifest 与清单片」「留痕归档段」「冷层变更段」「字段取值与兼容通则」「对象状态的判定」，
//! REQ-AMB-STO-AUDIT-INVARIANTS-V5「`deletions` 的结构」；ADR-0057）。
//!
//! - 读取忽略未定义的字段（向前兼容；与配置装载的 `deny_unknown_fields` 相反）。
//! - 写出时字段顺序固定；可为 null 的字段为 null 时也写出键（读取时键不得缺）。
//! - 取得的内容不是合法 JSON、或不符合块对该对象规定的字段与类型，即「内容无法解析」（[`Unparseable`]）。
//!   快照带整数 `format_version` 且其值为 1、2、3 或大于 4 时不算，按格式版本不兼容情形处置
//!   （[`SnapshotContent::Unsupported`]；版本号 3 留空，本仓不写出）；`format_version` 为 0 或不是整数算内容无法解析。
//! - 清单片的 `format_version` 不等于引用它的快照的（含第 1、2 版整份写出、没有 `kind` 的桶 manifest），段的
//!   `format_version` 不等于 2，都算内容无法解析。

use std::collections::HashSet;
use std::fmt;

use amberlog_common::dataset::Dataset;
use amberlog_common::schema::{ColumnType, TIMESTAMP_COLUMN};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::chain::{
    BucketRef, ColdChangeSegment, ColdDeletion, ColdDeletionReason, ColdFileEntry, ColdSummary,
    Deletion, DeletionReason, Digest, FileEntry, Handover, Piece, PieceLink, PieceRef,
    RetiredBucket, RetiredSegment, SegmentId, SegmentRef, Snapshot, SnapshotId, Summary,
};
use crate::layout::{self, HourBucket};
use crate::schema::{SchemaColumn, TIMESTAMP_FIELD_ID};

/// 本期写出的快照与清单片的格式版本（ADR-0057 第 9 条）。
pub const FORMAT_VERSION: u64 = 4;

/// 两种留痕段的格式版本（第 4 版格式不改段）。
pub const SEGMENT_FORMAT_VERSION: u64 = 2;

/// 对象内容无法解析（REQ-AMB-STO-HOT-LAYOUT-V4 定义的术语），附原因。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unparseable(pub String);

impl fmt::Display for Unparseable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

fn bad(what: impl Into<String>) -> Unparseable {
    Unparseable(what.into())
}

/// 读取一个快照的结果。
#[derive(Debug, Clone, PartialEq)]
pub enum SnapshotContent {
    /// 本期认识的格式版本（4）。
    Supported(Box<Snapshot>),
    /// `format_version` 为 1、2、3 或大于 4 的整数：格式版本不兼容情形（其余内容不再解析）。
    Unsupported { format_version: u64 },
}

// ---------------------------------------------------------------------------
// 持久格式（字段顺序即写出顺序）
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize)]
struct SnapshotJson {
    format_version: u64,
    snapshot_id: u64,
    #[serde(deserialize_with = "Option::deserialize")]
    parent_snapshot_id: Option<u64>,
    #[serde(deserialize_with = "Option::deserialize")]
    parent_digest: Option<String>,
    created_at: i64,
    schema: Vec<ColumnJson>,
    buckets: Vec<BucketRefJson>,
    summary: SummaryJson,
    #[serde(deserialize_with = "Option::deserialize")]
    retired_segment: Option<SegmentRefJson>,
    #[serde(deserialize_with = "Option::deserialize")]
    cold_deletion_segment: Option<SegmentRefJson>,
}

#[derive(Serialize, Deserialize)]
struct ColumnJson {
    name: String,
    #[serde(rename = "type")]
    column_type: String,
    field_id: u32,
}

#[derive(Serialize, Deserialize)]
struct BucketRefJson {
    bucket: String,
    manifest: String,
    content_digest: String,
    record_count: u64,
    size_bytes: u64,
    #[serde(deserialize_with = "Option::deserialize")]
    time_min: Option<i64>,
    #[serde(deserialize_with = "Option::deserialize")]
    time_max: Option<i64>,
}

#[derive(Serialize, Deserialize)]
struct SummaryJson {
    dataset: String,
    record_count: u64,
    size_bytes: u64,
    #[serde(deserialize_with = "Option::deserialize")]
    time_min: Option<i64>,
    #[serde(deserialize_with = "Option::deserialize")]
    time_max: Option<i64>,
    cold: ColdSummaryJson,
}

#[derive(Serialize, Deserialize)]
struct ColdSummaryJson {
    record_count: u64,
    size_bytes: u64,
    #[serde(deserialize_with = "Option::deserialize")]
    time_min: Option<i64>,
    #[serde(deserialize_with = "Option::deserialize")]
    time_max: Option<i64>,
}

#[derive(Serialize, Deserialize)]
struct SegmentRefJson {
    path: String,
    content_digest: String,
}

#[derive(Serialize, Deserialize)]
struct CurrentJson {
    snapshot_id: u64,
}

#[derive(Serialize, Deserialize)]
struct PieceRefJson {
    path: String,
    content_digest: String,
}

#[derive(Serialize, Deserialize)]
struct FullPieceJson {
    format_version: u64,
    bucket: String,
    kind: String,
    files: Vec<FileJson>,
    deletions: Vec<DeletionJson>,
    handovers: Vec<HandoverJson>,
    #[serde(deserialize_with = "Option::deserialize")]
    ledger_previous: Option<PieceRefJson>,
}

#[derive(Serialize, Deserialize)]
struct DeltaPieceJson {
    format_version: u64,
    bucket: String,
    kind: String,
    previous: PieceRefJson,
    files_added: Vec<FileJson>,
    deletions: Vec<DeletionJson>,
    handovers_added: Vec<HandoverJson>,
}

const KIND_FULL: &str = "full";
const KIND_DELTA: &str = "delta";

#[derive(Serialize, Deserialize)]
struct FileJson {
    path: String,
    min_timestamp: i64,
    max_timestamp: i64,
    record_count: u64,
    size_bytes: u64,
    content_digest: String,
    added_in: u64,
    /// 只对这一字段宽松：任何 JSON 值（含 null）都原样留下，缺失为 `None`，写出时 `None` 不写键。
    #[serde(
        default,
        deserialize_with = "any_value",
        skip_serializing_if = "Option::is_none"
    )]
    column_stats: Option<Value>,
}

fn any_value<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Value>, D::Error> {
    Value::deserialize(d).map(Some)
}

#[derive(Serialize, Deserialize)]
struct DeletionJson {
    path: String,
    content_digest: String,
    reason: String,
    #[serde(deserialize_with = "Option::deserialize")]
    added_in: Option<u64>,
    #[serde(deserialize_with = "Option::deserialize")]
    max_timestamp: Option<i64>,
    #[serde(deserialize_with = "Option::deserialize")]
    cutoff: Option<i64>,
    deleted_at: i64,
    removed_in: u64,
}

#[derive(Serialize, Deserialize)]
struct HandoverJson {
    table_location: String,
    metadata_version: u64,
    iceberg_snapshot_id: i64,
    snapshot_id: u64,
    files: Vec<ColdFileJson>,
}

#[derive(Serialize, Deserialize)]
struct ColdFileJson {
    path: String,
    min_timestamp: i64,
    max_timestamp: i64,
    record_count: u64,
    size_bytes: u64,
    content_digest: String,
}

#[derive(Serialize, Deserialize)]
struct RetiredSegmentJson {
    format_version: u64,
    segment_id: u64,
    #[serde(deserialize_with = "Option::deserialize")]
    previous: Option<SegmentRefJson>,
    created_at: i64,
    buckets: Vec<RetiredBucketJson>,
}

#[derive(Serialize, Deserialize)]
struct RetiredBucketJson {
    bucket: String,
    manifest: String,
    content_digest: String,
}

#[derive(Serialize, Deserialize)]
struct ColdChangeSegmentJson {
    format_version: u64,
    segment_id: u64,
    #[serde(deserialize_with = "Option::deserialize")]
    previous: Option<SegmentRefJson>,
    created_at: i64,
    deletions: Vec<ColdDeletionJson>,
    additions: Vec<ColdFileJson>,
}

#[derive(Serialize, Deserialize)]
struct ColdDeletionJson {
    path: String,
    content_digest: String,
    max_timestamp: i64,
    reason: String,
    #[serde(deserialize_with = "Option::deserialize")]
    cutoff: Option<i64>,
    deleted_at: i64,
    removed_in: u64,
    cold_removed_in: i64,
}

// ---------------------------------------------------------------------------
// 编码
// ---------------------------------------------------------------------------

fn segment_ref_json(r: &SegmentRef) -> SegmentRefJson {
    SegmentRefJson {
        path: r.path.clone(),
        content_digest: r.content_digest.to_string(),
    }
}

fn cold_file_json(f: &ColdFileEntry) -> ColdFileJson {
    ColdFileJson {
        path: f.path.clone(),
        min_timestamp: f.min_timestamp,
        max_timestamp: f.max_timestamp,
        record_count: f.record_count,
        size_bytes: f.size_bytes,
        content_digest: f.content_digest.to_string(),
    }
}

/// 快照的完整字节。
pub fn encode_snapshot(s: &Snapshot) -> Vec<u8> {
    let json = SnapshotJson {
        format_version: s.format_version,
        snapshot_id: s.snapshot_id.0,
        parent_snapshot_id: s.parent_snapshot_id.map(|p| p.0),
        parent_digest: s.parent_digest.map(|d| d.to_string()),
        created_at: s.created_at,
        schema: s
            .schema
            .iter()
            .map(|c| ColumnJson {
                name: c.name.clone(),
                column_type: c.column_type.as_str().to_owned(),
                field_id: c.field_id,
            })
            .collect(),
        buckets: s
            .buckets
            .iter()
            .map(|b| BucketRefJson {
                bucket: b.bucket.clone(),
                manifest: b.manifest.clone(),
                content_digest: b.content_digest.to_string(),
                record_count: b.record_count,
                size_bytes: b.size_bytes,
                time_min: b.time_min,
                time_max: b.time_max,
            })
            .collect(),
        summary: SummaryJson {
            dataset: s.summary.dataset.clone(),
            record_count: s.summary.record_count,
            size_bytes: s.summary.size_bytes,
            time_min: s.summary.time_min,
            time_max: s.summary.time_max,
            cold: ColdSummaryJson {
                record_count: s.summary.cold.record_count,
                size_bytes: s.summary.cold.size_bytes,
                time_min: s.summary.cold.time_min,
                time_max: s.summary.cold.time_max,
            },
        },
        retired_segment: s.retired_segment.as_ref().map(segment_ref_json),
        cold_deletion_segment: s.cold_deletion_segment.as_ref().map(segment_ref_json),
    };
    serde_json::to_vec(&json).expect("快照可序列化")
}

/// 提示 `_current` 的完整字节：`{"snapshot_id":<整数>}`。
pub fn encode_current(id: SnapshotId) -> Vec<u8> {
    serde_json::to_vec(&CurrentJson { snapshot_id: id.0 }).expect("提示可序列化")
}

fn piece_ref_json(r: &PieceRef) -> PieceRefJson {
    PieceRefJson {
        path: r.path.clone(),
        content_digest: r.content_digest.to_string(),
    }
}

/// 清单片的完整字节。
pub fn encode_piece(p: &Piece) -> Vec<u8> {
    let files = p
        .files
        .iter()
        .map(|f| FileJson {
            path: f.path.clone(),
            min_timestamp: f.min_timestamp,
            max_timestamp: f.max_timestamp,
            record_count: f.record_count,
            size_bytes: f.size_bytes,
            content_digest: f.content_digest.to_string(),
            added_in: f.added_in.0,
            column_stats: f.column_stats.clone(),
        })
        .collect();
    let deletions = p
        .deletions
        .iter()
        .map(|d| DeletionJson {
            path: d.path.clone(),
            content_digest: d.content_digest.to_string(),
            reason: reason_str(d.reason).to_owned(),
            added_in: d.added_in.map(|a| a.0),
            max_timestamp: d.max_timestamp,
            cutoff: d.cutoff,
            deleted_at: d.deleted_at,
            removed_in: d.removed_in.0,
        })
        .collect();
    let handovers = p
        .handovers
        .iter()
        .map(|h| HandoverJson {
            table_location: h.table_location.clone(),
            metadata_version: h.metadata_version,
            iceberg_snapshot_id: h.iceberg_snapshot_id,
            snapshot_id: h.snapshot_id.0,
            files: h.files.iter().map(cold_file_json).collect(),
        })
        .collect();
    // `column_stats` 的 Float64 按 `hits` 的写法写出（REQ-AMB-QRY-COLUMN-PRUNING）
    match &p.link {
        PieceLink::Full { ledger_previous } => amberlog_common::json::to_vec(&FullPieceJson {
            format_version: p.format_version,
            bucket: p.bucket.clone(),
            kind: KIND_FULL.to_owned(),
            files,
            deletions,
            handovers,
            ledger_previous: ledger_previous.as_ref().map(piece_ref_json),
        }),
        PieceLink::Delta { previous } => amberlog_common::json::to_vec(&DeltaPieceJson {
            format_version: p.format_version,
            bucket: p.bucket.clone(),
            kind: KIND_DELTA.to_owned(),
            previous: piece_ref_json(previous),
            files_added: files,
            deletions,
            handovers_added: handovers,
        }),
    }
}

/// 留痕归档段的完整字节。
pub fn encode_retired_segment(s: &RetiredSegment) -> Vec<u8> {
    let json = RetiredSegmentJson {
        format_version: s.format_version,
        segment_id: s.segment_id.0,
        previous: s.previous.as_ref().map(segment_ref_json),
        created_at: s.created_at,
        buckets: s
            .buckets
            .iter()
            .map(|b| RetiredBucketJson {
                bucket: b.bucket.clone(),
                manifest: b.manifest.clone(),
                content_digest: b.content_digest.to_string(),
            })
            .collect(),
    };
    serde_json::to_vec(&json).expect("留痕归档段可序列化")
}

/// 冷层变更段的完整字节。
pub fn encode_cold_change_segment(s: &ColdChangeSegment) -> Vec<u8> {
    let json = ColdChangeSegmentJson {
        format_version: s.format_version,
        segment_id: s.segment_id.0,
        previous: s.previous.as_ref().map(segment_ref_json),
        created_at: s.created_at,
        deletions: s
            .deletions
            .iter()
            .map(|d| ColdDeletionJson {
                path: d.path.clone(),
                content_digest: d.content_digest.to_string(),
                max_timestamp: d.max_timestamp,
                reason: cold_reason_str(d.reason).to_owned(),
                cutoff: d.cutoff,
                deleted_at: d.deleted_at,
                removed_in: d.removed_in.0,
                cold_removed_in: d.cold_removed_in,
            })
            .collect(),
        additions: s.additions.iter().map(cold_file_json).collect(),
    };
    serde_json::to_vec(&json).expect("冷层变更段可序列化")
}

// ---------------------------------------------------------------------------
// 解码
// ---------------------------------------------------------------------------

/// 解码提示 `_current`。
pub fn decode_current(bytes: &[u8]) -> Result<SnapshotId, Unparseable> {
    let json: CurrentJson =
        serde_json::from_slice(bytes).map_err(|e| bad(format!("_current: {e}")))?;
    snapshot_id(json.snapshot_id).ok_or_else(|| bad("_current: snapshot_id must be at least 1"))
}

/// 解码数据集 `dataset` 在路径 `manifest/<expected>.json` 上的快照。
pub fn decode_snapshot(
    bytes: &[u8],
    dataset: &Dataset,
    expected: SnapshotId,
) -> Result<SnapshotContent, Unparseable> {
    let value: Value = serde_json::from_slice(bytes).map_err(|e| bad(format!("snapshot: {e}")))?;
    let object = value
        .as_object()
        .ok_or_else(|| bad("snapshot: not a JSON object"))?;
    let version = object
        .get("format_version")
        .and_then(Value::as_u64)
        .ok_or_else(|| bad("snapshot: format_version must be an integer"))?;
    if version < 1 {
        return Err(bad("snapshot: format_version must be at least 1"));
    }
    if version != FORMAT_VERSION {
        return Ok(SnapshotContent::Unsupported {
            format_version: version,
        });
    }
    let json: SnapshotJson =
        serde_json::from_value(value).map_err(|e| bad(format!("snapshot: {e}")))?;
    let id = snapshot_id(json.snapshot_id).ok_or_else(|| bad("snapshot: snapshot_id is 0"))?;
    if id != expected {
        return Err(bad(format!(
            "snapshot: snapshot_id {id} does not match its file name ({expected})"
        )));
    }
    let parent = match json.parent_snapshot_id {
        None => None,
        Some(p) => Some(snapshot_id(p).ok_or_else(|| bad("snapshot: parent_snapshot_id is 0"))?),
    };
    let parent_digest = json.parent_digest.as_deref().map(digest).transpose()?;
    if parent.is_some() != parent_digest.is_some() {
        return Err(bad(
            "snapshot: parent_digest must be null exactly when parent_snapshot_id is null",
        ));
    }
    let schema = decode_schema(json.schema)?;
    let buckets = json
        .buckets
        .into_iter()
        .map(decode_bucket_ref)
        .collect::<Result<Vec<_>, _>>()?;
    let mut seen = HashSet::new();
    if !buckets.iter().all(|b| seen.insert(b.bucket.clone())) {
        return Err(bad("snapshot: a bucket appears twice in buckets"));
    }
    if json.summary.dataset != dataset.to_string() {
        return Err(bad(format!(
            "snapshot: summary.dataset {:?} is not {dataset}",
            json.summary.dataset
        )));
    }
    let cold = &json.summary.cold;
    if cold.time_min.is_some() != cold.time_max.is_some() {
        return Err(bad(
            "snapshot: summary.cold time_min and time_max must both be null or both be set",
        ));
    }
    Ok(SnapshotContent::Supported(Box::new(Snapshot {
        format_version: version,
        snapshot_id: id,
        parent_snapshot_id: parent,
        parent_digest,
        created_at: json.created_at,
        schema,
        buckets,
        summary: Summary {
            dataset: json.summary.dataset,
            record_count: json.summary.record_count,
            size_bytes: json.summary.size_bytes,
            time_min: json.summary.time_min,
            time_max: json.summary.time_max,
            cold: ColdSummary {
                record_count: cold.record_count,
                size_bytes: cold.size_bytes,
                time_min: cold.time_min,
                time_max: cold.time_max,
            },
        },
        retired_segment: json
            .retired_segment
            .map(|r| decode_segment_ref(r, layout::parse_retired_segment_path))
            .transpose()?,
        cold_deletion_segment: json
            .cold_deletion_segment
            .map(|r| decode_segment_ref(r, layout::parse_cold_change_segment_path))
            .transpose()?,
    })))
}

/// 解码经格式版本为 `snapshot_version` 的快照读到的、桶 `bucket` 的一个清单片（头片，或经 `previous`、`ledger_previous`
/// 读到者）。摘要核对与链的还原不在这里（见 [`crate::chain::walk_pieces`]）。
pub fn decode_piece(
    bytes: &[u8],
    snapshot_version: u64,
    bucket: &str,
) -> Result<Piece, Unparseable> {
    let value: Value = serde_json::from_slice(bytes).map_err(|e| bad(format!("piece: {e}")))?;
    let version = value
        .get("format_version")
        .and_then(Value::as_u64)
        .ok_or_else(|| bad("piece: format_version must be an integer"))?;
    if version != snapshot_version {
        return Err(bad(format!(
            "piece: format_version {version} differs from its snapshot's ({snapshot_version})"
        )));
    }
    let hour = HourBucket::parse(bucket).ok_or_else(|| bad("piece: invalid bucket"))?;
    let (piece_bucket, link, files, deletions, handovers) =
        match value.get("kind").and_then(Value::as_str) {
            Some(KIND_FULL) => {
                let json: FullPieceJson =
                    serde_json::from_value(value).map_err(|e| bad(format!("piece: {e}")))?;
                let ledger_previous = json
                    .ledger_previous
                    .map(|r| decode_piece_ref(r, hour))
                    .transpose()?;
                (
                    json.bucket,
                    PieceLink::Full { ledger_previous },
                    json.files,
                    json.deletions,
                    json.handovers,
                )
            }
            Some(KIND_DELTA) => {
                let json: DeltaPieceJson =
                    serde_json::from_value(value).map_err(|e| bad(format!("piece: {e}")))?;
                let previous = decode_piece_ref(json.previous, hour)?;
                (
                    json.bucket,
                    PieceLink::Delta { previous },
                    json.files_added,
                    json.deletions,
                    json.handovers_added,
                )
            }
            _ => return Err(bad("piece: kind must be \"full\" or \"delta\"")),
        };
    if piece_bucket != bucket {
        return Err(bad(format!(
            "piece: bucket {piece_bucket:?} is not {bucket:?} as referenced"
        )));
    }
    Ok(Piece {
        format_version: version,
        bucket: piece_bucket,
        link,
        files: files
            .into_iter()
            .map(|f| decode_file(f, hour))
            .collect::<Result<Vec<_>, _>>()?,
        deletions: deletions
            .into_iter()
            .map(|d| decode_deletion(d, hour))
            .collect::<Result<Vec<_>, _>>()?,
        handovers: handovers
            .into_iter()
            .map(decode_handover)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

/// `previous` 与 `ledger_previous`：路径须是同一桶的清单片路径。
fn decode_piece_ref(r: PieceRefJson, hour: HourBucket) -> Result<PieceRef, Unparseable> {
    check_bucket_manifest_path(&r.path, hour, "piece")?;
    Ok(PieceRef {
        content_digest: digest(&r.content_digest)?,
        path: r.path,
    })
}

/// 解码路径为 `manifest/retired/<expected>.json` 的留痕归档段。摘要核对不在这里。
pub fn decode_retired_segment(
    bytes: &[u8],
    expected: SegmentId,
) -> Result<RetiredSegment, Unparseable> {
    let value: Value =
        serde_json::from_slice(bytes).map_err(|e| bad(format!("retired segment: {e}")))?;
    check_segment_version(&value, "retired segment")?;
    let json: RetiredSegmentJson =
        serde_json::from_value(value).map_err(|e| bad(format!("retired segment: {e}")))?;
    let segment_id = check_segment_id(json.segment_id, expected, "retired segment")?;
    let buckets = json
        .buckets
        .into_iter()
        .map(|b| {
            let hour = HourBucket::parse(&b.bucket)
                .ok_or_else(|| bad(format!("retired segment: invalid bucket {:?}", b.bucket)))?;
            check_bucket_manifest_path(&b.manifest, hour, "retired segment")?;
            Ok(RetiredBucket {
                content_digest: digest(&b.content_digest)?,
                bucket: b.bucket,
                manifest: b.manifest,
            })
        })
        .collect::<Result<Vec<_>, Unparseable>>()?;
    Ok(RetiredSegment {
        format_version: SEGMENT_FORMAT_VERSION,
        segment_id,
        previous: json
            .previous
            .map(|r| decode_segment_ref(r, layout::parse_retired_segment_path))
            .transpose()?,
        created_at: json.created_at,
        buckets,
    })
}

/// 解码路径为 `manifest/cold-deletions/<expected>.json` 的冷层变更段。摘要核对不在这里。
pub fn decode_cold_change_segment(
    bytes: &[u8],
    expected: SegmentId,
) -> Result<ColdChangeSegment, Unparseable> {
    let value: Value =
        serde_json::from_slice(bytes).map_err(|e| bad(format!("cold change segment: {e}")))?;
    check_segment_version(&value, "cold change segment")?;
    let json: ColdChangeSegmentJson =
        serde_json::from_value(value).map_err(|e| bad(format!("cold change segment: {e}")))?;
    let segment_id = check_segment_id(json.segment_id, expected, "cold change segment")?;
    let deletions = json
        .deletions
        .into_iter()
        .map(|d| {
            let reason = match d.reason.as_str() {
                "ttl" => ColdDeletionReason::Ttl,
                "compaction" => ColdDeletionReason::Compaction,
                other => {
                    return Err(bad(format!(
                        "cold change segment: unknown deletion reason {other:?}"
                    )));
                }
            };
            check_cold_path(&d.path)?;
            Ok(ColdDeletion {
                content_digest: digest(&d.content_digest)?,
                removed_in: snapshot_id(d.removed_in)
                    .ok_or_else(|| bad("cold change segment: removed_in is 0"))?,
                path: d.path,
                max_timestamp: d.max_timestamp,
                reason,
                cutoff: d.cutoff,
                deleted_at: d.deleted_at,
                cold_removed_in: d.cold_removed_in,
            })
        })
        .collect::<Result<Vec<_>, Unparseable>>()?;
    Ok(ColdChangeSegment {
        format_version: SEGMENT_FORMAT_VERSION,
        segment_id,
        previous: json
            .previous
            .map(|r| decode_segment_ref(r, layout::parse_cold_change_segment_path))
            .transpose()?,
        created_at: json.created_at,
        deletions,
        additions: json
            .additions
            .into_iter()
            .map(decode_cold_file)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn snapshot_id(n: u64) -> Option<SnapshotId> {
    (n >= 1).then_some(SnapshotId(n))
}

fn check_segment_version(value: &Value, what: &str) -> Result<(), Unparseable> {
    match value.get("format_version").and_then(Value::as_u64) {
        Some(SEGMENT_FORMAT_VERSION) => Ok(()),
        _ => Err(bad(format!("{what}: format_version must be 2"))),
    }
}

fn check_segment_id(n: u64, expected: SegmentId, what: &str) -> Result<SegmentId, Unparseable> {
    if n != expected.0 || n == 0 {
        return Err(bad(format!(
            "{what}: segment_id {n} does not match its file name ({expected})"
        )));
    }
    Ok(expected)
}

/// 段引用的路径须合 `parse` 所认的模板：读取方据此取对象，不合模板的路径不得被跟随。
fn decode_segment_ref(
    r: SegmentRefJson,
    parse: fn(&str) -> Option<SegmentId>,
) -> Result<SegmentRef, Unparseable> {
    if parse(&r.path).is_none() {
        return Err(bad(format!(
            "segment reference path {:?} does not follow the template",
            r.path
        )));
    }
    Ok(SegmentRef {
        content_digest: digest(&r.content_digest)?,
        path: r.path,
    })
}

fn decode_schema(columns: Vec<ColumnJson>) -> Result<Vec<SchemaColumn>, Unparseable> {
    let mut names = HashSet::new();
    let mut ids = HashSet::new();
    let mut out = Vec::with_capacity(columns.len());
    for c in columns {
        let column_type = match c.column_type.as_str() {
            "Int64" => ColumnType::Int64,
            "Float64" => ColumnType::Float64,
            "Boolean" => ColumnType::Boolean,
            "Utf8" => ColumnType::Utf8,
            other => return Err(bad(format!("snapshot: unknown column type {other:?}"))),
        };
        if c.name.is_empty() || c.field_id == 0 {
            return Err(bad(
                "snapshot: schema element has an empty name or field_id 0",
            ));
        }
        if !names.insert(c.name.clone()) || !ids.insert(c.field_id) {
            return Err(bad(format!(
                "snapshot: schema repeats the name or field_id of {:?}",
                c.name
            )));
        }
        out.push(SchemaColumn {
            name: c.name,
            column_type,
            field_id: c.field_id,
        });
    }
    let has_timestamp = out.iter().any(|c| {
        c.name == TIMESTAMP_COLUMN
            && c.column_type == ColumnType::Int64
            && c.field_id == TIMESTAMP_FIELD_ID
    });
    if !has_timestamp {
        return Err(bad("snapshot: schema lacks _timestamp (Int64, field_id 1)"));
    }
    Ok(out)
}

fn decode_bucket_ref(b: BucketRefJson) -> Result<BucketRef, Unparseable> {
    let hour = HourBucket::parse(&b.bucket)
        .ok_or_else(|| bad(format!("snapshot: invalid bucket {:?}", b.bucket)))?;
    check_bucket_manifest_path(&b.manifest, hour, "snapshot")?;
    let content_digest = digest(&b.content_digest)?;
    if b.time_min.is_some() != b.time_max.is_some() {
        return Err(bad(
            "snapshot: time_min and time_max must both be null or both be set",
        ));
    }
    Ok(BucketRef {
        bucket: b.bucket,
        manifest: b.manifest,
        content_digest,
        record_count: b.record_count,
        size_bytes: b.size_bytes,
        time_min: b.time_min,
        time_max: b.time_max,
    })
}

fn check_bucket_manifest_path(path: &str, hour: HourBucket, what: &str) -> Result<(), Unparseable> {
    let ok = path
        .strip_prefix(&format!("manifest/buckets/{hour}-"))
        .and_then(|rest| rest.strip_suffix(".json"))
        .is_some_and(layout::is_object_id);
    if ok {
        Ok(())
    } else {
        Err(bad(format!(
            "{what}: piece path {path:?} does not follow the template"
        )))
    }
}

fn decode_file(f: FileJson, hour: HourBucket) -> Result<FileEntry, Unparseable> {
    check_data_path(&f.path, hour)?;
    Ok(FileEntry {
        content_digest: digest(&f.content_digest)?,
        added_in: snapshot_id(f.added_in).ok_or_else(|| bad("piece: added_in is 0"))?,
        path: f.path,
        min_timestamp: f.min_timestamp,
        max_timestamp: f.max_timestamp,
        record_count: f.record_count,
        size_bytes: f.size_bytes,
        column_stats: f.column_stats,
    })
}

fn decode_deletion(d: DeletionJson, hour: HourBucket) -> Result<Deletion, Unparseable> {
    check_data_path(&d.path, hour)?;
    let reason = match d.reason.as_str() {
        "orphan" => DeletionReason::Orphan,
        "compaction" => DeletionReason::Compaction,
        "ttl" => DeletionReason::Ttl,
        "rollback" => DeletionReason::Rollback,
        "lake" => DeletionReason::Lake,
        other => {
            return Err(bad(format!("piece: unknown deletion reason {other:?}")));
        }
    };
    let added_in = match d.added_in {
        None => None,
        Some(a) => Some(snapshot_id(a).ok_or_else(|| bad("piece: added_in is 0"))?),
    };
    Ok(Deletion {
        content_digest: digest(&d.content_digest)?,
        removed_in: snapshot_id(d.removed_in).ok_or_else(|| bad("piece: removed_in is 0"))?,
        path: d.path,
        reason,
        added_in,
        max_timestamp: d.max_timestamp,
        cutoff: d.cutoff,
        deleted_at: d.deleted_at,
    })
}

fn decode_handover(h: HandoverJson) -> Result<Handover, Unparseable> {
    Ok(Handover {
        snapshot_id: snapshot_id(h.snapshot_id)
            .ok_or_else(|| bad("piece: handover snapshot_id is 0"))?,
        table_location: h.table_location,
        metadata_version: h.metadata_version,
        iceberg_snapshot_id: h.iceberg_snapshot_id,
        files: h
            .files
            .into_iter()
            .map(decode_cold_file)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn decode_cold_file(f: ColdFileJson) -> Result<ColdFileEntry, Unparseable> {
    check_cold_path(&f.path)?;
    Ok(ColdFileEntry {
        content_digest: digest(&f.content_digest)?,
        path: f.path,
        min_timestamp: f.min_timestamp,
        max_timestamp: f.max_timestamp,
        record_count: f.record_count,
        size_bytes: f.size_bytes,
    })
}

/// 冷层数据文件的路径相对冷层表位置、不以 `/` 开头（REQ-AMB-STO-HOT-LAYOUT-V4）；不含 `..` 段，读取方不会跟随它离开表目录。
fn check_cold_path(path: &str) -> Result<(), Unparseable> {
    if path.is_empty()
        || path.starts_with('/')
        || path.split('/').any(|s| s.is_empty() || s == "..")
    {
        return Err(bad(format!(
            "cold data file path {path:?} is not relative to the table location"
        )));
    }
    Ok(())
}

/// 数据文件路径须是该小时桶下的 `data/YYYY/MM/DD/HH/<ULID>.parquet`：
/// 读取方据此取对象，不合模板的路径（含 `..`）不得被跟随。
fn check_data_path(path: &str, hour: HourBucket) -> Result<(), Unparseable> {
    let template = layout::data_file_path(hour, "");
    let dir = template.strip_suffix(".parquet").unwrap_or(&template);
    let ok = path
        .strip_prefix(dir)
        .and_then(|rest| rest.strip_suffix(".parquet"))
        .is_some_and(layout::is_object_id);
    if ok {
        Ok(())
    } else {
        Err(bad(format!(
            "piece: data file path {path:?} does not follow the template of bucket {hour}"
        )))
    }
}

fn digest(s: &str) -> Result<Digest, Unparseable> {
    Digest::parse(s).ok_or_else(|| {
        bad(format!(
            "content_digest {s:?} is not sha256:<64 lower-case hex>"
        ))
    })
}

fn reason_str(r: DeletionReason) -> &'static str {
    match r {
        DeletionReason::Orphan => "orphan",
        DeletionReason::Compaction => "compaction",
        DeletionReason::Ttl => "ttl",
        DeletionReason::Rollback => "rollback",
        DeletionReason::Lake => "lake",
    }
}

fn cold_reason_str(r: ColdDeletionReason) -> &'static str {
    match r {
        ColdDeletionReason::Ttl => "ttl",
        ColdDeletionReason::Compaction => "compaction",
    }
}

#[cfg(test)]
mod tests;

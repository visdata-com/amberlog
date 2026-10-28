//! WAL 的持久格式：段头、帧与条目的二进制编码（格式不对外，只约定了长度 + CRC32C 校验与 8 MiB 换段）。
//!
//! 段文件 = 段头 + 若干帧。
//! - 段头 20 字节：魔数 `AMBWAL01`、本段第一个位置（u64）、前 16 字节的 CRC32C（u32）。
//! - 帧：魔数（u32）、校验和（u32，覆盖其后的长度与载荷）、载荷长度（u32）、载荷。
//!   魔数使重放在遇到损坏之后能向后搜出其后的完整帧，据此区分「尾部损坏」与「非尾部损坏」。
//! - 载荷首字节是帧种类：一次追加的全部条目为一帧（重放时要么全部出现、要么全不出现）；
//!   作废帧列出写入失败而被作废的位置区间（ADR-0021 前提③）。
//!
//! 整数一律小端；字符串为 u32 字节数 + UTF-8。

use amberlog_common::dataset::Dataset;
use amberlog_common::record::{FieldValue, Record};

use super::crc::crc32c;
use super::{Basis, SentElement, WalEntry, WalPosition};
use crate::chain::{Digest, SnapshotId};
use crate::commit::CommitIntent;

pub const SEGMENT_MAGIC: &[u8; 8] = b"AMBWAL01";
pub const SEGMENT_HEADER_LEN: usize = 20;
pub const FRAME_MAGIC: u32 = 0xA3B1_7C29;
pub const FRAME_HEADER_LEN: usize = 12;

const FRAME_BATCH: u8 = 1;
const FRAME_VOID: u8 = 2;

const ENTRY_RECORDS: u8 = 1;
const ENTRY_DATASET_EXISTS: u8 = 2;
const ENTRY_COUNTS: u8 = 3;
const ENTRY_SENT: u8 = 4;
const ENTRY_COMMIT_INTENT: u8 = 5;
const ENTRY_BASELINE: u8 = 6;
const ENTRY_COUNT_INTENT: u8 = 7;
const ENTRY_DISCARD: u8 = 8;
/// 计数增量的第二种写法（早期版本写出）：在 [`ENTRY_COUNTS`] 之后另带 `rejected` 与来源标识。
/// [`ENTRY_COUNTS`] 仍可读：200 批次的记录不是被接受就是被逐条拒绝，`rejected` 读作 `received − durable`，没有来源标识。
const ENTRY_COUNTS_SOURCED: u8 = 9;
/// 提交意图的第二种写法（带数据文件路径时写出）：在 [`ENTRY_COMMIT_INTENT`] 之后另带本次上传的数据文件
/// 路径。不带路径的意图仍按 [`ENTRY_COMMIT_INTENT`] 写出；该写法读出的意图路径为空。
const ENTRY_COMMIT_INTENT_FILES: u8 = 10;
/// 计数增量的第三种写法（现行写法）：在 [`ENTRY_COUNTS_SOURCED`] 之后另带按记录时间戳分钟分组的确认数。
/// [`ENTRY_COUNTS`] 与 [`ENTRY_COUNTS_SOURCED`] 仍可读，读出的 `durable_ts` 为 `None`。
const ENTRY_COUNTS_TIMESTAMPED: u8 = 11;
/// `_sent` 的第二种写法（现行写法）：每个元素另带 `basis`。[`ENTRY_SENT`] 仍可读，元素读作按送达。
const ENTRY_SENT_BASIS: u8 = 12;
/// 提交意图的第三种写法（分段提交切在 b 所指条目中间时写出）：[`ENTRY_COMMIT_INTENT_FILES`] 的内容（路径数可为 0）
/// 之后另带该条目已进入本次及更早提交的记录数（REQ-AMB-ING-HOT-BACKPRESSURE-V5）。种类码 11、12 留给计数条目。
const ENTRY_COMMIT_INTENT_PARTIAL: u8 = 13;

const VALUE_INT64: u8 = 1;
const VALUE_FLOAT64: u8 = 2;
const VALUE_BOOLEAN: u8 = 3;
const VALUE_UTF8: u8 = 4;

/// 一帧的内容。
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    /// 一次追加：条目依次占据 `first`、`first + 1`……
    Batch {
        first: WalPosition,
        entries: Vec<WalEntry>,
    },
    /// 这些闭区间内的位置作废：同位置的条目不在重放中出现。
    Void(Vec<(WalPosition, WalPosition)>),
}

pub fn segment_header(first: WalPosition) -> [u8; SEGMENT_HEADER_LEN] {
    let mut h = [0u8; SEGMENT_HEADER_LEN];
    h[..8].copy_from_slice(SEGMENT_MAGIC);
    h[8..16].copy_from_slice(&first.0.to_le_bytes());
    let crc = crc32c(&h[..16]);
    h[16..].copy_from_slice(&crc.to_le_bytes());
    h
}

/// 解析段头；不完整或校验不符时为 `None`。
pub fn parse_segment_header(bytes: &[u8]) -> Option<WalPosition> {
    let h = bytes.get(..SEGMENT_HEADER_LEN)?;
    if &h[..8] != SEGMENT_MAGIC || crc32c(&h[..16]).to_le_bytes() != h[16..20] {
        return None;
    }
    Some(WalPosition(u64::from_le_bytes(h[8..16].try_into().ok()?)))
}

/// 把一帧编码后追加到 `out`。
pub fn encode_frame(frame: &Frame, out: &mut Vec<u8>) {
    let mut payload = Vec::new();
    match frame {
        Frame::Batch { first, entries } => {
            payload.push(FRAME_BATCH);
            put_u64(&mut payload, first.0);
            put_u32(&mut payload, entries.len() as u32);
            for e in entries {
                encode_entry(e, &mut payload);
            }
        }
        Frame::Void(ranges) => {
            payload.push(FRAME_VOID);
            put_u32(&mut payload, ranges.len() as u32);
            for (a, b) in ranges {
                put_u64(&mut payload, a.0);
                put_u64(&mut payload, b.0);
            }
        }
    }
    let mut body = Vec::with_capacity(4 + payload.len());
    put_u32(&mut body, payload.len() as u32);
    body.extend_from_slice(&payload);
    put_u32(out, FRAME_MAGIC);
    put_u32(out, crc32c(&body));
    out.extend_from_slice(&body);
}

/// 从 `bytes` 开头解析一帧，返回帧与它占用的字节数；不完整、校验不符或载荷无法解码时为 `None`。
pub fn decode_frame(bytes: &[u8]) -> Option<(Frame, usize)> {
    if bytes.len() < FRAME_HEADER_LEN {
        return None;
    }
    if u32::from_le_bytes(bytes[0..4].try_into().ok()?) != FRAME_MAGIC {
        return None;
    }
    let crc = u32::from_le_bytes(bytes[4..8].try_into().ok()?);
    let len = u32::from_le_bytes(bytes[8..12].try_into().ok()?) as usize;
    let end = FRAME_HEADER_LEN.checked_add(len)?;
    let body = bytes.get(8..end)?;
    if crc32c(body) != crc {
        return None;
    }
    let mut r = Reader(&bytes[FRAME_HEADER_LEN..end]);
    let frame = match r.u8()? {
        FRAME_BATCH => {
            let first = WalPosition(r.u64()?);
            let n = r.u32()? as usize;
            let mut entries = Vec::with_capacity(n.min(1 << 16));
            for _ in 0..n {
                entries.push(decode_entry(&mut r)?);
            }
            Frame::Batch { first, entries }
        }
        FRAME_VOID => {
            let n = r.u32()? as usize;
            let mut ranges = Vec::with_capacity(n.min(1 << 16));
            for _ in 0..n {
                ranges.push((WalPosition(r.u64()?), WalPosition(r.u64()?)));
            }
            Frame::Void(ranges)
        }
        _ => return None,
    };
    if !r.0.is_empty() {
        return None;
    }
    Some((frame, end))
}

/// 自 `from` 起向后搜索下一个完整、校验相符的帧的起点。
pub fn find_next_frame(bytes: &[u8], from: usize) -> Option<usize> {
    let magic = FRAME_MAGIC.to_le_bytes();
    let mut i = from;
    while i + FRAME_HEADER_LEN <= bytes.len() {
        if bytes[i..i + 4] == magic && decode_frame(&bytes[i..]).is_some() {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// 一个记录条目（[`WalEntry::Records`]）编码后的字节数，与 [`encode_entry`] 的写法一一对应
/// （内存中积压的记账口径，REQ-AMB-ING-HOT-BACKPRESSURE「按这些记录在 WAL 中所占的字节计」）。
pub fn records_len(dataset: &Dataset, records: &[Record]) -> u64 {
    let str_len = |s: &str| 4 + s.len() as u64;
    let mut n = 1 + str_len(&dataset.org) + str_len(&dataset.stream) + 4;
    for rec in records {
        n += 8 + 4;
        for (name, value) in &rec.fields {
            n += str_len(name)
                + 1
                + match value {
                    FieldValue::Int64(_) | FieldValue::Float64(_) => 8,
                    FieldValue::Boolean(_) => 1,
                    FieldValue::Utf8(v) => str_len(v),
                };
        }
    }
    n
}

fn encode_entry(entry: &WalEntry, out: &mut Vec<u8>) {
    match entry {
        WalEntry::Records { dataset, records } => {
            out.push(ENTRY_RECORDS);
            put_dataset(out, dataset);
            put_u32(out, records.len() as u32);
            for rec in records {
                put_i64(out, rec.timestamp_us);
                put_u32(out, rec.fields.len() as u32);
                for (name, value) in &rec.fields {
                    put_str(out, name);
                    match value {
                        FieldValue::Int64(v) => {
                            out.push(VALUE_INT64);
                            put_i64(out, *v);
                        }
                        FieldValue::Float64(v) => {
                            out.push(VALUE_FLOAT64);
                            put_u64(out, v.to_bits());
                        }
                        FieldValue::Boolean(v) => {
                            out.push(VALUE_BOOLEAN);
                            out.push(u8::from(*v));
                        }
                        FieldValue::Utf8(v) => {
                            out.push(VALUE_UTF8);
                            put_str(out, v);
                        }
                    }
                }
            }
        }
        WalEntry::DatasetExists { dataset } => {
            out.push(ENTRY_DATASET_EXISTS);
            put_dataset(out, dataset);
        }
        WalEntry::Counts {
            dataset,
            window_start_us,
            received,
            durable,
            rejected,
            source,
            durable_ts,
        } => {
            out.push(if durable_ts.is_some() {
                ENTRY_COUNTS_TIMESTAMPED
            } else {
                ENTRY_COUNTS_SOURCED
            });
            put_dataset(out, dataset);
            put_i64(out, *window_start_us);
            put_u64(out, *received);
            put_u64(out, *durable);
            put_u64(out, *rejected);
            match source {
                None => out.push(0),
                Some(s) => {
                    out.push(1);
                    put_str(out, s);
                }
            }
            if let Some(groups) = durable_ts {
                put_u32(out, groups.len() as u32);
                for (window, n) in groups {
                    put_i64(out, *window);
                    put_u64(out, *n);
                }
            }
        }
        WalEntry::Sent { dataset, elements } => {
            out.push(ENTRY_SENT_BASIS);
            put_dataset(out, dataset);
            put_u32(out, elements.len() as u32);
            for e in elements {
                put_str(out, &e.source);
                put_i64(out, e.window_start_us);
                put_u64(out, e.sent);
                out.push(match e.basis {
                    Basis::Delivery => 0,
                    Basis::Timestamp => 1,
                });
            }
        }
        WalEntry::CommitIntent(intent) => {
            out.push(match (intent.through_partial, intent.files.is_empty()) {
                (Some(_), _) => ENTRY_COMMIT_INTENT_PARTIAL,
                (None, true) => ENTRY_COMMIT_INTENT,
                (None, false) => ENTRY_COMMIT_INTENT_FILES,
            });
            put_dataset(out, &intent.dataset);
            put_u64(out, intent.snapshot_id.0);
            put_u64(out, intent.through.0);
            out.extend_from_slice(&intent.digest.0);
            if !intent.files.is_empty() || intent.through_partial.is_some() {
                put_u32(out, intent.files.len() as u32);
                for f in &intent.files {
                    put_str(out, f);
                }
            }
            if let Some(k) = intent.through_partial {
                put_u32(out, k);
            }
        }
        WalEntry::Baseline {
            dataset,
            snapshot_id,
        } => {
            out.push(ENTRY_BASELINE);
            put_dataset(out, dataset);
            match snapshot_id {
                None => out.push(0),
                Some(id) => {
                    out.push(1);
                    put_u64(out, id.0);
                }
            }
        }
        WalEntry::CountIntent {
            dataset,
            hour_start_us,
            through,
            digest,
        } => {
            out.push(ENTRY_COUNT_INTENT);
            put_dataset(out, dataset);
            put_i64(out, *hour_start_us);
            put_u64(out, through.0);
            out.extend_from_slice(&digest.0);
        }
        WalEntry::Discard { dataset, through } => {
            out.push(ENTRY_DISCARD);
            put_dataset(out, dataset);
            put_u64(out, through.0);
        }
    }
}

fn decode_entry(r: &mut Reader<'_>) -> Option<WalEntry> {
    let tag = r.u8()?;
    Some(match tag {
        ENTRY_RECORDS => {
            let dataset = r.dataset()?;
            let n = r.u32()? as usize;
            let mut records = Vec::with_capacity(n.min(1 << 16));
            for _ in 0..n {
                let timestamp_us = r.i64()?;
                let m = r.u32()? as usize;
                let mut fields = Vec::with_capacity(m.min(1 << 12));
                for _ in 0..m {
                    let name = r.string()?;
                    let value = match r.u8()? {
                        VALUE_INT64 => FieldValue::Int64(r.i64()?),
                        VALUE_FLOAT64 => FieldValue::Float64(f64::from_bits(r.u64()?)),
                        VALUE_BOOLEAN => match r.u8()? {
                            0 => FieldValue::Boolean(false),
                            1 => FieldValue::Boolean(true),
                            _ => return None,
                        },
                        VALUE_UTF8 => FieldValue::Utf8(r.string()?),
                        _ => return None,
                    };
                    fields.push((name, value));
                }
                records.push(Record {
                    timestamp_us,
                    fields,
                });
            }
            WalEntry::Records { dataset, records }
        }
        ENTRY_DATASET_EXISTS => WalEntry::DatasetExists {
            dataset: r.dataset()?,
        },
        ENTRY_COUNTS => {
            let (dataset, window_start_us, received, durable) =
                (r.dataset()?, r.i64()?, r.u64()?, r.u64()?);
            WalEntry::Counts {
                dataset,
                window_start_us,
                received,
                durable,
                rejected: received.saturating_sub(durable),
                source: None,
                durable_ts: None,
            }
        }
        ENTRY_COUNTS_SOURCED | ENTRY_COUNTS_TIMESTAMPED => WalEntry::Counts {
            dataset: r.dataset()?,
            window_start_us: r.i64()?,
            received: r.u64()?,
            durable: r.u64()?,
            rejected: r.u64()?,
            source: match r.u8()? {
                0 => None,
                1 => Some(r.string()?),
                _ => return None,
            },
            durable_ts: if tag == ENTRY_COUNTS_TIMESTAMPED {
                let n = r.u32()? as usize;
                let mut groups = Vec::with_capacity(n.min(1 << 16));
                for _ in 0..n {
                    groups.push((r.i64()?, r.u64()?));
                }
                Some(groups)
            } else {
                None
            },
        },
        ENTRY_SENT | ENTRY_SENT_BASIS => {
            let dataset = r.dataset()?;
            let n = r.u32()? as usize;
            let mut elements = Vec::with_capacity(n.min(1 << 16));
            for _ in 0..n {
                elements.push(SentElement {
                    source: r.string()?,
                    window_start_us: r.i64()?,
                    sent: r.u64()?,
                    basis: if tag == ENTRY_SENT_BASIS {
                        match r.u8()? {
                            0 => Basis::Delivery,
                            1 => Basis::Timestamp,
                            _ => return None,
                        }
                    } else {
                        Basis::Delivery
                    },
                });
            }
            WalEntry::Sent { dataset, elements }
        }
        ENTRY_COMMIT_INTENT | ENTRY_COMMIT_INTENT_FILES | ENTRY_COMMIT_INTENT_PARTIAL => {
            let mut intent = CommitIntent {
                dataset: r.dataset()?,
                snapshot_id: SnapshotId(r.u64()?),
                through: WalPosition(r.u64()?),
                through_partial: None,
                digest: r.digest()?,
                files: Vec::new(),
            };
            if tag != ENTRY_COMMIT_INTENT {
                let n = r.u32()? as usize;
                intent.files.reserve(n.min(1 << 16));
                for _ in 0..n {
                    intent.files.push(r.string()?);
                }
            }
            if tag == ENTRY_COMMIT_INTENT_PARTIAL {
                intent.through_partial = Some(r.u32()?);
            }
            WalEntry::CommitIntent(intent)
        }
        ENTRY_BASELINE => WalEntry::Baseline {
            dataset: r.dataset()?,
            snapshot_id: match r.u8()? {
                0 => None,
                1 => Some(SnapshotId(r.u64()?)),
                _ => return None,
            },
        },
        ENTRY_COUNT_INTENT => WalEntry::CountIntent {
            dataset: r.dataset()?,
            hour_start_us: r.i64()?,
            through: WalPosition(r.u64()?),
            digest: r.digest()?,
        },
        ENTRY_DISCARD => WalEntry::Discard {
            dataset: r.dataset()?,
            through: WalPosition(r.u64()?),
        },
        _ => return None,
    })
}

fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_i64(out: &mut Vec<u8>, v: i64) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    put_u32(out, s.len() as u32);
    out.extend_from_slice(s.as_bytes());
}

fn put_dataset(out: &mut Vec<u8>, d: &Dataset) {
    put_str(out, &d.org);
    put_str(out, &d.stream);
}

struct Reader<'a>(&'a [u8]);

impl Reader<'_> {
    fn take(&mut self, n: usize) -> Option<&[u8]> {
        if self.0.len() < n {
            return None;
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Some(head)
    }

    fn u8(&mut self) -> Option<u8> {
        Some(self.take(1)?[0])
    }

    fn u32(&mut self) -> Option<u32> {
        Some(u32::from_le_bytes(self.take(4)?.try_into().ok()?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    fn i64(&mut self) -> Option<i64> {
        Some(i64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    fn string(&mut self) -> Option<String> {
        let n = self.u32()? as usize;
        String::from_utf8(self.take(n)?.to_vec()).ok()
    }

    fn dataset(&mut self) -> Option<Dataset> {
        Some(Dataset::new(self.string()?, self.string()?))
    }

    fn digest(&mut self) -> Option<Digest> {
        Some(Digest(self.take(32)?.try_into().ok()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn app() -> Dataset {
        Dataset::new("default", "app")
    }

    #[test]
    fn records_len_matches_encoding() {
        for entry in all_kinds() {
            if let WalEntry::Records { dataset, records } = &entry {
                let mut out = Vec::new();
                encode_entry(&entry, &mut out);
                assert_eq!(records_len(dataset, records), out.len() as u64);
            }
        }
    }

    fn all_kinds() -> Vec<WalEntry> {
        vec![
            WalEntry::DatasetExists { dataset: app() },
            WalEntry::Records {
                dataset: app(),
                records: vec![Record {
                    timestamp_us: -5,
                    fields: vec![
                        ("a".into(), FieldValue::Int64(-1)),
                        ("b.c".into(), FieldValue::Float64(1.5)),
                        ("d".into(), FieldValue::Boolean(true)),
                        ("e".into(), FieldValue::Utf8("中文".into())),
                    ],
                }],
            },
            WalEntry::Counts {
                dataset: app(),
                window_start_us: 60_000_000,
                received: 3,
                durable: 2,
                rejected: 1,
                source: None,
                durable_ts: None,
            },
            WalEntry::Counts {
                dataset: app(),
                window_start_us: 60_000_000,
                received: 4,
                durable: 1,
                rejected: 3,
                source: Some("fw-1".into()),
                durable_ts: None,
            },
            WalEntry::Counts {
                dataset: app(),
                window_start_us: 7_200_000_000,
                received: 4,
                durable: 3,
                rejected: 1,
                source: Some("fw-1".into()),
                durable_ts: Some(vec![(-60_000_000, 1), (60_000_000, 2)]),
            },
            WalEntry::Counts {
                dataset: app(),
                window_start_us: 7_200_000_000,
                received: 1,
                durable: 0,
                rejected: 1,
                source: None,
                durable_ts: Some(Vec::new()),
            },
            WalEntry::Sent {
                dataset: app(),
                elements: vec![
                    SentElement {
                        source: "fb-1".into(),
                        window_start_us: 0,
                        sent: 9,
                        basis: Basis::Delivery,
                    },
                    SentElement {
                        source: "fb-1".into(),
                        window_start_us: 0,
                        sent: 7,
                        basis: Basis::Timestamp,
                    },
                ],
            },
            WalEntry::CommitIntent(CommitIntent {
                dataset: app(),
                snapshot_id: SnapshotId(4),
                through: WalPosition(17),
                through_partial: None,
                digest: Digest([7; 32]),
                files: Vec::new(),
            }),
            WalEntry::CommitIntent(CommitIntent {
                dataset: app(),
                snapshot_id: SnapshotId(6),
                through: WalPosition(19),
                through_partial: Some(3),
                digest: Digest([9; 32]),
                files: Vec::new(),
            }),
            WalEntry::CommitIntent(CommitIntent {
                dataset: app(),
                snapshot_id: SnapshotId(7),
                through: WalPosition(20),
                through_partial: Some(1),
                digest: Digest([6; 32]),
                files: vec!["data/2023/11/14/22/c.parquet".into()],
            }),
            WalEntry::CommitIntent(CommitIntent {
                dataset: app(),
                snapshot_id: SnapshotId(5),
                through: WalPosition(18),
                through_partial: None,
                digest: Digest([8; 32]),
                files: vec![
                    "data/2023/11/14/22/a.parquet".into(),
                    "data/2023/11/14/23/b.parquet".into(),
                ],
            }),
            WalEntry::Baseline {
                dataset: app(),
                snapshot_id: None,
            },
            WalEntry::Baseline {
                dataset: app(),
                snapshot_id: Some(SnapshotId(3)),
            },
            WalEntry::CountIntent {
                dataset: app(),
                hour_start_us: 0,
                through: WalPosition(2),
                digest: Digest([1; 32]),
            },
            WalEntry::Discard {
                dataset: app(),
                through: WalPosition(5),
            },
        ]
    }

    #[test]
    fn frames_round_trip() {
        let frames = [
            Frame::Batch {
                first: WalPosition(9),
                entries: all_kinds(),
            },
            Frame::Void(vec![(WalPosition(1), WalPosition(3))]),
        ];
        let mut bytes = Vec::new();
        for f in &frames {
            encode_frame(f, &mut bytes);
        }
        let (f1, n1) = decode_frame(&bytes).unwrap();
        let (f2, n2) = decode_frame(&bytes[n1..]).unwrap();
        assert_eq!([f1, f2], frames);
        assert_eq!(n1 + n2, bytes.len());
    }

    #[test]
    fn damaged_frames_are_rejected_and_skipped_over() {
        let mut bytes = Vec::new();
        let batch = |p| Frame::Batch {
            first: WalPosition(p),
            entries: all_kinds(),
        };
        encode_frame(&batch(1), &mut bytes);
        let second = bytes.len();
        encode_frame(&batch(9), &mut bytes);

        // 截短：不完整
        assert!(decode_frame(&bytes[..second - 1]).is_none());
        // 首帧中部翻转一个字节：校验不符，但能向后搜出第二帧
        let mut flipped = bytes.clone();
        flipped[second / 2] ^= 0xff;
        assert!(decode_frame(&flipped).is_none());
        assert_eq!(find_next_frame(&flipped, 1), Some(second));
        assert_eq!(find_next_frame(&flipped, second + 1), None);
    }

    /// 早期版本写出的计数增量（不带 `rejected` 与来源标识）照常可读。
    #[test]
    fn legacy_counts_entry_is_readable() {
        let mut bytes = vec![ENTRY_COUNTS];
        put_dataset(&mut bytes, &app());
        put_i64(&mut bytes, 60_000_000);
        put_u64(&mut bytes, 3);
        put_u64(&mut bytes, 2);
        assert_eq!(
            decode_entry(&mut Reader(&bytes)),
            Some(WalEntry::Counts {
                dataset: app(),
                window_start_us: 60_000_000,
                received: 3,
                durable: 2,
                rejected: 1,
                source: None,
                durable_ts: None,
            })
        );
    }

    /// 早期版本写出的 `_sent` 条目（元素不带 `basis`）照常可读，元素读作按送达。
    #[test]
    fn legacy_sent_entry_is_readable() {
        let mut bytes = vec![ENTRY_SENT];
        put_dataset(&mut bytes, &app());
        put_u32(&mut bytes, 1);
        put_str(&mut bytes, "fb-1");
        put_i64(&mut bytes, 60_000_000);
        put_u64(&mut bytes, 4);
        assert_eq!(
            decode_entry(&mut Reader(&bytes)),
            Some(WalEntry::Sent {
                dataset: app(),
                elements: vec![SentElement {
                    source: "fb-1".into(),
                    window_start_us: 60_000_000,
                    sent: 4,
                    basis: Basis::Delivery,
                }],
            })
        );
    }

    #[test]
    fn segment_header_round_trip() {
        let h = segment_header(WalPosition(42));
        assert_eq!(parse_segment_header(&h), Some(WalPosition(42)));
        let mut bad = h;
        bad[9] ^= 1;
        assert_eq!(parse_segment_header(&bad), None);
        assert_eq!(parse_segment_header(&h[..19]), None);
    }
}

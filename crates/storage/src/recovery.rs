//! 重放的解释：把 WAL 交出的条目恢复成可见集的内容（REQ-AMB-LIFE-DURABILITY「重启时的 WAL 重放」「`wal.dir` 过时时的滞留记录」、
//! ADR-0021 第 4 条）。由生命周期编排在开始监听之前调用，此时快照提交已读入全部已存在数据集的基线。
//!
//! 对每个数据集：
//! 1. **水位**：提交意图按位置排列，某条意图之后若有同一数据集更大快照号的意图，它已提交；只有同号意图则它未成（按未提交处理）。
//!    最后一条意图交给 [`Committer::resolve_intent`] 判明。水位取已提交意图中最大的 b，水位及以前的记录已在链上，不写回可见集，
//!    并对 WAL 补发一次记录的回收通知。意图记了 b 所指条目只提交了前 k 条（分段提交切在条目中间，REQ-AMB-ING-HOT-BACKPRESSURE-V5）
//!    时，水位落在该条目的第 k 条之后：该条目其余的记录照常写回。作废标记（[`WalEntry::Discard`]）覆盖的记录同样不写回。
//! 2. **过时判定**：本 `wal.dir` 能证明自己写出的最大快照号 P_known 取「已提交意图的 P」与「最近一条基线快照号」的较大者；
//!    数据集有滞留记录（水位之后的记录）、不处于停用状态、且链上当前快照号大于 P_known 时，本目录对它过时：
//!    按 WAL 顺序逐条对照可见集当前 schema 重判——按 REQ-AMB-DM-SCHEMA-DRIFT 重新确定各字段的目标列（类型不同的落入影子列）、
//!    按 REQ-AMB-DM-FIELD-LIMITS 判列数上限，会使列数超限者丢弃、其余收回；随后追加作废标记、当前快照号的基线与收回的记录，
//!    使下次重启既不再判过时、也不让被丢弃的记录复活，并返回一条 `wal_stale` 报告。
//! 3. **写回**：其余滞留记录按 WAL 顺序原样写回可见集（不重新施加受理校验）。
//! 4. **补提创世快照**：带存在性条目的数据集调用 [`Committer::ensure_dataset`]，失败不使启动失败（它按提交失败规则继续重试）。
//!
//! 治理计数的条目（计数增量、`_sent`、计数意图）归 REQ-AMB-OBS-INGEST-COUNTERS，这里不解释。

use std::collections::{BTreeMap, HashMap};

use amberlog_common::dataset::Dataset;
use amberlog_common::record::Record;
use amberlog_common::schema::{ColumnType, SchemaLookup, base_name, place_record};

use crate::chain::SnapshotId;
use crate::commit::{Baseline, CommitIntent, Committer, IntentOutcome};
use crate::gate::{DatasetGates, DatasetState};
use crate::memtable::{MemtableView, MemtableWriter};
use crate::registry::{DatasetRegistry, Persistence};
use crate::schema::StreamColumns;
use crate::store::Unavailable;
use crate::wal::{Release, Wal, WalEntry, WalPosition};

/// 一个过时数据集的重判结果（稳定日志事件 `wal_stale` 的内容）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaleReport {
    pub dataset: Dataset,
    pub recovered: u64,
    pub discarded: u64,
    /// 作废标记与收回的记录是否已追加进 WAL；没有追加成功时，本次运行照样按重判结果作答，下次重启重新判定。
    pub persisted: bool,
}

/// 重放解释的结果。
#[derive(Debug, Default)]
pub struct Recovered {
    pub stale: Vec<StaleReport>,
    /// 补提创世快照时判明意图遇对象存储不可访问等、没有做完的数据集（不使启动失败）。
    pub unresolved: Vec<Dataset>,
}

#[derive(Default)]
struct PerDataset {
    intents: Vec<(WalPosition, CommitIntent)>,
    baseline: Option<Option<SnapshotId>>,
    /// 作废标记覆盖到的最大位置（没有时为 0）。
    discard_through: u64,
    records: Vec<(WalPosition, Vec<Record>)>,
    exists: bool,
}

/// 各方句柄。
pub struct Parts<'a> {
    pub wal: &'a dyn Wal,
    pub committer: &'a dyn Committer,
    pub memtable: &'a dyn MemtableView,
    pub writer: &'a dyn MemtableWriter,
    pub gates: &'a DatasetGates,
    pub registry: &'a DatasetRegistry,
    /// `ingest.max_columns`：过时 `wal.dir` 的重判按它判列数上限。
    pub max_columns: usize,
}

/// 解释重放交出的 `entries`（按位置递增）。`baselines` 是 [`Committer::load`] 的结果。
pub async fn recover(
    entries: Vec<(WalPosition, WalEntry)>,
    baselines: &[Baseline],
    parts: Parts<'_>,
) -> Recovered {
    let mut by_dataset: BTreeMap<Dataset, PerDataset> = BTreeMap::new();
    for (pos, entry) in entries {
        match entry {
            WalEntry::Records { dataset, records } => {
                by_dataset
                    .entry(dataset)
                    .or_default()
                    .records
                    .push((pos, records));
            }
            WalEntry::DatasetExists { dataset } => {
                by_dataset.entry(dataset).or_default().exists = true
            }
            WalEntry::CommitIntent(intent) => by_dataset
                .entry(intent.dataset.clone())
                .or_default()
                .intents
                .push((pos, intent)),
            WalEntry::Baseline {
                dataset,
                snapshot_id,
            } => by_dataset.entry(dataset).or_default().baseline = Some(snapshot_id),
            WalEntry::Discard { dataset, through } => {
                let d = by_dataset.entry(dataset).or_default();
                d.discard_through = d.discard_through.max(through.0);
            }
            WalEntry::Counts { .. } | WalEntry::Sent { .. } | WalEntry::CountIntent { .. } => {}
        }
    }
    let current: HashMap<&Dataset, Option<SnapshotId>> = baselines
        .iter()
        .map(|b| (&b.dataset, b.snapshot_id))
        .collect();

    let mut out = Recovered::default();
    for (dataset, d) in by_dataset {
        let chain_current = current.get(&dataset).copied().flatten();
        // (b, 该条目已提交的记录数)；整条已提交为 u32::MAX
        let mark = |i: &CommitIntent| (i.through, i.through_partial.unwrap_or(u32::MAX));
        let mut watermark = (WalPosition(0), u32::MAX);
        let mut p_known = d.baseline.flatten();
        let mut stale_from: Option<WalPosition> = None;

        for (i, (_, intent)) in d.intents.iter().enumerate() {
            let later = &d.intents[i + 1..];
            if later.is_empty() {
                match parts.committer.resolve_intent(intent.clone()).await {
                    Ok(IntentOutcome::Committed) => {
                        watermark = watermark.max(mark(intent));
                        p_known = p_known.max(Some(intent.snapshot_id));
                    }
                    Ok(IntentOutcome::Foreign) => stale_from = Some(watermark.0),
                    Ok(IntentOutcome::Absent | IntentOutcome::Disabled) => {}
                    Err(Unavailable(_)) => out.unresolved.push(dataset.clone()),
                }
            } else if later
                .iter()
                .any(|(_, l)| l.snapshot_id > intent.snapshot_id)
            {
                watermark = watermark.max(mark(intent));
                p_known = p_known.max(Some(intent.snapshot_id));
            }
        }
        let (wm, wm_records) = watermark;
        // 水位所在条目只提交了一部分时，回收通知只到它之前
        let released = if wm_records == u32::MAX {
            wm
        } else {
            WalPosition(wm.0.saturating_sub(1))
        };
        if released.0 > 0 {
            parts.wal.release(Release::Records {
                dataset: dataset.clone(),
                through: released,
            });
        }

        let discarded = WalPosition(d.discard_through);
        // (位置, 该条目中已提交而不写回的记录数, 其余的记录)
        let residual: Vec<(WalPosition, u32, Vec<Record>)> = d
            .records
            .into_iter()
            .filter_map(|(p, records)| {
                if p <= discarded || p < wm || (p == wm && wm_records == u32::MAX) {
                    return None;
                }
                if p == wm {
                    let skip = wm_records.min(records.len() as u32);
                    let rest = records[skip as usize..].to_vec();
                    return (!rest.is_empty()).then_some((p, skip, rest));
                }
                Some((p, 0, records))
            })
            .collect();
        let disabled = parts.gates.state(&dataset) != DatasetState::Normal;
        let stale = !residual.is_empty()
            && !disabled
            && (stale_from.is_some() || chain_current.is_some_and(|c| Some(c) > p_known));

        if stale {
            let residual = residual.into_iter().map(|(p, _, r)| (p, r)).collect();
            out.stale
                .push(rejudge(&dataset, residual, chain_current, &parts).await);
        } else {
            for (pos, skip, records) in residual {
                if skip == 0 {
                    parts.writer.write(&dataset, pos, records);
                } else {
                    parts.writer.write_tail(&dataset, pos, skip, records);
                }
            }
        }

        if d.exists {
            // 存在已在本地 WAL 中持久：登记进可见集的进程内目录与存在性登记表，再补提创世快照
            parts.writer.register(&dataset, StreamColumns::genesis());
            parts.registry.record(&dataset, Persistence::LocalWal);
            parts.committer.ensure_dataset(&dataset).await;
        }
    }
    out
}

/// 按可见集当前 schema 逐条重判过时数据集的滞留记录，持久化结果后写回收回者。
///
/// 记录里的字段名是受理时确定的目标列名：取其基名与取值类型，与受理路径同一套落列规则与列数上限重新确定目标列。
async fn rejudge(
    dataset: &Dataset,
    residual: Vec<(WalPosition, Vec<Record>)>,
    chain_current: Option<SnapshotId>,
    parts: &Parts<'_>,
) -> StaleReport {
    let mut schema = Overlay {
        base: parts.memtable.schema(dataset),
        added: HashMap::new(),
    };
    let through = residual.last().map_or(WalPosition(0), |(p, _)| *p);
    let mut kept = Vec::new();
    let mut discarded = 0;
    for (_, records) in residual {
        for mut record in records {
            let placement = place_record(
                record
                    .fields
                    .iter()
                    .map(|(n, v)| (base_name(n), v.column_type())),
                &schema,
                parts.max_columns,
            );
            let Some(placement) = placement else {
                discarded += 1;
                continue;
            };
            for (field, target) in record.fields.iter_mut().zip(placement.targets) {
                field.0 = target;
            }
            schema.added.extend(placement.new_columns);
            kept.push(record);
        }
    }
    let recovered = kept.len() as u64;

    let mut entries = vec![
        WalEntry::Discard {
            dataset: dataset.clone(),
            through,
        },
        WalEntry::Baseline {
            dataset: dataset.clone(),
            snapshot_id: chain_current,
        },
    ];
    if !kept.is_empty() {
        entries.push(WalEntry::Records {
            dataset: dataset.clone(),
            records: kept.clone(),
        });
    }
    let persisted = match parts.wal.append(entries).await {
        Ok(last) => {
            if !kept.is_empty() {
                parts.writer.write(dataset, last, kept);
            }
            true
        }
        Err(_) => {
            // 追加不成：本次运行照样只写回收回者，下次重启重新判定。位置取原滞留区的末位，该位置上的 WAL 条目不是
            // 收回的记录，故整批留在内存、不溢出（溢出读回会读到原始记录）
            if !kept.is_empty() {
                parts.writer.write_resident(dataset, through, kept);
            }
            false
        }
    };
    StaleReport {
        dataset: dataset.clone(),
        recovered,
        discarded,
        persisted,
    }
}

/// 重判所依据的当前 schema：可见集中的列加上本次重判已收回的记录登记的列。
struct Overlay {
    base: Option<StreamColumns>,
    added: HashMap<String, ColumnType>,
}

impl SchemaLookup for Overlay {
    fn column_type(&self, name: &str) -> Option<ColumnType> {
        self.base
            .as_ref()
            .and_then(|s| s.column_type(name))
            .or_else(|| self.added.get(name).copied())
    }

    fn column_count(&self) -> usize {
        self.base.as_ref().map_or(1, |s| s.column_count()) + self.added.len()
    }
}

#[cfg(test)]
mod tests;

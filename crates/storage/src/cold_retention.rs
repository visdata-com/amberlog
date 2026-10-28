//! 冷层保留（REQ-AMB-STO-COLD-RETENTION）：定期把旧的冷层 Iceberg 快照从冷层元数据里移除。被释放的清单列表与清单由
//! 冷层孤儿清理（[`crate::cold::store::ColdOrphans`]）删除，`v<N>.metadata.json` 一律保留；本模块不删除任何对象、不写任何
//! 热层对象。
//!
//! 桶生命周期的一项周期工作（[`BucketTask`]），周期为 `cold_retention.check_interval`，第一轮在开始受理之后 2 秒内
//! （-INTERVAL）。每次执行对每个存在且正常的数据集记下开始时刻（[`lifecycle::now_us`]），以恰好一次
//! [`ColdUpkeep::expire`] 移除全部不受保护的快照（-EXPIRE）；尚无冷层表或没有可移除的快照时不写（-NO-EMPTY）。
//!
//! 受保护与否在冷层过期提交的每次尝试中（换号重试时重新）按所基于的冷层当前版本与重新读取的热层当前快照判定
//! （[`Protect`]，-TERM-PROTECTED）：① 冷层当前快照；② 沿 `parent-snapshot-id` 最近 `cold_retention.snapshots` 个与
//! `timestamp-ms` 不早于开始时刻减 `cold_retention.age` 者；③ 处于交接窗口的 `lake` 快照（[`ColdTable::handover_window`]）；
//! ④ 冷层变更段尚未写出的 `ttl`、`compaction` 快照（[`ColdTable::untraced`]）；⑤ 子快照的 `timestamp-ms` 晚于开始时刻减
//! `storage.delete_delay` 者；⑥ 引用了已不存活、尚未到可删时刻（冷层变更段留痕的 `deleted_at` 加 `storage.delete_delay`，
//! 与 [`crate::purge`] 同一口径；尚无留痕视为未到）的冷层数据文件者。第 ②、⑤、⑥ 类的时钟都取开始时刻。
//!
//! 判定所作的读取都不改闸门：热层经 [`crate::ttl::QuietChain`]，段链经不缓存的读取，冷层读取本就不改。读取未全部成功只
//! 放弃本次执行，并经 [`ColdRetentionEvents::abandoned`] 输出一行非稳定 warn 日志（-READ-ABORT、-READ-ABORT-WARN）；重试
//! 耗尽由冷层提交按通则停用数据集（-EXHAUST）。停用的数据集不执行；停机过程开始后不开始新的执行。

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use amberlog_common::dataset::Dataset;

use crate::BoxFuture;
use crate::chain::{ChainError, ChainReader, FileEntry, SegmentReader};
use crate::cold::{
    self, ColdExpire, ColdReadError, ColdReader, ColdTable, ColdUpkeep, ColdWriteError,
    ExpireSelect,
};
use crate::gate::{DatasetGates, DatasetState};
use crate::in_use::SnapshotPin;
use crate::layout::HourBucket;
use crate::lifecycle;
use crate::maintenance::BucketTask;
use crate::registry::{DatasetRegistry, Persistence};

/// 冷层保留的配置。
#[derive(Debug, Clone, Copy)]
pub struct ColdRetentionConfig {
    /// `cold_retention.snapshots`
    pub snapshots: u64,
    /// `cold_retention.age`
    pub age: Duration,
    /// `cold_retention.check_interval`
    pub check_interval: Duration,
    /// `storage.delete_delay`
    pub delete_delay: Duration,
}

/// 冷层保留的可观察结果，由调用方接到日志。
pub trait ColdRetentionEvents: Send + Sync {
    /// 因所需读取未全部成功而放弃某数据集的本次执行：一行非稳定 warn 日志，含 org、stream 与原因。
    fn abandoned(&self, dataset: &Dataset, reason: &str);
}

/// 冷层保留用到的其他元素。
pub struct ColdRetentionDeps {
    pub registry: Arc<DatasetRegistry>,
    pub gates: Arc<DatasetGates>,
    /// 热层当前快照与判定交接窗口所需的桶 manifest：不改闸门（生产为 [`crate::ttl::QuietChain`]）。
    pub chain: Arc<dyn ChainReader>,
    /// 冷层变更段（生产为 [`crate::reader::FreshSegments`]）。
    pub segments: Arc<dyn SegmentReader>,
    pub cold_reader: Arc<dyn ColdReader>,
    pub upkeep: Arc<dyn ColdUpkeep>,
    pub events: Arc<dyn ColdRetentionEvents>,
}

/// 冷层保留的驱动。
pub struct ColdRetention {
    deps: Arc<ColdRetentionDeps>,
    config: ColdRetentionConfig,
    stopping: AtomicBool,
}

impl BucketTask for ColdRetention {
    fn period(&self) -> Duration {
        self.config.check_interval
    }

    fn first_delay(&self) -> Duration {
        Duration::from_secs(2).min(self.period())
    }

    fn run_once(&self) -> BoxFuture<'_, ()> {
        Box::pin(self.round())
    }

    fn stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
    }
}

impl ColdRetention {
    pub fn new(deps: ColdRetentionDeps, config: ColdRetentionConfig) -> ColdRetention {
        ColdRetention {
            deps: Arc::new(deps),
            config,
            stopping: AtomicBool::new(false),
        }
    }

    fn stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    async fn round(&self) {
        for dataset in self.deps.registry.datasets() {
            if self.stopping() {
                return;
            }
            if self.deps.registry.persistence(&dataset) != Some(Persistence::Stored)
                || self.deps.gates.state(&dataset) != DatasetState::Normal
            {
                continue;
            }
            self.run(&dataset).await;
        }
    }

    /// 对 `dataset` 执行一次冷层保留。
    async fn run(&self, dataset: &Dataset) {
        let protect = Arc::new(Protect {
            deps: Arc::clone(&self.deps),
            config: self.config,
            start_us: lifecycle::now_us(),
            pin: Mutex::default(),
        });
        let result = self
            .deps
            .upkeep
            .expire(
                dataset,
                ColdExpire {
                    select: protect.clone(),
                },
            )
            .await;
        // 过期提交返回之后才释放判定所读热层快照的登记
        protect.release();
        match result {
            // 重试耗尽已由冷层提交停用数据集
            Ok(_)
            | Err(
                ColdWriteError::Exhausted | ColdWriteError::Disabled(_) | ColdWriteError::Stale,
            ) => {}
            Err(
                ColdWriteError::Unavailable(m)
                | ColdWriteError::Integrity(m)
                | ColdWriteError::Rewrite(m),
            ) => {
                self.deps.events.abandoned(dataset, &m);
            }
        }
    }
}

/// 一次冷层保留执行的判定：每次尝试按所基于的冷层当前版本与重新读取的热层当前快照选出不受保护的快照。
struct Protect {
    deps: Arc<ColdRetentionDeps>,
    config: ColdRetentionConfig,
    /// 本次执行开始时刻（第 ②、⑤、⑥ 类的时钟）。
    start_us: i64,
    /// 最近一次尝试所读热层当前快照在在用快照登记里的登记（REQ-AMB-STO-SNAPSHOT-GC-IN-USE）。
    pin: Mutex<Option<SnapshotPin>>,
}

impl ExpireSelect for Protect {
    fn select<'a>(
        &'a self,
        dataset: &'a Dataset,
        table: &'a ColdTable,
    ) -> BoxFuture<'a, Result<Vec<i64>, ColdReadError>> {
        Box::pin(self.unprotected(dataset, table))
    }
}

fn chain_error(what: &str, e: ChainError) -> ColdReadError {
    match e {
        ChainError::Unavailable(m) => ColdReadError::Unavailable(format!("{what}: {m}")),
        other => ColdReadError::Integrity(format!("{what}: {other:?}")),
    }
}

fn micros(d: Duration) -> i64 {
    i64::try_from(d.as_micros()).unwrap_or(i64::MAX)
}

impl Protect {
    fn release(&self) {
        self.pin.lock().expect("登记锁未中毒").take();
    }

    async fn unprotected(
        &self,
        dataset: &Dataset,
        table: &ColdTable,
    ) -> Result<Vec<i64>, ColdReadError> {
        let deps = &self.deps;
        let chain = &deps.chain;

        // 热层当前快照（重新读取）
        let mut pin = chain.reading(dataset);
        let Some(id) = chain
            .current(dataset)
            .await
            .map_err(|e| chain_error("hot current snapshot cannot be read", e))?
        else {
            return Err(ColdReadError::Integrity(
                "the cold table exists but the hot chain has no snapshot".into(),
            ));
        };
        pin.settle(id);
        *self.pin.lock().expect("登记锁未中毒") = Some(pin);
        let snapshot = chain
            .snapshot(dataset, id)
            .await
            .map_err(|e| chain_error("hot current snapshot cannot be read", e))?;

        // ④ 冷层变更段尚未写出的 `ttl`、`compaction` 快照
        let segments = deps
            .segments
            .cold_change_segments(dataset, &snapshot)
            .await
            .map_err(|e| chain_error("cold change segment cannot be read", e))?;
        let traced = cold::traced(&segments);
        let mut protected: HashSet<i64> = table
            .untraced(&traced)
            .into_iter()
            .map(|s| s.snapshot_id)
            .collect();

        // ③ 处于交接窗口的 `lake` 快照
        let mut live: BTreeMap<HourBucket, Vec<FileEntry>> = BTreeMap::new();
        for hour in table.lake_buckets() {
            let label = hour.to_string();
            let Some(r) = snapshot.buckets.iter().find(|b| b.bucket == label) else {
                continue;
            };
            if r.record_count == 0 {
                continue;
            }
            let m = chain
                .bucket_manifest(dataset, &snapshot, r)
                .await
                .map_err(|e| chain_error("bucket manifest cannot be read", e))?;
            live.insert(hour, m.files.clone());
        }
        protected.extend(
            table
                .handover_window(&live)
                .into_iter()
                .map(|w| w.iceberg_snapshot_id),
        );

        // ①、②、⑤
        protected.extend(retained(table, &self.config, self.start_us));

        // ⑥ 只对其余各类都不保护的快照读其清单列表与清单
        let candidates: Vec<i64> = table
            .snapshots
            .iter()
            .map(|s| s.snapshot_id)
            .filter(|id| !protected.contains(id))
            .collect();
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let alive: HashSet<&str> = table.files.iter().map(|f| f.path.as_str()).collect();
        let delay = micros(self.config.delete_delay);
        let mut deletable_at: HashMap<&str, i64> = HashMap::new();
        for d in segments.iter().flat_map(|s| s.deletions.iter()) {
            let at = d.deleted_at.saturating_add(delay);
            deletable_at
                .entry(d.path.as_str())
                .and_modify(|t| *t = (*t).min(at))
                .or_insert(at);
        }
        let mut out = Vec::new();
        for id in candidates {
            let refs = deps
                .cold_reader
                .snapshot_references(dataset, table, id)
                .await?;
            let holds = refs.iter().any(|path| {
                !alive.contains(path.as_str())
                    && deletable_at
                        .get(path.as_str())
                        .is_none_or(|at| *at > self.start_us)
            });
            if !holds {
                out.push(id);
            }
        }
        Ok(out)
    }
}

/// 第 ①、②、⑤ 类受保护的快照：冷层当前快照；沿 `parent-snapshot-id` 自当前快照起最近 `snapshots` 个；`timestamp-ms`
/// 不早于 `start_us` 减 `age` 者；子快照的 `timestamp-ms` 晚于 `start_us` 减 `delete_delay` 者。
fn retained(table: &ColdTable, config: &ColdRetentionConfig, start_us: i64) -> HashSet<i64> {
    let by_id: HashMap<i64, &cold::ColdSnapshot> =
        table.snapshots.iter().map(|s| (s.snapshot_id, s)).collect();
    let mut out: HashSet<i64> = table.current_snapshot_id.into_iter().collect();
    let mut next = table.current_snapshot_id;
    let mut count = 0;
    while let Some(id) = next
        && count < config.snapshots
        && let Some(s) = by_id.get(&id)
    {
        out.insert(id);
        count += 1;
        next = s.parent_snapshot_id;
    }
    let age_floor = start_us.saturating_sub(micros(config.age));
    let child_floor = start_us.saturating_sub(micros(config.delete_delay));
    for s in &table.snapshots {
        if s.timestamp_ms.saturating_mul(1000) >= age_floor {
            out.insert(s.snapshot_id);
        }
        if let Some(parent) = s.parent_snapshot_id
            && s.timestamp_ms.saturating_mul(1000) > child_floor
        {
            out.insert(parent);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cold::{ColdOperation, ColdSnapshot};

    const MIN_US: i64 = 60_000_000;

    fn table(chain: &[(i64, Option<i64>, i64)]) -> ColdTable {
        ColdTable {
            location: "file:///x/amberlog/_iceberg/default/app".into(),
            metadata_version: chain.len() as u64,
            current_snapshot_id: chain.last().map(|c| c.0),
            fields: Vec::new(),
            snapshots: chain
                .iter()
                .map(|&(id, parent, ts_ms)| ColdSnapshot {
                    snapshot_id: id,
                    parent_snapshot_id: parent,
                    timestamp_ms: ts_ms,
                    operation: ColdOperation::Other,
                })
                .collect(),
            files: Vec::new(),
        }
    }

    fn config(snapshots: u64, age: Duration, delete_delay: Duration) -> ColdRetentionConfig {
        ColdRetentionConfig {
            snapshots,
            age,
            check_interval: Duration::from_secs(5),
            delete_delay,
        }
    }

    fn sorted(s: HashSet<i64>) -> Vec<i64> {
        let mut v: Vec<_> = s.into_iter().collect();
        v.sort_unstable();
        v
    }

    #[test]
    fn retained_counts_along_parent_chain_and_age() {
        // 1 ← 2 ← 3，各隔 1 分钟；开始时刻在 3 之后 10 分钟
        let t = table(&[(1, None, 0), (2, Some(1), 60_000), (3, Some(2), 120_000)]);
        let start = 120_000 * 1000 + 10 * MIN_US;
        let tiny = Duration::from_secs(1);
        assert_eq!(sorted(retained(&t, &config(1, tiny, tiny), start)), [3]);
        assert_eq!(sorted(retained(&t, &config(2, tiny, tiny), start)), [2, 3]);
        // 按时长：`timestamp-ms` 不早于开始时刻减 11 分钟的 2、3
        let age = Duration::from_secs(11 * 60);
        assert_eq!(sorted(retained(&t, &config(1, age, tiny), start)), [2, 3]);
    }

    #[test]
    fn retained_keeps_parent_of_recent_child_only() {
        let t = table(&[(1, None, 0), (2, Some(1), 60_000), (3, Some(2), 120_000)]);
        let start = 120_000 * 1000 + 30_000_000;
        let tiny = Duration::from_secs(1);
        // delete_delay 1 分钟：3 的 `timestamp-ms` 晚于开始时刻减 1 分钟，其父 2 受保护；2 的不晚，1 不受保护
        let delay = Duration::from_secs(60);
        assert_eq!(sorted(retained(&t, &config(1, tiny, delay), start)), [2, 3]);
        // 子快照已被过期的快照不属第 ⑤ 类
        let gone = table(&[(2, Some(1), 60_000), (3, Some(2), 120_000)]);
        assert_eq!(sorted(retained(&gone, &config(1, tiny, tiny), start)), [3]);
    }
}

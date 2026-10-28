//! 延后物理删除（REQ-AMB-STO-AUDIT-INVARIANTS-V4「物理删除的次序与延后」）：因入湖、到期删除或封口合并离开
//! 热层存活文件集的数据文件与 `rollback` 留痕的数据文件，在其留痕的 `deleted_at` 加 `storage.delete_delay` 之后、1 小时
//! 之内物理删除。
//!
//! 桶生命周期的一项周期工作（[`BucketTask`]），每分钟一轮，对每个存在且正常的数据集：
//! 1. 读本进程已知最新快照 `buckets` 所引用的桶 manifest，把其中 `reason` 为 `lake`、`ttl`、`compaction` 或 `rollback`、
//!    尚未处理过的留痕记为待删（删除时刻 = `deleted_at` + `storage.delete_delay`；留痕所在快照的 `created_at` 就是
//!    `deleted_at`，快照已创建成功）；
//! 2. 本进程对该数据集的第一轮另列一次 `data/`：已不存在的待删文件直接记为已处理；仍存在、不在存活文件集、所在桶已移入
//!    留痕归档段的文件，到最近一次归档该桶的段所引用的桶 manifest 里找它的留痕（上一个进程留下的、未删成的文件由此补删）；
//! 3. 删除已到删除时刻的待删文件；
//! 4. 冷层数据文件（REQ-AMB-STO-COLD-TABLE-DELETE-DELAY、-DELETE-DEADLINE）：读最新快照经 `cold_deletion_segment` 可达的
//!    冷层变更段，本进程尚未读过的段中每条删除留痕记为待删（删除时刻 = `deleted_at` + `storage.delete_delay`）；本进程对
//!    该数据集第一次读到冷层变更段时另列一次冷层表的 `data/`，已不存在的不再发删除请求（重启后的补删由此只针对仍存在的
//!    文件）；删除已到删除时刻的。
//!
//! 每次物理删除之前经过测试专用暂停点 `delete.before_physical`（`fault-injection` 下可放行），之后再看一次停机与数据集状态。
//! 读取不改变数据集状态：读不到、无法解析或对象存储不可访问只结束该数据集的本轮。数据集停用时不删；停机过程开始后不再
//! 开始新的一轮，进行中的由 [`crate::maintenance::BucketTasks`] 中止。

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use amberlog_common::dataset::Dataset;

use crate::BoxFuture;
use crate::chain::{BucketManifest, BucketRef, DeletionReason, SegmentReader, Snapshot};
use crate::fault;
use crate::gate::DatasetState;
use crate::layout;
use crate::maintenance::BucketTask;
use crate::reader::StoreChain;
use crate::registry::Persistence;

/// 两轮之间的间隔：远小于块规定的 1 小时时限。
const PERIOD: Duration = Duration::from_secs(60);
/// 开始受理之后第一轮的延迟（重启后自行补删已到删除时刻的文件）。
const FIRST_DELAY: Duration = Duration::from_secs(5);

/// 一个数据集的删除进度。
#[derive(Default)]
struct Progress {
    /// 本进程已列过一次 `data/`。
    swept: bool,
    /// 待删文件 → 删除时刻（UTC 微秒）。
    pending: HashMap<String, i64>,
    /// 已删除或已确认不存在的文件，按所在桶；只留仍在最新快照 `buckets` 中的桶（归档后它的留痕不再被读到）。
    handled: HashMap<String, HashSet<String>>,
    /// 已读过的冷层变更段的段号（段写出后不变）。
    cold_segments: HashSet<u64>,
    /// 本进程已列过一次冷层表的 `data/`。
    cold_swept: bool,
    /// 待删的冷层数据文件（相对冷层表目录）→ 删除时刻（UTC 微秒）。
    cold_pending: HashMap<String, i64>,
}

/// 本轮读取没有完成。
struct Skip;

/// 延后物理删除的驱动。
pub struct Purge {
    chain: Arc<StoreChain>,
    delete_delay: Duration,
    progress: Mutex<HashMap<Dataset, Progress>>,
    stopping: AtomicBool,
}

impl BucketTask for Purge {
    fn period(&self) -> Duration {
        PERIOD
    }

    fn first_delay(&self) -> Duration {
        FIRST_DELAY
    }

    fn run_once(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            for dataset in self.chain.registry.datasets() {
                if self.stopping() {
                    return;
                }
                if self.chain.registry.persistence(&dataset) != Some(Persistence::Stored)
                    || !self.normal(&dataset)
                {
                    continue;
                }
                let _ = self.purge(&dataset).await;
            }
        })
    }

    fn stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
    }
}

impl Purge {
    pub fn new(chain: Arc<StoreChain>, delete_delay: Duration) -> Purge {
        Purge {
            chain,
            delete_delay,
            progress: Mutex::default(),
            stopping: AtomicBool::new(false),
        }
    }

    fn stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    fn normal(&self, dataset: &Dataset) -> bool {
        self.chain.gates.state(dataset) == DatasetState::Normal
    }

    fn progress<R>(&self, dataset: &Dataset, f: impl FnOnce(&mut Progress) -> R) -> R {
        let mut all = self.progress.lock().expect("删除进度锁未中毒");
        f(all.entry(dataset.clone()).or_default())
    }

    fn due(&self, deleted_at: i64) -> i64 {
        deleted_at.saturating_add(i64::try_from(self.delete_delay.as_micros()).unwrap_or(i64::MAX))
    }

    async fn purge(&self, dataset: &Dataset) -> Result<(), Skip> {
        let Some((snapshot, _pin)) = self.chain.pin_latest(dataset) else {
            return Ok(());
        };
        // 1. 最新快照中的留痕
        let mut live: HashSet<String> = HashSet::new();
        let mut found: Vec<(String, String, i64)> = Vec::new();
        for b in &snapshot.buckets {
            let m = self.manifest(dataset, &snapshot, b).await?;
            live.extend(m.files.iter().map(|f| f.path.clone()));
            for d in tombstones(&m) {
                found.push((b.bucket.clone(), d.0, d.1));
            }
        }
        let current: HashSet<&str> = snapshot.buckets.iter().map(|b| b.bucket.as_str()).collect();
        let swept = self.progress(dataset, |p| {
            p.handled
                .retain(|bucket, _| current.contains(bucket.as_str()));
            for (bucket, path, deleted_at) in found {
                if !p.handled.get(&bucket).is_some_and(|h| h.contains(&path)) {
                    p.pending.entry(path).or_insert(self.due(deleted_at));
                }
            }
            p.swept
        });

        // 2. 本进程第一轮：列一次 `data/`
        if !swept {
            self.sweep(dataset, &snapshot, &live, &current).await?;
        }

        // 3. 删除已到删除时刻的
        let now = crate::lifecycle::now_us();
        let mut due: Vec<(i64, String)> = self.progress(dataset, |p| {
            p.pending
                .iter()
                .filter(|(_, at)| **at <= now)
                .map(|(path, at)| (*at, path.clone()))
                .collect()
        });
        due.sort();
        let store = &self.chain.store;
        for (_, path) in due {
            if !self.may_delete(dataset).await {
                return Ok(());
            }
            if !live.contains(&path) {
                store
                    .delete(&store.object_path(dataset, &path))
                    .await
                    .map_err(|_| Skip)?;
            }
            self.finish(dataset, &path, &current);
        }

        // 4. 冷层数据文件
        if snapshot.cold_deletion_segment.is_some() {
            self.purge_cold(dataset, &snapshot).await?;
        }
        Ok(())
    }

    /// 物理删除之前：经过暂停点，再确认未在停机、数据集仍正常。
    async fn may_delete(&self, dataset: &Dataset) -> bool {
        if self.stopping() || !self.normal(dataset) {
            return false;
        }
        fault::pause("delete.before_physical");
        fault::gate("delete.before_physical").await;
        !self.stopping() && self.normal(dataset)
    }

    /// 冷层变更段所记、已到删除时刻的冷层数据文件。
    async fn purge_cold(&self, dataset: &Dataset, snapshot: &Snapshot) -> Result<(), Skip> {
        let segments = self
            .chain
            .cold_change_segments(dataset, snapshot)
            .await
            .map_err(|_| Skip)?;
        let swept = self.progress(dataset, |p| {
            for seg in &segments {
                if !p.cold_segments.insert(seg.segment_id.0) {
                    continue;
                }
                for d in &seg.deletions {
                    p.cold_pending
                        .entry(d.path.clone())
                        .or_insert(self.due(d.deleted_at));
                }
            }
            p.cold_swept
        });
        let store = &self.chain.store;
        let table = store.cold_table_prefix(dataset);
        if !swept {
            let mut existing: HashSet<String> = HashSet::new();
            for o in store
                .list(&table.clone().join("data"))
                .await
                .map_err(|_| Skip)?
            {
                if let Some(parts) = o.location.prefix_match(&table) {
                    existing.insert(
                        parts
                            .map(|x| x.as_ref().to_owned())
                            .collect::<Vec<_>>()
                            .join("/"),
                    );
                }
            }
            self.progress(dataset, |p| {
                p.cold_pending.retain(|path, _| existing.contains(path));
                p.cold_swept = true;
            });
        }

        let now = crate::lifecycle::now_us();
        let mut due: Vec<(i64, String)> = self.progress(dataset, |p| {
            p.cold_pending
                .iter()
                .filter(|(_, at)| **at <= now)
                .map(|(path, at)| (*at, path.clone()))
                .collect()
        });
        due.sort();
        for (_, path) in due {
            if !self.may_delete(dataset).await {
                return Ok(());
            }
            let key = path
                .split('/')
                .fold(table.clone(), |key, part| key.join(part));
            store.delete(&key).await.map_err(|_| Skip)?;
            self.progress(dataset, |p| p.cold_pending.remove(&path));
        }
        Ok(())
    }

    /// 列一次 `data/`：已不存在的待删文件记为已处理；仍存在、不存活、所在桶已归档的文件到归档段里找留痕。
    async fn sweep(
        &self,
        dataset: &Dataset,
        snapshot: &Snapshot,
        live: &HashSet<String>,
        current: &HashSet<&str>,
    ) -> Result<(), Skip> {
        let store = &self.chain.store;
        let prefix = store.dataset_prefix(dataset);
        let mut existing: HashMap<String, String> = HashMap::new();
        for o in store
            .list(&prefix.clone().join("data"))
            .await
            .map_err(|_| Skip)?
        {
            let Some(parts) = o.location.prefix_match(&prefix) else {
                continue;
            };
            let path = parts
                .map(|x| x.as_ref().to_owned())
                .collect::<Vec<_>>()
                .join("/");
            if let Some(hour) = layout::parse_data_file_path(&path) {
                existing.insert(path, hour.to_string());
            }
        }
        let retired_buckets: HashSet<String> = existing
            .iter()
            .filter(|(path, bucket)| !live.contains(*path) && !current.contains(bucket.as_str()))
            .map(|(_, bucket)| bucket.clone())
            .collect();
        let mut found: Vec<(String, i64)> = Vec::new();
        if !retired_buckets.is_empty() {
            let segments = self
                .chain
                .retired_segments(dataset, snapshot)
                .await
                .map_err(|_| Skip)?;
            for bucket in &retired_buckets {
                // 段链自新到旧，第一个含该桶的就是它最近一次被归档时的最后一份桶 manifest
                let Some(r) = segments
                    .iter()
                    .flat_map(|s| &s.buckets)
                    .find(|r| &r.bucket == bucket)
                else {
                    continue;
                };
                let reference = BucketRef {
                    bucket: r.bucket.clone(),
                    manifest: r.manifest.clone(),
                    content_digest: r.content_digest,
                    record_count: 0,
                    size_bytes: 0,
                    time_min: None,
                    time_max: None,
                };
                let m = self.manifest(dataset, snapshot, &reference).await?;
                found.extend(
                    tombstones(&m)
                        .into_iter()
                        .filter(|(path, _)| existing.contains_key(path) && !live.contains(path)),
                );
            }
        }
        self.progress(dataset, |p| {
            let gone: Vec<String> = p
                .pending
                .keys()
                .filter(|path| !existing.contains_key(*path))
                .cloned()
                .collect();
            for path in gone {
                p.pending.remove(&path);
                if let Some(bucket) = layout::parse_data_file_path(&path).map(|h| h.to_string())
                    && current.contains(bucket.as_str())
                {
                    p.handled.entry(bucket).or_default().insert(path);
                }
            }
            for (path, deleted_at) in found {
                p.pending.entry(path).or_insert(self.due(deleted_at));
            }
            p.swept = true;
        });
        Ok(())
    }

    fn finish(&self, dataset: &Dataset, path: &str, current: &HashSet<&str>) {
        self.progress(dataset, |p| {
            p.pending.remove(path);
            if let Some(bucket) = layout::parse_data_file_path(path).map(|h| h.to_string())
                && current.contains(bucket.as_str())
            {
                p.handled.entry(bucket).or_default().insert(path.to_owned());
            }
        });
    }

    /// 读取并核对一个桶 manifest，不改变数据集状态。
    async fn manifest(
        &self,
        dataset: &Dataset,
        snapshot: &Snapshot,
        bucket: &BucketRef,
    ) -> Result<Arc<BucketManifest>, Skip> {
        match self
            .chain
            .read_bucket_manifest(dataset, snapshot, bucket)
            .await
        {
            Ok(Ok(m)) => Ok(m),
            _ => Err(Skip),
        }
    }
}

/// 桶 manifest 中 `reason` 为 `lake`、`ttl`、`compaction` 或 `rollback` 的留痕：(path, deleted_at)。`orphan` 留痕的文件由
/// 巡检留痕后立即删除，不在此列。
fn tombstones(m: &BucketManifest) -> Vec<(String, i64)> {
    m.deletions
        .iter()
        .filter(|d| d.reason != DeletionReason::Orphan)
        .map(|d| (d.path.clone(), d.deleted_at))
        .collect()
}

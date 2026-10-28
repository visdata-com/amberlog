//! 封口合并（REQ-AMB-STO-SEAL-COMPACTION-V4）：把已封口桶的小文件合并为较少的数据文件，合并前核对每个输入的摘要。
//!
//! - 触发：每个检查周期（远小于 60 秒）对每个存在且正常的数据集，取本进程已知的最新快照，逐个检查处于 `sealed`
//!   状态（以合并开始时的时钟判定，[`crate::lifecycle`]；`pending_lake` 的桶交给入湖）的桶：小文件（`size_bytes` 小于
//!   `compaction.target_file_bytes` 的一半）中有相邻可合并组即合并一次，输入由 [`select`] 取、输出恰好一个文件。全部
//!   小文件构成可合并的一组时以全部小文件为输入；比其余之和大 2 倍以上的小文件不与它们一起合并。
//! - 核对：以完整 GET 读出全部输入，逐个比对 SHA-256 与 `content_digest`，全部核对完毕且全部相符之后才写出与上传
//!   输出。任一输入不存在或不符即放弃（计 `aborted`），本进程此后不再合并该桶；不符的输入各报一次 `digest_mismatch`
//!   （按数据集与路径去重，与入湖共用 [`DigestReports`]）。对象存储不可访问只放弃本次，不计数，下一周期重来。
//! - 与入湖互斥：自确定合并输入起至提交或放弃止占用该桶（[`BucketClaims`]），入湖占着的桶本周期跳过。提交之前再判一次：
//!   桶已不是 `sealed`、该桶有处于交接窗口的冷层提交、或无法判定（冷层读取失败，或尚无冷层表而快照 `summary.cold` 记有
//!   冷层记录）即放弃本次（不计数、不停用，下一周期重新判定）。
//! - 输出沿用 flush 写数据文件的同一写法（[`write_merged`]，含列统计），列为各输入列之并，缺列处为 NULL；
//!   可写 bloom 的列写等值 bloom（REQ-AMB-QRY-EQUALITY-BLOOM-WRITE-OUTPUTS ①）。核对摘要要先读出全部输入的完整字节，
//!   之后边读边写：逐个输入、逐批解码写出，不把输入全部解码进内存（一次合并的输入至多约两个目标大小，解码后是它的
//!   十倍上下）。
//! - 决定是否合并之前读桶 manifest 不登记停用状态（`StoreChain::peek_bucket_manifest`）：读不到、无法解析或对象存储
//!   不可访问即本周期跳过该桶；停用只由查询、提交与装载触发（REQ-AMB-STO-HOT-FSCK）。
//! - 提交经快照提交的合并提交（与 flush 同属该数据集的串行提交）；冲突重试时若输入已不在最新快照的存活集中则放弃
//!   （不计 `aborted`，下一周期重新判定）。合并只使输入离开存活集，不物理删除。
//! - 按周期运行（[`BucketTask`]）；停机过程开始后不再开始新的合并，进行中的合并由 [`crate::maintenance::BucketTasks`] 中止。
//! - 每次合并结束（提交或放弃）后，分配器的空闲内存多就还给系统（[`memory::release_if_much_free`]）。

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use amberlog_common::dataset::Dataset;
use bytes::Bytes;

use crate::BoxFuture;
use crate::bloom::{self, IndexHints};
use crate::chain::{Digest, FileEntry, Snapshot};
use crate::cold::{ColdReader, ColdScope};
use crate::committer::{CommitError, CompactionRequest};
use crate::fault;
use crate::flush::write_merged;
use crate::flusher::Flusher;
use crate::gate::DatasetState;
use crate::lake::{BucketClaims, Claimant, DigestReports};
use crate::layout::HourBucket;
use crate::lifecycle::{self, BucketState, Lifecycle};
use crate::maintenance::BucketTask;
use crate::memory;
use crate::registry::Persistence;
use crate::schema::StreamColumns;

/// 合并的配置。
#[derive(Debug, Clone, Copy)]
pub struct CompactionConfig {
    pub lifecycle: Lifecycle,
    /// `compaction.target_file_bytes`
    pub target_file_bytes: u64,
    /// 检查周期。
    pub check_interval: Duration,
}

/// 合并的可观察结果，由调用方接到指标与稳定日志事件。
pub trait CompactionEvents: Send + Sync {
    /// 一次合并的结果：提交成功为 `true`，因摘要核对（含输入不存在）放弃为 `false`。
    fn compaction(&self, dataset: &Dataset, ok: bool);
    /// 合并输入完整字节的摘要与其 `content_digest` 不符（每个数据集的每个路径至多一次）。
    fn digest_mismatch(&self, dataset: &Dataset, path: &str);
}

/// 合并与入湖共用的进程内状态，以及判定交接窗口所读的冷层。
pub struct CompactionShared {
    pub claims: Arc<BucketClaims>,
    pub reports: Arc<DigestReports>,
    pub cold: Arc<dyn ColdReader>,
    /// 本进程启动时读入的索引提示：合并输出按它写等值 bloom（REQ-AMB-QRY-EQUALITY-BLOOM-WRITE-OUTPUTS ①）。
    pub hints: Arc<IndexHints>,
}

/// 封口合并的驱动。
pub struct Compactor {
    flusher: Arc<Flusher>,
    config: CompactionConfig,
    events: Arc<dyn CompactionEvents>,
    shared: CompactionShared,
    /// 因摘要核对放弃过、本进程不再合并的（数据集，桶）。
    stopped: Mutex<HashSet<(Dataset, String)>>,
    stopping: AtomicBool,
}

/// 一次合并没有提交的原因。
enum Abort {
    /// 对象存储不可访问等：本次放弃，下一周期重来。
    Retry,
    /// 摘要核对不过（含输入不存在）。
    Digest,
}

impl BucketTask for Compactor {
    fn period(&self) -> Duration {
        self.config.check_interval
    }

    fn run_once(&self) -> BoxFuture<'_, ()> {
        Box::pin(Compactor::run_once(self))
    }

    fn stop(&self) {
        Compactor::stop(self);
    }
}

impl Compactor {
    pub fn new(
        flusher: Arc<Flusher>,
        config: CompactionConfig,
        events: Arc<dyn CompactionEvents>,
        shared: CompactionShared,
    ) -> Compactor {
        Compactor {
            flusher,
            config,
            events,
            shared,
            stopped: Mutex::default(),
            stopping: AtomicBool::new(false),
        }
    }

    /// 停机过程开始：此后不再开始新的合并。
    pub fn stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
    }

    fn stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    /// 对每个存在且正常的数据集检查一遍。
    pub async fn run_once(&self) {
        let chain = self.flusher.committer().chain();
        for dataset in chain.registry.datasets() {
            if self.stopping() {
                return;
            }
            if chain.registry.persistence(&dataset) != Some(Persistence::Stored)
                || chain.gates.state(&dataset) != DatasetState::Normal
            {
                continue;
            }
            self.compact_dataset(&dataset).await;
        }
    }

    async fn compact_dataset(&self, dataset: &Dataset) {
        let chain = self.flusher.committer().chain();
        let Some((snapshot, _pin)) = chain.pin_latest(dataset) else {
            return;
        };
        let now = lifecycle::now_us();
        for b in &snapshot.buckets {
            if self.stopping() || chain.gates.state(dataset) != DatasetState::Normal {
                return;
            }
            let Some(hour) = HourBucket::parse(&b.bucket) else {
                continue;
            };
            if b.record_count == 0 || self.config.lifecycle.state(hour, now) != BucketState::Sealed
            {
                continue;
            }
            let key = (dataset.clone(), b.bucket.clone());
            if self.stopped.lock().expect("未中毒").contains(&key) {
                continue;
            }
            // 入湖占着该桶：本周期不合并
            let Some(_claim) = self
                .shared
                .claims
                .claim(dataset, &b.bucket, Claimant::Compaction)
            else {
                continue;
            };
            // 检查读取不登记停用：读不到即本周期不合并该桶；合并提交里仍按提交读取
            let Some(manifest) = chain.peek_bucket_manifest(dataset, &snapshot, b).await else {
                continue;
            };
            let inputs = select(&manifest.files, self.config.target_file_bytes);
            if inputs.len() < 2 {
                continue;
            }
            let result = self.compact(dataset, hour, &snapshot, inputs).await;
            // 解码出的批次已全部释放：空闲内存多就还给系统，不让各次合并在不同分配区里逐级堆积
            memory::release_if_much_free();
            match result {
                Ok(()) | Err(Abort::Retry) => {}
                Err(Abort::Digest) => {
                    self.stopped.lock().expect("未中毒").insert(key);
                    self.events.compaction(dataset, false);
                }
            }
        }
    }

    async fn compact(
        &self,
        dataset: &Dataset,
        hour: HourBucket,
        snapshot: &Snapshot,
        inputs: Vec<FileEntry>,
    ) -> Result<(), Abort> {
        let schema = &snapshot.schema;
        let store = &self.flusher.committer().chain().store;
        // 全部输入核对完毕、全部相符之后才写出输出
        let mut bytes: Vec<Bytes> = Vec::with_capacity(inputs.len());
        let mut failed = false;
        for f in &inputs {
            match store.read(&store.object_path(dataset, &f.path)).await {
                Err(_) => return Err(Abort::Retry),
                Ok(None) => failed = true,
                Ok(Some(got)) => {
                    if Digest::of(&got.bytes) != f.content_digest {
                        failed = true;
                        if self.shared.reports.first(dataset, &f.path) {
                            self.events.digest_mismatch(dataset, &f.path);
                        }
                    }
                    bytes.push(got.bytes);
                }
            }
        }
        if failed {
            return Err(Abort::Digest);
        }
        if self.stopping() {
            return Err(Abort::Retry);
        }

        // 边读边写：逐个输入、逐批解码写出，不把全部输入解码进内存
        let columns = StreamColumns::from_columns(schema.to_vec());
        let bloom = bloom::bloom_columns(&self.shared.hints, dataset, schema);
        let output = write_merged(&columns, hour, bytes, &bloom).map_err(|_| Abort::Retry)?;
        let request = CompactionRequest {
            bucket: hour,
            inputs,
            outputs: vec![output],
            sealed_until: self.config.lifecycle.lake_at_us(hour),
        };
        fault::gate("compaction.before_commit").await;
        if self.stopping() || !self.may_commit(dataset, hour, snapshot).await {
            return Err(Abort::Retry);
        }
        match self
            .flusher
            .committer()
            .commit_compaction(dataset, &request)
            .await
        {
            Ok(_) => {
                self.flusher.record_ok(dataset);
                self.events.compaction(dataset, true);
                Ok(())
            }
            Err(CommitError::Exhausted | CommitError::Disabled(_)) => {
                self.flusher.record_failed(dataset);
                Err(Abort::Retry)
            }
            // 对象存储不可访问不计入提交指标；输入已离开存活集则下一周期重新判定
            Err(CommitError::Unavailable | CommitError::Wal(_) | CommitError::Stale) => {
                Err(Abort::Retry)
            }
        }
    }

    /// 提交之前的判定（REQ-AMB-STO-SEAL-COMPACTION-V2）：此刻桶仍是 `sealed`，且确定该桶没有处于交接窗口的冷层提交。
    /// 判定所作的读取不改变数据集状态，读不到即按无法判定处置。
    async fn may_commit(&self, dataset: &Dataset, hour: HourBucket, snapshot: &Snapshot) -> bool {
        if self.config.lifecycle.state(hour, lifecycle::now_us()) != BucketState::Sealed {
            return false;
        }
        let cold = &self.shared.cold;
        let table = match cold.version(dataset).await {
            Err(_) => return false,
            // 尚无冷层表：快照记有冷层记录时冷层表已被整体删除，无法判定
            Ok(None) => return snapshot.summary.cold.record_count == 0,
            Ok(Some(v)) => match cold
                .table_at(dataset, v, ColdScope::Days(i32::MIN..=i32::MIN))
                .await
            {
                Ok(t) => t,
                Err(_) => return false,
            },
        };
        if !table.lake_buckets().contains(&hour) {
            return true;
        }
        let chain = self.flusher.committer().chain();
        let label = hour.to_string();
        let Some(r) = snapshot.buckets.iter().find(|b| b.bucket == label) else {
            return false;
        };
        let Some(m) = chain.peek_bucket_manifest(dataset, snapshot, r).await else {
            return false;
        };
        let live = std::collections::BTreeMap::from([(hour, m.files.clone())]);
        table.handover_window(&live).is_empty()
    }
}

/// 合并输入（REQ-AMB-STO-SEAL-COMPACTION-V4）：小文件（`size_bytes` 小于 `target` 的一半）按大小升序排成一列，从最小的
/// 一个起逐个作起点，向后逐个累加到和超过 `target` 为止，其间记下最远的终点——终点处的文件不大于起点至它前一个之和的
/// 2 倍，起点至终点即构成可合并的一组；取第一个找得到终点的起点。有相邻可合并组时一定找得到（它的和不大于 `target`，
/// 从它的起点累加走得到它的终点）；全部小文件构成可合并的一组时取到的就是全部。找不到返回空。
///
/// 每个起点至多累加到和超过 `target`：刚封口的桶有几百个 flush 文件时也只是几十万次加法。
fn select(files: &[FileEntry], target: u64) -> Vec<FileEntry> {
    let mut small: Vec<&FileEntry> = files
        .iter()
        .filter(|f| f.size_bytes.saturating_mul(2) < target)
        .collect();
    small.sort_by_key(|f| f.size_bytes);
    for l in 0..small.len() {
        let mut sum = small[l].size_bytes;
        let mut end = None;
        for (r, f) in small.iter().enumerate().skip(l + 1) {
            if sum.saturating_add(f.size_bytes) > target {
                break;
            }
            if f.size_bytes <= sum.saturating_mul(2) {
                end = Some(r);
            }
            sum += f.size_bytes;
        }
        if let Some(r) = end {
            return small[l..=r].iter().map(|&f| f.clone()).collect();
        }
    }
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chain::SnapshotId;

    /// `sizes` 各为一个文件（路径为其下标）时选中文件的大小，升序。
    fn picked(sizes: &[u64], target: u64) -> Vec<u64> {
        let files: Vec<FileEntry> = sizes
            .iter()
            .enumerate()
            .map(|(i, &size_bytes)| FileEntry {
                path: format!("data/2026/08/22/10/{i}.parquet"),
                min_timestamp: 1,
                max_timestamp: 2,
                record_count: 1,
                size_bytes,
                content_digest: Digest::of(&i.to_le_bytes()),
                added_in: SnapshotId(1),
                column_stats: None,
            })
            .collect();
        select(&files, target)
            .iter()
            .map(|f| f.size_bytes)
            .collect()
    }

    #[test]
    fn select_takes_all_of_comparable_files() {
        assert_eq!(picked(&[110, 100, 120], 1000), [100, 110, 120]);
    }

    #[test]
    fn select_spares_much_larger_small_file() {
        // 200 是小文件（小于 500），但大于 10 + 11 + 12 的 2 倍
        assert_eq!(picked(&[200, 12, 10, 11], 1000), [10, 11, 12]);
        assert_eq!(picked(&[200, 12], 1000), Vec::<u64>::new());
    }

    #[test]
    fn select_skips_files_of_half_target_or_more() {
        assert_eq!(picked(&[500, 700], 1000), Vec::<u64>::new());
        assert_eq!(picked(&[500, 12, 10], 1000), [10, 12]);
        // 目标大小为奇数：一半为 21.5
        assert_eq!(picked(&[21, 20], 43), [20, 21]);
        assert_eq!(picked(&[21, 20], 42), Vec::<u64>::new());
    }

    #[test]
    fn select_pairs_files_within_double() {
        assert_eq!(picked(&[190, 100], 1000), [100, 190]);
        assert_eq!(picked(&[200, 100], 1000), [100, 200]);
        assert_eq!(picked(&[201, 100], 1000), Vec::<u64>::new());
    }

    #[test]
    fn select_stops_at_target() {
        let sizes: Vec<u64> = (100..110).collect();
        let got = picked(&sizes, 350);
        assert_eq!(got, [100, 101, 102]);
        assert!(got.iter().sum::<u64>() <= 350);
    }

    #[test]
    fn select_moves_to_next_start() {
        // 一半为 10.5：从 2 起，10 大于 2 的 2 倍，再加 10 和为 22 超过 21；从 10 起取两个 10
        assert_eq!(picked(&[10, 2, 10], 21), [10, 10]);
        // 终点前的文件可以大于起点的 2 倍：11 不大于 1 + 10 的 2 倍，三个构成可合并的一组
        assert_eq!(picked(&[1, 10, 11], 42), [1, 10, 11]);
    }
}

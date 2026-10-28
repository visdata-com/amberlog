//! 桶生命周期的周期工作：封口合并、热层巡检、入湖、TTL、留痕归档、延后物理删除与冷层孤儿清理。
//!
//! 每项工作实现 [`BucketTask`]，由生命周期编排在开始受理 `/api/` 请求之后经 [`BucketTasks::start`] 一并启动，
//! 停机过程开始时经 [`BucketTasks::stop`] 一并停下：此后不开始新的一轮，进行中的直接中止
//! （REQ-AMB-STO-SEAL-COMPACTION、REQ-AMB-STO-HOT-FSCK、REQ-AMB-STO-AUDIT-INVARIANTS-V3「不在停机过程中开始新一轮」）。
//!
//! 延后物理删除同时处理热层与冷层数据文件：冷层文件按热层快照所引用冷层变更段的 `deleted_at` 加
//! `storage.delete_delay`，直接经对象存储删除。冷层孤儿清理只涉冷层（[`crate::cold::ColdReader::referenced`] 与
//! [`crate::cold::ColdWriter::in_flight`]）。
//!
//! 入湖与 TTL 之间共享 [`LakeInputs`]：TTL 不移走进行中的入湖已确定的入湖输入（REQ-AMB-STO-TTL）。

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use amberlog_common::dataset::Dataset;

use crate::BoxFuture;

/// 桶生命周期的一项周期工作。
pub trait BucketTask: Send + Sync {
    /// 两轮之间的间隔。
    fn period(&self) -> Duration;

    /// 第一轮在 [`BucketTasks::start`] 之后多久开始；缺省为一个间隔。须在开始受理之后尽快补做的工作（入湖补做交接、
    /// TTL 补写冷层变更段，都要在开始受理 `/api/` 请求起 120 秒内）以较短的延迟覆盖它。
    fn first_delay(&self) -> Duration {
        self.period()
    }

    /// 对每个数据集做一轮。
    fn run_once(&self) -> BoxFuture<'_, ()>;

    /// 停机过程开始：此后不再开始新的一轮（进行中的由 [`BucketTasks::stop`] 中止）。
    fn stop(&self);
}

/// 一组周期工作。
#[derive(Default)]
pub struct BucketTasks {
    tasks: Vec<Arc<dyn BucketTask>>,
    running: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl BucketTasks {
    pub fn new() -> BucketTasks {
        BucketTasks::default()
    }

    pub fn add(&mut self, task: Arc<dyn BucketTask>) {
        self.tasks.push(task);
    }

    /// 启动各项工作的周期。生命周期编排在开始受理 `/api/` 请求时调用（与 flush 周期同一时点），
    /// 各项第一轮在其 [`BucketTask::first_delay`] 之后开始。错过的轮次顺延，不补跑。
    pub fn start(&self) {
        let mut running = self.running.lock().expect("周期任务表锁未中毒");
        for task in &self.tasks {
            let task = Arc::clone(task);
            running.push(tokio::spawn(async move {
                let period = task.period();
                let first = tokio::time::Instant::now() + task.first_delay();
                let mut tick = tokio::time::interval_at(first, period);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    task.run_once().await;
                }
            }));
        }
    }

    /// 停下全部工作：先令其不再开始新的一轮，再中止进行中的，返回时都已结束。
    pub async fn stop(&self) {
        for task in &self.tasks {
            task.stop();
        }
        let running = std::mem::take(&mut *self.running.lock().expect("周期任务表锁未中毒"));
        for handle in &running {
            handle.abort();
        }
        for handle in running {
            let _ = handle.await;
        }
    }
}

/// 进行中的入湖已确定的入湖输入（相对数据集前缀的热层数据文件路径），按数据集；入湖登记、TTL 避让
/// （REQ-AMB-STO-TTL-LAKE-GUARD）。
///
/// TTL 从判定到期文件起、到 TTL 热层提交返回止持有该数据集的 [`TtlGuard`]；其间 [`LakeInputs::begin`] 返回 `false`，
/// 入湖不开始、下一次检查再来。这样 TTL 判定时查得的进行中入湖输入（[`LakeInputs::contains`]）在它提交之前不会变多。
#[derive(Debug, Default)]
pub struct LakeInputs {
    state: Mutex<HashMap<Dataset, LakeState>>,
}

#[derive(Debug, Default)]
struct LakeState {
    inputs: HashSet<String>,
    ttl_guards: usize,
}

impl LakeInputs {
    pub fn new() -> LakeInputs {
        LakeInputs::default()
    }

    /// 登记数据集 `dataset` 本次入湖的输入（同一数据集同一时刻至多一个入湖，再登记即替换）。该数据集的 TTL 守卫被持有时
    /// 不登记、返回 `false`，入湖不得开始。
    #[must_use]
    pub fn begin(&self, dataset: &Dataset, paths: impl IntoIterator<Item = String>) -> bool {
        let mut state = self.lock();
        let entry = state.entry(dataset.clone()).or_default();
        if entry.ttl_guards > 0 {
            return false;
        }
        entry.inputs = paths.into_iter().collect();
        true
    }

    /// 数据集 `dataset` 的入湖结束（交接提交成功或放弃）。
    pub fn end(&self, dataset: &Dataset) {
        if let Some(entry) = self.lock().get_mut(dataset) {
            entry.inputs.clear();
        }
    }

    /// 此刻是否为数据集 `dataset` 进行中入湖的输入。
    pub fn contains(&self, dataset: &Dataset, path: &str) -> bool {
        self.lock()
            .get(dataset)
            .is_some_and(|e| e.inputs.contains(path))
    }

    /// TTL 对数据集 `dataset` 的守卫：持有期间该数据集不开始新的入湖。
    pub fn ttl_guard(self: &Arc<Self>, dataset: &Dataset) -> TtlGuard {
        self.lock().entry(dataset.clone()).or_default().ttl_guards += 1;
        TtlGuard {
            inputs: Arc::clone(self),
            dataset: dataset.clone(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<Dataset, LakeState>> {
        self.state.lock().expect("入湖输入表锁未中毒")
    }
}

/// 见 [`LakeInputs::ttl_guard`]；丢弃即释放。
#[derive(Debug)]
pub struct TtlGuard {
    inputs: Arc<LakeInputs>,
    dataset: Dataset,
}

impl Drop for TtlGuard {
    fn drop(&mut self) {
        if let Some(entry) = self.inputs.lock().get_mut(&self.dataset) {
            entry.ttl_guards -= 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use super::*;

    struct Counting {
        runs: AtomicUsize,
        stopped: AtomicBool,
    }

    impl BucketTask for Counting {
        fn period(&self) -> Duration {
            Duration::from_millis(10)
        }

        fn run_once(&self) -> BoxFuture<'_, ()> {
            Box::pin(async move {
                if !self.stopped.load(Ordering::SeqCst) {
                    self.runs.fetch_add(1, Ordering::SeqCst);
                }
            })
        }

        fn stop(&self) {
            self.stopped.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn tasks_run_periodically_and_stop() {
        let task = Arc::new(Counting {
            runs: AtomicUsize::new(0),
            stopped: AtomicBool::new(false),
        });
        let mut tasks = BucketTasks::new();
        tasks.add(Arc::clone(&task) as Arc<dyn BucketTask>);
        tasks.start();
        while task.runs.load(Ordering::SeqCst) < 2 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        tasks.stop().await;
        assert!(task.stopped.load(Ordering::SeqCst));
        let after = task.runs.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(task.runs.load(Ordering::SeqCst), after);
    }

    #[test]
    fn lake_inputs_are_per_dataset_and_ttl_guard_blocks_begin() {
        let app = Dataset::new("default", "app");
        let web = Dataset::new("default", "web");
        let inputs = Arc::new(LakeInputs::new());
        assert!(inputs.begin(&app, ["data/a.parquet".to_owned()]));
        assert!(inputs.contains(&app, "data/a.parquet"));
        assert!(!inputs.contains(&web, "data/a.parquet"));
        inputs.end(&app);
        assert!(!inputs.contains(&app, "data/a.parquet"));

        let guard = inputs.ttl_guard(&app);
        assert!(!inputs.begin(&app, ["data/b.parquet".to_owned()]));
        assert!(!inputs.contains(&app, "data/b.parquet"));
        // 别的数据集不受影响
        assert!(inputs.begin(&web, ["data/c.parquet".to_owned()]));
        drop(guard);
        assert!(inputs.begin(&app, ["data/b.parquet".to_owned()]));
    }
}

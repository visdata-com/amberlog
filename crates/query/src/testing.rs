//! 测试用替身：[`DataFiles`]、[`CounterObjects`]、[`BucketStates`] 与 [`AlertHistory`] 的内存实现。
//!
//! 只在本 crate 的测试与打开 `testing` feature 的构建里编入；其他 crate 在 `[dev-dependencies]` 里打开它。

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};

use amberlog_common::dataset::Dataset;
use amberlog_storage::BoxFuture;
use amberlog_storage::chain::{BucketRef, FileEntry};
use amberlog_storage::registry::DatasetRegistry;
use arrow::record_batch::RecordBatch;

use crate::files::{DataFiles, FileBatches, FileError};
use crate::system::{
    AlertHistory, BucketStates, CounterError, CounterObjects, CounterWindow, EvaluationRow,
    Governance, HistoryError,
};

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().expect("替身的锁未中毒")
}

/// 按路径（桶 manifest 条目的 `path`）预设内容的数据文件，记下读过哪些路径与每次要的列；未预设的读取返回
/// [`FileError::NotFound`]。不按要的列与时间范围筛选，交出预设的全部批次（调用方按列名对齐、按时间过滤）。
#[derive(Debug, Default)]
pub struct FakeFiles {
    files: Mutex<HashMap<String, Result<Vec<RecordBatch>, FileError>>>,
    /// 交出预设的批次之后再报的失败（解码中途的失败）。
    tails: Mutex<HashMap<String, FileError>>,
    reads: Mutex<Vec<String>>,
    columns: Mutex<Vec<Vec<String>>>,
}

impl FakeFiles {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&self, path: &str, batches: Vec<RecordBatch>) {
        lock(&self.files).insert(path.to_owned(), Ok(batches));
    }

    pub fn fail(&self, path: &str, error: FileError) {
        lock(&self.files).insert(path.to_owned(), Err(error));
    }

    /// 打开 `path` 成功、交出 `batches` 之后以 `error` 失败（解码中途的失败）。
    pub fn fail_after(&self, path: &str, batches: Vec<RecordBatch>, error: FileError) {
        self.put(path, batches);
        lock(&self.tails).insert(path.to_owned(), error);
    }

    /// 读过的路径，按读取顺序。
    pub fn reads(&self) -> Vec<String> {
        lock(&self.reads).clone()
    }

    /// 每次读取要的列，按读取顺序。
    pub fn columns(&self) -> Vec<Vec<String>> {
        lock(&self.columns).clone()
    }
}

impl DataFiles for FakeFiles {
    fn read<'a>(
        &'a self,
        _dataset: &'a Dataset,
        file: &'a FileEntry,
        columns: &'a [String],
        _time: std::ops::Range<i64>,
    ) -> BoxFuture<'a, Result<FileBatches, FileError>> {
        lock(&self.reads).push(file.path.clone());
        lock(&self.columns).push(columns.to_vec());
        let tail = lock(&self.tails).get(&file.path).cloned();
        let r = lock(&self.files)
            .get(&file.path)
            .cloned()
            .unwrap_or_else(|| {
                Err(FileError::NotFound(format!(
                    "FakeFiles: {} not set",
                    file.path
                )))
            });
        let r = r.map(|batches| {
            Box::new(batches.into_iter().map(Ok).chain(tail.map(Err))) as FileBatches
        });
        Box::pin(std::future::ready(r))
    }
}

/// 按数据集预设窗口的计数对象，记下每次读取的 (数据集, start, end)；未预设的数据集没有任何计数对象。
/// 返回预设窗口中 `window_start` 落在所读范围内的那些。
#[derive(Debug, Default)]
pub struct FakeCounters {
    windows: Mutex<HashMap<Dataset, Result<Vec<CounterWindow>, CounterError>>>,
    /// 读不出的小时（只作用于 [`CounterObjects::current_hours`]）。
    failed_hours: Mutex<HashMap<(Dataset, i64), CounterError>>,
    reads: Mutex<Vec<(Dataset, i64, i64)>>,
}

impl FakeCounters {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&self, dataset: &Dataset, windows: Vec<CounterWindow>) {
        lock(&self.windows).insert(dataset.clone(), Ok(windows));
    }

    pub fn fail(&self, dataset: &Dataset, error: CounterError) {
        lock(&self.windows).insert(dataset.clone(), Err(error));
    }

    /// 后台判定读 `dataset` 在 `hour` 的计数时报 `error`。
    pub fn fail_hour(&self, dataset: &Dataset, hour: i64, error: CounterError) {
        lock(&self.failed_hours).insert((dataset.clone(), hour), error);
    }

    pub fn reads(&self) -> Vec<(Dataset, i64, i64)> {
        lock(&self.reads).clone()
    }
}

impl CounterObjects for FakeCounters {
    fn windows<'a>(
        &'a self,
        dataset: &'a Dataset,
        start: i64,
        end: i64,
    ) -> BoxFuture<'a, Result<Vec<CounterWindow>, CounterError>> {
        lock(&self.reads).push((dataset.clone(), start, end));
        let r = match lock(&self.windows).get(dataset).cloned() {
            None => Ok(Vec::new()),
            Some(Ok(ws)) => Ok(ws
                .into_iter()
                .filter(|w| (start..end).contains(&w.window_start))
                .collect()),
            Some(Err(e)) => Err(e),
        };
        Box::pin(std::future::ready(r))
    }

    fn current_hours<'a>(
        &'a self,
        dataset: &'a Dataset,
        start: i64,
        end: i64,
    ) -> BoxFuture<'a, Vec<crate::system::HourCounts>> {
        const HOUR: i64 = 3_600_000_000;
        let mut out = Vec::new();
        if end > start {
            let mut hour = start.div_euclid(HOUR) * HOUR;
            while hour < end {
                let r = match lock(&self.failed_hours)
                    .get(&(dataset.clone(), hour))
                    .cloned()
                {
                    Some(e) => Err(e),
                    None => match lock(&self.windows).get(dataset).cloned() {
                        None => Ok(Vec::new()),
                        Some(Ok(ws)) => Ok(ws
                            .into_iter()
                            .filter(|w| {
                                (start..end).contains(&w.window_start)
                                    && (hour..hour + HOUR).contains(&w.window_start)
                            })
                            .collect()),
                        Some(Err(e)) => Err(e),
                    },
                };
                out.push((hour, r));
                hour += HOUR;
            }
        }
        Box::pin(std::future::ready(out))
    }
}

/// 补发中的状态：集合中的 (数据集, 来源) 补发中；`all` 为真时一切来源都补发中。
#[derive(Debug, Default)]
pub struct FakeBackfill {
    sources: Mutex<Vec<(Dataset, String)>>,
    all: std::sync::atomic::AtomicBool,
}

impl FakeBackfill {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&self, dataset: &Dataset, source: &str) {
        lock(&self.sources).push((dataset.clone(), source.to_owned()));
    }

    pub fn clear(&self) {
        lock(&self.sources).clear();
        self.all.store(false, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn set_all(&self) {
        self.all.store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

impl crate::system::verdict::Backfill for FakeBackfill {
    fn backfilling(&self, dataset: &Dataset, source: &str, _t: i64) -> bool {
        self.all.load(std::sync::atomic::Ordering::Relaxed)
            || lock(&self.sources)
                .iter()
                .any(|(d, s)| d == dataset && s == source)
    }
}

/// 缺省的宽限期 `reconciliation.grace`（5 分钟），微秒。
pub const GRACE_US: i64 = 300_000_000;

/// 桶标签（`YYYYMMDDHH`）在集合中的桶处于 `pending_lake`，其余不处于。
#[derive(Debug, Default)]
pub struct FakeBucketStates {
    pending: Mutex<Vec<String>>,
    /// 每次判定所用的时刻。
    seen: Mutex<Vec<i64>>,
}

impl FakeBucketStates {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_pending(&self, bucket: &str) {
        lock(&self.pending).push(bucket.to_owned());
    }

    pub fn seen(&self) -> Vec<i64> {
        lock(&self.seen).clone()
    }
}

impl BucketStates for FakeBucketStates {
    fn pending_lake(&self, bucket: &BucketRef, now_us: i64) -> bool {
        lock(&self.seen).push(now_us);
        lock(&self.pending).contains(&bucket.bucket)
    }
}

/// [`FakeAlertHistory`] 记下的一次读取：(org, 点名规则, start, end)。
pub type AlertRead = (String, Option<BTreeSet<String>>, i64, i64);

/// 预设的已写出评估记录，记下每次读取的 (org, 点名规则, start, end)；预设了失败时每次读取都返回它。
/// 返回 `org` 相同、`scheduled_at` 落在所读范围内、有点名时规则被点名的那些。
#[derive(Debug, Default)]
pub struct FakeAlertHistory {
    rows: Mutex<Vec<(String, EvaluationRow)>>,
    failure: Mutex<Option<HistoryError>>,
    reads: Mutex<Vec<AlertRead>>,
}

impl FakeAlertHistory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&self, org: &str, row: EvaluationRow) {
        lock(&self.rows).push((org.to_owned(), row));
    }

    pub fn fail(&self, error: HistoryError) {
        *lock(&self.failure) = Some(error);
    }

    pub fn reads(&self) -> Vec<AlertRead> {
        lock(&self.reads).clone()
    }
}

impl AlertHistory for FakeAlertHistory {
    fn evaluations<'a>(
        &'a self,
        org: &'a str,
        rules: Option<&'a BTreeSet<String>>,
        start: i64,
        end: i64,
    ) -> BoxFuture<'a, Result<Vec<EvaluationRow>, HistoryError>> {
        lock(&self.reads).push((org.to_owned(), rules.cloned(), start, end));
        let r = match lock(&self.failure).clone() {
            Some(e) => Err(e),
            None => Ok(lock(&self.rows)
                .iter()
                .filter(|(o, r)| {
                    o == org
                        && (start..end).contains(&r.scheduled_at)
                        && rules.is_none_or(|n| n.contains(&r.rule))
                })
                .map(|(_, r)| r.clone())
                .collect()),
        };
        Box::pin(std::future::ready(r))
    }
}

/// 以替身组装的系统表依赖：没有数据集、没有计数对象、没有待入湖的桶、没有评估记录、未配置保留期。
pub fn governance() -> (
    Governance,
    Arc<DatasetRegistry>,
    Arc<FakeCounters>,
    Arc<FakeBucketStates>,
) {
    let registry = Arc::new(DatasetRegistry::new());
    let counters = Arc::new(FakeCounters::new());
    let buckets = Arc::new(FakeBucketStates::new());
    (
        Governance {
            registry: registry.clone(),
            counters: counters.clone(),
            buckets: buckets.clone(),
            alerts: Arc::new(FakeAlertHistory::new()),
            retention: None,
            backfill: Arc::new(FakeBackfill::new()),
            grace_us: GRACE_US,
        },
        registry,
        counters,
        buckets,
    )
}

/// 默认配置下的执行名额与单查询内存上限（`query.max_concurrency` 4、`query.max_memory_bytes` 512 MiB）。
pub fn admission() -> Arc<crate::limits::Admission> {
    Arc::default()
}

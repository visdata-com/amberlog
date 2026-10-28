//! 评估记录的写出、续写、保留期删除与系统表 `_alert_evaluations` 的行集（REQ-AMB-ALR-EVALUATIONS，ADR-0051 第 8 条）。
//!
//! 评估记录对象 `<storage.prefix>/_system/alerts/<rule>/<YYYY>/<MM>/<DD>/<HH>.json` 的格式只由本模块懂：
//! 调度在每次评估结束时经 [`Recorder`] 交出一条记录，本模块按对象攒在内存里，由后台任务写出；查询端经
//! [`AlertHistory`] 取已写出的记录。两者是同一个 [`History`]：为作答读到版本过高或无法解析的对象，此后写出也不覆盖它
//! （REQ-AMB-ALR-EVALUATIONS-UNPARSEABLE）。
//!
//! 只在 `amberlog serve` 的启动流程中构造（REQ-AMB-ALR-EVALUATIONS-SERVE-ONLY）：
//! 1. 装配时 [`History::new`]；
//! 2. 开始受理 `/api/` 请求时 [`History::spawn`]（写出与保留期删除）；
//! 3. 停机时中止该任务，在途评估都交出记录之后 [`History::flush_once`]。
//!
//! 写出（REQ-AMB-ALR-EVALUATIONS B 组）：
//! - 每个对象在内存里有「已写出的元素」与「尚未写出的元素」。每 [`TICK`] 对有尚未写出元素的每个对象做一次普通覆盖写，
//!   内容为两者按 `scheduled_at` 合并；写成后尚未写出的并入已写出的。
//! - 本进程第一次写某对象之前先读它：不存在从空开始；可解析以其元素为底；读不到（不可访问或超时）留待下一轮，绝不在
//!   未读到的情况下覆盖；无法解析或版本过高则此后不再写它（[`Base::Frozen`]），应写进它的记录留在内存里、计入未写出条数。
//! - 写不出的留待下一轮；停机时每个对象只再试一次。
//! - 已写完、早于上一个小时的对象移出内存；之后再有记录要写进它时，按「第一次写」先重读。

mod object;
mod retention;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use amberlog_query::system::{AlertHistory, EvaluationRow, HistoryError};
use amberlog_storage::BoxFuture;
use amberlog_storage::lifecycle::now_us;
use amberlog_storage::store::{Store, Unavailable};
use tokio::task::JoinSet;

use self::object::{Entry, ObjectKey};
use super::Recorder;
use super::outcome::EvaluationRecord;

/// 写出的节拍（每隔不超过 30 秒）。
const TICK: Duration = Duration::from_secs(5);

/// 单个对象存储请求的时限：超时即按不可访问处置。
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

/// 作答时同时读取的对象数。
const READ_CONCURRENCY: usize = 16;

/// 评估记录的写出器与读取者。
pub struct History {
    store: Store,
    /// `alerts.history_retention`。
    retention: Duration,
    /// `hot_fsck.interval`：保留期检查的节拍（REQ-AMB-ALR-EVALUATIONS-RETENTION）。
    check_interval: Duration,
    objects: Mutex<BTreeMap<ObjectKey, Object>>,
    /// 已结束、尚未写出的评估记录条数（`amberlog_alert_history_unwritten`）。
    unwritten: AtomicU64,
}

/// 内存中的一个评估记录对象。
#[derive(Debug, Default)]
struct Object {
    /// 规则的 `org`（由交出的记录带来；只在作答时读到坏对象而登记的，尚无记录时为空）。
    org: String,
    base: Base,
    /// 已写出（或续写前读到）的元素，按 `scheduled_at` 升序。
    written: Vec<Entry>,
    /// 尚未写出的元素，按交出的先后。
    pending: Vec<Entry>,
}

/// 写出前对该对象现状的了解。
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum Base {
    /// 本进程尚未读到它：第一次写之前先读。
    #[default]
    Unloaded,
    /// 已读到（不存在或可解析）或已由本进程写出。
    Loaded,
    /// 读到版本过高或无法解析：此后不再写它。
    Frozen,
}

impl History {
    pub fn new(store: Store, retention: Duration, check_interval: Duration) -> Arc<History> {
        Arc::new(History {
            store,
            retention,
            check_interval,
            objects: Mutex::new(BTreeMap::new()),
            unwritten: AtomicU64::new(0),
        })
    }

    /// 写出与保留期删除的后台任务，直到被中止：每 [`TICK`] 写出有新记录的对象（写不出的留待下一轮）；自调用时起
    /// 一个 `check_interval` 之后、每隔 `check_interval` 删除过期的评估记录对象。在开始受理 `/api/` 请求时调用。
    pub fn spawn(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            let writing = async {
                loop {
                    tokio::time::sleep(TICK).await;
                    me.write_due().await;
                    me.evict();
                }
            };
            let pruning = async {
                let mut ticks = tokio::time::interval_at(
                    tokio::time::Instant::now() + me.check_interval,
                    me.check_interval,
                );
                ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    ticks.tick().await;
                    retention::sweep(&me.store, me.retention).await;
                }
            };
            tokio::join!(writing, pruning);
        })
    }

    /// 停机时对每个有未写出记录的对象尝试写出一次，失败不重试（REQ-AMB-ALR-EVALUATIONS-SHUTDOWN-FLUSH）。
    /// 在途评估都已交出记录之后调用；可与 flush、计数对象的写出同时进行。
    pub async fn flush_once(&self) {
        self.write_due().await;
    }

    /// 已结束、尚未写进评估记录对象的评估记录条数（REQ-AMB-ALR-EVALUATIONS-UNWRITTEN-METRIC）。
    pub fn unwritten(&self) -> u64 {
        self.unwritten.load(Ordering::SeqCst)
    }

    fn objects(&self) -> MutexGuard<'_, BTreeMap<ObjectKey, Object>> {
        self.objects.lock().expect("评估记录表未中毒")
    }

    /// 对有尚未写出元素、且未被登记为不再写的每个对象写出一次。
    async fn write_due(&self) {
        let due: Vec<ObjectKey> = self
            .objects()
            .iter()
            .filter(|(_, o)| o.base != Base::Frozen && !o.pending.is_empty())
            .map(|(k, _)| k.clone())
            .collect();
        // 依次写出：对象数为规则数乘以有新记录的小时数，一轮的耗时远小于写出时限
        for key in due {
            self.write_one(key).await;
        }
    }

    async fn write_one(&self, key: ObjectKey) {
        let path = key.path();
        if self.base(&key) == Some(Base::Unloaded) {
            let Some(Ok(current)) = timed(self.store.system_read(&path)).await else {
                return;
            };
            let parsed = current.map(|bytes| object::parse(&bytes)).transpose();
            let mut objects = self.objects();
            let Some(o) = objects.get_mut(&key) else {
                return;
            };
            match parsed {
                Ok(doc) => {
                    o.base = Base::Loaded;
                    o.written = doc.map(|d| d.evaluations).unwrap_or_default();
                }
                Err(_) => {
                    o.base = Base::Frozen;
                    return;
                }
            }
        }

        let (bytes, merged, taken) = {
            let objects = self.objects();
            let Some(o) = objects.get(&key) else {
                return;
            };
            if o.base != Base::Loaded || o.pending.is_empty() {
                return;
            }
            let merged = object::merge(&o.written, &o.pending);
            (
                object::encode(&o.org, &key.rule, &merged),
                merged,
                o.pending.len(),
            )
        };
        let Some(Ok(())) = timed(self.store.system_put(&path, bytes)).await else {
            return;
        };
        let mut objects = self.objects();
        if let Some(o) = objects.get_mut(&key) {
            o.written = merged;
            o.pending.drain(..taken);
            self.unwritten.fetch_sub(taken as u64, Ordering::SeqCst);
        }
    }

    fn base(&self, key: &ObjectKey) -> Option<Base> {
        self.objects().get(key).map(|o| o.base)
    }

    /// 写完且早于上一个小时的对象移出内存（登记为不再写的留着）。
    fn evict(&self) {
        let current = ObjectKey::of("", now_us()).hour;
        self.objects().retain(|k, o| {
            o.base == Base::Frozen || !o.pending.is_empty() || k.hour >= current - object::HOUR_US
        });
    }

    /// 为作答读到无法解析或版本过高的对象：此后写出也不覆盖它。
    fn freeze(&self, key: &ObjectKey) {
        self.objects().entry(key.clone()).or_default().base = Base::Frozen;
    }

    /// 作答所需的对象（REQ-AMB-ALR-EVALUATIONS-NAMED-READ、-RANGE-READ）：有点名规则时只列举这些规则的前缀
    /// （名字不合规则名正则的，不可能有对象，不列举），没有时列举 `_system/alerts/`；只留路径合模板、小时与范围有交集的。
    async fn listed(
        &self,
        rules: Option<&BTreeSet<String>>,
        start: i64,
        end: i64,
    ) -> Result<Vec<ObjectKey>, HistoryError> {
        let prefixes: Vec<String> = match rules {
            None => vec![object::ROOT.to_owned()],
            Some(names) => names
                .iter()
                .filter(|n| object::rule_name_shape(n))
                .map(|n| object::rule_prefix(n))
                .collect(),
        };
        let mut keys = BTreeSet::new();
        for prefix in prefixes {
            let paths = timed(self.store.system_list(&prefix))
                .await
                .ok_or_else(|| timeout("list", &prefix))?
                .map_err(unavailable)?;
            keys.extend(
                paths
                    .iter()
                    .filter_map(|p| ObjectKey::parse(p))
                    .filter(|k| k.overlaps(start, end)),
            );
        }
        Ok(keys.into_iter().collect())
    }
}

impl Recorder for History {
    fn record(&self, record: EvaluationRecord) {
        let key = ObjectKey::of(&record.rule, record.scheduled_at);
        let entry = Entry::of(&record);
        let mut objects = self.objects();
        let o = objects.entry(key).or_default();
        o.org = record.org;
        o.pending.push(entry);
        self.unwritten.fetch_add(1, Ordering::SeqCst);
    }
}

impl AlertHistory for History {
    /// 每次作答都重读对象存储（REQ-AMB-ALR-EVALUATIONS-SOURCE）。读取遇不可访问为 503 一类、无法解析为 500 一类；
    /// 两者都有时报先遇到的（-READ-ANY）。读到的无法解析的对象全部登记为不再写，再报错。
    fn evaluations<'a>(
        &'a self,
        org: &'a str,
        rules: Option<&'a BTreeSet<String>>,
        start: i64,
        end: i64,
    ) -> BoxFuture<'a, Result<Vec<EvaluationRow>, HistoryError>> {
        Box::pin(async move {
            let keys = self.listed(rules, start, end).await?;
            let mut first_error: Option<HistoryError> = None;
            let mut rows = Vec::new();
            for (key, got) in read_all(&self.store, keys).await {
                let doc = match got {
                    Ok(None) => continue,
                    Ok(Some(bytes)) => object::parse(&bytes),
                    Err(e) => {
                        first_error.get_or_insert(e);
                        continue;
                    }
                };
                match doc {
                    Ok(doc) if doc.org == org => rows.extend(
                        doc.evaluations
                            .iter()
                            .filter(|e| (start..end).contains(&e.scheduled_at))
                            .map(|e| e.row(&doc.rule)),
                    ),
                    Ok(_) => {}
                    Err(why) => {
                        self.freeze(&key);
                        first_error.get_or_insert(HistoryError::Unparseable(format!(
                            "evaluation record object _system/{} cannot be parsed: {why}",
                            key.path()
                        )));
                    }
                }
            }
            match first_error {
                Some(e) => Err(e),
                None => Ok(rows),
            }
        })
    }
}

/// 读一个对象的结果：不存在为 `Ok(None)`。
type Read = Result<Option<axum::body::Bytes>, HistoryError>;

/// 以至多 [`READ_CONCURRENCY`] 个并发读取各对象，结果按 `keys` 的次序返回。
async fn read_all(store: &Store, keys: Vec<ObjectKey>) -> Vec<(ObjectKey, Read)> {
    let mut results: Vec<Option<Read>> = (0..keys.len()).map(|_| None).collect();
    let mut set = JoinSet::new();
    for (i, key) in keys.iter().enumerate() {
        if set.len() >= READ_CONCURRENCY
            && let Some(Ok((j, r))) = set.join_next().await
        {
            results[j] = Some(r);
        }
        let store = store.clone();
        let path = key.path();
        set.spawn(async move {
            let r = match timed(store.system_read(&path)).await {
                None => Err(timeout("read", &path)),
                Some(r) => r.map_err(unavailable),
            };
            (i, r)
        });
    }
    while let Some(joined) = set.join_next().await {
        if let Ok((j, r)) = joined {
            results[j] = Some(r);
        }
    }
    keys.into_iter()
        .zip(results)
        .map(|(k, r)| {
            let r = r.unwrap_or_else(|| Err(HistoryError::Unavailable("read task failed".into())));
            (k, r)
        })
        .collect()
}

fn unavailable(e: Unavailable) -> HistoryError {
    HistoryError::Unavailable(e.0)
}

fn timeout(op: &str, path: &str) -> HistoryError {
    HistoryError::Unavailable(format!(
        "{op} _system/{path} did not finish within {}s",
        REQUEST_TIMEOUT.as_secs()
    ))
}

/// 超时的请求记为失败（`None`）。
async fn timed<T>(f: impl std::future::Future<Output = T>) -> Option<T> {
    tokio::time::timeout(REQUEST_TIMEOUT, f).await.ok()
}

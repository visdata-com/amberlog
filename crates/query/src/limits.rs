//! `_search` 的资源上限（REQ-AMB-QRY-RESOURCE-LIMITS，ADR-0045）：执行名额与每查询一份的内存池。
//!
//! - 执行名额：进程内计数，至多 `query.max_concurrency` 个 `_search` 同时占用；名额已满时准入立即失败
//!   （`too_many_queries`），不排队。名额由 [`Slot`] 持有，`Slot` 被丢弃即释放：`Engine::search` 在作答结果完整算出后
//!   返回，`Slot` 随之丢弃，此后才写响应，名额因此在响应的最后一个字节之前释放（SLOT-HOLD）。
//! - 内存：每个准入的 `_search` 一份容量为 `query.max_memory_bytes` 的 DataFusion 内存池，不与其他查询合计（MEMORY）；
//!   磁盘管理器禁用，引擎不溢写、不建临时文件（NO-SPILL）。排序（含前 K 行排序与两种缺省排序）与分组向池登记预留
//!   （DataFusion 55.1 实测）；预留失败时引擎报资源耗尽，映射为 `query_memory_exceeded`（MEMORY-ABORT）。
//!   读进来的批次与内存表不计入（METERED-SCOPE）；输入逐批交给执行，不计量的部分不随被扫数据量增长（[`crate::input`]）。
//! - 并行度：每查询至多 [`MAX_PARTITIONS`] 个并行分区（核数更少时取核数）。前 K 行排序等算子每个分区各暂存一批，计量
//!   峰值约为分区数乘一批；分区数取核数时，大核数机器上一条只取 100 行的查询也要上百 MiB，各分区轮流解码还使分配器的
//!   空闲内存散在更多分配区里。8 个分区时命中多的大范围查询只慢约两成（32 核机器实测）。
//! - 执行结束之后的内存（RSS-NO-GROWTH，REQ-AMB-QRY-RESOURCE-LIMITS-V2）：查询读进来的批次在执行结束时全部释放，但 glibc
//!   malloc 按线程分配区保留用过的内存、不还给系统，查询在不同的工作线程上执行，匿名常驻内存便随查询个数增长到各分配区
//!   高水位之和。所以有两种时机调用 `malloc_trim`，把各分配区的空闲内存还给系统：
//!   1. 每个占用者释放名额时，分配器的空闲内存超过 [`memory::FREE_BEFORE_RELEASE`] 就归还（[`memory::release_if_much_free`]，
//!      与封口合并、入湖、冷层合并共用）。查询
//!      首尾相接、名额从不空闲时（一个客户端连续提交，或几个客户端轮流占着名额），空闲内存也不越积越多。
//!   2. 执行名额空闲下来（最后一个占用者释放）之后 [`IDLE_BEFORE_RELEASE`] 内没有新的查询取得名额，就归还一次
//!      （[`release_when_idle`]），收拾不到 1 的阈值的余量。
//!
//!   空闲内存不多时不在查询之间归还：归还之后的下一次查询要重新向系统申请内存页，每次都归还会使连续的重查询吞吐明显
//!   下降。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use amberlog_common::error::Reason;
use amberlog_storage::memory;
use datafusion::error::DataFusionError;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::error::QueryError;

/// 执行名额与单查询内存上限，全进程一份（全部 `_search` 共用同一组名额，SCOPE）。
#[derive(Debug)]
pub struct Admission {
    max_concurrency: usize,
    max_memory_bytes: usize,
    running: AtomicUsize,
    /// 累计取得名额的次数：判定名额空闲之后是否有新的查询取得过名额。
    admitted: AtomicU64,
}

impl Admission {
    /// `query.max_concurrency` 与 `query.max_memory_bytes`（都不小于 1，由配置校验保证）。
    pub fn new(max_concurrency: usize, max_memory_bytes: usize) -> Arc<Admission> {
        Arc::new(Admission {
            max_concurrency,
            max_memory_bytes,
            running: AtomicUsize::new(0),
            admitted: AtomicU64::new(0),
        })
    }

    /// 此刻占用执行名额的 `_search` 数（`amberlog_search_running`）。
    pub fn running(&self) -> usize {
        self.running.load(Ordering::SeqCst)
    }

    /// 准入：取得一个执行名额；已有 `query.max_concurrency` 个占用时立即以 `too_many_queries` 失败。
    pub(crate) fn admit(self: &Arc<Self>) -> Result<Slot, QueryError> {
        let mut n = self.running.load(Ordering::SeqCst);
        loop {
            if n >= self.max_concurrency {
                return Err(QueryError::new(
                    Reason::TooManyQueries,
                    format!(
                        "too many queries are running (query.max_concurrency = {})",
                        self.max_concurrency
                    ),
                ));
            }
            match self
                .running
                .compare_exchange(n, n + 1, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break,
                Err(now) => n = now,
            }
        }
        self.admitted.fetch_add(1, Ordering::SeqCst);
        Ok(Slot {
            admission: Arc::clone(self),
        })
    }
}

/// 默认配置的上限：`query.max_concurrency` 4、`query.max_memory_bytes` 536870912（REQ-AMB-CFG-STARTUP-V7 默认值表）。
impl Default for Admission {
    fn default() -> Self {
        Admission {
            max_concurrency: 4,
            max_memory_bytes: 536_870_912,
            running: AtomicUsize::new(0),
            admitted: AtomicU64::new(0),
        }
    }
}

/// 每查询的并行分区数上限（执行环境的 `target_partitions`，数据源随之交出同样多的分区）。
const MAX_PARTITIONS: usize = 8;

/// 一个已准入的 `_search` 所占的执行名额，丢弃即释放。
pub(crate) struct Slot {
    admission: Arc<Admission>,
}

impl Slot {
    /// 本查询的执行环境：一份容量为 `query.max_memory_bytes` 的内存池，磁盘管理器禁用，并行度至多
    /// [`MAX_PARTITIONS`]。同一查询的各段执行（聚合查询的两段）共用这一个环境，计量合在本查询之内。
    pub(crate) fn context(&self) -> Result<SessionContext, QueryError> {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(GreedyMemoryPool::new(
                self.admission.max_memory_bytes,
            )))
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_mode(DiskManagerMode::Disabled),
            )
            .build_arc()
            .map_err(|e| QueryError::internal(e.to_string()))?;
        let config = SessionConfig::new();
        let partitions = config.target_partitions().min(MAX_PARTITIONS);
        Ok(SessionContext::new_with_config_rt(
            config.with_target_partitions(partitions),
            runtime,
        ))
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        // 先记下取得名额的次数再释放：此后有查询取得名额，次数必然变化
        let admitted = self.admission.admitted.load(Ordering::SeqCst);
        let last = self.admission.running.fetch_sub(1, Ordering::SeqCst) == 1;
        memory::release_if_much_free();
        if last {
            release_when_idle(Arc::clone(&self.admission), admitted);
        }
    }
}

/// 执行名额空闲之后等多久再归还空闲内存（明显短于查询之间可能的间隔，又足以让首尾相接的查询接上）。
const IDLE_BEFORE_RELEASE: Duration = Duration::from_secs(1);

/// 名额空闲（`admitted` 为空闲时累计取得名额的次数）之后 [`IDLE_BEFORE_RELEASE`] 内一直没有查询取得名额，就把分配器各
/// 分配区的空闲内存还给系统（glibc `malloc_trim(0)`）；其间有查询取得过名额即作罢，由它释放名额时另行判定。归还放到阻塞
/// 线程池里做，不占查询与响应的线程。不在 tokio 运行时之内（如同步的单元测试）时不做。其他平台与分配器上是空函数。
fn release_when_idle(admission: Arc<Admission>, admitted: u64) {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        runtime.spawn(async move {
            tokio::time::sleep(IDLE_BEFORE_RELEASE).await;
            if admission.admitted.load(Ordering::SeqCst) != admitted {
                return;
            }
            let _ = tokio::task::spawn_blocking(|| {
                // SAFETY: malloc_trim 只整理分配器自己的空闲内存，没有入参指针，可在任意线程调用。
                unsafe {
                    libc::malloc_trim(0);
                }
            })
            .await;
        });
    }
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    let _ = (admission, admitted);
}

/// 执行阶段的引擎错误：根错误为资源耗尽（内存池预留失败）即 `query_memory_exceeded`，其余按内部错误。
pub(crate) fn execution_error(e: DataFusionError) -> QueryError {
    if matches!(e.find_root(), DataFusionError::ResourcesExhausted(_)) {
        QueryError::new(
            Reason::QueryMemoryExceeded,
            "the query needs more memory than query.max_memory_bytes allows",
        )
    } else {
        QueryError::internal(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admit_until_full_then_release() {
        let a = Admission::new(2, 1 << 20);
        let s1 = a.admit().unwrap();
        let s2 = a.admit().unwrap();
        assert_eq!(a.running(), 2);
        assert_eq!(a.admit().err().unwrap().reason, Reason::TooManyQueries);
        drop(s1);
        assert_eq!(a.running(), 1);
        let _s3 = a.admit().unwrap();
        drop(s2);
        assert_eq!(a.running(), 1);
    }

    #[test]
    fn resources_exhausted_maps_to_query_memory_exceeded() {
        let e = DataFusionError::ResourcesExhausted("x".into()).context("while sorting");
        assert_eq!(execution_error(e).reason, Reason::QueryMemoryExceeded);
        let e = DataFusionError::Execution("boom".into());
        assert_eq!(execution_error(e).reason, Reason::Internal);
    }
}

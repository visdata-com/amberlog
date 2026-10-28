use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use amberlog_query::system::{CounterError, CounterObjects, CounterWindow};
use amberlog_storage::BoxFuture;
use amberlog_storage::registry::{DatasetRegistry, Persistence};

use super::*;

/// 记下每次后台判定开始读计数的次数；第 `block_from` 次起的读取一直挂起（模拟阻塞在读取计数的一步）。
struct Counters {
    reads: AtomicUsize,
    block_from: usize,
}

impl CounterObjects for Counters {
    fn windows<'a>(
        &'a self,
        _: &'a Dataset,
        _: i64,
        _: i64,
    ) -> BoxFuture<'a, Result<Vec<CounterWindow>, CounterError>> {
        Box::pin(std::future::ready(Ok(Vec::new())))
    }

    fn current_hours<'a>(
        &'a self,
        _: &'a Dataset,
        _: i64,
        _: i64,
    ) -> BoxFuture<'a, Vec<amberlog_query::system::HourCounts>> {
        let n = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
        let block = n >= self.block_from;
        Box::pin(async move {
            if block {
                std::future::pending::<()>().await;
            }
            Vec::new()
        })
    }
}

#[derive(Default)]
struct NoEvents(Mutex<usize>);

impl VerdictEvents for NoEvents {
    fn missing(&self, _: &Dataset, _: i64, _: &str, _: u64) {
        *self.0.lock().unwrap() += 1;
    }
    fn silent(&self, _: &Dataset, _: &str, _: i64) {
        *self.0.lock().unwrap() += 1;
    }
}

/// 进入停机过程后不再开始新的后台判定：周期 1 秒，第一次后台判定做完、第二次阻塞在读取计数时发出停机
/// （启动流程中止任务），此后 3 个周期内没有新开始的后台判定，任务在 1 个周期内结束，不等阻塞的那一次。
#[tokio::test]
async fn missed_collection_no_new_round_after_shutdown() {
    let registry = Arc::new(DatasetRegistry::new());
    registry.record(&Dataset::new("default", "app"), Persistence::Stored);
    let counters = Arc::new(Counters {
        reads: AtomicUsize::new(0),
        block_from: 2,
    });
    let background = Arc::new(Background::new(
        registry,
        counters.clone(),
        Arc::new(amberlog_query::testing::FakeBackfill::new()),
        Arc::new(NoEvents::default()),
        60_000_000,
        Box::new(|| Some(0)),
    ));
    let period = Duration::from_secs(1);
    let task = spawn(background, period);
    // 等第二次开始、阻塞
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while counters.reads.load(Ordering::SeqCst) < 2 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "第二次后台判定没有开始"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    task.abort();
    let ended = tokio::time::timeout(period, task).await;
    assert!(ended.is_ok(), "任务没有在 1 个周期内结束");
    tokio::time::sleep(3 * period).await;
    assert_eq!(
        counters.reads.load(Ordering::SeqCst),
        2,
        "停机之后开始了新的后台判定"
    );
}

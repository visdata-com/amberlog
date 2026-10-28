//! 系统表与后台判定的依赖接线（REQ-AMB-GOV-LEDGER-V3、REQ-AMB-GOV-RECONCILIATION-V3、REQ-AMB-GOV-MISSED-COLLECTION）：把治理计数的读回（amberlog-ingest）与
//! 桶状态的判定（桶生命周期）接到 amberlog-query 的窄接口 [`CounterObjects`]、[`BucketStates`] 上。
//! query 不依赖 ingest，两者在接入面这里相接，crate 依赖方向不变。

use std::sync::Arc;

use amberlog_common::dataset::Dataset;
use amberlog_ingest::counters::{Counters, PersistedWindow, ReadError};
use amberlog_query::system::{
    BucketStates, CounterError, CounterObjects, CounterWindow, Governance, Retention,
};
use amberlog_storage::BoxFuture;
use amberlog_storage::chain::BucketRef;
use amberlog_storage::layout::HourBucket;

use amberlog_storage::lifecycle::{BucketState, Lifecycle};

use super::super::{Settings, Storage};

pub(crate) fn governance(storage: &Storage, settings: &Settings) -> Governance {
    Governance {
        registry: Arc::clone(&storage.registry),
        counters: Arc::new(PersistedCounters(Arc::clone(&storage.counters))),
        buckets: Arc::new(LifecycleBuckets(settings.lifecycle)),
        alerts: Arc::clone(&storage.alerts),
        retention: settings.retention.as_ref().map(|r| Retention {
            text: r.text.clone(),
            micros: i64::try_from(r.period.as_micros()).unwrap_or(i64::MAX),
        }),
        backfill: settings.backfill.clone(),
        grace_us: settings.backfill.grace_us(),
    }
}

/// 计数对象的读取（[`Counters::read_persisted`]、[`Counters::current_hours`]）。
pub(crate) struct PersistedCounters(pub(crate) Arc<Counters>);

fn window(w: PersistedWindow) -> CounterWindow {
    CounterWindow {
        window_start: w.window_start,
        window_end: w.window_end,
        received: w.received,
        durable: w.durable,
        sent: w.sent,
        durable_ts: w.durable_ts,
        durable_ts_by_source: w.durable_ts_by_source,
        sent_ts: w.sent_ts,
    }
}

fn error(e: ReadError) -> CounterError {
    match e {
        ReadError::Unavailable(m) => CounterError::Unavailable(m),
        ReadError::Unparseable(m) => CounterError::Unparseable(m),
    }
}

impl CounterObjects for PersistedCounters {
    fn windows<'a>(
        &'a self,
        dataset: &'a Dataset,
        start: i64,
        end: i64,
    ) -> BoxFuture<'a, Result<Vec<CounterWindow>, CounterError>> {
        Box::pin(async move {
            self.0
                .read_persisted(dataset, start, end)
                .await
                .map(|ws| ws.into_iter().map(window).collect())
                .map_err(error)
        })
    }

    fn current_hours<'a>(
        &'a self,
        dataset: &'a Dataset,
        start: i64,
        end: i64,
    ) -> BoxFuture<'a, Vec<amberlog_query::system::HourCounts>> {
        Box::pin(async move {
            self.0
                .current_hours(dataset, start, end)
                .await
                .into_iter()
                .map(|(hour, r)| {
                    (
                        hour,
                        r.map(|ws| ws.into_iter().map(window).collect())
                            .map_err(error),
                    )
                })
                .collect()
        })
    }
}

/// 桶生命周期（REQ-AMB-STO-BUCKET-LIFECYCLE）的状态判定。
struct LifecycleBuckets(Lifecycle);

impl BucketStates for LifecycleBuckets {
    fn pending_lake(&self, bucket: &BucketRef, now_us: i64) -> bool {
        HourBucket::parse(&bucket.bucket)
            .is_some_and(|hour| self.0.state(hour, now_us) == BucketState::PendingLake)
    }
}

//! 摄取管道的一次写入：原始请求体字节 + 目标数据集 + 服务端接收时刻 → 逐条状态应答（REQ-AMB-ING-ACK-RESPONSE）。
//!
//! 输入不含 HTTP 概念：请求级校验（停机闸门、认证、路径段、数据集状态闸门、
//! `Content-Encoding`、请求体大小与解码）由接入面在交付之前完成。一次写入依次：
//! 1. 解析请求体（整体非法 → `invalid_json`；单对象与标量批次化；含取值缺陷的元素逐条 `malformed_record`）；
//! 2. 确保数据集存在（快照提交）；创世快照尚未提交成功时，数据集的存在随本批写入 WAL；
//! 3. 取得该数据集的判定锁，按 stream 当前 schema 逐条判定、确定各字段的目标列（同批序号较小的已接受记录登记的列对其后各条可见），
//!    并把被接受记录落入的列登记进当前 schema（在途，[`MemtableWriter::reserve`]）；
//! 4. 仍在判定锁内追加 WAL（位置在调用时排定，判定与登记的先后因此与 WAL 先后一致），随即放开判定锁、等待 fsync；
//! 5. 按 WAL 位置的顺序：fsync 成功 → 写入可见集（记录即刻可查、在途列确认），再应答；
//!    失败 → 503 `unavailable`，撤回本批对在途列的引用（无其他在途批次引用、也未被确认的列退出当前 schema）。
//!
//! 判定锁使同一数据集的「确定目标列 + 登记新列」不可分，并发的两批不会为同一基名与同一类型各登记一列
//! （REQ-AMB-DM-SCHEMA-DRIFT、REQ-AMB-STO-HOT-LAYOUT-V2）；放开判定锁之后其他批次即可判定，在途登记的列对它们属于当前 schema。
//! 第 4 步之后的工作在独立的任务里执行，请求被取消也会做完，同一数据集的第 5 步不会因此卡住。
//!
//! 接上治理计数（[`Pipeline::with_counters`]）时，被确认批次的计数增量（含 `rejected` 与来源标识）随记录同一次追加
//! （ADR-0023 第 1 条），全拒批次（`received`、`rejected`）与 503 批次（`received`、`received_503`）只在内存中计入；
//! 来源标识（REQ-AMB-ING-JSON-ENDPOINT-V3）只交给计数，不进入记录的构造与逐条处置；`_sent` 请求（[`Pipeline::report_sent`]）同样在判定锁内追加、按 WAL 位置顺序计入，
//! 使同一数据集的可重放计入按 WAL 位置顺序生效（第 3 条）。

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex};

use amberlog_common::dataset::Dataset;
use amberlog_common::error::{ErrorBody, Reason, RecordReason};
use amberlog_common::json::{Element, parse_ingest_body};
use amberlog_common::record::Record;
use amberlog_common::schema::ColumnType;
use amberlog_storage::chain::SnapshotId;
use amberlog_storage::commit::{Committer, Existence};
use amberlog_storage::fault;
use amberlog_storage::layout::HourBucket;
use amberlog_storage::lifecycle::{BucketState, Lifecycle};
use amberlog_storage::memtable::{MemtableView, MemtableWriter};
use amberlog_storage::registry::{DatasetRegistry, Persistence};
use amberlog_storage::schema::StreamColumns;
use amberlog_storage::wal::{Wal, WalEntry, WalPosition};
use serde_json::{Map, Value, json};

use crate::counters::{Counters, SentError, Tally, durable_ts_groups, parse_sent, window_of};
use crate::record::process_batch;
use crate::timestamp::TimestampWindow;

/// `ingest.max_columns` 的缺省值（REQ-AMB-DM-FIELD-LIMITS）。
pub const DEFAULT_MAX_COLUMNS: usize = 1000;

/// 摄取一侧用到的存储句柄与受理配置。
pub struct Pipeline {
    inner: Arc<Inner>,
}

struct Inner {
    wal: Arc<dyn Wal>,
    memtable: Arc<dyn MemtableView>,
    writer: Arc<dyn MemtableWriter>,
    committer: Arc<dyn Committer>,
    /// 数据集的存在持久到了哪一步（存在随本批写入 WAL 成功后登记为 [`Persistence::LocalWal`]）。
    registry: Arc<DatasetRegistry>,
    window: TimestampWindow,
    /// `ingest.max_columns`。
    max_columns: usize,
    /// 各数据集在本进程受理记录时所依据的 schema 基线快照号（启动时读入；本进程新建的数据集为 `None`）。
    baselines: Mutex<HashMap<Dataset, Option<SnapshotId>>>,
    /// 本进程已为其在 WAL 中记下基线快照号的数据集。
    baseline_logged: Mutex<HashSet<Dataset>>,
    slots: Mutex<HashMap<Dataset, Arc<Slot>>>,
    counters: Option<Arc<Counters>>,
    /// 迟到记录的判定（REQ-AMB-STO-BUCKET-LIFECYCLE）。
    lifecycle: Lifecycle,
}

/// 一个数据集的判定锁与 WAL 之后各步的次序。
#[derive(Default)]
struct Slot {
    judge: tokio::sync::Mutex<()>,
    order: Sequencer,
}

/// 按追加 WAL 的先后发号，WAL 之后的步骤按号依次执行（可见集写入须按 WAL 位置递增，[`MemtableWriter::write`]）。
#[derive(Default)]
struct Sequencer {
    /// （下一个号，当前轮到的号）
    state: Mutex<(u64, u64)>,
    turn_changed: tokio::sync::Notify,
}

impl Sequencer {
    /// 发号：须与追加 WAL 在同一把判定锁内。
    fn ticket(&self) -> u64 {
        let mut s = self.state.lock().expect("未中毒");
        s.0 += 1;
        s.0 - 1
    }

    /// 等到轮到 `ticket`；返回的守卫释放时轮到下一个号（含 panic 时）。
    async fn turn(&self, ticket: u64) -> Turn<'_> {
        loop {
            let changed = self.turn_changed.notified();
            let mut changed = std::pin::pin!(changed);
            changed.as_mut().enable();
            if self.state.lock().expect("未中毒").1 == ticket {
                return Turn(self);
            }
            changed.await;
        }
    }
}

struct Turn<'a>(&'a Sequencer);

impl Drop for Turn<'_> {
    fn drop(&mut self) {
        self.0.state.lock().expect("未中毒").1 += 1;
        self.0.turn_changed.notify_waiters();
    }
}

/// 一个 `status` 条目：批次中每条记录的处置结果。
#[derive(Debug, Clone, PartialEq)]
pub struct Status {
    pub name: String,
    pub successful: u64,
    pub failed: u64,
    /// 失败记录的 `index` 与 `reason`，按 `index` 升序。
    pub errors: Vec<(usize, RecordReason)>,
    /// 被接受的记录中的迟到记录，按接收时刻目标桶的状态分路（不进应答体，供指标计数）。
    pub late: Late,
    /// 被接受的记录中最早的 `_timestamp`（受理时归一后的取值）；没有被接受的记录为 `None`。不进应答体，
    /// 供漏采判定的补发中状态判断本批有没有迟到记录（REQ-AMB-GOV-MISSED-COLLECTION-TERM-LATE-RECORD）。
    pub earliest_timestamp_us: Option<i64>,
}

/// 迟到记录按接收时刻目标桶的状态分路的条数（REQ-AMB-STO-BUCKET-LIFECYCLE「迟到记录」）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Late {
    pub sealed: u64,
    pub pending_lake: u64,
}

impl Status {
    /// `status` 数组的一个元素（HTTP 200 响应体与 400 `details.status` 共用）。
    pub fn to_json(&self) -> Value {
        let errors: Vec<Value> = self
            .errors
            .iter()
            .map(|(index, reason)| {
                json!({
                    "index": index,
                    "reason": reason.as_str(),
                    "message": record_message(*reason),
                })
            })
            .collect();
        json!({
            "name": self.name,
            "successful": self.successful,
            "failed": self.failed,
            "errors": errors,
        })
    }

    /// HTTP 200 的响应体：`{"code":200,"status":[…]}`。
    pub fn ok_body(&self) -> Value {
        json!({ "code": 200, "status": [self.to_json()] })
    }
}

/// 一次 `_sent` 请求不以 HTTP 200 作答的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SentRejected {
    /// 400 `invalid_json` 或 `invalid_request`。
    Invalid(SentError),
    /// WAL 写入或 fsync 失败（503 `unavailable`），带 WAL 错误的原文（只进日志，不进响应）。
    Unavailable(String),
}

/// 一次写入不以 HTTP 200 作答的结果。
#[derive(Debug, Clone, PartialEq)]
pub enum Rejected {
    /// 请求体整体不是合法 JSON（400 `invalid_json`）。
    InvalidJson,
    /// 批次含记录且全部被逐条拒绝（400 `all_records_rejected`，明细在 `details.status`）。
    AllRecordsRejected(Status),
    /// WAL 写入或 fsync 失败（503 `unavailable`）：本批不视为已确认，此后不可见，其新列不进 schema。带 WAL 错误的
    /// 原文（只进日志，不进响应）。
    Unavailable(String),
}

impl Rejected {
    pub fn error_body(&self) -> ErrorBody {
        match self {
            Rejected::InvalidJson => {
                ErrorBody::new(Reason::InvalidJson, "the request body is not valid JSON")
            }
            Rejected::AllRecordsRejected(status) => {
                let mut details = Map::new();
                details.insert("status".into(), Value::Array(vec![status.to_json()]));
                ErrorBody::new(
                    Reason::AllRecordsRejected,
                    "every record in the batch was rejected; retrying the same batch will not succeed",
                )
                .with_details(details)
            }
            Rejected::Unavailable(_) => ErrorBody::new(
                Reason::Unavailable,
                "the batch could not be made durable; it was not accepted and may be retried",
            ),
        }
    }
}

fn record_message(reason: RecordReason) -> &'static str {
    match reason {
        RecordReason::MalformedRecord => {
            "the record contains invalid UTF-8, an unpaired surrogate, a number out of range, or nesting deeper than 128"
        }
        RecordReason::NotAnObject => "the record is not a JSON object",
        RecordReason::ReservedField => "top-level keys starting with '_' are reserved",
        RecordReason::InvalidFieldName => {
            "a field name is invalid or two fields flatten to the same name"
        }
        RecordReason::TimestampInvalid => "_timestamp is not a valid timestamp",
        RecordReason::TimestampOutOfRange => "_timestamp is outside the accepted time window",
        RecordReason::TypeConflict => {
            "a field's type differs from the stream's existing column type"
        }
        RecordReason::TooManyColumns => {
            "the record would make the stream exceed ingest.max_columns"
        }
    }
}

impl Pipeline {
    pub fn new(
        wal: Arc<dyn Wal>,
        memtable: Arc<dyn MemtableView>,
        writer: Arc<dyn MemtableWriter>,
        committer: Arc<dyn Committer>,
        registry: Arc<DatasetRegistry>,
        window: TimestampWindow,
        baselines: HashMap<Dataset, Option<SnapshotId>>,
    ) -> Pipeline {
        Pipeline {
            inner: Arc::new(Inner {
                wal,
                memtable,
                writer,
                committer,
                registry,
                window,
                max_columns: DEFAULT_MAX_COLUMNS,
                baselines: Mutex::new(baselines),
                baseline_logged: Mutex::new(Default::default()),
                slots: Mutex::new(HashMap::new()),
                counters: None,
                lifecycle: Lifecycle::default(),
            }),
        }
    }

    fn building(&mut self) -> &mut Inner {
        Arc::get_mut(&mut self.inner).expect("构建期间管道尚未共享")
    }

    /// 以 `lifecycle.seal_grace`、`lifecycle.lake_watermark` 的取值判定迟到记录（缺省为两者的默认值）。
    pub fn with_lifecycle(mut self, lifecycle: Lifecycle) -> Pipeline {
        self.building().lifecycle = lifecycle;
        self
    }

    /// 接上治理计数：此后记计数增量、受理 `_sent`。
    pub fn with_counters(mut self, counters: Arc<Counters>) -> Pipeline {
        self.building().counters = Some(counters);
        self
    }

    /// 设定 `ingest.max_columns`（缺省 [`DEFAULT_MAX_COLUMNS`]）。
    pub fn with_max_columns(mut self, max_columns: usize) -> Pipeline {
        self.building().max_columns = max_columns;
        self
    }

    /// 处理一次不带来源标识的 `_json` 写入。
    pub async fn ingest(
        &self,
        dataset: &Dataset,
        body: &[u8],
        received_at_us: i64,
    ) -> Result<Status, Rejected> {
        self.ingest_from(dataset, body, received_at_us, None).await
    }

    /// 处理一次 `_json` 写入；`source` 为请求合法的来源标识（接入面已按取值规则校验），只用于按来源计数。
    pub async fn ingest_from(
        &self,
        dataset: &Dataset,
        body: &[u8],
        received_at_us: i64,
        source: Option<String>,
    ) -> Result<Status, Rejected> {
        let elements = parse_batch(body).ok_or(Rejected::InvalidJson)?;
        let inner = Arc::clone(&self.inner);
        let dataset = dataset.clone();
        detached(async move {
            inner
                .ingest(&dataset, elements, received_at_us, source.as_deref())
                .await
        })
        .await
    }

    /// 处理一次 `_sent` 上报（REQ-AMB-OBS-INGEST-COUNTERS）：校验请求体（任一元素不合规即整个请求被拒）→
    /// 确保数据集存在（同 `_json`）→ 在该数据集的判定锁内把全部元素作为一个 WAL 条目追加 → fsync 成功后按 WAL 位置顺序计入治理计数。
    /// 返回受理的元素（按数组顺序）。未接治理计数时不应被调用。
    pub async fn report_sent(
        &self,
        dataset: &Dataset,
        body: &[u8],
    ) -> Result<Vec<amberlog_storage::wal::SentElement>, SentRejected> {
        let elements = parse_sent(body).map_err(SentRejected::Invalid)?;
        let inner = Arc::clone(&self.inner);
        let dataset = dataset.clone();
        detached(async move { inner.report_sent(&dataset, elements).await }).await
    }
}

/// 在独立的任务里做完 `work`：调用方被取消也不中断（WAL 之后的步骤与发出的号必须做完）。
async fn detached<T: Send + 'static>(work: impl Future<Output = T> + Send + 'static) -> T {
    match tokio::spawn(work).await {
        Ok(v) => v,
        Err(e) => std::panic::resume_unwind(e.into_panic()),
    }
}

impl Inner {
    fn slot(&self, dataset: &Dataset) -> Arc<Slot> {
        let mut slots = self.slots.lock().expect("摄取锁表未中毒");
        Arc::clone(slots.entry(dataset.clone()).or_default())
    }

    async fn ingest(
        &self,
        dataset: &Dataset,
        elements: Vec<Element>,
        received_at_us: i64,
        source: Option<&str>,
    ) -> Result<Status, Rejected> {
        let window = window_of(received_at_us);
        let received = elements.len() as u64;
        let existence = self.committer.ensure_dataset(dataset).await;
        // 进程内目录由可见集持有：新建（含存在尚未持久化）的数据集以创世 schema 登记，已在目录中的不变
        self.writer.register(dataset, StreamColumns::genesis());

        let slot = self.slot(dataset);
        let mut judging = slot.judge.lock().await;
        let schema = self
            .memtable
            .schema(dataset)
            .unwrap_or_else(StreamColumns::genesis);
        let outcome = process_batch(
            &elements,
            received_at_us,
            &self.window,
            &schema,
            self.max_columns,
        );

        let mut accepted: Vec<Record> = Vec::new();
        let mut errors = Vec::new();
        for (index, result) in outcome.results.into_iter().enumerate() {
            match result {
                Ok(record) => accepted.push(record),
                Err(reason) => errors.push((index, reason)),
            }
        }
        // 迟到记录按接收时刻（同一请求的记录共用）与目标桶的封口时刻、入湖水位时刻分路（不看目标桶此刻是否含存活热层
        // 数据文件，REQ-AMB-STO-BUCKET-LIFECYCLE-V2）
        let mut late = Late::default();
        for r in &accepted {
            match self
                .lifecycle
                .state(HourBucket::of(r.timestamp_us), received_at_us)
            {
                BucketState::Open => {}
                BucketState::Sealed => late.sealed += 1,
                BucketState::PendingLake => late.pending_lake += 1,
            }
        }
        let status = Status {
            name: dataset.stream.clone(),
            successful: accepted.len() as u64,
            failed: errors.len() as u64,
            errors,
            late,
            earliest_timestamp_us: accepted.iter().map(|r| r.timestamp_us).min(),
        };

        // 被接受记录落入的列：新列自此属于当前 schema，在途列记下本批的引用
        let columns = target_columns(&accepted);
        self.writer.reserve(dataset, &columns);
        if fault::claim_ingest_hold() {
            // 测试专用（fault-injection）：本批停在登记之后、追加 WAL 之前，期间放开判定锁让其他批次判定
            drop(judging);
            fault::ingest_hold_released().await;
            judging = slot.judge.lock().await;
        }

        let mut entries = Vec::new();
        if existence == Existence::Pending {
            entries.push(WalEntry::DatasetExists {
                dataset: dataset.clone(),
            });
        }
        let first_acceptance = !accepted.is_empty()
            && !self
                .baseline_logged
                .lock()
                .expect("未中毒")
                .contains(dataset);
        if first_acceptance {
            let snapshot_id = self
                .baselines
                .lock()
                .expect("未中毒")
                .get(dataset)
                .copied()
                .flatten();
            entries.push(WalEntry::Baseline {
                dataset: dataset.clone(),
                snapshot_id,
            });
        }
        let counts = self.counters.as_ref().filter(|_| !accepted.is_empty());
        // 按记录时间戳的确认数与本批同一次追加（REQ-AMB-OBS-INGEST-COUNTERS-V3、ADR-0053 第 1 条）
        let durable_ts = durable_ts_groups(accepted.iter().map(|r| r.timestamp_us));
        if counts.is_some() {
            entries.push(WalEntry::Counts {
                dataset: dataset.clone(),
                window_start_us: window,
                received,
                durable: status.successful,
                rejected: status.failed,
                source: source.map(str::to_owned),
                durable_ts: Some(durable_ts.clone()),
            });
        }
        if !accepted.is_empty() {
            entries.push(WalEntry::Records {
                dataset: dataset.clone(),
                records: accepted.clone(),
            });
        }

        if !entries.is_empty() {
            // 位置在调用时排定：判定锁内追加，同一数据集的判定先后即 WAL 先后
            let append = self.wal.append(entries);
            let ticket = slot.order.ticket();
            drop(judging);
            let appended = append.await;
            let _turn = slot.order.turn(ticket).await;
            let last = match appended {
                Ok(last) => last,
                Err(e) => {
                    self.writer.unreserve(dataset, &columns);
                    if let Some(c) = &self.counters {
                        let tally = Tally {
                            received,
                            received_503: received,
                            ..Tally::default()
                        };
                        c.record_unconfirmed(dataset, window, tally, source);
                    }
                    return Err(Rejected::Unavailable(e.to_string()));
                }
            };
            if existence == Existence::Pending {
                self.registry.record(dataset, Persistence::LocalWal);
            }
            if first_acceptance {
                self.baseline_logged
                    .lock()
                    .expect("未中毒")
                    .insert(dataset.clone());
            }
            if let Some(c) = counts {
                // Counts 紧挨在最后一个条目 Records 之前
                let tally = Tally {
                    received,
                    durable: status.successful,
                    rejected: status.failed,
                    received_503: 0,
                };
                c.record_durable(
                    dataset,
                    window,
                    tally,
                    source,
                    &durable_ts,
                    WalPosition(last.0 - 1),
                );
            }
            if !accepted.is_empty() {
                // Records 是本次追加的最后一个条目
                self.writer.write(dataset, last, accepted);
            }
        }
        if status.successful == 0
            && let Some(c) = &self.counters
        {
            let tally = Tally {
                received,
                rejected: status.failed,
                ..Tally::default()
            };
            c.record_unconfirmed(dataset, window, tally, source);
        }

        if status.successful == 0 && status.failed > 0 {
            return Err(Rejected::AllRecordsRejected(status));
        }
        Ok(status)
    }

    async fn report_sent(
        &self,
        dataset: &Dataset,
        elements: Vec<amberlog_storage::wal::SentElement>,
    ) -> Result<Vec<amberlog_storage::wal::SentElement>, SentRejected> {
        let existence = self.committer.ensure_dataset(dataset).await;
        self.writer.register(dataset, StreamColumns::genesis());

        let slot = self.slot(dataset);
        let judging = slot.judge.lock().await;
        let mut entries = Vec::new();
        if existence == Existence::Pending {
            entries.push(WalEntry::DatasetExists {
                dataset: dataset.clone(),
            });
        }
        if !elements.is_empty() {
            entries.push(WalEntry::Sent {
                dataset: dataset.clone(),
                elements: elements.clone(),
            });
        }
        if entries.is_empty() {
            return Ok(elements);
        }
        let append = self.wal.append(entries);
        let ticket = slot.order.ticket();
        drop(judging);
        let appended = append.await;
        let _turn = slot.order.turn(ticket).await;
        let last = appended.map_err(|e| SentRejected::Unavailable(e.to_string()))?;
        if existence == Existence::Pending {
            self.registry.record(dataset, Persistence::LocalWal);
        }
        if !elements.is_empty()
            && let Some(c) = &self.counters
        {
            // Sent 是本次追加的最后一个条目
            c.record_sent(dataset, &elements, last);
        }
        Ok(elements)
    }
}

/// 被接受的记录落入的全部列（目标列名与类型），按首次出现的顺序、不重复。
fn target_columns(records: &[Record]) -> Vec<(String, ColumnType)> {
    let mut seen = HashSet::new();
    records
        .iter()
        .flat_map(|r| &r.fields)
        .filter(|(name, _)| seen.insert(name.as_str()))
        .map(|(name, value)| (name.clone(), value.column_type()))
        .collect()
}

/// 解析请求体并批次化：数组的每个元素一条记录，单个对象或标量为仅含一个元素的批次；整体不是合法 JSON 时为 `None`
/// （含取值缺陷的元素不使请求体整体非法，REQ-AMB-ING-RECORD-ISOLATION）。
fn parse_batch(body: &[u8]) -> Option<Vec<Element>> {
    parse_ingest_body(body).ok()
}

#[cfg(test)]
mod tests;

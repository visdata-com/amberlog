//! 查询执行的数据源：把输入逐批交给 DataFusion，读一批交一批，不先把全部输入攒在内存里（REQ-AMB-QRY-RESOURCE-LIMITS-V2
//! METERED-SCOPE：不计量的占用不随被扫数据量增长）。
//!
//! - **只取用到的列**：表接口 [`Table`] 把优化器算出的列投影交给数据源，数据源只解码这些列（[`Source::next`] 的 `schema`）。
//! - **只读一遍**：同一个数据源只执行一次（聚合查询的第二段读的是第一段的结果，不再读它）。
//! - **多个分区按需取批**：表交出与执行并行度（`target_partitions`）相同个数的分区，引擎因此不再插入轮转分发；各分区
//!   在下游处理完上一批之后才从同一个数据源取下一批，中间没有队列。已交出、尚未被取走下一批的批次因此至多与分区数
//!   相同，不随被扫数据量增长（轮转分发的队列没有容量上限，下游较慢的分区会积压批次）。读取仍由同一个数据源串行进行，
//!   读取失败与「数据已移动」的重判照旧逐个文件确定地发生。
//! - **全有或全无**（ADR-0037）：执行没有取完输入就结束时（如 `LIMIT 0`），[`Input::finish`] 把其余输入读完，作答所需的
//!   每个对象都要读到；读取中途的失败记在旁路，先于执行本身的错误报告（读取的 503、500 排在 `numeric_overflow` 之前，
//!   REQ-AMB-QRY-SEARCH-ENDPOINT 的报告顺序）。
//! - **暂停点**：交出全部输入之后、报告输入结束之前可以停在一个暂停点（`fault::gate`），使执行停在中途：此时下游已收下
//!   全部输入，排序与分组暂存的数据仍计在本查询的内存池里。不开 `fault-injection` 的构建里暂停点是空函数。

use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Mutex};

use amberlog_common::schema::TIMESTAMP_COLUMN;
use amberlog_storage::{BoxFuture, fault};
use arrow::array::{ArrayRef, new_null_array};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use tokio::sync::watch;

use crate::error::QueryError;

/// 逐批交出输入的一方。
pub(crate) trait Source: Send {
    /// 下一批，列恰为 `schema`（表 schema 按投影取出的列，按列名对齐）；`None` 为输入结束。返回错误之后不再被调用。
    fn next<'a>(
        &'a mut self,
        schema: &'a SchemaRef,
    ) -> BoxFuture<'a, Option<Result<RecordBatch, QueryError>>>;
}

/// 已在内存中的批次（系统表）。
pub(crate) struct Memory(pub VecDeque<RecordBatch>);

impl Source for Memory {
    fn next<'a>(
        &'a mut self,
        schema: &'a SchemaRef,
    ) -> BoxFuture<'a, Option<Result<RecordBatch, QueryError>>> {
        let next = self.0.pop_front().map(|b| project(schema, &b));
        Box::pin(async move { next })
    }
}

/// 按列名从 `batch` 取出 `schema` 的各列（`batch` 已含全部列）。
fn project(schema: &SchemaRef, batch: &RecordBatch) -> Result<RecordBatch, QueryError> {
    let columns = schema
        .fields()
        .iter()
        .map(|f| match batch.column_by_name(f.name()) {
            Some(c) => Arc::clone(c),
            None => new_null_array(f.data_type(), batch.num_rows()),
        })
        .collect::<Vec<ArrayRef>>();
    with_rows(schema, columns, batch.num_rows())
}

/// 以 `columns` 为列、`rows` 行的批次（没有列时也带行数：`count(*)` 只看行数）。
pub(crate) fn with_rows(
    schema: &SchemaRef,
    columns: Vec<ArrayRef>,
    rows: usize,
) -> Result<RecordBatch, QueryError> {
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options)
        .map_err(|e| QueryError::internal(e.to_string()))
}

/// 读取的进度：数据源、暂停点、是否已结束与读取中途的失败。
struct State {
    source: Box<dyn Source>,
    pause: Option<&'static str>,
    ended: bool,
    failure: Option<QueryError>,
}

impl State {
    /// 下一批；输入结束时先等已交出的批次都处理完（`out` 归零），再停在暂停点。失败记进 `failure`，此后视为结束。
    async fn next(
        &mut self,
        schema: &SchemaRef,
        out: &watch::Sender<usize>,
    ) -> Option<Result<RecordBatch, ()>> {
        if self.ended {
            return None;
        }
        match self.source.next(schema).await {
            Some(Ok(b)) => Some(Ok(b)),
            Some(Err(e)) => {
                self.failure = Some(e);
                self.ended = true;
                Some(Err(()))
            }
            None => {
                if let Some(step) = self.pause {
                    // 其他分区取下一批（或结束）时先把它们手上的一批交还，不需要本锁，等待不会死锁
                    let _ = out.subscribe().wait_for(|n| *n == 0).await;
                    fault::gate(step).await;
                }
                self.ended = true;
                None
            }
        }
    }
}

/// 数据源与执行共用的部分。
struct Shared {
    state: futures::lock::Mutex<State>,
    /// 已交给某个分区、该分区尚未回来取下一批（也没有结束）的批数，至多为分区数。
    out: watch::Sender<usize>,
    /// 已执行过的分区（每个分区只执行一次）。
    executed: Mutex<Vec<usize>>,
    /// 执行时所取的列；没有执行过为 `None`。
    projected: Mutex<Option<SchemaRef>>,
}

/// 一次查询的输入。
pub(crate) struct Input {
    schema: SchemaRef,
    shared: Arc<Shared>,
}

impl Input {
    /// 表 schema 为 `schema`、输入由 `source` 逐批交出；`pause` 为交出全部输入之后、报告输入结束之前的暂停点。
    pub(crate) fn new(
        schema: SchemaRef,
        source: Box<dyn Source>,
        pause: Option<&'static str>,
    ) -> Input {
        let state = State {
            source,
            pause,
            ended: false,
            failure: None,
        };
        Input {
            schema,
            shared: Arc::new(Shared {
                state: futures::lock::Mutex::new(state),
                out: watch::Sender::new(0),
                executed: Mutex::new(Vec::new()),
                projected: Mutex::new(None),
            }),
        }
    }

    /// 交给 DataFusion 的表。
    pub(crate) fn table(&self) -> Arc<dyn TableProvider> {
        Arc::new(Table {
            schema: Arc::clone(&self.schema),
            shared: Arc::clone(&self.shared),
        })
    }

    /// 执行结束之后调用：读取中途失败过即返回该失败；`drain` 时把执行没有取完的输入读完（只取执行所用的列，没有执行过
    /// 时只取 `_timestamp`），读完之前遇到的失败同样返回。
    pub(crate) async fn finish(&self, drain: bool) -> Result<(), QueryError> {
        let mut state = self.shared.state.lock().await;
        if let Some(e) = state.failure.take() {
            return Err(e);
        }
        if !drain || state.ended {
            return Ok(());
        }
        let projected = self.shared.projected.lock().expect("投影锁未中毒").clone();
        let schema = match projected {
            Some(s) => s,
            None => Arc::new(match self.schema.index_of(TIMESTAMP_COLUMN) {
                Ok(i) => self.schema.project(&[i]).expect("列号在 schema 中"),
                Err(_) => Schema::empty(),
            }),
        };
        while let Some(r) = state.next(&schema, &self.shared.out).await {
            if r.is_err() {
                return Err(state.failure.take().expect("失败已记下"));
            }
        }
        Ok(())
    }
}

/// 表接口：扫描时把列投影交给数据源。
struct Table {
    schema: SchemaRef,
    shared: Arc<Shared>,
}

impl fmt::Debug for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Table").finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for Table {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = match projection {
            Some(p) => Arc::new(self.schema.project(p)?),
            None => Arc::clone(&self.schema),
        };
        let partitions = (0..state.config().target_partitions().max(1))
            .map(|index| {
                Arc::new(Partition {
                    index,
                    schema: Arc::clone(&schema),
                    shared: Arc::clone(&self.shared),
                }) as Arc<dyn PartitionStream>
            })
            .collect();
        Ok(Arc::new(StreamingTableExec::try_new(
            schema,
            partitions,
            None,
            Vec::<LexOrdering>::new(),
            false,
            None,
        )?))
    }
}

/// 一个分区：按投影后的 `schema`，在下游取下一批时才从数据源取一批交出。
struct Partition {
    index: usize,
    schema: SchemaRef,
    shared: Arc<Shared>,
}

impl fmt::Debug for Partition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Partition").finish_non_exhaustive()
    }
}

impl PartitionStream for Partition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let schema = Arc::clone(&self.schema);
        let again = {
            let mut executed = self.shared.executed.lock().expect("执行记录锁未中毒");
            let again = executed.contains(&self.index);
            executed.push(self.index);
            again
        };
        if again {
            let e = DataFusionError::Internal("the query input is read only once".into());
            let once = futures::stream::once(async move { Err(e) });
            return Box::pin(RecordBatchStreamAdapter::new(schema, once));
        }
        *self.shared.projected.lock().expect("投影锁未中毒") = Some(Arc::clone(&schema));
        let reader = Reader {
            shared: Arc::clone(&self.shared),
            schema: Arc::clone(&schema),
            holding: false,
        };
        let stream = futures::stream::unfold(reader, |mut reader| async move {
            let item = match reader.next().await? {
                Ok(b) => Ok(b),
                // 失败本身记在旁路，由 `Input::finish` 报告
                Err(()) => Err(DataFusionError::Execution(
                    "reading the query input failed".into(),
                )),
            };
            Some((item, reader))
        });
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }
}

/// 一个分区的读取端：记着手上是否有一批已交给下游、尚未回来取下一批。
struct Reader {
    shared: Arc<Shared>,
    schema: SchemaRef,
    holding: bool,
}

impl Reader {
    async fn next(&mut self) -> Option<Result<RecordBatch, ()>> {
        // 下游来取下一批，说明上一批已处理完
        self.give_back();
        let next = {
            let mut state = self.shared.state.lock().await;
            state.next(&self.schema, &self.shared.out).await
        };
        if let Some(Ok(_)) = next {
            self.shared.out.send_modify(|n| *n += 1);
            self.holding = true;
            // 放锁时唤醒的下一个分区进了本工作线程的 LIFO 槽（tokio 不许别的工作线程窃取它），本分区若接着在这里处理这一
            // 批，它要等到处理完才能去取批，各分区便首尾相接、串行执行。先让出一次：下一个分区在这里取批，本分区排进
            // 可被窃取的队列，由空闲的工作线程接着处理。
            tokio::task::yield_now().await;
        }
        next
    }

    fn give_back(&mut self) {
        if std::mem::take(&mut self.holding) {
            self.shared.out.send_modify(|n| *n -= 1);
        }
    }
}

impl Drop for Reader {
    fn drop(&mut self) {
        self.give_back();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field};
    use datafusion::prelude::{SessionConfig, SessionContext};
    use futures::StreamExt;

    use super::*;

    /// 交出 `total` 批、每批一行，记下被取过几批。
    struct Counting {
        total: usize,
        taken: Arc<AtomicUsize>,
    }

    impl Source for Counting {
        fn next<'a>(
            &'a mut self,
            schema: &'a SchemaRef,
        ) -> BoxFuture<'a, Option<Result<RecordBatch, QueryError>>> {
            let n = self.taken.fetch_add(1, Ordering::SeqCst);
            let next = (n < self.total).then(|| {
                let ts: ArrayRef = Arc::new(Int64Array::from(vec![n as i64]));
                with_rows(schema, vec![ts], 1)
            });
            Box::pin(async move { next })
        }
    }

    /// 各分区只在下游来取时才从数据源取一批：每个分区各取一批之后都不再来取，数据源只被取过分区数那么多批，没有
    /// 提前读进来、排在队列里的批次。
    #[tokio::test]
    async fn partitions_take_batches_only_on_demand() {
        const PARTITIONS: usize = 4;
        const TOTAL: usize = 50;
        let schema = Arc::new(Schema::new(vec![Field::new(
            TIMESTAMP_COLUMN,
            DataType::Int64,
            false,
        )]));
        let taken = Arc::new(AtomicUsize::new(0));
        let source = Counting {
            total: TOTAL,
            taken: Arc::clone(&taken),
        };
        let input = Input::new(Arc::clone(&schema), Box::new(source), None);
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_target_partitions(PARTITIONS),
        );
        let plan = input
            .table()
            .scan(&ctx.state(), None, &[], None)
            .await
            .unwrap();
        assert_eq!(
            plan.properties().output_partitioning().partition_count(),
            PARTITIONS
        );

        let mut streams: Vec<_> = (0..PARTITIONS)
            .map(|i| plan.execute(i, ctx.task_ctx()).unwrap())
            .collect();
        for s in &mut streams {
            assert_eq!(s.next().await.unwrap().unwrap().num_rows(), 1);
        }
        tokio::task::yield_now().await;
        assert_eq!(taken.load(Ordering::SeqCst), PARTITIONS);
        assert_eq!(*input.shared.out.borrow(), PARTITIONS);

        // 只有第一个分区继续取：其余输入都由它取走，其他分区手上始终只有各自的一批
        let mut rows = PARTITIONS;
        while let Some(b) = streams[0].next().await {
            rows += b.unwrap().num_rows();
            assert!(*input.shared.out.borrow() <= PARTITIONS);
        }
        assert_eq!(rows, TOTAL);
        drop(streams);
        assert_eq!(*input.shared.out.borrow(), 0);
        input.finish(true).await.unwrap();
    }

    /// 同一分区执行第二次即报错（只读一遍）。
    #[tokio::test]
    async fn each_partition_executes_once() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            TIMESTAMP_COLUMN,
            DataType::Int64,
            false,
        )]));
        let input = Input::new(Arc::clone(&schema), Box::new(Memory(VecDeque::new())), None);
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(2));
        let plan = input
            .table()
            .scan(&ctx.state(), None, &[], None)
            .await
            .unwrap();
        let mut first = plan.execute(1, ctx.task_ctx()).unwrap();
        assert!(first.next().await.is_none());
        let mut again = plan.execute(1, ctx.task_ctx()).unwrap();
        assert!(again.next().await.unwrap().is_err());
        let mut other = plan.execute(0, ctx.task_ctx()).unwrap();
        assert!(other.next().await.is_none());
    }
}

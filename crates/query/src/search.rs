//! 一次 `_search` 从请求体到结果（REQ-AMB-QRY-SEARCH-ENDPOINT-V5 与 REQ-AMB-QRY-PLAN-VISIBILITY）。
//!
//! 按报告顺序推进，每一步只在前面各步都通过后才做：
//! 1. 请求体（`invalid_json`、`invalid_request`、`invalid_time_range`）与 SQL（`invalid_sql`、`sql_unsupported`）；
//!    `FROM` 引用系统表时转入 [`crate::system`]，以下各步只对 stream；
//! 2. 目标 stream：`FROM` 名不合 stream 命名、或不在进程内目录中即 `stream_not_found`（只查 [`MemtableView::schema`]，
//!    不看对象存储）；
//! 3. 数据集闸门状态：两种停用状态按状态作答，不读对象存储；之后准入，取得执行名额（名额已满即 `too_many_queries`，
//!    [`crate::limits`]，REQ-AMB-QRY-SEARCH-ENDPOINT-V7）；
//! 4. 一致对（ADR-0021 第 3 条）：先取可见集的候选视图，再确定当前快照 Q 并读快照 Q；之后确定冷层当前版本
//!    （REQ-AMB-QRY-CROSS-TIER-ORDER，见 [`crate::cold`]）；
//! 5. 绑定列与类型检查（`unknown_column`、`invalid_sql`）；
//! 6. 规划：热层按快照 `buckets` 与桶 manifest `files` 的时间区间剪枝（两端都含，对 `[start_time, end_time)` 判交集）；
//!    冷层读冷层当前版本的元数据与清单，按清单条目剪枝并剔除处于交接窗口的冷层提交新增的文件；
//! 7. 读取与执行：可见集中参与本查询的代（未绑定或绑定号大于 Q）与剪枝后留下的热层、冷层数据文件的行合为一张表，逐批
//!    交给 DataFusion（[`scan`]：一个文件读完再开下一个，只解码用到的列，不先把全部输入读进内存）；作答所需的数据文件不存在
//!    时重新确定两层的当前状态，已不在存活文件集即「数据已移动」（503、不停用，REQ-AMB-QRY-CROSS-TIER-MOVED）。聚合查询按
//!    [`crate::aggregate`] 分两段执行（`numeric_overflow` 在第一段全部组算出之后检出；读取的失败先于它报告，执行没有取完的
//!    输入在作答前读完，[`Input::finish`]）。执行用本查询自己的内存池，越限即 `query_memory_exceeded`（[`crate::limits`]）。
//!
//! 结果全有或全无（ADR-0037）：作答须读的任一对象读不到即以错误作答，不返回据其余数据得出的结果，响应没有「部分结果」字段。

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use amberlog_common::dataset::{Dataset, is_valid_stream_name};
use amberlog_common::error::Reason;
use amberlog_common::schema::{ColumnType, TIMESTAMP_COLUMN};
use amberlog_storage::chain::{ChainReader, FileEntry};
use amberlog_storage::cold::{ColdReadError, ColdReader};
use amberlog_storage::fault;
use amberlog_storage::gate::DatasetGates;
use amberlog_storage::memtable::MemtableView;
use amberlog_storage::schema::StreamColumns;
use arrow::array::{Array, ArrayRef, AsArray, new_null_array};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Float64Type, Int64Type, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::{SessionContext, ident, lit};
use jiff::tz::TimeZone;
use serde::ser::{Serialize, SerializeMap, Serializer};
use serde_json::{Map, Number, Value};

use crate::aggregate;
use crate::bind::{self, Bound, Shape};
use crate::bloom;
use crate::cold;
use crate::error::QueryError;
use crate::files::DataFiles;
use crate::input::{self, Input};
use crate::limits::{self, Admission};
use crate::prune;
use crate::request::{self, Limits, SearchRequest};
use crate::sql::{self, Query};
use crate::system::{self, Governance, SystemTable};

/// `_search` 用到的存储侧句柄与配置。
#[derive(Clone)]
pub struct Engine {
    pub memtable: Arc<dyn MemtableView>,
    pub chain: Arc<dyn ChainReader>,
    pub gates: Arc<DatasetGates>,
    pub files: Arc<dyn DataFiles>,
    /// 冷层表的读取（跨层查询的冷侧，REQ-AMB-QRY-CROSS-TIER）。
    pub cold: Arc<dyn ColdReader>,
    pub limits: Limits,
    /// 系统表（REQ-AMB-QRY-SEARCH-ENDPOINT-V4）。
    pub governance: Governance,
    /// 执行名额与单查询内存上限（REQ-AMB-QRY-RESOURCE-LIMITS），全进程一份。
    pub admission: Arc<Admission>,
}

/// 成功响应（REQ-AMB-QRY-SEARCH-ENDPOINT-V5「成功响应的结构」）。
#[derive(Debug, Clone, PartialEq)]
pub struct SearchResponse {
    pub took: u64,
    /// NULL 列不写键。
    pub hits: Vec<Hit>,
    pub from: u64,
    pub size: u64,
    pub scan_records: u64,
    pub scan_files: u64,
    pub scan_bytes: u64,
}

/// 一个 hit：「输出列名 → 值」，键按 `SELECT` 列表中输出项的先后排列（`SELECT *` 时按字段 id）。
///
/// 不用 serde_json 的 `Map`：它写出的键序取决于 `preserve_order` 特性（开时按插入次序，不开时按键名排序）。该特性
/// 现经 DataFusion 的依赖传递打开；hit 的键序由本类型自己保证，不依赖它。
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Hit(pub Vec<(String, Value)>);

impl Hit {
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.0.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// 转成 serde_json 的对象（键序随 `preserve_order` 特性而定，只供测试比对取值）。
    pub fn to_map(&self) -> Map<String, Value> {
        self.0.iter().cloned().collect()
    }
}

impl Serialize for Hit {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut m = serializer.serialize_map(Some(self.0.len()))?;
        for (k, v) in &self.0 {
            m.serialize_entry(k, v)?;
        }
        m.end()
    }
}

/// 响应体的字段。serde 按声明次序写出：`took, hits, total, from, size, scan_records,
/// scan_files, scan_bytes`（块未规定顶层字段的先后，本次序沿用早期版本的写法）。
#[derive(serde::Serialize)]
struct Body<'a> {
    took: u64,
    hits: &'a [Hit],
    total: u64,
    from: u64,
    size: u64,
    scan_records: u64,
    scan_files: u64,
    scan_bytes: u64,
}

impl SearchResponse {
    /// 响应体：八个字段，`total` 为本响应中的 hit 数（键序随 `preserve_order` 特性而定，只供测试比对取值）。
    pub fn to_json(&self) -> Value {
        let mut m = Map::new();
        m.insert("took".into(), self.took.into());
        m.insert(
            "hits".into(),
            Value::Array(
                self.hits
                    .iter()
                    .map(|h| Value::Object(h.to_map()))
                    .collect(),
            ),
        );
        m.insert("total".into(), (self.hits.len() as u64).into());
        m.insert("from".into(), self.from.into());
        m.insert("size".into(), self.size.into());
        m.insert("scan_records".into(), self.scan_records.into());
        m.insert("scan_files".into(), self.scan_files.into());
        m.insert("scan_bytes".into(), self.scan_bytes.into());
        Value::Object(m)
    }

    /// 响应体的字节。hit 的键按输出项的先后写出；数字按 `hits` 的写法（[`amberlog_common::json::to_vec`]：
    /// `100.0`、`1e300`、`1e-7`）。
    pub fn to_body(&self) -> Vec<u8> {
        amberlog_common::json::to_vec(&Body {
            took: self.took,
            hits: &self.hits,
            total: self.hits.len() as u64,
            from: self.from,
            size: self.size,
            scan_records: self.scan_records,
            scan_files: self.scan_files,
            scan_bytes: self.scan_bytes,
        })
    }
}

/// 告警规则的 `sql` 在启动时能做的检查（REQ-AMB-ALR-RULES-SQL-STATIC）：只做不读对象存储即可判定的几步——能否解析与
/// 是否在子集内（`invalid_sql`、`sql_unsupported`），`FROM` 的名称是系统表名或合 stream 命名；引用系统表时再按表的
/// 列集合绑定（`unknown_column` 与第二个 `invalid_sql`）。引用 stream 时不看它是否存在、列是否存在。
pub fn check_static(sql: &str) -> Result<(), QueryError> {
    let query = sql::parse(sql)?;
    if let Some(table) = SystemTable::from_name(&query.stream) {
        bind::bind(&query, &table.schema(), &TimeZone::UTC)?;
        return Ok(());
    }
    if !is_valid_stream_name(&query.stream) {
        return Err(stream_not_found(&query.stream));
    }
    Ok(())
}

fn stream_not_found(name: &str) -> QueryError {
    QueryError::new(
        Reason::StreamNotFound,
        format!("stream `{name}` does not exist"),
    )
}

/// 文件或桶的时间区间 `[min, max]`（两端都含）与查询范围 `[start, end)` 是否有交集。
fn overlaps(min: i64, max: i64, req: &SearchRequest) -> bool {
    min < req.end_time && max >= req.start_time
}

/// 占用执行名额的查询从哪里来（REQ-AMB-QRY-RESOURCE-LIMITS-V2「占用执行名额的查询」）：二者走同一条报告顺序、
/// 共用同一个 [`Admission`] 与同样的单查询内存上限，只是取得名额之后的暂停点不同。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Caller {
    /// `POST /api/{org}/_search`。
    Search,
    /// REQ-AMB-ALR-RULES 所称每次评估的查询（ADR-0051 第 4 条：直接调用查询规划与执行，不经 HTTP）。
    Evaluation,
}

impl Caller {
    /// 取得执行名额之后、第一次读取对象存储之前的暂停点：`query.after_admission` 只作用于 `_search`，评估用
    /// `alert.after_admission`（REQ-AMB-ALR-RULES 块「验收方向」）。
    fn after_admission(self) -> &'static str {
        match self {
            Caller::Search => "query.after_admission",
            Caller::Evaluation => "alert.after_admission",
        }
    }

    /// 数据源交出全部输入、报告输入结束之前的暂停点（REQ-AMB-QRY-RESOURCE-LIMITS-V2 场景约定）：只作用于 stream 的
    /// `_search`，不作用于评估的查询。
    fn after_input(self) -> Option<&'static str> {
        match self {
            Caller::Search => Some("query.after_input"),
            Caller::Evaluation => None,
        }
    }
}

/// 一次查询执行的结果：输出列的 schema、结果批次与扫描统计（系统表恒为 0）。
struct Output {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    scan_records: u64,
    scan_files: u64,
    scan_bytes: u64,
}

/// 一次评估的查询：规则的 SQL 与查询范围 `[start_time, end_time)`（ADR-0051 第 3 条，`[T − window, T)`）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvalQuery {
    pub sql: String,
    pub start_time: i64,
    pub end_time: i64,
}

/// 一次评估的查询结果（REQ-AMB-ALR-RULES-TRUNCATED）。
#[derive(Debug, Clone, PartialEq)]
pub struct Evaluated {
    /// 结果的输出列名与类型，按输出次序（hit 不写 NULL 的键，列条件据此判定列是否在结果里、是否为数值列）。
    pub columns: Vec<(String, ColumnType)>,
    /// 按查询的最终排序取的前 `query.max_size` 行。
    pub rows: Vec<Hit>,
    /// 结果行数（不计 `size` 与 `from`）多于 `query.max_size`。
    pub truncated: bool,
}

impl Engine {
    /// 对 `org` 下的数据执行 `_search`。`org` 已由接入面校验。
    pub async fn search(&self, org: &str, body: &[u8]) -> Result<SearchResponse, QueryError> {
        let started = Instant::now();
        // 系统收到请求的时刻：与桶生命周期共用时钟（fault-injection 下带测试时钟偏移）
        let now_us = amberlog_storage::lifecycle::now_us();
        let req = request::parse(body, &self.limits)?;
        let out = self.run(org, &req, now_us, Caller::Search).await?;
        Ok(SearchResponse {
            took: started.elapsed().as_millis() as u64,
            hits: render(&out.batches)?,
            from: req.from,
            size: req.size,
            scan_records: out.scan_records,
            scan_files: out.scan_files,
            scan_bytes: out.scan_bytes,
        })
    }

    /// 告警评估的查询（REQ-AMB-QRY-RESOURCE-LIMITS-V2、ADR-0051 第 4 条）：结果等同于请求体为规则的 SQL、`q` 的查询范围、
    /// `size` 为 `query.max_size`、`from` 为 0 的 `_search`。不解析请求体；SQL 起的各步与 `_search` 同一报告顺序，
    /// 在同一位置向同一个 [`Admission`] 取名额，名额已满即以 `too_many_queries` 返回、不等待；名额在本函数返回时
    /// （得出结果或错误）释放，调用方丢弃本函数的 future 时随之释放。越限计数 `amberlog_search_rejected_total` 只由
    /// `_search` 的处理器递增，这里不计。
    pub async fn evaluate(&self, org: &str, q: &EvalQuery) -> Result<Evaluated, QueryError> {
        let now_us = amberlog_storage::lifecycle::now_us();
        let max = self.limits.max_size;
        // 多取 1 行，判出结果是否多于 `query.max_size` 行
        let req = SearchRequest {
            sql: q.sql.clone(),
            start_time: q.start_time,
            end_time: q.end_time,
            size: max.saturating_add(1),
            from: 0,
            // 告警评估的查询不带时区（histogram 调用按 UTC 对齐）
            timezone: TimeZone::UTC,
        };
        let out = self.run(org, &req, now_us, Caller::Evaluation).await?;
        let columns = out
            .schema
            .fields()
            .iter()
            .map(|f| Ok((f.name().clone(), column_type(f.data_type())?)))
            .collect::<Result<Vec<_>, QueryError>>()?;
        let mut rows = render(&out.batches)?;
        let truncated = rows.len() as u64 > max;
        rows.truncate(usize::try_from(max).unwrap_or(usize::MAX));
        Ok(Evaluated {
            columns,
            rows,
            truncated,
        })
    }

    /// 自 SQL 起按报告顺序执行一次查询（`req` 已通过请求体校验，或由评估直接给出）。
    async fn run(
        &self,
        org: &str,
        req: &SearchRequest,
        now_us: i64,
        caller: Caller,
    ) -> Result<Output, QueryError> {
        let query = sql::parse(&req.sql)?;

        if let Some(table) = SystemTable::from_name(&query.stream) {
            return self
                .search_system(table, org, req, &query, now_us, caller)
                .await;
        }
        if !is_valid_stream_name(&query.stream) {
            return Err(stream_not_found(&query.stream));
        }
        let dataset = Dataset::new(org, query.stream.as_str());
        let schema = self
            .memtable
            .schema(&dataset)
            .ok_or_else(|| stream_not_found(&query.stream))?;
        if let Some(reason) = self.gates.state(&dataset).reason() {
            return Err(QueryError::new(reason, "dataset is disabled until restart"));
        }
        // 准入：第一次读对象存储之前（REQ-AMB-QRY-RESOURCE-LIMITS-ADMISSION-POINT）；名额随 `slot` 在返回时释放
        let slot = self.admission.admit()?;
        fault::gate(caller.after_admission()).await;

        // 一致对：候选视图先于当前快照（ADR-0021 第 3 条①②）
        let candidates = self.memtable.candidates(&dataset);
        // 在用快照登记（REQ-AMB-STO-SNAPSHOT-GC-IN-USE）：先登记、后定快照，持有到作答完毕
        let mut pin = self.chain.reading(&dataset);
        let q = self.chain.current(&dataset).await?;
        if let Some(id) = q {
            pin.settle(id);
        }
        let snapshot = match q {
            Some(id) => Some(self.chain.snapshot(&dataset, id).await?),
            None => None,
        };
        // 先热后冷（REQ-AMB-QRY-CROSS-TIER-ORDER）；创世快照尚未提交成功、只以内存表作答时不确定冷层当前版本（-PER-QUERY）
        fault::gate("query.after_hot_snapshot").await;
        let cold_version = match &snapshot {
            Some(s) => cold::version(self.cold.as_ref(), &self.gates, &dataset, s).await?,
            None => None,
        };
        // 确定冷层当前版本之后、读取其清单列表之前（REQ-AMB-QRY-CROSS-TIER-V2 的场景用）
        fault::gate("query.after_cold_version").await;

        let bound = bind::bind(&query, &schema, &req.timezone)?;

        // 规划：两级时间剪枝，再按文件级列统计剪枝（REQ-AMB-QRY-COLUMN-PRUNING，只对热层），最后按等值 bloom 剪枝
        let checks = prune::checks(query.filter.as_ref());
        let mut files: Vec<(&str, FileEntry)> = Vec::new();
        if let Some(snapshot) = &snapshot {
            for bucket in &snapshot.buckets {
                let (Some(min), Some(max)) = (bucket.time_min, bucket.time_max) else {
                    continue; // 墓碑桶
                };
                if !overlaps(min, max, req) {
                    continue;
                }
                let manifest = self
                    .chain
                    .bucket_manifest(&dataset, snapshot, bucket)
                    .await?;
                files.extend(
                    manifest
                        .files
                        .iter()
                        .filter(|f| overlaps(f.min_timestamp, f.max_timestamp, req))
                        .filter(|f| !prune::impossible(&checks, f, &schema))
                        .map(|f| (bucket.bucket.as_str(), f.clone())),
                );
            }
        }
        let mut cold_part = match (&snapshot, cold_version) {
            (Some(s), Some(v)) => Some(
                cold::plan(
                    self.cold.as_ref(),
                    self.chain.as_ref(),
                    &self.gates,
                    &dataset,
                    s,
                    v,
                    req.start_time,
                    req.end_time,
                )
                .await?,
            ),
            _ => None,
        };
        // 再按文件里实际有的等值 bloom 剪枝（REQ-AMB-QRY-EQUALITY-BLOOM-READ-SKIP、-READ-BY-FILE）：只读尾部元数据与
        // bloom，被判掉的文件不进入扫描，不计入扫描统计；读不到即不下结论，照常交给整文件读取
        let eq = bloom::EqConds::of(&checks, &schema);
        if !eq.is_empty() {
            let picks = eq.hot_picks();
            let mut kept = Vec::with_capacity(files.len());
            for (bucket, f) in files {
                match self.files.blooms(&dataset, &f, &picks).await {
                    Some(b) if eq.skip(&b) => {}
                    _ => kept.push((bucket, f)),
                }
            }
            files = kept;
            if let Some(part) = &mut cold_part {
                let picks = eq.cold_picks(&schema);
                let mut kept = Vec::with_capacity(part.files.len());
                for f in std::mem::take(&mut part.files) {
                    match self.cold.blooms(&dataset, &f, &picks).await {
                        Some(b) if eq.skip(&b) => {}
                        _ => kept.push(f),
                    }
                }
                part.files = kept;
            }
        }
        fault::gate("query.after_bind").await;

        // 输入：可见集的代先于热层与冷层数据文件，逐批交给执行（[`scan`]）；扫描统计按规划结果计入
        // （冷层数据文件按清单条目的字节数与记录数，REQ-AMB-QRY-CROSS-TIER-SCAN-*）
        let table = table_schema(&schema);
        let ts_only = Arc::new(
            table
                .project(&[table.index_of(TIMESTAMP_COLUMN).expect("表含 _timestamp")])
                .expect("列号在表中"),
        );
        let mut scan_records = 0u64;
        let mut resident = VecDeque::new();
        for b in candidates.participating(q) {
            scan_records += rows_in_range(&align(&ts_only, &b)?, req);
            resident.push_back(b);
        }
        let (mut scan_bytes, mut scan_files) = (0u64, 0u64);
        let mut parts = VecDeque::new();
        for (bucket, f) in files {
            scan_files += 1;
            scan_bytes += f.size_bytes;
            scan_records += f.record_count;
            parts.push_back(scan::Part::Hot {
                bucket: bucket.to_owned(),
                file: f,
            });
        }
        let cold_table = match cold_part {
            Some(part) => {
                for f in part.files {
                    scan_files += 1;
                    scan_bytes += f.size_bytes;
                    scan_records += f.record_count;
                    parts.push_back(scan::Part::Cold(f));
                }
                Some(part.table)
            }
            None => None,
        };
        let source = scan::Scan::new(
            self.clone(),
            dataset,
            schema,
            resident,
            parts,
            cold_table,
            req.start_time..req.end_time,
        );
        let input = Input::new(Arc::clone(&table), Box::new(source), caller.after_input());

        let ts = || ident(TIMESTAMP_COLUMN);
        let order = Order {
            range: Some(
                ts().gt_eq(lit(req.start_time))
                    .and(ts().lt(lit(req.end_time))),
            ),
            default: &[(TIMESTAMP_COLUMN, true)],
            tie_break: &[],
        };
        let (schema, batches) = execute(&slot.context()?, &input, &bound, req, &order).await?;
        Ok(Output {
            schema,
            batches,
            scan_records,
            scan_files,
            scan_bytes,
        })
    }
}

/// 作答所需而不存在的数据文件。
enum Missing<'a> {
    /// 热层数据文件：所在桶的标签与相对数据集前缀的路径。
    Hot { bucket: &'a str, path: &'a str },
    /// 冷层数据文件：相对冷层表位置的路径与分区日。
    Cold { path: &'a str, day: i32 },
}

impl Engine {
    /// 作答所需的数据文件不存在（`message` 为读取时的说明）：重新确定当前快照与冷层当前版本（先热后冷），该文件已不在
    /// 其所属一层的存活文件集中即「数据已移动」，回 503、不停用（REQ-AMB-QRY-CROSS-TIER-MOVED、-MOVED-NOT-DISABLE）；
    /// 仍在即按该层的现行规则作答：热层回 500，冷层回 500 并使数据集进入 manifest 不可读情形（-CORRUPT）。
    /// 重判本身的读取失败按各自的分类作答。
    async fn missing(&self, dataset: &Dataset, file: Missing<'_>, message: String) -> QueryError {
        let live = match self.still_live(dataset, &file).await {
            Ok(live) => live,
            Err(e) => return e,
        };
        match (live, file) {
            (false, _) => QueryError::moved(format!("data moved while the query ran: {message}")),
            (true, Missing::Hot { .. }) => QueryError::internal(message),
            (true, Missing::Cold { .. }) => {
                cold::error(&self.gates, dataset, ColdReadError::NotFound(message))
            }
        }
    }

    async fn still_live(&self, dataset: &Dataset, file: &Missing<'_>) -> Result<bool, QueryError> {
        let mut pin = self.chain.reading(dataset);
        let Some(id) = self.chain.current(dataset).await? else {
            return Ok(false);
        };
        pin.settle(id);
        let snapshot = self.chain.snapshot(dataset, id).await?;
        match *file {
            Missing::Hot { bucket, path } => {
                let Some(r) = snapshot.buckets.iter().find(|b| b.bucket == bucket) else {
                    return Ok(false);
                };
                let manifest = self.chain.bucket_manifest(dataset, &snapshot, r).await?;
                Ok(manifest.files.iter().any(|f| f.path == path))
            }
            Missing::Cold { path, day } => {
                match cold::version(self.cold.as_ref(), &self.gates, dataset, &snapshot).await? {
                    None => Ok(false),
                    Some(v) => {
                        cold::is_live(self.cold.as_ref(), &self.gates, dataset, v, path, day).await
                    }
                }
            }
        }
    }
}

impl Engine {
    /// `FROM` 引用系统表：按表的列集合绑定（`unknown_column`、`invalid_sql`），再生成整张表并执行。
    /// 没有 `_timestamp` 附加过滤；缺省排序与同键行的次序按定义该表的块。
    async fn search_system(
        &self,
        table: SystemTable,
        org: &str,
        req: &SearchRequest,
        query: &Query,
        now_us: i64,
        caller: Caller,
    ) -> Result<Output, QueryError> {
        let bound = bind::bind(query, &table.schema(), &req.timezone)?;
        // 准入：按表的列集合绑定之后、读取系统表所需的对象之前（REQ-AMB-QRY-SEARCH-ENDPOINT-V7）
        let slot = self.admission.admit()?;
        fault::gate(caller.after_admission()).await;
        let cx = system::Context {
            governance: &self.governance,
            chain: self.chain.as_ref(),
            gates: &self.gates,
            org,
            start_time: req.start_time,
            end_time: req.end_time,
            now_us,
        };
        let batch = system::table(table, &cx, query).await?;
        let order = Order {
            range: None,
            default: table.default_order(),
            tie_break: table.default_order(),
        };
        let input = Input::new(
            table.arrow_schema(),
            Box::new(input::Memory(VecDeque::from([batch]))),
            None,
        );
        let (schema, batches) = execute(&slot.context()?, &input, &bound, req, &order).await?;
        Ok(Output {
            schema,
            batches,
            scan_records: 0,
            scan_files: 0,
            scan_bytes: 0,
        })
    }
}

/// 查询的表 schema：stream 当前 schema 的全部列；`_timestamp` 以 Int64（UTC 微秒）出现，与其列类型一致。
fn table_schema(schema: &StreamColumns) -> SchemaRef {
    Arc::new(Schema::new(
        schema
            .columns()
            .iter()
            .map(|c| {
                if c.name == TIMESTAMP_COLUMN {
                    Field::new(TIMESTAMP_COLUMN, DataType::Int64, false)
                } else {
                    let t = match c.column_type {
                        ColumnType::Int64 => DataType::Int64,
                        ColumnType::Float64 => DataType::Float64,
                        ColumnType::Boolean => DataType::Boolean,
                        ColumnType::Utf8 => DataType::Utf8,
                    };
                    Field::new(&c.name, t, true)
                }
            })
            .collect::<Vec<_>>(),
    ))
}

/// 把可见集、热层或冷层数据文件的批次按列名对齐到表 schema（冷层批次已按字段编号取成 stream 当前的列名）：缺的列补 NULL，`_timestamp` 由时间戳转为 Int64。
/// 列类型与 schema 不符说明文件内容与快照不一致，按内容无法解析处理。
fn align(table: &SchemaRef, batch: &RecordBatch) -> Result<RecordBatch, QueryError> {
    let columns = table
        .fields()
        .iter()
        .map(|field| -> Result<ArrayRef, QueryError> {
            let Some(col) = batch.column_by_name(field.name()) else {
                if field.name() == TIMESTAMP_COLUMN {
                    return Err(QueryError::internal("batch lacks the _timestamp column"));
                }
                return Ok(new_null_array(field.data_type(), batch.num_rows()));
            };
            match (col.data_type(), field.data_type()) {
                (a, b) if a == b => Ok(Arc::clone(col)),
                (
                    DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _),
                    DataType::Int64,
                ) => cast(col, &DataType::Int64).map_err(|e| QueryError::internal(e.to_string())),
                (a, b) => Err(QueryError::internal(format!(
                    "column `{}` has type {a} in data but {b} in the stream schema",
                    field.name()
                ))),
            }
        })
        .collect::<Result<Vec<_>, _>>()?;
    input::with_rows(table, columns, batch.num_rows())
}

fn rows_in_range(batch: &RecordBatch, req: &SearchRequest) -> u64 {
    let ts = batch
        .column_by_name(TIMESTAMP_COLUMN)
        .expect("已对齐")
        .as_primitive::<Int64Type>();
    ts.iter()
        .flatten()
        .filter(|&t| t >= req.start_time && t < req.end_time)
        .count() as u64
}

/// 执行时的范围过滤与排序。
struct Order<'a> {
    /// 与 WHERE 取逻辑与的附加过滤（stream 为时间范围，系统表没有）。
    range: Option<datafusion::prelude::Expr>,
    /// 非聚合查询不含 `ORDER BY` 时的排序（列，是否降序）。
    default: &'a [(&'a str, bool)],
    /// 非聚合查询含 `ORDER BY` 时接在其后的排序键：排序键相同的行按它排列（系统表的块有此规定，stream 没有）。
    tie_break: &'a [(&'a str, bool)],
}

/// 执行：附加过滤与 WHERE 取逻辑与，再按查询的形状：
/// - 非聚合查询：排序（无 `ORDER BY` 时按缺省排序）→ SQL 的 `LIMIT` → 取输出列（以输出列名命名）→ 跳过 `from` 行、
///   取至多 `size` 行；
/// - 聚合查询：按 [`aggregate::execute`] 分两段（不套用缺省排序）。
///
/// 排序中 NULL 视为最大（升序在后、降序在前）；Float64 的 `-0.0` 与 `0.0` 为同一排序键（[`aggregate::sort_key`]）。
/// `ctx` 带本查询的内存池（[`limits::Slot::context`]），内存越限即 `query_memory_exceeded`。输入经 [`Input`] 逐批交出；
/// 读取输入中途的失败先于执行本身的错误报告，执行没有取完的输入在作答之前读完（全有或全无，[`Input::finish`]）。
async fn execute(
    ctx: &SessionContext,
    input: &Input,
    bound: &Bound,
    req: &SearchRequest,
    order: &Order<'_>,
) -> Result<(SchemaRef, Vec<RecordBatch>), QueryError> {
    let result = run_plan(ctx, input, bound, req, order).await;
    input.finish(result.is_ok()).await?;
    result
}

async fn run_plan(
    ctx: &SessionContext,
    input: &Input,
    bound: &Bound,
    req: &SearchRequest,
    order: &Order<'_>,
) -> Result<(SchemaRef, Vec<RecordBatch>), QueryError> {
    let df_err = limits::execution_error;
    let mut df = ctx.read_table(input.table()).map_err(df_err)?;

    let predicate = match (order.range.clone(), &bound.filter) {
        (Some(r), Some(f)) => Some(r.and(f.clone())),
        (r, f) => r.or_else(|| f.clone()),
    };
    if let Some(p) = predicate {
        df = df.filter(p).map_err(df_err)?;
    }

    let (outputs, order_by) = match &bound.shape {
        Shape::Aggregate(plan) => {
            return aggregate::execute(ctx, df, plan, bound.limit, (req.from, req.size)).await;
        }
        Shape::Rows { outputs, order_by } => (outputs, order_by),
    };
    let keys: Vec<(datafusion::prelude::Expr, bool)> = if order_by.is_empty() {
        order.default.iter().map(|(c, d)| (ident(*c), *d)).collect()
    } else {
        order_by
            .iter()
            .cloned()
            .chain(order.tie_break.iter().map(|(c, d)| (ident(*c), *d)))
            .collect()
    };
    let sort = keys
        .into_iter()
        .map(|(e, descending)| {
            Ok(aggregate::sort_key(e, df.schema())?.sort(!descending, descending))
        })
        .collect::<Result<Vec<_>, QueryError>>()?;
    df = df.sort(sort).map_err(df_err)?;
    if let Some(n) = bound.limit {
        df = df
            .limit(0, Some(usize::try_from(n).unwrap_or(usize::MAX)))
            .map_err(df_err)?;
    }
    df = df
        .select(outputs.iter().map(|(_, e)| e.clone()).collect::<Vec<_>>())
        .map_err(df_err)?;
    df = df
        .limit(
            usize::try_from(req.from).unwrap_or(usize::MAX),
            Some(usize::try_from(req.size).unwrap_or(usize::MAX)),
        )
        .map_err(df_err)?;
    let schema = Arc::clone(df.schema().inner());
    Ok((schema, df.collect().await.map_err(df_err)?))
}

/// 结果列的 Arrow 类型 → 列类型（结果列只有这四种，见 [`render`]）。
fn column_type(t: &DataType) -> Result<ColumnType, QueryError> {
    match t {
        DataType::Int64 => Ok(ColumnType::Int64),
        DataType::Float64 => Ok(ColumnType::Float64),
        DataType::Boolean => Ok(ColumnType::Boolean),
        DataType::Utf8 => Ok(ColumnType::Utf8),
        other => Err(QueryError::internal(format!(
            "unexpected result column type {other}"
        ))),
    }
}

/// 行 → hit：Int64 为 JSON 整数，Float64 为含小数点或指数的最短往返形式，Boolean 为布尔，Utf8 为字符串；NULL 不写键。
fn render(batches: &[RecordBatch]) -> Result<Vec<Hit>, QueryError> {
    let mut hits = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        for row in 0..batch.num_rows() {
            let mut hit = Hit::default();
            for (field, col) in schema.fields().iter().zip(batch.columns()) {
                if col.is_null(row) {
                    continue;
                }
                let v = match col.data_type() {
                    DataType::Int64 => Value::from(col.as_primitive::<Int64Type>().value(row)),
                    DataType::Float64 => {
                        let f = col.as_primitive::<Float64Type>().value(row);
                        Number::from_f64(f).map(Value::Number).ok_or_else(|| {
                            QueryError::internal(format!("non-finite Float64 value {f}"))
                        })?
                    }
                    DataType::Boolean => Value::Bool(col.as_boolean().value(row)),
                    DataType::Utf8 => Value::String(col.as_string::<i32>().value(row).to_owned()),
                    other => {
                        return Err(QueryError::internal(format!(
                            "unexpected result column type {other}"
                        )));
                    }
                };
                hit.0.push((field.name().clone(), v));
            }
            hits.push(hit);
        }
    }
    Ok(hits)
}

mod scan;

#[cfg(test)]
mod tests;

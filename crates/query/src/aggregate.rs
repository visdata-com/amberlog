//! 聚合查询的执行（REQ-AMB-QRY-AGGREGATION）。
//!
//! 分两段：
//! 1. 对输入行（附加时间过滤与 `WHERE` 之后）按分组项与各聚合计算出**全部组**的结果并整体收集，逐组核对 Int64 `sum` 是否
//!    落在 Int64 之内、Float64 `sum` 与 `avg` 是否有限，任一不合即 `numeric_overflow`、不返回任何行。溢出判定因此覆盖
//!    全部组，与 `ORDER BY`、`LIMIT`、`from`、`size` 截到哪几组无关。
//! 2. 把这些结果放进新的内存表，再做 `ORDER BY`、SQL 的 `LIMIT`、取输出列（以输出列名显式命名）与 `from`、`size` 截取。
//!
//! 各聚合函数：`count` 与 Float64 的 `sum`、`min`、`max` 用 DataFusion 内建聚合；Int64 列的 `sum` 与 `avg` 用自写的聚合
//! （以 i128 累加非 NULL 取值并另计个数，最终判断是否落在 Int64 之内；`avg` 为精确和就近舍入为双精度数后除以个数，只舍入
//! 一次），不用内建 `sum`（64 位回绕）与 `avg`（先逐值转 Float64）；Float64 列的 `avg` 由第一段同一组的 `sum` 除以
//! `count(<列>)` 求得，与本块所定的 `sum` 一致。Float64 分组列先把 `-0.0` 规范为 `0.0`，使两者归入同一组。

use std::fmt;
use std::sync::{Arc, LazyLock};

use amberlog_common::error::Reason;
use amberlog_common::schema::ColumnType;
use arrow::array::{Array, ArrayRef, AsArray, BinaryArray, Float64Array};
use arrow::compute::concat_batches;
use arrow::datatypes::{
    DataType, Field, FieldRef, Float64Type, Int64Type, Schema, SchemaRef, UInt64Type,
};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, DataFusionError, Result, ScalarValue, exec_err};
use datafusion::dataframe::DataFrame;
use datafusion::datasource::MemTable;
use datafusion::functions_aggregate::expr_fn::{count, max, min, sum};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Expr, ExprSchemable, Signature, Volatility, when,
};
use datafusion::prelude::{SessionContext, ident, lit};
use jiff::tz::TimeZone;

use crate::error::QueryError;
use crate::sql::AggFunc;

/// 一次聚合：函数与参数列（`count(*)` 无参数列）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Agg {
    pub func: AggFunc,
    /// 参数列名与其类型。
    pub arg: Option<(String, ColumnType)>,
}

/// 分组项：列，或 histogram 调用（间隔为微秒）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupKey {
    Column(String, ColumnType),
    Histogram(i64),
}

/// 第二段内存表中的一列。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Slot {
    Group(usize),
    Agg(usize),
}

impl Slot {
    fn name(self) -> String {
        match self {
            Slot::Group(i) => format!("__g{i}"),
            Slot::Agg(j) => format!("__a{j}"),
        }
    }
}

/// 绑定后的聚合查询。
#[derive(Debug, Clone)]
pub struct AggregatePlan {
    pub groups: Vec<GroupKey>,
    pub aggs: Vec<Agg>,
    /// 输出列名与其所在的列，按 `SELECT` 列表的次序（同名同表达式只出现一次）。
    pub outputs: Vec<(String, Slot)>,
    /// `ORDER BY` 各项与是否降序；空表示不含 `ORDER BY`（不承诺各组的先后）。
    pub order_by: Vec<(Slot, bool)>,
    /// 请求的时区：histogram 分组项按它的本地挂钟对齐。
    pub zone: TimeZone,
}

fn df_err(e: DataFusionError) -> QueryError {
    if e.to_string().contains(OVERFLOW_MARKER) {
        overflow()
    } else {
        crate::limits::execution_error(e)
    }
}

fn overflow() -> QueryError {
    QueryError::new(
        Reason::NumericOverflow,
        "the aggregate result is not representable (Int64 sum out of range, or a non-finite Float64 sum or avg)",
    )
}

/// 第一段：在已过滤的输入 `input` 上算出全部组，核对溢出，得到第二段的内存表（列为 `__g<i>` 与 `__a<j>`）。
async fn first_stage(input: DataFrame, plan: &AggregatePlan) -> Result<RecordBatch, QueryError> {
    let groups: Vec<Expr> = plan
        .groups
        .iter()
        .enumerate()
        .map(|(i, g)| group_expr(g, &plan.zone).alias(Slot::Group(i).name()))
        .collect();
    let mut exprs = Vec::new();
    for (j, a) in plan.aggs.iter().enumerate() {
        let name = Slot::Agg(j).name();
        let column = || ident(&a.arg.as_ref().expect("只有 count(*) 没有参数列").0);
        let float = matches!(a.arg, Some((_, ColumnType::Float64)));
        match a.func {
            AggFunc::Count => match &a.arg {
                None => exprs.push(count(lit(1i64)).alias(name)),
                Some(_) => exprs.push(count(column()).alias(name)),
            },
            AggFunc::Sum if float => exprs.push(sum(column()).alias(name)),
            AggFunc::Sum => exprs.push(INT_SUM.call(vec![column()]).alias(name)),
            AggFunc::Avg if float => {
                exprs.push(sum(column()).alias(name.clone()));
                exprs.push(count(column()).alias(format!("{name}_n")));
            }
            AggFunc::Avg => exprs.push(INT_AVG.call(vec![column()]).alias(name)),
            AggFunc::Min => exprs.push(min(column()).alias(name)),
            AggFunc::Max => exprs.push(max(column()).alias(name)),
        }
    }
    let df = input.aggregate(groups, exprs).map_err(df_err)?;
    let schema = Arc::new(df.schema().as_arrow().clone());
    let batches = df.collect().await.map_err(df_err)?;
    let raw = concat_batches(&schema, &batches).map_err(|e| QueryError::internal(e.to_string()))?;

    let mut fields = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();
    for i in 0..plan.groups.len() {
        let name = Slot::Group(i).name();
        let col = raw.column_by_name(&name).expect("第一段的分组列");
        fields.push(Field::new(&name, col.data_type().clone(), true));
        columns.push(Arc::clone(col));
    }
    for (j, a) in plan.aggs.iter().enumerate() {
        let name = Slot::Agg(j).name();
        let col = raw.column_by_name(&name).expect("第一段的聚合列");
        let float = matches!(a.arg, Some((_, ColumnType::Float64)));
        let col: ArrayRef = match a.func {
            AggFunc::Avg if float => {
                let n = raw
                    .column_by_name(&format!("{name}_n"))
                    .expect("第一段的个数列")
                    .as_primitive::<Int64Type>();
                let s = col.as_primitive::<Float64Type>();
                let avg: Float64Array = s
                    .iter()
                    .zip(n.iter())
                    .map(|(s, n)| match (s, n) {
                        (Some(s), Some(n)) if n > 0 => Some(s / n as f64),
                        _ => None,
                    })
                    .collect();
                Arc::new(avg)
            }
            _ => Arc::clone(col),
        };
        if matches!(a.func, AggFunc::Sum | AggFunc::Avg) && col.data_type() == &DataType::Float64 {
            let finite = col
                .as_primitive::<Float64Type>()
                .iter()
                .flatten()
                .all(f64::is_finite);
            if !finite {
                return Err(overflow());
            }
        }
        fields.push(Field::new(&name, col.data_type().clone(), true));
        columns.push(col);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| QueryError::internal(e.to_string()))
}

/// Float64 表达式的 `-0.0` 规范为 `0.0`：`CASE WHEN e >= -0.0 AND e <= 0.0 THEN 0.0 ELSE e END`（按全序或 IEEE 相等
/// 比较都把两者落在 `[-0.0, 0.0]` 之内；NULL 走 `ELSE` 仍为 NULL）。
fn zero_normalized(e: Expr) -> Expr {
    let f = |v: f64| lit(ScalarValue::Float64(Some(v)));
    when(
        e.clone().gt_eq(f(-0.0)).and(e.clone().lt_eq(f(0.0))),
        f(0.0),
    )
    .otherwise(e)
    .expect("CASE 表达式")
}

/// 排序键：Float64 的键先把 `-0.0` 规范为 `0.0`，使两者为同一排序键、先后交给其后的排序键
/// （REQ-AMB-QRY-SEARCH-ENDPOINT-V5「Float64 值在 `ORDER BY` 排序中按数学值比较」；Arrow 按全序比较，`-0.0` 小于 `0.0`）。
/// 其他类型原样。`schema` 为求值排序键的输入。
pub(crate) fn sort_key(e: Expr, schema: &DFSchema) -> Result<Expr, QueryError> {
    Ok(match e.get_type(schema).map_err(df_err)? {
        DataType::Float64 => zero_normalized(e),
        _ => e,
    })
}

/// 分组表达式：Float64 列的 `-0.0` 规范为 `0.0`。
fn group_expr(g: &GroupKey, zone: &TimeZone) -> Expr {
    match g {
        GroupKey::Column(c, ColumnType::Float64) => zero_normalized(ident(c)),
        GroupKey::Column(c, _) => ident(c),
        GroupKey::Histogram(interval) => crate::histogram::expr(*interval, zone),
    }
}

/// 执行聚合查询：`input` 为已过滤的输入行，`limit` 为 SQL 的 `LIMIT`，`(from, size)` 为请求的截取。
/// 第二段在 `ctx`（与第一段同一个，带本查询的内存池）里执行。返回输出列的 schema 与结果批次。
pub async fn execute(
    ctx: &SessionContext,
    input: DataFrame,
    plan: &AggregatePlan,
    limit: Option<u64>,
    (from, size): (u64, u64),
) -> Result<(SchemaRef, Vec<RecordBatch>), QueryError> {
    let table = first_stage(input, plan).await?;
    let mem = MemTable::try_new(table.schema(), vec![vec![table]]).map_err(df_err)?;
    let mut df = ctx.read_table(Arc::new(mem)).map_err(df_err)?;
    if !plan.order_by.is_empty() {
        let sort = plan
            .order_by
            .iter()
            .map(|(slot, descending)| {
                Ok(sort_key(ident(slot.name()), df.schema())?.sort(!descending, *descending))
            })
            .collect::<Result<Vec<_>, QueryError>>()?;
        df = df.sort(sort).map_err(df_err)?;
    }
    if let Some(n) = limit {
        df = df
            .limit(0, Some(usize::try_from(n).unwrap_or(usize::MAX)))
            .map_err(df_err)?;
    }
    df = df
        .select(
            plan.outputs
                .iter()
                .map(|(name, slot)| ident(slot.name()).alias(name))
                .collect::<Vec<_>>(),
        )
        .map_err(df_err)?;
    df = df
        .limit(
            usize::try_from(from).unwrap_or(usize::MAX),
            Some(usize::try_from(size).unwrap_or(usize::MAX)),
        )
        .map_err(df_err)?;
    let schema = Arc::clone(df.schema().inner());
    Ok((schema, df.collect().await.map_err(df_err)?))
}

/// Int64 求和超出 Int64 时聚合报错所带的标记（DataFusion 把它包进自己的错误后，按说明文字认出）。
const OVERFLOW_MARKER: &str = "amberlog: Int64 sum out of range";

#[derive(Debug)]
struct Int64SumOutOfRange;

impl fmt::Display for Int64SumOutOfRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(OVERFLOW_MARKER)
    }
}

impl std::error::Error for Int64SumOutOfRange {}

static INT_SUM: LazyLock<AggregateUDF> =
    LazyLock::new(|| AggregateUDF::new_from_impl(ExactInt::new(Output::Sum)));
static INT_AVG: LazyLock<AggregateUDF> =
    LazyLock::new(|| AggregateUDF::new_from_impl(ExactInt::new(Output::Avg)));

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Output {
    Sum,
    Avg,
}

/// Int64 列的精确和：以 i128 累加（2^64 行以内不会溢出）。状态为和（16 字节小端）与非 NULL 个数。
#[derive(Debug, PartialEq, Eq, Hash)]
struct ExactInt {
    output: Output,
    signature: Signature,
}

impl ExactInt {
    fn new(output: Output) -> Self {
        ExactInt {
            output,
            signature: Signature::exact(vec![DataType::Int64], Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ExactInt {
    fn name(&self) -> &str {
        match self.output {
            Output::Sum => "amberlog_int_sum",
            Output::Avg => "amberlog_int_avg",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(match self.output {
            Output::Sum => DataType::Int64,
            Output::Avg => DataType::Float64,
        })
    }

    fn accumulator(&self, _args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ExactIntAcc {
            output: self.output,
            sum: 0,
            n: 0,
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format!("{}_sum", args.name),
                DataType::Binary,
                true,
            )),
            Arc::new(Field::new(
                format!("{}_n", args.name),
                DataType::UInt64,
                true,
            )),
        ])
    }
}

#[derive(Debug)]
struct ExactIntAcc {
    output: Output,
    sum: i128,
    n: u64,
}

impl Accumulator for ExactIntAcc {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let [values] = values else {
            return exec_err!("exact integer sum takes one argument");
        };
        for v in values.as_primitive::<Int64Type>().iter().flatten() {
            self.sum += i128::from(v);
            self.n += 1;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let [sums, ns] = states else {
            return exec_err!("exact integer sum state has two fields");
        };
        let sums: &BinaryArray = sums.as_binary();
        let ns = ns.as_primitive::<UInt64Type>();
        for (s, n) in sums.iter().zip(ns.iter()) {
            let (Some(s), Some(n)) = (s, n) else {
                continue;
            };
            let Ok(bytes) = <[u8; 16]>::try_from(s) else {
                return exec_err!("exact integer sum state is malformed");
            };
            self.sum += i128::from_le_bytes(bytes);
            self.n += n;
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Binary(Some(self.sum.to_le_bytes().to_vec())),
            ScalarValue::UInt64(Some(self.n)),
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.n == 0 {
            return Ok(match self.output {
                Output::Sum => ScalarValue::Int64(None),
                Output::Avg => ScalarValue::Float64(None),
            });
        }
        match self.output {
            Output::Sum => match i64::try_from(self.sum) {
                Ok(s) => Ok(ScalarValue::Int64(Some(s))),
                Err(_) => Err(DataFusionError::External(Box::new(Int64SumOutOfRange))),
            },
            // 整数到浮点的 `as` 按就近取偶舍入：精确和只舍入一次
            Output::Avg => Ok(ScalarValue::Float64(Some(self.sum as f64 / self.n as f64))),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn acc(output: Output, values: &[Option<i64>]) -> ExactIntAcc {
        let mut a = ExactIntAcc {
            output,
            sum: 0,
            n: 0,
        };
        let arr: ArrayRef = Arc::new(arrow::array::Int64Array::from(values.to_vec()));
        a.update_batch(&[arr]).unwrap();
        a
    }

    #[test]
    fn exact_int_sum_and_avg() {
        let mut a = acc(Output::Sum, &[Some(i64::MAX), Some(1)]);
        assert!(
            a.evaluate()
                .unwrap_err()
                .to_string()
                .contains(OVERFLOW_MARKER)
        );
        let mut a = acc(Output::Sum, &[Some(i64::MAX), Some(1), Some(-2)]);
        assert_eq!(
            a.evaluate().unwrap(),
            ScalarValue::Int64(Some(i64::MAX - 1))
        );
        let mut a = acc(Output::Sum, &[None, None]);
        assert_eq!(a.evaluate().unwrap(), ScalarValue::Int64(None));
        let mut a = acc(Output::Avg, &[Some(9_007_199_254_740_993), Some(1)]);
        assert_eq!(
            a.evaluate().unwrap(),
            ScalarValue::Float64(Some(4_503_599_627_370_497.0))
        );
        let mut a = acc(Output::Avg, &[Some(i64::MAX), Some(i64::MAX)]);
        assert_eq!(
            a.evaluate().unwrap(),
            ScalarValue::Float64(Some(9_223_372_036_854_775_808.0))
        );
    }

    #[test]
    fn exact_int_state_round_trip() {
        let mut a = acc(Output::Sum, &[Some(i64::MAX), Some(i64::MAX)]);
        let state: Vec<ArrayRef> = a
            .state()
            .unwrap()
            .into_iter()
            .map(|v| v.to_array().unwrap())
            .collect();
        let mut b = acc(Output::Sum, &[Some(i64::MIN), Some(i64::MIN)]);
        b.merge_batch(&state).unwrap();
        assert_eq!(b.evaluate().unwrap(), ScalarValue::Int64(Some(-2)));
    }
}

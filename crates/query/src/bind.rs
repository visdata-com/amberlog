//! 绑定列与类型检查（REQ-AMB-QRY-SEARCH-ENDPOINT-V5「输出列名与名字的解析」「绑定列与类型检查」「SQL 语义」），
//! 产出交给 DataFusion 的表达式。
//!
//! 报告顺序里这一步占两位：先查全部列引用是否都在「stream 当前 schema」（系统表为其列集合）中（`unknown_column`），再查
//! 其余（一律 `invalid_sql`）：比较两侧不可比较、一侧为 `NULL` 字面量、`[NOT] IN` 列表含 `NULL` 字面量、`[NOT] LIKE`
//! 左边不是 Utf8 列、模式以未被转义的 `\` 结尾；输出列名重复而不是同一表达式；histogram 调用的间隔写法或范围不对
//! （REQ-AMB-QRY-HISTOGRAM）；`str_match` 的列不是 Utf8 列（REQ-AMB-QRY-TEXT-MATCH）；聚合查询的 `SELECT *`、未分组又非
//! 聚合调用的项、`sum`/`avg` 的参数不是数值列（REQ-AMB-QRY-AGGREGATION）。
//!
//! 名字的解析：`WHERE`、`SELECT` 列表与函数参数中的名字只指表的列；`GROUP BY` 与 `ORDER BY` 中作为一项的单个名字先按
//! 输出列名、再按表的列解析。输出列名与命名都自建，不用 DataFusion 的缺省命名。
//!
//! 不依赖 DataFusion 的隐式类型转换：Int64/Float64 列与数值字面量的比较在这里改写为同一数域内的比较
//! （Int64 列按精确值比整数界，不先近似为 Float64；Float64 列比字面量就近舍入所得的双精度数）。
//! 表中的 `_timestamp` 以 Int64（UTC 微秒）出现，与其列类型一致。

use std::cmp::Ordering;

use amberlog_common::error::Reason;
use amberlog_common::schema::{ColumnType, TIMESTAMP_COLUMN};
use amberlog_storage::schema::StreamColumns;
use bigdecimal::{BigDecimal, RoundingMode, Signed, ToPrimitive};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{BinaryExpr, Expr, Like, Operator};
use datafusion::prelude::{ident, lit};
use jiff::tz::TimeZone;

use crate::aggregate::{Agg, AggregatePlan, GroupKey, Slot};
use crate::error::QueryError;
use crate::histogram;
use crate::sql::{
    AggCall, AggFunc, CmpOp, Cond, Item, Literal, Operand, Projection, Query, SelectItem,
};
use crate::text_match;

/// 绑定后的查询。
#[derive(Debug, Clone)]
pub struct Bound {
    pub filter: Option<Expr>,
    pub shape: Shape,
    pub limit: Option<u64>,
}

/// 结果的形状。
#[derive(Debug, Clone)]
pub enum Shape {
    /// 非聚合查询：每个输入行一行。
    Rows {
        /// 输出列名与其在表上的表达式，按 `SELECT` 列表的次序（`*` 为全部列，按字段 id）；同名的同一表达式只出现一次。
        outputs: Vec<(String, Expr)>,
        /// 排序键（表上的表达式）与是否降序；空表示 SQL 不含 `ORDER BY`。
        order_by: Vec<(Expr, bool)>,
    },
    /// 聚合查询（REQ-AMB-QRY-AGGREGATION）。
    Aggregate(AggregatePlan),
}

/// 按 `schema` 绑定 `query`；`zone` 为请求的时区，只用于 histogram 调用的取值。
pub fn bind(query: &Query, schema: &StreamColumns, zone: &TimeZone) -> Result<Bound, QueryError> {
    let items: &[SelectItem] = match &query.projection {
        Projection::All => &[],
        Projection::Items(items) => items,
    };
    let is_output = |name: &str| match &query.projection {
        Projection::All => schema.get(name).is_some(),
        Projection::Items(items) => items.iter().any(|i| i.output_name() == name),
    };

    // 第一位：列是否存在
    let mut referenced: Vec<&str> = Vec::new();
    for i in items {
        item_columns(&i.item, &mut referenced);
    }
    if let Some(filter) = &query.filter {
        collect_columns(filter, &mut referenced);
    }
    let named = query
        .group_by
        .iter()
        .chain(query.order_by.iter().map(|k| &k.item));
    for item in named {
        match item {
            Item::Column(name) if is_output(name) => {}
            item => item_columns(item, &mut referenced),
        }
    }
    if let Some(missing) = referenced.into_iter().find(|c| schema.get(c).is_none()) {
        return Err(QueryError::new(
            Reason::UnknownColumn,
            format!("column `{missing}` does not exist in `{}`", query.stream),
        ));
    }

    // 第二位：其余检查
    let binder = Binder { schema, zone };
    let intervals = Intervals::of(query)?;
    check_duplicate_outputs(items, &intervals)?;
    let filter = query.filter.as_ref().map(|c| binder.cond(c)).transpose()?;
    let shape = if query.is_aggregate() {
        Shape::Aggregate(binder.aggregate(query, items, &intervals)?)
    } else {
        binder.rows(query, items, &intervals)
    };
    Ok(Bound {
        filter,
        shape,
        limit: query.limit,
    })
}

/// 一项所引用的表的列（函数参数中的名字只指表的列；histogram 调用引用 `_timestamp`）。
fn item_columns<'q>(item: &'q Item, out: &mut Vec<&'q str>) {
    match item {
        Item::Column(c) => out.push(c),
        Item::Agg(AggCall { arg: Some(c), .. }) => out.push(c),
        Item::Agg(_) => {}
        Item::Histogram(_) => out.push(TIMESTAMP_COLUMN),
    }
}

fn collect_columns<'q>(cond: &'q Cond, out: &mut Vec<&'q str>) {
    let operand = |o: &'q Operand, out: &mut Vec<&'q str>| {
        if let Operand::Column(c) = o {
            out.push(c);
        }
    };
    match cond {
        Cond::And(v) | Cond::Or(v) => v.iter().for_each(|c| collect_columns(c, out)),
        Cond::Not(c) => collect_columns(c, out),
        Cond::Cmp { left, right, .. } => {
            operand(left, out);
            operand(right, out);
        }
        Cond::In {
            operand: o, list, ..
        } => {
            operand(o, out);
            list.iter().for_each(|i| operand(i, out));
        }
        Cond::Like { operand: o, .. } | Cond::IsNull { operand: o, .. } => operand(o, out),
        Cond::StrMatch { column, .. } => out.push(column),
        Cond::MatchAll { .. } => {}
    }
}

/// 查询中各 histogram 调用的间隔原文到微秒数的换算（写法或范围不对即 `invalid_sql`）。
struct Intervals;

impl Intervals {
    fn of(query: &Query) -> Result<Intervals, QueryError> {
        let select = match &query.projection {
            Projection::All => &[][..],
            Projection::Items(items) => items,
        };
        let all = select
            .iter()
            .map(|i| &i.item)
            .chain(&query.group_by)
            .chain(query.order_by.iter().map(|k| &k.item));
        for item in all {
            if let Item::Histogram(text) = item {
                interval(text)?;
            }
        }
        Ok(Intervals)
    }

    /// 已核对过的间隔换算所得的微秒数。
    fn get(&self, text: &str) -> i64 {
        histogram::interval_micros(text).expect("间隔已核对")
    }
}

fn interval(text: &str) -> Result<i64, QueryError> {
    histogram::interval_micros(text).ok_or_else(|| {
        QueryError::invalid_sql(format!(
            "histogram interval `{text}` is invalid; use <integer><ms|s|m|h|d> with a positive value"
        ))
    })
}

/// 两项是否为同一表达式：同一列名；函数名与参数都相同的聚合调用；间隔换算所得相等的 histogram 调用。
fn same_expr(a: &Item, b: &Item, intervals: &Intervals) -> bool {
    match (a, b) {
        (Item::Histogram(x), Item::Histogram(y)) => intervals.get(x) == intervals.get(y),
        (a, b) => a == b,
    }
}

/// 输出列名相同而不是同一表达式即 `invalid_sql`。
fn check_duplicate_outputs(items: &[SelectItem], intervals: &Intervals) -> Result<(), QueryError> {
    for (k, a) in items.iter().enumerate() {
        for b in &items[..k] {
            if a.output_name() == b.output_name() && !same_expr(&a.item, &b.item, intervals) {
                return Err(QueryError::invalid_sql(format!(
                    "output column name `{}` is used by two different select list items",
                    a.output_name()
                )));
            }
        }
    }
    Ok(())
}

/// `GROUP BY`、`ORDER BY` 中的名字解析到的输出项（不是输出列名为 `None`）。重名的不同表达式已按输出列名重复报告。
fn output_item<'q>(items: &'q [SelectItem], name: &str) -> Option<&'q Item> {
    items
        .iter()
        .find(|i| i.output_name() == name)
        .map(|i| &i.item)
}

fn incomparable(what: impl Into<String>) -> QueryError {
    QueryError::invalid_sql(what)
}

struct Binder<'s> {
    schema: &'s StreamColumns,
    zone: &'s TimeZone,
}

/// 绑定后的操作数：列（名与类型）或字面量。
enum Side<'q> {
    Column(&'q str, ColumnType),
    Literal(&'q Literal),
}

impl Binder<'_> {
    fn side<'q>(&self, o: &'q Operand) -> Side<'q> {
        match o {
            Operand::Column(c) => Side::Column(c, self.column_type(c)),
            Operand::Literal(l) => Side::Literal(l),
        }
    }

    fn cond(&self, cond: &Cond) -> Result<Expr, QueryError> {
        Ok(match cond {
            Cond::And(v) => self
                .all(v)?
                .into_iter()
                .reduce(Expr::and)
                .expect("AND 至少两项"),
            Cond::Or(v) => self
                .all(v)?
                .into_iter()
                .reduce(Expr::or)
                .expect("OR 至少两项"),
            Cond::Not(c) => Expr::Not(Box::new(self.cond(c)?)),
            Cond::Cmp { op, left, right } => self.cmp(*op, left, right)?,
            Cond::In {
                negated,
                operand,
                list,
            } => {
                let (op, join): (CmpOp, fn(Expr, Expr) -> Expr) = if *negated {
                    (CmpOp::NotEq, Expr::and)
                } else {
                    (CmpOp::Eq, Expr::or)
                };
                list.iter()
                    .map(|item| self.cmp(op, operand, item))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .reduce(join)
                    .expect("IN 列表非空")
            }
            Cond::Like {
                negated,
                operand,
                pattern,
            } => {
                let Side::Column(name, ColumnType::Utf8) = self.side(operand) else {
                    return Err(incomparable(
                        "the left operand of [NOT] LIKE must be a Utf8 column",
                    ));
                };
                if ends_with_lone_escape(pattern) {
                    return Err(QueryError::invalid_sql(
                        "LIKE pattern must not end with an unescaped backslash",
                    ));
                }
                Expr::Like(Like::new(
                    *negated,
                    Box::new(ident(name)),
                    Box::new(lit(pattern.as_str())),
                    Some('\\'),
                    false,
                ))
            }
            Cond::IsNull { negated, operand } => match self.side(operand) {
                Side::Column(name, _) if *negated => ident(name).is_not_null(),
                Side::Column(name, _) => ident(name).is_null(),
                Side::Literal(l) => lit(matches!(l, Literal::Null) != *negated),
            },
            Cond::StrMatch { column, text } => {
                if self.column_type(column) != ColumnType::Utf8 {
                    return Err(QueryError::invalid_sql(format!(
                        "the first argument of str_match() must be a Utf8 column; `{column}` is {}",
                        self.column_type(column).as_str()
                    )));
                }
                text_match::str_match(column, text)
            }
            Cond::MatchAll { text } => {
                let searched: Vec<String> = self
                    .schema
                    .columns()
                    .iter()
                    .filter(|c| c.column_type == ColumnType::Utf8)
                    .map(|c| c.name.clone())
                    .collect();
                text_match::match_all(&searched, text)
            }
        })
    }

    fn column_type(&self, c: &str) -> ColumnType {
        self.schema
            .get(c)
            .expect("列引用已在第一位核对过")
            .column_type
    }

    /// 非聚合查询：输出项为列或逐行的 histogram 调用；`ORDER BY` 的名字先按输出列名解析。
    fn rows(&self, query: &Query, items: &[SelectItem], intervals: &Intervals) -> Shape {
        let expr = |item: &Item| match item {
            Item::Column(c) => ident(c),
            Item::Histogram(text) => histogram::expr(intervals.get(text), self.zone),
            Item::Agg(_) => unreachable!("非聚合查询不含聚合调用"),
        };
        let mut outputs: Vec<(String, Expr)> = Vec::new();
        match &query.projection {
            Projection::All => {
                outputs.extend(
                    self.schema
                        .columns()
                        .iter()
                        .map(|c| (c.name.clone(), ident(&c.name))),
                );
            }
            Projection::Items(_) => {
                for i in items {
                    let name = i.output_name();
                    if !outputs.iter().any(|(n, _)| n == name) {
                        outputs.push((name.to_owned(), expr(&i.item).alias(name)));
                    }
                }
            }
        }
        let order_by = query
            .order_by
            .iter()
            .map(|k| {
                let e = match &k.item {
                    Item::Column(name) => match output_item(items, name) {
                        Some(item) => expr(item),
                        None => ident(name),
                    },
                    item => expr(item),
                };
                (e, k.descending)
            })
            .collect();
        Shape::Rows { outputs, order_by }
    }

    /// 聚合查询：解析分组项，核对各项都是聚合调用或指代分组项，列出要算的聚合。
    fn aggregate(
        &self,
        query: &Query,
        items: &[SelectItem],
        intervals: &Intervals,
    ) -> Result<AggregatePlan, QueryError> {
        if query.projection == Projection::All {
            return Err(QueryError::invalid_sql(
                "SELECT * is not allowed in an aggregate query; list grouped columns and aggregate calls",
            ));
        }
        let mut plan = AggregatePlan {
            groups: Vec::new(),
            aggs: Vec::new(),
            outputs: Vec::new(),
            order_by: Vec::new(),
            zone: self.zone.clone(),
        };
        for g in &query.group_by {
            let key = match g {
                Item::Column(name) => match output_item(items, name) {
                    Some(Item::Column(c)) => self.group_column(c),
                    Some(Item::Histogram(text)) => GroupKey::Histogram(intervals.get(text)),
                    Some(Item::Agg(_)) => {
                        return Err(QueryError::unsupported(format!(
                            "GROUP BY `{name}` refers to an aggregate call"
                        )));
                    }
                    None => self.group_column(name),
                },
                Item::Histogram(text) => GroupKey::Histogram(intervals.get(text)),
                Item::Agg(_) => unreachable!("GROUP BY 中的聚合调用已在子集校验中拒绝"),
            };
            if !plan.groups.contains(&key) {
                plan.groups.push(key);
            }
        }
        for i in items {
            let name = i.output_name();
            if plan.outputs.iter().any(|(n, _)| n == name) {
                continue;
            }
            let slot = self.slot(&mut plan, &i.item, intervals)?;
            plan.outputs.push((name.to_owned(), slot));
        }
        for k in &query.order_by {
            let slot = match &k.item {
                Item::Column(name) => match plan.outputs.iter().find(|(n, _)| n == name) {
                    Some((_, slot)) => *slot,
                    None => self.slot(&mut plan, &k.item, intervals)?,
                },
                item => self.slot(&mut plan, item, intervals)?,
            };
            plan.order_by.push((slot, k.descending));
        }
        Ok(plan)
    }

    fn group_column(&self, c: &str) -> GroupKey {
        GroupKey::Column(c.to_owned(), self.column_type(c))
    }

    /// 聚合查询中一项所在的列：聚合调用为它的聚合（必要时新增），列与 histogram 调用须指代某个分组项。
    fn slot(
        &self,
        plan: &mut AggregatePlan,
        item: &Item,
        intervals: &Intervals,
    ) -> Result<Slot, QueryError> {
        let not_grouped = |what: String| {
            QueryError::invalid_sql(format!(
                "{what} must appear in GROUP BY or be used in an aggregate call"
            ))
        };
        match item {
            Item::Column(c) => plan
                .groups
                .iter()
                .position(|g| matches!(g, GroupKey::Column(n, _) if n == c))
                .map(Slot::Group)
                .ok_or_else(|| not_grouped(format!("column `{c}`"))),
            Item::Histogram(text) => {
                let i = intervals.get(text);
                plan.groups
                    .iter()
                    .position(|g| *g == GroupKey::Histogram(i))
                    .map(Slot::Group)
                    .ok_or_else(|| not_grouped(format!("histogram(_timestamp, '{text}')")))
            }
            Item::Agg(call) => {
                let agg = self.agg(call)?;
                let j = match plan.aggs.iter().position(|a| *a == agg) {
                    Some(j) => j,
                    None => {
                        plan.aggs.push(agg);
                        plan.aggs.len() - 1
                    }
                };
                Ok(Slot::Agg(j))
            }
        }
    }

    fn agg(&self, call: &AggCall) -> Result<Agg, QueryError> {
        let arg = call.arg.as_ref().map(|c| (c.clone(), self.column_type(c)));
        if let (AggFunc::Sum | AggFunc::Avg, Some((c, t))) = (call.func, &arg)
            && !matches!(t, ColumnType::Int64 | ColumnType::Float64)
        {
            return Err(QueryError::invalid_sql(format!(
                "{}() needs an Int64 or Float64 column; `{c}` is {}",
                call.func.name(),
                t.as_str()
            )));
        }
        Ok(Agg {
            func: call.func,
            arg,
        })
    }

    fn all(&self, v: &[Cond]) -> Result<Vec<Expr>, QueryError> {
        v.iter().map(|c| self.cond(c)).collect()
    }

    fn cmp(&self, op: CmpOp, left: &Operand, right: &Operand) -> Result<Expr, QueryError> {
        match (self.side(left), self.side(right)) {
            (_, Side::Literal(Literal::Null)) | (Side::Literal(Literal::Null), _) => Err(
                incomparable("comparison with the NULL literal is not allowed; use IS [NOT] NULL"),
            ),
            (Side::Column(a, ta), Side::Column(b, tb)) => {
                if ta == tb {
                    Ok(binary(ident(a), op, ident(b)))
                } else {
                    Err(incomparable(format!(
                        "cannot compare column `{a}` ({}) with column `{b}` ({})",
                        ta.as_str(),
                        tb.as_str()
                    )))
                }
            }
            (Side::Column(c, t), Side::Literal(l)) => column_literal(c, t, op, l),
            (Side::Literal(l), Side::Column(c, t)) => column_literal(c, t, op.flip(), l),
            (Side::Literal(a), Side::Literal(b)) => literal_literal(op, a, b).map(lit),
        }
    }
}

fn binary(l: Expr, op: CmpOp, r: Expr) -> Expr {
    let op = match op {
        CmpOp::Eq => Operator::Eq,
        CmpOp::NotEq => Operator::NotEq,
        CmpOp::Lt => Operator::Lt,
        CmpOp::LtEq => Operator::LtEq,
        CmpOp::Gt => Operator::Gt,
        CmpOp::GtEq => Operator::GtEq,
    };
    Expr::BinaryExpr(BinaryExpr::new(Box::new(l), op, Box::new(r)))
}

/// 对非 NULL 行恒真、对 NULL 行为 NULL 的条件（SQL 三值逻辑下与「该列 op 某个它永远满足的值」等价）。
fn always_true(c: &str) -> Expr {
    ident(c).eq(ident(c))
}

/// 对非 NULL 行恒假、对 NULL 行为 NULL 的条件。
fn always_false(c: &str) -> Expr {
    ident(c).not_eq(ident(c))
}

fn column_literal(c: &str, t: ColumnType, op: CmpOp, l: &Literal) -> Result<Expr, QueryError> {
    let mismatch = || {
        Err(incomparable(format!(
            "cannot compare column `{c}` ({}) with this literal",
            t.as_str()
        )))
    };
    match (t, l) {
        (ColumnType::Int64, Literal::Number(x, _)) => Ok(int_cmp(c, op, x)),
        (ColumnType::Float64, Literal::Number(x, _)) => Ok(float_cmp(c, op, x)),
        (ColumnType::Utf8, Literal::String(s)) => Ok(binary(ident(c), op, lit(s.as_str()))),
        (ColumnType::Boolean, Literal::Bool(b)) => Ok(binary(ident(c), op, lit(*b))),
        _ => mismatch(),
    }
}

/// `x` 落在 i64 之内时为其值；否则为它越出的方向。
fn to_i64(x: &BigDecimal) -> Result<i64, Ordering> {
    x.to_i64().ok_or_else(|| {
        if x.is_negative() {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    })
}

/// Int64 列与数值字面量 `x` 的精确比较：非整数的 `x` 换成等价的整数界（`c < x` 即 `c < ceil(x)`，
/// `c <= x` 即 `c <= floor(x)`），越出 i64 的界按恒真或恒假处理。
fn int_cmp(c: &str, op: CmpOp, x: &BigDecimal) -> Expr {
    let floor = to_i64(&x.with_scale_round(0, RoundingMode::Floor));
    let ceil = to_i64(&x.with_scale_round(0, RoundingMode::Ceiling));
    let col = || ident(c);
    let i = |v: i64| lit(ScalarValue::Int64(Some(v)));
    match op {
        CmpOp::Eq | CmpOp::NotEq => {
            let eq = op == CmpOp::Eq;
            match (x.is_integer(), floor) {
                (true, Ok(v)) => binary(col(), op, i(v)),
                _ if eq => always_false(c),
                _ => always_true(c),
            }
        }
        // c < x ⇔ c < ceil(x)；c >= x ⇔ c >= ceil(x)
        CmpOp::Lt | CmpOp::GtEq => match ceil {
            Ok(v) => binary(col(), op, i(v)),
            Err(Ordering::Greater) if op == CmpOp::Lt => always_true(c),
            Err(Ordering::Greater) => always_false(c),
            Err(_) if op == CmpOp::Lt => always_false(c),
            Err(_) => always_true(c),
        },
        // c <= x ⇔ c <= floor(x)；c > x ⇔ c > floor(x)
        CmpOp::LtEq | CmpOp::Gt => match floor {
            Ok(v) => binary(col(), op, i(v)),
            Err(Ordering::Greater) if op == CmpOp::LtEq => always_true(c),
            Err(Ordering::Greater) => always_false(c),
            Err(_) if op == CmpOp::LtEq => always_false(c),
            Err(_) => always_true(c),
        },
    }
}

/// Float64 列与数值字面量 `x` 的比较：先把 `x` 按就近舍入（恰在正中取偶）换为双精度数 `d`，再比两侧的数学值
/// （REQ-AMB-QRY-SEARCH-ENDPOINT-V3）。Rust 的 `f64` 解析是正确舍入的，超出有限范围得同号无穷大；列值都是有限数，与无穷大比较按恒真
/// 或恒假处理。零单独处理：`-0.0` 与 `0.0` 数学上都是 0，用 `[-0.0, 0.0]` 两端比较，不论引擎按 IEEE 相等还是
/// 全序比较都成立。
fn float_cmp(c: &str, op: CmpOp, x: &BigDecimal) -> Expr {
    let col = || ident(c);
    let f = |v: f64| lit(ScalarValue::Float64(Some(v)));
    let d: f64 = x
        .to_string()
        .parse()
        .expect("BigDecimal 的十进制文本可解析为 f64");
    if d == 0.0 {
        let zero_band = || col().gt_eq(f(-0.0)).and(col().lt_eq(f(0.0)));
        return match op {
            CmpOp::Eq => zero_band(),
            CmpOp::NotEq => Expr::Not(Box::new(zero_band())),
            CmpOp::Lt => col().lt(f(-0.0)),
            CmpOp::LtEq => col().lt_eq(f(0.0)),
            CmpOp::Gt => col().gt(f(0.0)),
            CmpOp::GtEq => col().gt_eq(f(-0.0)),
        };
    }
    if d.is_infinite() {
        // 列值 < +∞、> -∞ 恒成立
        let below = d > 0.0;
        return match op {
            CmpOp::Eq => always_false(c),
            CmpOp::NotEq => always_true(c),
            CmpOp::Lt | CmpOp::LtEq if below => always_true(c),
            CmpOp::Gt | CmpOp::GtEq if !below => always_true(c),
            _ => always_false(c),
        };
    }
    binary(col(), op, f(d))
}

/// 两个字面量之间的比较，绑定时求值。只有同类的字面量可比（数值按精确值、字符串按码点、布尔 `false < true`）。
fn literal_literal(op: CmpOp, a: &Literal, b: &Literal) -> Result<bool, QueryError> {
    let ord = match (a, b) {
        (Literal::Number(x, _), Literal::Number(y, _)) => x.cmp(y),
        (Literal::String(x), Literal::String(y)) => x.cmp(y),
        (Literal::Bool(x), Literal::Bool(y)) => x.cmp(y),
        _ => return Err(incomparable("cannot compare literals of different types")),
    };
    Ok(match op {
        CmpOp::Eq => ord.is_eq(),
        CmpOp::NotEq => ord.is_ne(),
        CmpOp::Lt => ord.is_lt(),
        CmpOp::LtEq => ord.is_le(),
        CmpOp::Gt => ord.is_gt(),
        CmpOp::GtEq => ord.is_ge(),
    })
}

/// 模式是否以一个未被转义的 `\` 结尾（`\` 转义其后的一个字符，含 `\` 本身）。
fn ends_with_lone_escape(pattern: &str) -> bool {
    let mut chars = pattern.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' && chars.next().is_none() {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use amberlog_common::error::Reason;
    use amberlog_storage::schema::SchemaColumn;

    fn schema() -> StreamColumns {
        let cols = [
            ("_timestamp", ColumnType::Int64),
            ("a", ColumnType::Int64),
            ("f", ColumnType::Float64),
            ("b", ColumnType::Boolean),
            ("m", ColumnType::Utf8),
        ];
        StreamColumns::from_columns(
            cols.iter()
                .enumerate()
                .map(|(i, (n, t))| SchemaColumn {
                    name: (*n).into(),
                    column_type: *t,
                    field_id: i as u32 + 1,
                })
                .collect(),
        )
    }

    fn reason(sql: &str) -> Result<Bound, Reason> {
        bind(&crate::sql::parse(sql).unwrap(), &schema(), &TimeZone::UTC).map_err(|e| e.reason)
    }

    /// 输出列名，按输出次序。
    fn outputs(b: Bound) -> Vec<String> {
        match b.shape {
            Shape::Rows { outputs, .. } => outputs.into_iter().map(|(n, _)| n).collect(),
            Shape::Aggregate(plan) => plan.outputs.into_iter().map(|(n, _)| n).collect(),
        }
    }

    /// 场景「类型不可比较与 LIKE 的绑定检查」的条件表（绑定这一层）。
    #[test]
    fn type_check_table() {
        for cond in [
            "a = 'x'",
            "a = f",
            "b = 1",
            "m = 1",
            "a = NULL",
            "NULL = a",
            "a IN (1, NULL)",
            "a NOT IN (1, 'x')",
            "a LIKE 'x'",
            "'x' LIKE 'x'",
            "m LIKE 'x\\'",
            "m LIKE '\\\\\\'",
            "1 = 'x'",
            "TRUE = 1",
        ] {
            assert_eq!(
                reason(&format!("SELECT a FROM tc WHERE {cond}")).err(),
                Some(Reason::InvalidSql),
                "{cond}"
            );
        }
        for cond in [
            "a = 1.0",
            "f > 1",
            "b = TRUE",
            "m NOT LIKE 'y%'",
            "m LIKE 'x\\\\'",
            "_timestamp >= 0",
            "a = _timestamp",
            "1 < 2",
            "NULL IS NULL",
        ] {
            assert!(
                reason(&format!("SELECT a FROM tc WHERE {cond}")).is_ok(),
                "{cond}"
            );
        }
    }

    /// 列不存在先于类型检查（报告顺序），SELECT、WHERE 与 ORDER BY 中的引用都算。
    #[test]
    fn unknown_column_before_type_errors() {
        assert_eq!(
            reason("SELECT typo FROM app WHERE a = 'x'").err(),
            Some(Reason::UnknownColumn)
        );
        assert_eq!(
            reason("SELECT \"A\" FROM app").err(),
            Some(Reason::UnknownColumn)
        );
        assert_eq!(
            reason("SELECT a FROM app ORDER BY nope").err(),
            Some(Reason::UnknownColumn)
        );
        assert_eq!(
            reason("SELECT a FROM app WHERE x IN (1)").err(),
            Some(Reason::UnknownColumn)
        );
        assert_eq!(
            outputs(reason("SELECT _timestamp FROM app").unwrap()),
            ["_timestamp"]
        );
    }

    #[test]
    fn projection_star_and_duplicates() {
        assert_eq!(
            outputs(reason("SELECT * FROM app").unwrap()),
            ["_timestamp", "a", "f", "b", "m"]
        );
        assert_eq!(
            outputs(reason("SELECT m, a, m FROM app").unwrap()),
            ["m", "a"]
        );
        assert_eq!(
            outputs(
                reason("SELECT a AS m, m AS a, count(*), count(*) FROM app GROUP BY a, m").unwrap()
            ),
            ["m", "a", "count"]
        );
        assert_eq!(
            outputs(
                reason("SELECT histogram(_timestamp, '60s'), histogram(_timestamp, '1m') FROM app")
                    .unwrap()
            ),
            ["histogram"]
        );
    }

    /// 名字的解析与 `unknown_column`：`WHERE`、函数参数只指表的列；`GROUP BY`、`ORDER BY` 先按输出列名。
    #[test]
    fn name_resolution() {
        for sql in [
            "SELECT a AS x FROM app WHERE x = 1",
            "SELECT count(x) AS x FROM app",
            "SELECT count(*) AS n FROM app GROUP BY x",
            "SELECT a FROM app ORDER BY x",
            "SELECT histogram(_timestamp, '0s'), x FROM app",
        ] {
            assert_eq!(reason(sql).err(), Some(Reason::UnknownColumn), "{sql}");
        }
        for sql in [
            "SELECT a AS x FROM app ORDER BY x",
            "SELECT m AS a, count(*) AS n FROM app GROUP BY a ORDER BY a",
            "SELECT count(*) AS n FROM app GROUP BY a ORDER BY a, n DESC",
            "SELECT histogram(_timestamp, '24h') AS d FROM app GROUP BY histogram(_timestamp, '1d')",
            "SELECT histogram(_timestamp, '5m') AS t, count(*) FROM app GROUP BY t ORDER BY t",
            "SELECT f, count(*) FROM app GROUP BY f",
            "SELECT count(*) FROM app ORDER BY max(a)",
        ] {
            assert!(reason(sql).is_ok(), "{sql}");
        }
    }

    /// 后一位 `invalid_sql` 的各种情形（REQ-AMB-QRY-AGGREGATION、REQ-AMB-QRY-HISTOGRAM、REQ-AMB-QRY-TEXT-MATCH 与输出列名重复）。
    #[test]
    fn invalid_sql_after_columns() {
        for sql in [
            "SELECT count(*), count(a) FROM app",
            "SELECT a AS x, m AS x FROM app",
            "SELECT count(*) AS x, m AS x FROM app GROUP BY x",
            "SELECT histogram(_timestamp, '60s'), histogram(_timestamp, '2m') FROM app",
            "SELECT histogram(_timestamp, '5 minutes') FROM app",
            "SELECT histogram(_timestamp, '0s') FROM app",
            "SELECT a FROM app WHERE str_match(a, '1')",
            "SELECT m, count(*) FROM app GROUP BY a",
            "SELECT * FROM app GROUP BY a",
            "SELECT * FROM app ORDER BY count(*)",
            "SELECT sum(m) FROM app",
            "SELECT avg(b) FROM app",
            "SELECT histogram(_timestamp, '1h') AS h, count(*) FROM app GROUP BY histogram(_timestamp, '5m')",
            "SELECT histogram(_timestamp, '1h') AS h FROM app GROUP BY _timestamp",
            "SELECT count(*) FROM app GROUP BY a ORDER BY m",
        ] {
            assert_eq!(reason(sql).err(), Some(Reason::InvalidSql), "{sql}");
        }
        for sql in [
            "SELECT min(m), max(b), count(m), sum(f), avg(a) FROM app",
            "SELECT a FROM app WHERE str_match(m, 'x') AND match_all('y')",
        ] {
            assert!(reason(sql).is_ok(), "{sql}");
        }
    }

    #[test]
    fn lone_escape_detection() {
        assert!(ends_with_lone_escape("x\\"));
        assert!(ends_with_lone_escape("\\\\\\"));
        assert!(!ends_with_lone_escape("x\\\\"));
        assert!(!ends_with_lone_escape("%50\\%%"));
        assert!(!ends_with_lone_escape(""));
    }
}

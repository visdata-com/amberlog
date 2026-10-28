//! SQL 的解析与子集校验（REQ-AMB-QRY-SEARCH-ENDPOINT-V5「SQL 子集的形态与词法」「解析 SQL」）。
//!
//! 分两步，对应报告顺序中的 `invalid_sql` 与 `sql_unsupported` 两位：
//! 1. 能否解析：交给 PostgreSQL 17 原版的词法与语法分析器（`pg_query`，libpg_query 17）。报错，或除空白、注释与
//!    `;` 外不含任何内容，即 `invalid_sql`。
//! 2. 子集：在 pg_query 的语法树上按白名单校验，白名单之外一律 `sql_unsupported`。语法树里分不出来的写法
//!    （`E'…'`、`$$…$$` 与普通字符串都成为同一种字符串常量；`0x10`、`1_000` 与十进制都成为数值常量；
//!    `;;` 中的空语句不出现在语法树里；`-(1)` 与 `-1` 都折成负常量；别名前写没写 `AS`；`count(ALL a)` 与 `count(a)`）
//!    按词法分析所得的记号原文另行核对。
//!
//! 函数调用只收三个块所列的形态与位置：聚合调用（REQ-AMB-QRY-AGGREGATION）与 histogram 调用（REQ-AMB-QRY-HISTOGRAM）
//! 作为 `SELECT` 列表、`ORDER BY` 的一项本身，histogram 调用另可作为 `GROUP BY` 的一项；str_match 调用与 match_all 调用
//! （REQ-AMB-QRY-TEXT-MATCH）只作为 `WHERE` 中的一个完整条件。`GROUP BY` 中的名字恰指向一个聚合调用的输出项时也属聚合调用
//! 写错位置；同名的输出项不是同一表达式时留给绑定器按输出列名重复报告。
//!
//! 列是否存在、类型能否比较、名字解析到哪一项、间隔写法不在这里判定（要读数据集的 schema，排在 `stream_not_found`
//! 之后），见 [`crate::bind`]。

use std::str::FromStr;

use bigdecimal::BigDecimal;
use pg_query::NodeEnum;
use pg_query::protobuf::{
    AConst, AExpr, AExprKind, BoolExprType, FuncCall, LimitOption, NullTestType, ScanToken,
    SelectStmt, SetOperation, SortByDir, SortByNulls, Token, a_const,
};

use crate::error::QueryError;

/// 通过子集校验的查询。
#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    /// `FROM` 引用的名称（未加引号的已按小写解析）。是否是合法的 stream 名由调用方判定。
    pub stream: String,
    pub projection: Projection,
    pub filter: Option<Cond>,
    /// `GROUP BY` 各项（`Item::Column` 为一个名字，先按输出列名、再按表的列解析；不含 `Item::Agg`）。
    pub group_by: Vec<Item>,
    pub order_by: Vec<SortKey>,
    pub limit: Option<u64>,
}

impl Query {
    /// 是否为 REQ-AMB-QRY-AGGREGATION 所称聚合查询：含 `GROUP BY`，或 `SELECT` 列表、`ORDER BY` 含聚合调用。
    pub fn is_aggregate(&self) -> bool {
        let in_select = match &self.projection {
            Projection::All => false,
            Projection::Items(items) => items.iter().any(|i| matches!(i.item, Item::Agg(_))),
        };
        !self.group_by.is_empty()
            || in_select
            || self.order_by.iter().any(|k| matches!(k.item, Item::Agg(_)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Projection {
    All,
    Items(Vec<SelectItem>),
}

/// `SELECT` 列表的一项。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectItem {
    pub item: Item,
    /// `AS <别名>`（已按标识符规则解析）。
    pub alias: Option<String>,
}

impl SelectItem {
    /// 输出列名（REQ-AMB-QRY-SEARCH-ENDPOINT-V5）：别名、列名或函数名的小写。
    pub fn output_name(&self) -> &str {
        match (&self.alias, &self.item) {
            (Some(a), _) => a,
            (None, Item::Column(c)) => c,
            (None, Item::Agg(a)) => a.func.name(),
            (None, Item::Histogram(_)) => "histogram",
        }
    }
}

/// `SELECT` 列表、`GROUP BY` 与 `ORDER BY` 中的一项。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Item {
    /// 一个名字。`SELECT` 列表中只指表的列；`GROUP BY`、`ORDER BY` 中先按输出列名解析。
    Column(String),
    Agg(AggCall),
    /// histogram 调用，间隔为字符串字面量的原文取值（写法与范围由绑定器核对）。
    Histogram(String),
}

/// 聚合调用：`count(*)`，或以单个列名为唯一参数的五个聚合函数之一。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AggCall {
    pub func: AggFunc,
    /// 参数列名；`count(*)` 为 `None`。
    pub arg: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggFunc {
    pub fn name(self) -> &'static str {
        match self {
            AggFunc::Count => "count",
            AggFunc::Sum => "sum",
            AggFunc::Avg => "avg",
            AggFunc::Min => "min",
            AggFunc::Max => "max",
        }
    }

    fn from_name(name: &str) -> Option<AggFunc> {
        Some(match name {
            "count" => AggFunc::Count,
            "sum" => AggFunc::Sum,
            "avg" => AggFunc::Avg,
            "min" => AggFunc::Min,
            "max" => AggFunc::Max,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortKey {
    pub item: Item,
    pub descending: bool,
}

/// WHERE 条件。
#[derive(Debug, Clone, PartialEq)]
pub enum Cond {
    And(Vec<Cond>),
    Or(Vec<Cond>),
    Not(Box<Cond>),
    Cmp {
        op: CmpOp,
        left: Operand,
        right: Operand,
    },
    In {
        negated: bool,
        operand: Operand,
        list: Vec<Operand>,
    },
    Like {
        negated: bool,
        operand: Operand,
        /// 模式原文（字符串字面量的取值）；`\` 是转义符，由绑定器核对结尾。
        pattern: String,
    },
    IsNull {
        negated: bool,
        operand: Operand,
    },
    /// `str_match(<列>, '<文本>')`（REQ-AMB-QRY-TEXT-MATCH）。
    StrMatch {
        column: String,
        text: String,
    },
    /// `match_all('<文本>')`（REQ-AMB-QRY-TEXT-MATCH）。
    MatchAll {
        text: String,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl CmpOp {
    /// 交换左右两侧后的运算符（`1 < a` 即 `a > 1`）。
    pub fn flip(self) -> CmpOp {
        match self {
            CmpOp::Lt => CmpOp::Gt,
            CmpOp::LtEq => CmpOp::GtEq,
            CmpOp::Gt => CmpOp::Lt,
            CmpOp::GtEq => CmpOp::LtEq,
            op => op,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Operand {
    Column(String),
    Literal(Literal),
}

/// 查询子集中的字面量。数值按十进制原文精确保存，另记原文的写法。
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Number(BigDecimal, Numeral),
    String(String),
    Bool(bool),
    Null,
}

/// 数值字面量原文的写法：只有数字（可带负号）为 [`Numeral::Integer`]，含小数点或指数部分为 [`Numeral::Decimal`]。
/// 比较语义只看数值；REQ-AMB-QRY-EQUALITY-BLOOM-TERM-EQ-COND 只把前者算作整数字面量。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Numeral {
    Integer,
    Decimal,
}

impl Numeral {
    fn of(numeral: &str) -> Numeral {
        if numeral.contains(['.', 'e', 'E']) {
            Numeral::Decimal
        } else {
            Numeral::Integer
        }
    }
}

/// 解析并校验一条 SQL。
pub fn parse(sql: &str) -> Result<Query, QueryError> {
    let parsed = pg_query::parse(sql).map_err(|e| QueryError::invalid_sql(e.to_string()))?;
    let tokens = Tokens::scan(sql)?;
    let stmts = &parsed.protobuf.stmts;
    if stmts.is_empty() {
        return Err(QueryError::invalid_sql("SQL contains no statement"));
    }
    if stmts.len() > 1 {
        return Err(QueryError::unsupported(
            "only a single SELECT statement is supported",
        ));
    }
    tokens.check_lexical()?;
    let stmt = stmts[0].stmt.as_ref().and_then(|n| n.node.as_ref());
    let Some(NodeEnum::SelectStmt(select)) = stmt else {
        return Err(QueryError::unsupported("only SELECT is supported"));
    };
    Checker { tokens: &tokens }.select(select)
}

fn unsupported(what: &str) -> QueryError {
    QueryError::unsupported(format!("{what} is not supported"))
}

/// 词法分析所得的记号（不含空白；含注释）。
struct Tokens<'a> {
    sql: &'a str,
    tokens: Vec<ScanToken>,
}

impl<'a> Tokens<'a> {
    fn scan(sql: &'a str) -> Result<Self, QueryError> {
        // 能走到这里的 SQL 已能解析，词法分析不会失败；万一失败仍按无法解析作答。
        let tokens = pg_query::scan(sql)
            .map_err(|e| QueryError::invalid_sql(e.to_string()))?
            .tokens;
        Ok(Tokens { sql, tokens })
    }

    fn text(&self, t: &ScanToken) -> &'a str {
        &self.sql[t.start as usize..t.end as usize]
    }

    /// 不计注释的记号。
    fn significant(&self) -> impl Iterator<Item = (usize, &ScanToken)> {
        self.tokens
            .iter()
            .enumerate()
            .filter(|(_, t)| !matches!(t.token(), Token::SqlComment | Token::CComment))
    }

    /// 整条 SQL 的词法核对：至多一个 `;` 且只能在末尾；数值常量只收十进制写法；字符串常量只收普通单引号写法。
    fn check_lexical(&self) -> Result<(), QueryError> {
        let significant: Vec<&ScanToken> = self.significant().map(|(_, t)| t).collect();
        let semicolons = significant
            .iter()
            .filter(|t| t.token() == Token::Ascii59)
            .count();
        let ends_with_semicolon = significant
            .last()
            .is_some_and(|t| t.token() == Token::Ascii59);
        if semicolons > 1 || (semicolons == 1 && !ends_with_semicolon) {
            return Err(QueryError::unsupported(
                "only a single statement with an optional trailing `;` is supported",
            ));
        }
        for t in significant {
            match t.token() {
                Token::Iconst | Token::Fconst if !is_decimal_numeral(self.text(t)) => {
                    return Err(QueryError::unsupported(format!(
                        "numeric literal `{}` is not supported; use plain decimal notation",
                        self.text(t)
                    )));
                }
                Token::Sconst if !self.text(t).starts_with('\'') => {
                    return Err(QueryError::unsupported(format!(
                        "string literal `{}` is not supported; use a plain single-quoted string",
                        self.text(t)
                    )));
                }
                Token::Usconst | Token::Bconst | Token::Xconst => {
                    return Err(unsupported("this literal notation"));
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// 起始字节为 `location` 的记号的下标。
    fn at(&self, location: i32) -> Option<usize> {
        self.tokens.iter().position(|t| t.start == location)
    }

    /// 下标 `i` 之后第一个非注释记号的下标。
    fn next_significant(&self, i: usize) -> Option<usize> {
        (i + 1..self.tokens.len())
            .find(|&j| !matches!(self.tokens[j].token(), Token::SqlComment | Token::CComment))
    }

    /// 下标 `i` 之前最后一个非注释记号的下标。
    fn prev_significant(&self, i: usize) -> Option<usize> {
        (0..i)
            .rev()
            .find(|&j| !matches!(self.tokens[j].token(), Token::SqlComment | Token::CComment))
    }

    fn is_numeric(&self, i: usize) -> bool {
        matches!(self.tokens[i].token(), Token::Iconst | Token::Fconst)
    }
}

/// 十进制数值写法：一串数字，可带小数点及小数部分（小数点前后至少一侧有数字），可带 `e`/`E` 引导、
/// 可带正负号的十进制指数。
fn is_decimal_numeral(s: &str) -> bool {
    let (mantissa, exponent) = match s.find(['e', 'E']) {
        Some(i) => (&s[..i], Some(&s[i + 1..])),
        None => (s, None),
    };
    let (int, frac) = match mantissa.split_once('.') {
        Some((i, f)) => (i, Some(f)),
        None => (mantissa, None),
    };
    let digits = |p: &str| p.bytes().all(|b| b.is_ascii_digit());
    let mantissa_ok = digits(int)
        && frac.is_none_or(digits)
        && (!int.is_empty() || frac.is_some_and(|f| !f.is_empty()));
    let exponent_ok = exponent.is_none_or(|e| {
        let e = e.strip_prefix(['+', '-']).unwrap_or(e);
        !e.is_empty() && digits(e)
    });
    mantissa_ok && exponent_ok
}

/// 十进制原文（可带符号）的精确值。
fn decimal_value(negative: bool, numeral: &str) -> Result<BigDecimal, QueryError> {
    // BigDecimal 不收 `1.`、`.5`、`1.e3` 这类写法，先补齐小数点两侧。
    let (mantissa, exponent) = match numeral.find(['e', 'E']) {
        Some(i) => (&numeral[..i], &numeral[i..]),
        None => (numeral, ""),
    };
    let mut text = String::new();
    if negative {
        text.push('-');
    }
    if mantissa.starts_with('.') {
        text.push('0');
    }
    text.push_str(mantissa);
    if mantissa.ends_with('.') {
        text.push('0');
    }
    text.push_str(exponent);
    BigDecimal::from_str(&text).map_err(|_| {
        QueryError::unsupported(format!("numeric literal `{numeral}` is out of range"))
    })
}

struct Checker<'t, 'a> {
    tokens: &'t Tokens<'a>,
}

impl Checker<'_, '_> {
    fn select(&self, s: &SelectStmt) -> Result<Query, QueryError> {
        if s.op() != SetOperation::SetopNone || s.larg.is_some() || s.rarg.is_some() {
            return Err(unsupported("UNION, INTERSECT or EXCEPT"));
        }
        if s.with_clause.is_some() {
            return Err(unsupported("WITH"));
        }
        if s.into_clause.is_some() {
            return Err(unsupported("SELECT INTO"));
        }
        if !s.distinct_clause.is_empty() {
            return Err(unsupported("DISTINCT"));
        }
        if s.group_distinct {
            return Err(unsupported("GROUP BY DISTINCT"));
        }
        if s.having_clause.is_some() {
            return Err(unsupported("HAVING"));
        }
        if !s.window_clause.is_empty() {
            return Err(unsupported("WINDOW"));
        }
        if !s.values_lists.is_empty() {
            return Err(unsupported("VALUES"));
        }
        if !s.locking_clause.is_empty() {
            return Err(unsupported("FOR UPDATE and other locking clauses"));
        }
        if s.limit_offset.is_some() {
            return Err(unsupported("OFFSET"));
        }

        let stream = self.from(&s.from_clause)?;
        let projection = self.projection(&s.target_list)?;
        let filter = s
            .where_clause
            .as_deref()
            .map(|w| self.cond(w))
            .transpose()?;
        let group_by = s
            .group_clause
            .iter()
            .map(|n| self.group_item(n))
            .collect::<Result<Vec<_>, _>>()?;
        let order_by = s
            .sort_clause
            .iter()
            .map(|n| self.sort_key(n))
            .collect::<Result<_, _>>()?;
        let limit = match s.limit_option() {
            LimitOption::Default | LimitOption::Undefined => None,
            LimitOption::Count => Some(self.limit(s.limit_count.as_deref())?),
            LimitOption::WithTies => return Err(unsupported("FETCH … WITH TIES")),
        };
        group_by_names_not_aggregates(&projection, &group_by)?;
        Ok(Query {
            stream,
            projection,
            filter,
            group_by,
            order_by,
            limit,
        })
    }

    fn from(&self, from: &[pg_query::Node]) -> Result<String, QueryError> {
        let [only] = from else {
            return Err(QueryError::unsupported("FROM must name exactly one stream"));
        };
        let Some(NodeEnum::RangeVar(r)) = only.node.as_ref() else {
            return Err(unsupported(
                "a FROM item other than a stream name (subquery, JOIN, function…)",
            ));
        };
        if !r.catalogname.is_empty() || !r.schemaname.is_empty() {
            return Err(unsupported("a qualified stream name"));
        }
        if r.alias.is_some() {
            return Err(unsupported("a table alias"));
        }
        if !r.inh {
            return Err(unsupported("FROM ONLY"));
        }
        Ok(self.identifier(r.location, &r.relname))
    }

    fn projection(&self, targets: &[pg_query::Node]) -> Result<Projection, QueryError> {
        let mut items = Vec::with_capacity(targets.len());
        let mut star = false;
        for t in targets {
            let Some(NodeEnum::ResTarget(rt)) = t.node.as_ref() else {
                return Err(unsupported("this select list item"));
            };
            if !rt.indirection.is_empty() {
                return Err(unsupported("subscripts or field selection"));
            }
            let item = match rt.val.as_deref().and_then(|n| n.node.as_ref()) {
                Some(NodeEnum::ColumnRef(c)) => match c.fields.as_slice() {
                    [f] => match f.node.as_ref() {
                        Some(NodeEnum::String(s)) => {
                            Item::Column(self.identifier(c.location, &s.sval))
                        }
                        Some(NodeEnum::AStar(_)) if rt.name.is_empty() => {
                            star = true;
                            continue;
                        }
                        _ => return Err(unsupported("this column reference")),
                    },
                    _ => return Err(unsupported("a qualified column name")),
                },
                Some(NodeEnum::FuncCall(f)) => match self.call(f)? {
                    Call::Agg(a) => Item::Agg(a),
                    Call::Histogram(i) => Item::Histogram(i),
                },
                _ => {
                    return Err(QueryError::unsupported(
                        "the select list may only contain column names, aggregate calls, histogram calls or `*`",
                    ));
                }
            };
            let alias = if rt.name.is_empty() {
                None
            } else {
                Some(self.alias(rt.location, &rt.name)?)
            };
            items.push(SelectItem { item, alias });
        }
        match (star, items.is_empty()) {
            (true, true) if targets.len() == 1 => Ok(Projection::All),
            (false, false) => Ok(Projection::Items(items)),
            (true, _) => Err(unsupported("`*` combined with other select list items")),
            (false, true) => Err(unsupported("an empty select list")),
        }
    }

    /// 输出项的别名：须以 `AS` 写出，且不以 `_` 开头。pg_query 的语法树里写不写 `AS` 同形，按记号核对：
    /// 输出项自 `location` 起、到同层的 `,`、`FROM` 或 `;` 为止，最后一个记号是别名，它前面的记号须为 `AS`。
    /// 同层紧跟在 `AS` 之后的记号总是别名，即使它是 `FROM` 等关键字（PostgreSQL 17 收 `AS <列标签>`）。
    fn alias(&self, location: i32, parsed: &str) -> Result<String, QueryError> {
        let omitted = || unsupported("an alias without AS");
        let start = self.tokens.at(location).ok_or_else(omitted)?;
        let mut depth = 0i32;
        let mut last = None;
        let mut before_last = None;
        let mut i = Some(start);
        while let Some(j) = i {
            let after_as = depth == 0
                && last.is_some_and(|l: usize| self.tokens.tokens[l].token() == Token::As);
            if after_as {
                before_last = last;
                last = Some(j);
                break;
            }
            match self.tokens.tokens[j].token() {
                Token::Ascii40 => depth += 1,
                Token::Ascii41 => depth -= 1,
                Token::Ascii44 | Token::From | Token::Ascii59 if depth == 0 => break,
                _ => {}
            }
            before_last = last;
            last = Some(j);
            i = self.tokens.next_significant(j);
        }
        let (Some(alias), Some(as_kw)) = (last, before_last) else {
            return Err(omitted());
        };
        if self.tokens.tokens[as_kw].token() != Token::As {
            return Err(omitted());
        }
        let name = self.identifier(self.tokens.tokens[alias].start, parsed);
        if name.starts_with('_') {
            return Err(unsupported("an alias starting with `_`"));
        }
        Ok(name)
    }

    fn group_item(&self, n: &pg_query::Node) -> Result<Item, QueryError> {
        match n.node.as_ref() {
            Some(NodeEnum::ColumnRef(c)) => Ok(Item::Column(self.column_name(c)?)),
            Some(NodeEnum::FuncCall(f)) => match self.call(f)? {
                Call::Histogram(i) => Ok(Item::Histogram(i)),
                Call::Agg(_) => Err(unsupported("an aggregate call in GROUP BY")),
            },
            _ => Err(QueryError::unsupported(
                "GROUP BY may only contain names and histogram calls",
            )),
        }
    }

    fn sort_key(&self, n: &pg_query::Node) -> Result<SortKey, QueryError> {
        let Some(NodeEnum::SortBy(s)) = n.node.as_ref() else {
            return Err(unsupported("this ORDER BY item"));
        };
        let descending = match s.sortby_dir() {
            SortByDir::SortbyDefault | SortByDir::SortbyAsc | SortByDir::Undefined => false,
            SortByDir::SortbyDesc => true,
            SortByDir::SortbyUsing => return Err(unsupported("ORDER BY … USING")),
        };
        if !matches!(
            s.sortby_nulls(),
            SortByNulls::SortbyNullsDefault | SortByNulls::Undefined
        ) {
            return Err(unsupported("NULLS FIRST / NULLS LAST"));
        }
        if !s.use_op.is_empty() {
            return Err(unsupported("ORDER BY … USING"));
        }
        let item = match s.node.as_deref().and_then(|n| n.node.as_ref()) {
            Some(NodeEnum::ColumnRef(c)) => Item::Column(self.column_name(c)?),
            Some(NodeEnum::FuncCall(f)) => match self.call(f)? {
                Call::Agg(a) => Item::Agg(a),
                Call::Histogram(i) => Item::Histogram(i),
            },
            _ => {
                return Err(QueryError::unsupported(
                    "ORDER BY may only contain names, aggregate calls and histogram calls",
                ));
            }
        };
        Ok(SortKey { item, descending })
    }

    /// 作为 `SELECT`、`GROUP BY` 或 `ORDER BY` 一项本身的函数调用：聚合调用或 histogram 调用，形态不合即 `sql_unsupported`。
    fn call(&self, f: &FuncCall) -> Result<Call, QueryError> {
        let name = function_name(f)?;
        if let Some(func) = AggFunc::from_name(name) {
            return self.agg_call(func, f).map(Call::Agg);
        }
        match name {
            "histogram" => {
                plain_call(f)?;
                let [column, interval] = f.args.as_slice() else {
                    return Err(form("histogram(_timestamp, '<interval>')"));
                };
                match column.node.as_ref() {
                    Some(NodeEnum::ColumnRef(c)) if self.column_name(c)? == "_timestamp" => {}
                    _ => return Err(form("histogram(_timestamp, '<interval>')")),
                }
                let interval = string_constant(interval)
                    .ok_or_else(|| form("histogram(_timestamp, '<interval>')"))?;
                Ok(Call::Histogram(interval))
            }
            "str_match" | "match_all" => Err(QueryError::unsupported(format!(
                "{name}() may only be used as a condition in WHERE"
            ))),
            other => Err(QueryError::unsupported(format!(
                "function `{other}` is not supported"
            ))),
        }
    }

    fn agg_call(&self, func: AggFunc, f: &FuncCall) -> Result<AggCall, QueryError> {
        let shape = || {
            form(&format!(
                "{name}(<column>){star}",
                name = func.name(),
                star = if func == AggFunc::Count {
                    " or count(*)"
                } else {
                    ""
                }
            ))
        };
        if f.agg_distinct
            || f.agg_filter.is_some()
            || f.over.is_some()
            || f.agg_within_group
            || !f.agg_order.is_empty()
            || f.func_variadic
        {
            return Err(shape());
        }
        if f.agg_star {
            return if func == AggFunc::Count && f.args.is_empty() {
                Ok(AggCall { func, arg: None })
            } else {
                Err(shape())
            };
        }
        let [arg] = f.args.as_slice() else {
            return Err(shape());
        };
        let Some(NodeEnum::ColumnRef(c)) = arg.node.as_ref() else {
            return Err(shape());
        };
        let column = self.column_name(c)?;
        // `count(ALL a)` 在语法树里与 `count(a)` 同形：看函数名之后 `(` 的下一个记号
        let all = self
            .tokens
            .at(f.location)
            .and_then(|i| self.tokens.next_significant(i))
            .and_then(|i| self.tokens.next_significant(i))
            .is_some_and(|i| self.tokens.tokens[i].token() == Token::All);
        if all {
            return Err(shape());
        }
        Ok(AggCall {
            func,
            arg: Some(column),
        })
    }

    /// 作为 `WHERE` 中一个完整条件的函数调用：str_match 调用或 match_all 调用。
    fn text_match(&self, f: &FuncCall) -> Result<Cond, QueryError> {
        let name = function_name(f)?;
        match name {
            "str_match" => {
                plain_call(f)?;
                let shape = || form("str_match(<column>, '<text>')");
                let [column, text] = f.args.as_slice() else {
                    return Err(shape());
                };
                let Some(NodeEnum::ColumnRef(c)) = column.node.as_ref() else {
                    return Err(shape());
                };
                let column = self.column_name(c)?;
                let text = string_constant(text).ok_or_else(shape)?;
                Ok(Cond::StrMatch { column, text })
            }
            "match_all" => {
                plain_call(f)?;
                let shape = || form("match_all('<text>')");
                let [text] = f.args.as_slice() else {
                    return Err(shape());
                };
                let text = string_constant(text).ok_or_else(shape)?;
                Ok(Cond::MatchAll { text })
            }
            other if AggFunc::from_name(other).is_some() || other == "histogram" => Err(
                QueryError::unsupported(format!("{other}() may not be used in WHERE")),
            ),
            other => Err(QueryError::unsupported(format!(
                "function `{other}` is not supported"
            ))),
        }
    }

    /// `LIMIT <n>`：`<n>` 为不带正负号、不超过 i64 最大值的十进制整数常量，写在 `LIMIT` 关键字之后。
    fn limit(&self, count: Option<&pg_query::Node>) -> Result<u64, QueryError> {
        let bad = || {
            QueryError::unsupported(
                "LIMIT only accepts a non-negative decimal integer up to 9223372036854775807",
            )
        };
        let Some(NodeEnum::AConst(c)) = count.and_then(|n| n.node.as_ref()) else {
            return Err(bad());
        };
        if c.isnull || !matches!(c.val, Some(a_const::Val::Ival(_) | a_const::Val::Fval(_))) {
            return Err(bad());
        }
        let i = self.tokens.at(c.location).ok_or_else(bad)?;
        let text = self.tokens.text(&self.tokens.tokens[i]);
        let after_limit = self
            .tokens
            .prev_significant(i)
            .is_some_and(|p| self.tokens.tokens[p].token() == Token::Limit);
        if !after_limit || !self.tokens.is_numeric(i) || !text.bytes().all(|b| b.is_ascii_digit()) {
            return Err(bad());
        }
        text.parse::<i64>().map(|n| n as u64).map_err(|_| bad())
    }

    fn cond(&self, n: &pg_query::Node) -> Result<Cond, QueryError> {
        match n.node.as_ref() {
            Some(NodeEnum::BoolExpr(b)) => {
                let args = b
                    .args
                    .iter()
                    .map(|a| self.cond(a))
                    .collect::<Result<Vec<_>, _>>()?;
                match b.boolop() {
                    BoolExprType::AndExpr => Ok(Cond::And(args)),
                    BoolExprType::OrExpr => Ok(Cond::Or(args)),
                    BoolExprType::NotExpr => match <[Cond; 1]>::try_from(args) {
                        Ok([a]) => Ok(Cond::Not(Box::new(a))),
                        Err(_) => Err(unsupported("this NOT expression")),
                    },
                    BoolExprType::Undefined => Err(unsupported("this boolean expression")),
                }
            }
            Some(NodeEnum::AExpr(e)) => self.a_expr(e),
            Some(NodeEnum::FuncCall(f)) => self.text_match(f),
            Some(NodeEnum::NullTest(t)) => {
                if t.argisrow {
                    return Err(unsupported("row-valued IS NULL"));
                }
                let negated = match t.nulltesttype() {
                    NullTestType::IsNull => false,
                    NullTestType::IsNotNull => true,
                    NullTestType::Undefined => return Err(unsupported("this null test")),
                };
                Ok(Cond::IsNull {
                    negated,
                    operand: self.operand(t.arg.as_deref())?,
                })
            }
            _ => Err(QueryError::unsupported(
                "WHERE only supports comparisons, AND/OR/NOT, IS [NOT] NULL, [NOT] IN, [NOT] LIKE, str_match() and match_all()",
            )),
        }
    }

    fn a_expr(&self, e: &AExpr) -> Result<Cond, QueryError> {
        let op = match e.name.as_slice() {
            [n] => match n.node.as_ref() {
                Some(NodeEnum::String(s)) => s.sval.as_str(),
                _ => return Err(unsupported("this operator")),
            },
            _ => return Err(unsupported("a qualified operator")),
        };
        match e.kind() {
            AExprKind::AexprOp => {
                let op = match op {
                    "=" => CmpOp::Eq,
                    "<>" => CmpOp::NotEq,
                    "<" => CmpOp::Lt,
                    "<=" => CmpOp::LtEq,
                    ">" => CmpOp::Gt,
                    ">=" => CmpOp::GtEq,
                    other => {
                        return Err(QueryError::unsupported(format!(
                            "operator `{other}` is not supported"
                        )));
                    }
                };
                let Some(left) = e.lexpr.as_deref() else {
                    return Err(unsupported("a prefix operator"));
                };
                Ok(Cond::Cmp {
                    op,
                    left: self.operand(Some(left))?,
                    right: self.operand(e.rexpr.as_deref())?,
                })
            }
            AExprKind::AexprIn => {
                let negated = match op {
                    "=" => false,
                    "<>" => true,
                    _ => return Err(unsupported("this IN form")),
                };
                let Some(NodeEnum::List(list)) = e.rexpr.as_deref().and_then(|n| n.node.as_ref())
                else {
                    return Err(unsupported("IN with something other than a list of values"));
                };
                Ok(Cond::In {
                    negated,
                    operand: self.operand(e.lexpr.as_deref())?,
                    list: list
                        .items
                        .iter()
                        .map(|i| self.operand(Some(i)))
                        .collect::<Result<_, _>>()?,
                })
            }
            AExprKind::AexprLike => {
                let negated = match op {
                    "~~" => false,
                    "!~~" => true,
                    _ => return Err(unsupported("this LIKE form")),
                };
                let pattern = match self.operand(e.rexpr.as_deref()) {
                    Ok(Operand::Literal(Literal::String(p))) => p,
                    _ => {
                        return Err(QueryError::unsupported(
                            "the LIKE pattern must be a string literal (ESCAPE is not supported)",
                        ));
                    }
                };
                Ok(Cond::Like {
                    negated,
                    operand: self.operand(e.lexpr.as_deref())?,
                    pattern,
                })
            }
            AExprKind::AexprIlike => Err(unsupported("ILIKE")),
            AExprKind::AexprSimilar => Err(unsupported("SIMILAR TO")),
            _ => Err(QueryError::unsupported(
                "WHERE only supports comparisons, AND/OR/NOT, IS [NOT] NULL, [NOT] IN and [NOT] LIKE",
            )),
        }
    }

    /// 操作数：列名或字面量（数值字面量可带 `+` 或 `-` 号）。
    fn operand(&self, n: Option<&pg_query::Node>) -> Result<Operand, QueryError> {
        let operand_only = || {
            QueryError::unsupported(
                "operands must be column names or literals (no expressions, casts or function calls)",
            )
        };
        match n.and_then(|n| n.node.as_ref()) {
            Some(NodeEnum::ColumnRef(c)) => Ok(Operand::Column(self.column_name(c)?)),
            Some(NodeEnum::AConst(c)) => Ok(Operand::Literal(self.literal(c)?)),
            // 一元 `+`：只接受紧跟一个无符号数值常量的写法。
            Some(NodeEnum::AExpr(e))
                if e.kind() == AExprKind::AexprOp
                    && e.lexpr.is_none()
                    && matches!(e.name.as_slice(), [n] if matches!(n.node.as_ref(), Some(NodeEnum::String(s)) if s.sval == "+")) =>
            {
                let Some(NodeEnum::AConst(c)) = e.rexpr.as_deref().and_then(|n| n.node.as_ref())
                else {
                    return Err(operand_only());
                };
                let plus = self.tokens.at(e.location).ok_or_else(operand_only)?;
                let number = self.tokens.at(c.location).ok_or_else(operand_only)?;
                if self.tokens.next_significant(plus) != Some(number)
                    || !self.tokens.is_numeric(number)
                {
                    return Err(operand_only());
                }
                let text = self.tokens.text(&self.tokens.tokens[number]);
                decimal_value(false, text)
                    .map(|v| Operand::Literal(Literal::Number(v, Numeral::of(text))))
            }
            _ => Err(operand_only()),
        }
    }

    fn column_name(&self, c: &pg_query::protobuf::ColumnRef) -> Result<String, QueryError> {
        match c.fields.as_slice() {
            [f] => match f.node.as_ref() {
                Some(NodeEnum::String(s)) => Ok(self.identifier(c.location, &s.sval)),
                _ => Err(unsupported("`*` in this position")),
            },
            _ => Err(unsupported("a qualified column name")),
        }
    }

    /// 起始字节为 `location` 的标识符的完整名称。PostgreSQL 的词法分析把超过 63 字节（NAMEDATALEN − 1）的标识符截断，
    /// 语法树里的名称 `parsed` 因此可能不全；这里按原文还原：带双引号的去掉引号、`""` 还原为 `"`，不带引号的按 ASCII
    /// 转小写（与 PostgreSQL 对 UTF-8 标识符的处理相同）。原文是 `U&"…"` 等其他写法时沿用 `parsed`。
    fn identifier(&self, location: i32, parsed: &str) -> String {
        let Some(i) = self.tokens.at(location) else {
            return parsed.to_owned();
        };
        let text = self.tokens.text(&self.tokens.tokens[i]);
        let full = if let Some(inner) = text.strip_prefix('"').and_then(|t| t.strip_suffix('"')) {
            inner.replace("\"\"", "\"")
        } else if text.starts_with(|c: char| c.is_alphabetic() || c == '_')
            && !text.contains(['&', '"', '\''])
        {
            text.to_ascii_lowercase()
        } else {
            return parsed.to_owned();
        };
        // 截断只会去掉尾部：还原所得不以语法树里的名称开头，说明记号对错了位置，沿用语法树
        if full.starts_with(parsed) {
            full
        } else {
            parsed.to_owned()
        }
    }

    fn literal(&self, c: &AConst) -> Result<Literal, QueryError> {
        if c.isnull {
            return Ok(Literal::Null);
        }
        match &c.val {
            Some(a_const::Val::Boolval(b)) => Ok(Literal::Bool(b.boolval)),
            Some(a_const::Val::Sval(s)) => {
                // 字符串常量的原文写法已在词法核对中限定为普通单引号字符串。
                Ok(Literal::String(s.sval.clone()))
            }
            Some(a_const::Val::Ival(_) | a_const::Val::Fval(_)) => {
                // 语法分析把 `-1`、`-(1)`、`- -1` 都折成负常量，位置记在第一个 `-` 上；
                // 只有 `-` 之后紧跟无符号数值常量的才是带符号的数值字面量。
                let bad = || {
                    QueryError::unsupported(
                        "operands must be column names or literals (arithmetic is not supported)",
                    )
                };
                let i = self.tokens.at(c.location).ok_or_else(bad)?;
                let (negative, number) = if self.tokens.tokens[i].token() == Token::Ascii45 {
                    let n = self.tokens.next_significant(i).ok_or_else(bad)?;
                    (true, n)
                } else {
                    (false, i)
                };
                if !self.tokens.is_numeric(number) {
                    return Err(bad());
                }
                let text = self.tokens.text(&self.tokens.tokens[number]);
                decimal_value(negative, text).map(|v| Literal::Number(v, Numeral::of(text)))
            }
            Some(a_const::Val::Bsval(_)) => Err(unsupported("a bit-string literal")),
            None => Err(unsupported("this literal")),
        }
    }
}

/// 函数调用的分类结果。
enum Call {
    Agg(AggCall),
    Histogram(String),
}

fn form(shape: &str) -> QueryError {
    QueryError::unsupported(format!(
        "this call is not supported; the accepted form is {shape}"
    ))
}

/// 单段函数名（未加引号的已按小写解析）。
fn function_name(f: &FuncCall) -> Result<&str, QueryError> {
    match f.funcname.as_slice() {
        [n] => match n.node.as_ref() {
            Some(NodeEnum::String(s)) => Ok(s.sval.as_str()),
            _ => Err(unsupported("this function name")),
        },
        _ => Err(unsupported("a qualified function name")),
    }
}

/// 不带 `*`、`DISTINCT`、`FILTER`、`OVER`、`WITHIN GROUP`、参数内 `ORDER BY` 与 `VARIADIC` 的普通调用。
fn plain_call(f: &FuncCall) -> Result<(), QueryError> {
    if f.agg_star
        || f.agg_distinct
        || f.agg_filter.is_some()
        || f.over.is_some()
        || f.agg_within_group
        || !f.agg_order.is_empty()
        || f.func_variadic
    {
        return Err(unsupported("this form of function call"));
    }
    Ok(())
}

/// 字符串字面量参数的取值（写法已在词法核对中限定为普通单引号字符串）；不是字符串字面量为 `None`。
fn string_constant(n: &pg_query::Node) -> Option<String> {
    match n.node.as_ref() {
        Some(NodeEnum::AConst(c)) if !c.isnull => match &c.val {
            Some(a_const::Val::Sval(s)) => Some(s.sval.clone()),
            _ => None,
        },
        _ => None,
    }
}

/// `GROUP BY` 中的名字先按输出列名解析：恰指向聚合调用的输出项（同名各项都是同一个聚合调用）时属聚合调用写错位置。
fn group_by_names_not_aggregates(
    projection: &Projection,
    group_by: &[Item],
) -> Result<(), QueryError> {
    let Projection::Items(items) = projection else {
        return Ok(());
    };
    for g in group_by {
        let Item::Column(name) = g else { continue };
        let mut matched = items
            .iter()
            .filter(|i| i.output_name() == name)
            .map(|i| &i.item);
        let Some(Item::Agg(first)) = matched.next() else {
            continue;
        };
        if matched.all(|i| matches!(i, Item::Agg(a) if a == first)) {
            return Err(QueryError::unsupported(format!(
                "GROUP BY `{name}` refers to an aggregate call"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use amberlog_common::error::Reason;

    fn reason(sql: &str) -> Result<Query, Reason> {
        parse(sql).map_err(|e| e.reason)
    }

    /// 输出项都是列名时的列名列表。
    fn columns(p: &Projection) -> Vec<String> {
        let Projection::Items(items) = p else {
            panic!("{p:?}")
        };
        items
            .iter()
            .map(|i| match &i.item {
                Item::Column(c) => c.clone(),
                other => panic!("{other:?}"),
            })
            .collect()
    }

    fn num(s: &str) -> Operand {
        Operand::Literal(Literal::Number(
            BigDecimal::from_str(s).unwrap(),
            Numeral::of(s.trim_start_matches('-')),
        ))
    }

    /// 场景「子集之外的结构一律 sql_unsupported」（单元层，分类函数直接返回 `reason`）。
    /// 表中各行都在子集之外（`GROUP BY a`、`max(a)` 这样的写法在子集内，不在表中）。
    #[test]
    fn search_sql_unsupported_table() {
        let table = [
            "SELECT a FROM app GROUP BY a HAVING count(*) > 1",
            "SELECT DISTINCT a FROM app",
            "SELECT count(*) OVER () FROM app",
            "SELECT a b FROM app",
            "SELECT a AS _b FROM app",
            "SELECT a FROM app WHERE str_match(m, E'x')",
            "SELECT lower(m) FROM app",
            "SELECT 1 FROM app",
            "SELECT a FROM app JOIN app ON TRUE",
            "SELECT a FROM (SELECT a FROM app) t",
            "SELECT a FROM app UNION SELECT a FROM app",
            "SELECT a FROM app AS t",
            "SELECT a FROM public.app",
            "SELECT app.a FROM app",
            "SELECT a FROM app WHERE m = E'x'",
            "SELECT a FROM app WHERE m = $$x$$",
            "SELECT a FROM app WHERE m = DATE '2026-01-01'",
            "SELECT CAST(a AS text) FROM app",
            "SELECT a FROM app WHERE a = 1_000",
            "SELECT a FROM app LIMIT NULL",
            "SELECT a FROM app LIMIT 9223372036854775808",
            "SELECT a FROM app WHERE m LIKE m",
            "SELECT a FROM app; SELECT a FROM app",
        ];
        for sql in table {
            assert_eq!(reason(sql), Err(Reason::SqlUnsupported), "{sql}");
        }
    }

    fn accepted(table: &[(&str, Result<(), Reason>)]) {
        for (sql, want) in table {
            assert_eq!(reason(sql).map(|_| ()), *want, "{sql}");
        }
    }

    const OK: Result<(), Reason> = Ok(());
    const UNSUPPORTED: Result<(), Reason> = Err(Reason::SqlUnsupported);

    /// REQ-AMB-QRY-AGGREGATION 的场景「聚合调用的形态」。
    #[test]
    fn agg_call_form_table() {
        accepted(&[
            ("SELECT count(*) FROM app", OK),
            (
                "SELECT count(a), sum(a), avg(a), min(a), max(a) FROM app",
                OK,
            ),
            ("SELECT COUNT(\"a\") FROM app", OK),
            ("SELECT count(ALL a) FROM app", UNSUPPORTED),
            ("SELECT count(DISTINCT a) FROM app", UNSUPPORTED),
            ("SELECT count(a) FILTER (WHERE a > 1) FROM app", UNSUPPORTED),
            ("SELECT count(*) OVER () FROM app", UNSUPPORTED),
            (
                "SELECT max(a) WITHIN GROUP (ORDER BY a) FROM app",
                UNSUPPORTED,
            ),
            ("SELECT sum(a ORDER BY a) FROM app", UNSUPPORTED),
            ("SELECT sum(*) FROM app", UNSUPPORTED),
            ("SELECT count() FROM app", UNSUPPORTED),
            ("SELECT sum(a, a) FROM app", UNSUPPORTED),
            ("SELECT sum(1) FROM app", UNSUPPORTED),
            ("SELECT sum(-a) FROM app", UNSUPPORTED),
            ("SELECT pg_catalog.count(*) FROM app", UNSUPPORTED),
            ("SELECT count(*) + 1 FROM app", UNSUPPORTED),
            ("SELECT stddev(a) FROM app", UNSUPPORTED),
            (
                "SELECT a, count(*) FROM app GROUP BY a HAVING count(*) > 1",
                UNSUPPORTED,
            ),
            ("SELECT DISTINCT a FROM app", UNSUPPORTED),
        ]);
    }

    /// REQ-AMB-QRY-HISTOGRAM 的场景「histogram 调用的形态与位置」。
    #[test]
    fn histogram_call_form_table() {
        accepted(&[
            ("SELECT histogram(_timestamp, '5m') FROM app", OK),
            (
                "SELECT histogram(\"_timestamp\", '5m') AS t FROM app ORDER BY histogram(_timestamp, '5m')",
                OK,
            ),
            (
                "SELECT count(*) FROM app GROUP BY histogram(_timestamp, '5m')",
                OK,
            ),
            (
                "SELECT histogram(_timestamp, '5m', 1) FROM app",
                UNSUPPORTED,
            ),
            ("SELECT histogram('5m', _timestamp) FROM app", UNSUPPORTED),
            ("SELECT histogram(_timestamp, 5) FROM app", UNSUPPORTED),
            ("SELECT histogram(_timestamp, E'5m') FROM app", UNSUPPORTED),
            (
                "SELECT histogram(app._timestamp, '5m') FROM app",
                UNSUPPORTED,
            ),
            (
                "SELECT histogram(_timestamp, '5m') + 1 FROM app",
                UNSUPPORTED,
            ),
            (
                "SELECT max(histogram(_timestamp, '5m')) FROM app",
                UNSUPPORTED,
            ),
            (
                "SELECT histogram(_timestamp, '5m') OVER () FROM app",
                UNSUPPORTED,
            ),
        ]);
    }

    /// REQ-AMB-QRY-TEXT-MATCH 的场景「文本匹配调用的形态与位置」。
    #[test]
    fn text_call_form_table() {
        accepted(&[
            ("SELECT * FROM app WHERE str_match(m, 'a')", OK),
            (
                "SELECT * FROM app WHERE STR_MATCH(\"m\", 'a') AND NOT (match_all('b') OR a = 1)",
                OK,
            ),
            ("SELECT * FROM app WHERE match_all('')", OK),
            ("SELECT * FROM app WHERE str_match('m', 'a')", UNSUPPORTED),
            ("SELECT * FROM app WHERE str_match(m, NULL)", UNSUPPORTED),
            ("SELECT * FROM app WHERE str_match(m, E'a')", UNSUPPORTED),
            ("SELECT * FROM app WHERE str_match(app.m, 'a')", UNSUPPORTED),
            ("SELECT * FROM app WHERE match_all()", UNSUPPORTED),
            ("SELECT * FROM app WHERE match_all(NULL)", UNSUPPORTED),
            (
                "SELECT * FROM app WHERE match_all('a') IN (TRUE)",
                UNSUPPORTED,
            ),
            (
                "SELECT * FROM app WHERE match_all('a') IS NULL",
                UNSUPPORTED,
            ),
            (
                "SELECT * FROM app WHERE str_match(m, 'a') LIKE 'x'",
                UNSUPPORTED,
            ),
            ("SELECT * FROM app ORDER BY match_all('a')", UNSUPPORTED),
            (
                "SELECT count(*) FROM app GROUP BY str_match(m, 'a')",
                UNSUPPORTED,
            ),
            (
                "SELECT * FROM app WHERE match_all('a') OVER ()",
                UNSUPPORTED,
            ),
        ]);
    }

    /// 别名只认 `AS` 写法、不以 `_` 开头；聚合调用写错位置（`WHERE`、`GROUP BY`、另一调用的参数）回 `sql_unsupported`；
    /// `GROUP BY` 中的名字恰指向聚合调用的输出项时同样回 `sql_unsupported`，同名的不同表达式留给绑定器。
    #[test]
    fn aliases_and_misplaced_calls() {
        accepted(&[
            ("SELECT a AS b FROM app", OK),
            ("SELECT a AS \"B c\" FROM app", OK),
            (
                "SELECT count(*) AS n, a /* x */ AS \"x\" FROM app GROUP BY a",
                OK,
            ),
            // `AS` 后面那个记号总是别名，关键字也一样（PostgreSQL 17 收 `AS <列标签>`，含保留关键字）
            ("SELECT a AS from FROM app", OK),
            ("SELECT a AS where FROM app", OK),
            ("SELECT a AS select FROM app", OK),
            ("SELECT a AS from, b AS where FROM app", OK),
            ("SELECT count(*) AS from FROM app;", OK),
            ("SELECT a AS _b FROM app", UNSUPPORTED),
            ("SELECT a AS \"_b\" FROM app", UNSUPPORTED),
            ("SELECT a b FROM app", UNSUPPORTED),
            ("SELECT count(*) n FROM app", UNSUPPORTED),
            (
                "SELECT level FROM app WHERE count(*) > 1 GROUP BY level",
                UNSUPPORTED,
            ),
            ("SELECT count(*) AS n FROM app GROUP BY n", UNSUPPORTED),
            (
                "SELECT count(*) AS n, count(*) AS n FROM app GROUP BY n",
                UNSUPPORTED,
            ),
            ("SELECT count(*) FROM app GROUP BY count", UNSUPPORTED),
            ("SELECT count(*) AS x, level AS x FROM app GROUP BY x", OK),
            ("SELECT count(*), count(a) FROM app GROUP BY count", OK),
            ("SELECT max(count(*)) FROM app", UNSUPPORTED),
            ("SELECT count(DISTINCT host) FROM app", UNSUPPORTED),
            ("SELECT count(*) FROM app GROUP BY count(*)", UNSUPPORTED),
            ("SELECT a FROM app GROUP BY 1", UNSUPPORTED),
            ("SELECT a FROM app GROUP BY ROLLUP (a)", UNSUPPORTED),
            (
                "SELECT a FROM app WHERE histogram(_timestamp, '5m') = 0",
                UNSUPPORTED,
            ),
            ("SELECT match_all('a') FROM app", UNSUPPORTED),
            ("SELECT a FROM app WHERE lower(m) = 'x'", UNSUPPORTED),
        ]);
        for sql in [
            "SELECT a AS from FROM app",
            "SELECT a AS where FROM app",
            "SELECT a AS select FROM app",
        ] {
            assert!(
                pg_query::parse(sql).is_ok(),
                "PostgreSQL 17 应能解析：{sql}"
            );
        }
        let q = parse("SELECT a AS from, b AS where, c AS select FROM app").unwrap();
        let Projection::Items(items) = &q.projection else {
            panic!()
        };
        assert_eq!(
            items
                .iter()
                .map(SelectItem::output_name)
                .collect::<Vec<_>>(),
            ["from", "where", "select"]
        );
        let q =
            parse("SELECT a AS \"B c\", count(*), histogram(_timestamp, '5m') FROM app").unwrap();
        let Projection::Items(items) = &q.projection else {
            panic!()
        };
        assert_eq!(
            items
                .iter()
                .map(SelectItem::output_name)
                .collect::<Vec<_>>(),
            ["B c", "count", "histogram"]
        );
    }

    /// 以 PostgreSQL 17 的文法判定能否解析：17 能解析的子集外写法回 `sql_unsupported`，17 解析不了的回 `invalid_sql`
    /// （场景「SQL 能否解析按 PostgreSQL 17 的文法判定」「括号分组被接受，文法以 PostgreSQL 为准」的分类部分）。
    #[test]
    fn reference_grammar_is_postgresql_17() {
        for sql in [
            "SELECT a FROM (SELECT a FROM app)",
            "SELECT a FROM app WHERE a IS JSON",
            "SELECT JSON_VALUE(a, '$.x' RETURNING int) FROM app",
            "SELECT host::text FROM app",
        ] {
            assert_eq!(reason(sql), Err(Reason::SqlUnsupported), "{sql}");
        }
        for sql in [
            "SELECT 1abc FROM app",
            "INSERT INTO app VALUES (1) RETURNING WITH (OLD AS o) o.a",
            "SELECT `host` FROM app",
            "SELEC * FROM app",
        ] {
            assert_eq!(reason(sql), Err(Reason::InvalidSql), "{sql}");
        }
        let q = parse("SELECT host FROM app WHERE level = 'error' AND (host = 'a' OR host = 'b')")
            .unwrap();
        assert!(matches!(q.filter, Some(Cond::And(ref v)) if matches!(v[1], Cond::Or(_))));
    }

    /// 场景「LIMIT 只接受非负十进制整数，空 SQL 属写错」的分类部分，另加数值与字符串写法的边角。
    #[test]
    fn limit_numeric_and_empty_sql() {
        for sql in [
            "SELECT a FROM app LIMIT -1",
            "SELECT a FROM app LIMIT 1.5",
            "SELECT a FROM app LIMIT ALL",
            "SELECT a FROM app WHERE a = 0x1",
            "SELECT a FROM app;;",
            ";SELECT a FROM app",
            "SELECT a FROM app LIMIT '3'",
            "SELECT a FROM app LIMIT (1+2)",
            "SELECT a FROM app LIMIT 1e2",
            "SELECT a FROM app FETCH FIRST 3 ROWS ONLY",
            "SELECT a FROM app OFFSET 1",
            "SELECT a FROM app WHERE a = 0o7",
            "SELECT a FROM app WHERE a = 0b1",
            "SELECT a FROM app WHERE m = U&'x'",
            "SELECT a FROM app WHERE m = B'1'",
            "SELECT a FROM app WHERE m = X'1F'",
            "SELECT a FROM app WHERE a = -(1)",
            "SELECT a FROM app WHERE a = - -1",
            "SELECT a FROM app WHERE a = +-1",
            "SELECT a FROM app WHERE a = 1 + 1",
            "SELECT a FROM app WHERE -a = 1",
            "SELECT a FROM app WHERE m LIKE 'x' ESCAPE '!'",
            "SELECT a FROM app WHERE m ILIKE 'x'",
            "SELECT a FROM app WHERE a BETWEEN 1 AND 2",
            "SELECT a FROM app WHERE b",
            "SELECT a FROM app WHERE b IS TRUE",
            "SELECT a FROM app WHERE a IN (SELECT a FROM app)",
            "SELECT a FROM app ORDER BY a NULLS FIRST",
            "SELECT a FROM app ORDER BY 1",
            "SELECT DISTINCT a FROM app",
            "SELECT *, a FROM app",
            "SELECT a FROM ONLY app",
            "SELECT FROM app",
            "SELECT a FROM app WHERE a = $1",
        ] {
            assert_eq!(reason(sql), Err(Reason::SqlUnsupported), "{sql}");
        }
        for sql in [
            "",
            "   ",
            "-- only a comment",
            "/* c */ ;",
            ";",
            "SELECT a FROM app WHERE a = 1__0",
        ] {
            assert_eq!(reason(sql), Err(Reason::InvalidSql), "{sql:?}");
        }
        assert_eq!(
            parse("SELECT a FROM app LIMIT 9223372036854775807")
                .unwrap()
                .limit,
            Some(9_223_372_036_854_775_807)
        );
        assert_eq!(
            parse("SELECT a FROM app LIMIT 3000000000").unwrap().limit,
            Some(3_000_000_000)
        );
        assert_eq!(parse("SELECT a FROM app LIMIT 0").unwrap().limit, Some(0));
    }

    #[test]
    fn accepted_forms_and_values() {
        let q = parse("SELECT a /* c */ FROM app -- tail").unwrap();
        assert_eq!(columns(&q.projection), ["a"]);
        assert_eq!(parse("SELECT a FROM app;").unwrap().stream, "app");
        let q = parse("SELECT A, \"A\", \"host.name\" FROM APP").unwrap();
        assert_eq!(q.stream, "app");
        assert_eq!(columns(&q.projection), ["a", "A", "host.name"]);
        assert_eq!(parse("SELECT * FROM \"App\"").unwrap().stream, "App");
        assert_eq!(
            parse("SELECT * FROM app").unwrap().projection,
            Projection::All
        );

        let q = parse("SELECT a FROM app WHERE a IN (+1, -1, - 2.5e1, .5, 1., 1.e1)").unwrap();
        let Some(Cond::In { list, negated, .. }) = q.filter else {
            panic!()
        };
        assert!(!negated);
        // 数值相同，原文写法不同：带小数点或指数部分的记为 `Numeral::Decimal`
        let dec = |s: &str| {
            Operand::Literal(Literal::Number(
                BigDecimal::from_str(s).unwrap(),
                Numeral::Decimal,
            ))
        };
        assert_eq!(
            list,
            [
                num("1"),
                num("-1"),
                dec("-25"),
                dec("0.5"),
                dec("1"),
                dec("10")
            ]
        );

        let q = parse("SELECT a FROM app WHERE a != 1 AND m NOT LIKE 'y%' AND b = TRUE AND m IS NOT NULL AND n = NULL AND m = 'it''s' AND m = 'a\\b'").unwrap();
        let Some(Cond::And(conds)) = q.filter else {
            panic!()
        };
        assert!(matches!(
            conds[0],
            Cond::Cmp {
                op: CmpOp::NotEq,
                ..
            }
        ));
        assert!(
            matches!(conds[1], Cond::Like { negated: true, ref pattern, .. } if pattern == "y%")
        );
        assert!(matches!(
            conds[2],
            Cond::Cmp {
                right: Operand::Literal(Literal::Bool(true)),
                ..
            }
        ));
        assert!(matches!(conds[3], Cond::IsNull { negated: true, .. }));
        assert!(matches!(
            conds[4],
            Cond::Cmp {
                right: Operand::Literal(Literal::Null),
                ..
            }
        ));
        assert!(
            matches!(conds[5], Cond::Cmp { right: Operand::Literal(Literal::String(ref s)), .. } if s == "it's")
        );
        assert!(
            matches!(conds[6], Cond::Cmp { right: Operand::Literal(Literal::String(ref s)), .. } if s == "a\\b")
        );

        let q = parse("SELECT g, v FROM ob ORDER BY g, v DESC, w ASC").unwrap();
        assert_eq!(
            q.order_by
                .iter()
                .map(|k| match &k.item {
                    Item::Column(c) => (c.as_str(), k.descending),
                    other => panic!("{other:?}"),
                })
                .collect::<Vec<_>>(),
            [("g", false), ("v", true), ("w", false)]
        );
        // 9007199254740992.0 精确保存，不经 f64
        let q = parse("SELECT id FROM app WHERE id = 9007199254740993.0").unwrap();
        assert!(
            matches!(q.filter, Some(Cond::Cmp { ref right, .. }) if *right == num("9007199254740993.0"))
        );
    }

    /// 超过 63 字节的标识符不被截断（PostgreSQL 的词法分析按 NAMEDATALEN 截断，语法树里的名称不全）：
    /// SELECT、WHERE、ORDER BY 中的列名与 FROM 的名称都取原文。
    #[test]
    fn long_identifiers_not_truncated() {
        let long = format!("{}_x", "a".repeat(100));
        let quoted = format!("{}.Y\"q", "b".repeat(70));
        let quoted_sql = quoted.replace('"', "\"\"");
        let stream = "s".repeat(64);
        let sql = format!(
            "SELECT {long}, \"{quoted_sql}\" FROM {s_upper} WHERE \"{quoted_sql}\" = 1 AND {LONG} IS NULL ORDER BY {long}",
            s_upper = stream.to_uppercase(),
            LONG = long.to_uppercase(),
        );
        let q = parse(&sql).unwrap();
        assert_eq!(q.stream, stream);
        assert_eq!(columns(&q.projection), [long.clone(), quoted.clone()]);
        let Some(Cond::And(conds)) = q.filter else {
            panic!()
        };
        assert!(matches!(&conds[0], Cond::Cmp { left: Operand::Column(c), .. } if *c == quoted));
        assert!(
            matches!(&conds[1], Cond::IsNull { operand: Operand::Column(c), .. } if *c == long)
        );
        assert_eq!(q.order_by[0].item, Item::Column(long));
        // 多字节字符跨过截断点
        let cjk = "字".repeat(30);
        let q = parse(&format!("SELECT \"{cjk}\" FROM app")).unwrap();
        assert_eq!(columns(&q.projection), [cjk]);
    }

    #[test]
    fn decimal_numeral_forms() {
        for ok in [
            "0", "42", "3.14", ".5", "1.", "1e3", "1E+3", "1.5e-3", "1.e2",
        ] {
            assert!(is_decimal_numeral(ok), "{ok}");
        }
        for bad in ["0x1", "1_000", "0o7", "0b1", ".", "1e", "e3", "1.2.3"] {
            assert!(!is_decimal_numeral(bad), "{bad}");
        }
    }
}

//! 列类型、「stream 当前 schema」与落列规则（REQ-AMB-DM-RECORD-MODEL、REQ-AMB-DM-SCHEMA-DRIFT、REQ-AMB-DM-FIELD-LIMITS、
//! REQ-AMB-STO-HOT-LAYOUT 快照 `schema`）。
//!
//! 摄取（确定目标列、判列数上限）、存储（快照 `schema`、Parquet 列、过时 `wal.dir` 的重判）与查询（列是否存在）
//! 共用同一份列类型集合与落列规则，故放在 common。字段 id 的分配属存储（REQ-AMB-STO-HOT-LAYOUT），不在这里。

use std::collections::HashMap;

/// 时间列名（REQ-AMB-DM-RECORD-MODEL、REQ-AMB-TIME-TIMESTAMP）。
pub const TIMESTAMP_COLUMN: &str = "_timestamp";

/// 列类型：快照 `schema` 中 `type` 的四个取值（REQ-AMB-STO-HOT-LAYOUT）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ColumnType {
    Int64,
    Float64,
    Boolean,
    Utf8,
}

impl ColumnType {
    /// 快照 `schema` 中 `type` 字段的写法。
    pub fn as_str(self) -> &'static str {
        match self {
            ColumnType::Int64 => "Int64",
            ColumnType::Float64 => "Float64",
            ColumnType::Boolean => "Boolean",
            ColumnType::Utf8 => "Utf8",
        }
    }

    /// 影子列名的类型后缀（REQ-AMB-DM-SCHEMA-DRIFT）。
    pub fn shadow_suffix(self) -> &'static str {
        match self {
            ColumnType::Int64 => "int64",
            ColumnType::Float64 => "float64",
            ColumnType::Boolean => "boolean",
            ColumnType::Utf8 => "utf8",
        }
    }

    const ALL: [ColumnType; 4] = [
        ColumnType::Int64,
        ColumnType::Float64,
        ColumnType::Boolean,
        ColumnType::Utf8,
    ];
}

/// 「stream 当前 schema」的只读视图：确定目标列与判列数上限所依据的列集合。
pub trait SchemaLookup {
    fn column_type(&self, name: &str) -> Option<ColumnType>;

    /// 列数（含 `_timestamp` 与影子列）。
    fn column_count(&self) -> usize;
}

/// 影子列名 `<基名>__<类型后缀>`（REQ-AMB-DM-SCHEMA-DRIFT）。
pub fn shadow_column(base: &str, column_type: ColumnType) -> String {
    format!("{base}__{}", column_type.shadow_suffix())
}

/// 列名所属的基名：影子列名去掉 `__<类型后缀>`，其余列名即基名。
/// 客户端给出的字段名不含 `__`（REQ-AMB-DM-RECORD-MODEL 的命名约束），所以带 `__` 的列名只能是影子列名。
pub fn base_name(column: &str) -> &str {
    ColumnType::ALL
        .iter()
        .find_map(|t| {
            column
                .strip_suffix(t.shadow_suffix())
                .and_then(|rest| rest.strip_suffix("__"))
        })
        .unwrap_or(column)
}

/// 落列规则①～④（REQ-AMB-DM-SCHEMA-DRIFT）：基名为 `base`、推断类型为 `column_type` 的取值落入的目标列名，
/// 以及该列是否须登记为新列。
pub fn target_column<S: SchemaLookup + ?Sized>(
    base: &str,
    column_type: ColumnType,
    schema: &S,
) -> (String, bool) {
    // ① 同名同类型的列
    if schema.column_type(base) == Some(column_type) {
        return (base.to_owned(), false);
    }
    // ② 已有的该类型影子列
    let shadow = shadow_column(base, column_type);
    if schema.column_type(&shadow).is_some() {
        return (shadow, false);
    }
    // ③ 基名尚无列：以基名登记；④ 否则登记影子列
    if schema.column_type(base).is_none() {
        (base.to_owned(), true)
    } else {
        (shadow, true)
    }
}

/// 一条记录的落列：各字段（基名与推断类型，按展平后的出现顺序）的目标列名，与须登记的新列（按同一顺序）。
/// 须登记的新列会使列数超过 `max_columns` 时为 `None`（REQ-AMB-DM-FIELD-LIMITS：整条记录不登记任何列）。
pub fn place_record<'a, S: SchemaLookup + ?Sized>(
    fields: impl IntoIterator<Item = (&'a str, ColumnType)>,
    schema: &S,
    max_columns: usize,
) -> Option<Placement> {
    let mut placement = Placement::default();
    for (base, column_type) in fields {
        let (target, new) = target_column(base, column_type, schema);
        if new {
            placement.new_columns.push((target.clone(), column_type));
        }
        placement.targets.push(target);
    }
    let fits = placement.new_columns.is_empty()
        || schema.column_count() + placement.new_columns.len() <= max_columns;
    fits.then_some(placement)
}

/// [`place_record`] 的结果。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Placement {
    /// 与输入字段一一对应的目标列名。
    pub targets: Vec<String>,
    /// 须登记的新列，按字段顺序（同一记录内各字段的基名互不相同，新列不会重复）。
    pub new_columns: Vec<(String, ColumnType)>,
}

/// 一个 stream 的列集合，按列首次进入 schema 的顺序保存。
///
/// 恒含 `_timestamp`（Int64）：任何已存在的 stream 其当前 schema 自存在起即含该列，
/// 不以创世快照是否已提交为条件（REQ-AMB-DM-RECORD-MODEL）。
/// 由调用方把已提交快照的列、内存表已确认记录引入的列并入（各自的来源在存储侧维护）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamSchema {
    columns: Vec<(String, ColumnType)>,
    index: HashMap<String, usize>,
}

impl Default for StreamSchema {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamSchema {
    /// 只含 `_timestamp`（Int64）的 schema。
    pub fn new() -> Self {
        let mut schema = StreamSchema {
            columns: Vec::new(),
            index: HashMap::new(),
        };
        schema.insert(TIMESTAMP_COLUMN.to_owned(), ColumnType::Int64);
        schema
    }

    /// 全部列，按首次进入 schema 的顺序。
    pub fn columns(&self) -> &[(String, ColumnType)] {
        &self.columns
    }

    /// 并入一列。列已存在时不改变其类型与位置，返回 `false`（类型是否一致由调用方在此之前判定）。
    pub fn insert(&mut self, name: String, column_type: ColumnType) -> bool {
        if self.index.contains_key(&name) {
            return false;
        }
        self.index.insert(name.clone(), self.columns.len());
        self.columns.push((name, column_type));
        true
    }
}

impl SchemaLookup for StreamSchema {
    fn column_type(&self, name: &str) -> Option<ColumnType> {
        self.index.get(name).map(|&i| self.columns[i].1)
    }

    fn column_count(&self) -> usize {
        self.columns.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_always_has_timestamp() {
        let s = StreamSchema::new();
        assert_eq!(s.column_type(TIMESTAMP_COLUMN), Some(ColumnType::Int64));
        assert_eq!(s.columns().len(), 1);
    }

    #[test]
    fn schema_insert_keeps_first() {
        let mut s = StreamSchema::new();
        assert!(s.insert("a".into(), ColumnType::Utf8));
        assert!(!s.insert("a".into(), ColumnType::Int64));
        assert!(!s.insert(TIMESTAMP_COLUMN.into(), ColumnType::Utf8));
        assert_eq!(s.column_type("a"), Some(ColumnType::Utf8));
        assert_eq!(s.column_type(TIMESTAMP_COLUMN), Some(ColumnType::Int64));
        let names: Vec<&str> = s.columns().iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["_timestamp", "a"]);
    }

    #[test]
    fn target_column_rules() {
        let mut s = StreamSchema::new();
        s.insert("code".into(), ColumnType::Int64);
        s.insert("v".into(), ColumnType::Int64);
        s.insert("v__utf8".into(), ColumnType::Utf8);
        let t = |base: &str, ty| target_column(base, ty, &s);
        assert_eq!(t("code", ColumnType::Int64), ("code".into(), false));
        assert_eq!(t("v", ColumnType::Utf8), ("v__utf8".into(), false));
        assert_eq!(t("new", ColumnType::Utf8), ("new".into(), true));
        assert_eq!(t("code", ColumnType::Utf8), ("code__utf8".into(), true));
        assert_eq!(
            t("code", ColumnType::Float64),
            ("code__float64".into(), true)
        );
    }

    #[test]
    fn base_name_strips_shadow_suffix() {
        assert_eq!(base_name("code__utf8"), "code");
        assert_eq!(base_name("a.b__boolean"), "a.b");
        assert_eq!(base_name("n__float64"), "n");
        assert_eq!(base_name("n__int64"), "n");
        assert_eq!(base_name("utf8"), "utf8");
        assert_eq!(base_name("x_utf8"), "x_utf8");
        assert_eq!(base_name("_timestamp"), "_timestamp");
    }

    #[test]
    fn place_record_column_cap() {
        let mut s = StreamSchema::new();
        s.insert("a".into(), ColumnType::Int64);
        // 列数 2，上限 3：再登记一列可以，两列不行
        let p = place_record([("a", ColumnType::Int64), ("b", ColumnType::Utf8)], &s, 3).unwrap();
        assert_eq!(p.targets, ["a", "b"]);
        assert_eq!(p.new_columns, [("b".to_owned(), ColumnType::Utf8)]);
        assert!(
            place_record([("b", ColumnType::Int64), ("c", ColumnType::Int64)], &s, 3).is_none()
        );
        assert!(place_record([("a", ColumnType::Utf8), ("c", ColumnType::Int64)], &s, 3).is_none());
        // 全部落入已有列：即便已超上限也不拒绝
        assert!(place_record([("a", ColumnType::Int64)], &s, 1).is_some());
    }
}

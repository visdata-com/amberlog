//! 文本匹配函数 `str_match()` 与 `match_all()`（REQ-AMB-QRY-TEXT-MATCH）。
//!
//! 按子串匹配、不分词；文本中的 `%`、`_`、`\` 按字面。`str_match` 按码点逐一相同（区分大小写）；`match_all` 只把 ASCII
//! 大写字母视同小写字母，其他码点按原样。UTF-8 自同步，字节上的子串与码点序列上的连续子序列等价；多字节码点的各字节
//! 都不小于 0x80，只折叠 ASCII 字母不会改动它们。取值为 NULL 的列按不含文本处理，两个函数的值都不为 NULL，`NOT` 与 `OR`
//! 因此按二值逻辑得出结果。不走 `LIKE`/`ILIKE` 与按 Unicode 规则的小写函数。

use std::sync::{Arc, LazyLock};

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray};
use arrow::datatypes::DataType;
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::{ident, lit};

/// `str_match(<column>, '<text>')` 的表达式（`column` 为 Utf8 列）。
pub fn str_match(column: &str, text: &str) -> Expr {
    STR_MATCH.call(vec![lit(text), ident(column)])
}

/// `match_all('<text>')` 的表达式，被搜列为 `columns`（表的全部 Utf8 列）；没有 Utf8 列时恒为假。
pub fn match_all(columns: &[String], text: &str) -> Expr {
    if columns.is_empty() {
        return lit(false);
    }
    let mut args = vec![lit(text)];
    args.extend(columns.iter().map(ident));
    MATCH_ALL.call(args)
}

/// `haystack` 是否含 `needle`（码点逐一相同）。
pub fn contains(haystack: &str, needle: &str) -> bool {
    haystack.contains(needle)
}

/// 只把 ASCII 大写字母视同小写字母时，`haystack` 是否含 `needle`。
pub fn contains_ascii_case_insensitive(haystack: &str, needle: &str) -> bool {
    let (h, n) = (haystack.as_bytes(), needle.as_bytes());
    n.is_empty() || h.windows(n.len()).any(|w| w.eq_ignore_ascii_case(n))
}

static STR_MATCH: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(TextMatch::new(Mode::Exact)));
static MATCH_ALL: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(TextMatch::new(Mode::AsciiCaseInsensitive)));

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Mode {
    Exact,
    AsciiCaseInsensitive,
}

/// 参数为文本（常量）与一个或多个 Utf8 列；结果为真当且仅当某列取值不为 NULL 且含文本。
#[derive(Debug, PartialEq, Eq, Hash)]
struct TextMatch {
    mode: Mode,
    signature: Signature,
}

impl TextMatch {
    fn new(mode: Mode) -> Self {
        TextMatch {
            mode,
            signature: Signature::variadic(vec![DataType::Utf8], Volatility::Immutable),
        }
    }

    fn matches(&self, value: &str, text: &str) -> bool {
        match self.mode {
            Mode::Exact => contains(value, text),
            Mode::AsciiCaseInsensitive => contains_ascii_case_insensitive(value, text),
        }
    }
}

impl ScalarUDFImpl for TextMatch {
    fn name(&self) -> &str {
        match self.mode {
            Mode::Exact => "amberlog_str_match",
            Mode::AsciiCaseInsensitive => "amberlog_match_all",
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let Some((text, columns)) = args.args.split_first() else {
            return exec_err!("text match takes the text and at least one column");
        };
        let ColumnarValue::Scalar(ScalarValue::Utf8(Some(text))) = text else {
            return exec_err!("text match text must be a constant string");
        };
        let rows = args.number_rows;
        let mut hit = vec![false; rows];
        for c in columns {
            match c {
                ColumnarValue::Array(a) => {
                    let a = a.as_string::<i32>();
                    for (row, h) in hit.iter_mut().enumerate() {
                        if !*h && a.is_valid(row) && self.matches(a.value(row), text) {
                            *h = true;
                        }
                    }
                }
                ColumnarValue::Scalar(ScalarValue::Utf8(v)) => {
                    if v.as_deref().is_some_and(|v| self.matches(v, text)) {
                        hit.iter_mut().for_each(|h| *h = true);
                    }
                }
                other => return exec_err!("text match expects Utf8, got {}", other.data_type()),
            }
        }
        Ok(ColumnarValue::Array(
            Arc::new(BooleanArray::from(hit)) as ArrayRef
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substring_rules() {
        assert!(contains("50% done", "50%"));
        assert!(!contains("500 DONE", "done"));
        assert!(contains("a_b", "a_b"));
        assert!(!contains("axb", "a_b"));
        assert!(contains("C:\\x", "C:\\x"));
        assert!(contains("", ""));
        assert!(contains_ascii_case_insensitive(
            "Disk ERROR on sda",
            "error"
        ));
        assert!(contains_ascii_case_insensitive("error-node-1", "ERROR"));
        assert!(contains_ascii_case_insensitive("Émile", "ÉMILE"));
        assert!(!contains_ascii_case_insensitive("Émile", "émile"));
        assert!(contains_ascii_case_insensitive("x", ""));
        assert!(!contains_ascii_case_insensitive("", "a"));
        assert!(contains_ascii_case_insensitive("数据库连接失败", "连接"));
    }
}

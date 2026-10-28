//! 请求体 JSON 的判定与解析。
//!
//! 一套自写的扫描器同时服务两种请求体，合法 JSON 的界定只此一处：
//! - `_search` 与 `_sent`（[`parse_body`]）：整体判定（REQ-AMB-ING-JSON-ENDPOINT-V2「合法 JSON 的界定」）——
//!   UTF-8 字节序标记、非法 UTF-8、未成对代理项、按就近舍入超出 binary64 有限范围的数字、嵌套超过 128 层，一律整体非法；
//! - `_json`（[`parse_ingest_body`]）：只有字节序标记、字符串字面量之外的非法字节与文法错误使请求体整体非法；
//!   字符串内的非法 UTF-8、未成对代理项与元素嵌套超过 128 层只使所在元素成为含取值缺陷的元素
//!   （REQ-AMB-ING-RECORD-ISOLATION 缺陷①②④）；数字范围缺陷③要看字段的生效取值，由记录模型判定。
//!
//! 解析结果是保留数字原文的 [`Json`]（REQ-AMB-DM-RECORD-MODEL）；`_search` 与 `_sent` 再转为 serde_json 的 [`Value`]。
//! 重复键以最后出现的取值为准（REQ-AMB-ERR-ENVELOPE「请求体解析」）。协议整数字段只收不含小数部分与指数、
//! 落在 int64 范围内的 JSON 数字（[`protocol_integer`]）。
//!
//! 扫描是迭代的（显式栈），任意深的请求体都不会耗尽线程栈；构建出的 [`Json`] 深度不超过 [`MAX_NESTING_DEPTH`]，
//! 对它的递归处理（序列化、展平、转换、析构）因此有界。

use std::collections::HashMap;

use serde_json::Value;

/// 对象与数组的嵌套层数上限（顶层值或 `_json` 的元素本身为第 1 层，标量不计层）：
/// `_search`、`_sent` 的请求体超过即整体非法（REQ-AMB-ING-JSON-ENDPOINT-V2），
/// `_json` 的元素超过即为含取值缺陷的元素（REQ-AMB-ING-RECORD-ISOLATION 缺陷④）。
pub const MAX_NESTING_DEPTH: usize = 128;

/// 保留数字字面量原文的 JSON 取值，供摄取的记录模型使用（REQ-AMB-DM-RECORD-MODEL）。
///
/// 为什么不用 [`Value`]：记录模型要求数组与超深子树序列化出的 JSON 文本里「数字与输入中对应的数字字面量
/// 逐字符相同」，而 serde_json 即便开了 `arbitrary_precision` 也会改写原文——指数一律记作小写 `e`
/// 并补 `+`（`1E2`、`1e2` 都记作 `1e+2`），`-0` 按整数解析后记作 `0`。这里另行保留原文。
///
/// - `Number` 存输入中的字面量原文（已符合 JSON 数字文法）。
/// - `Object` 按键在输入中的出现顺序保存；重复键取最后出现的取值，位置取该键首次出现处。
#[derive(Debug, Clone, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    Number(String),
    String(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

impl Json {
    /// 转为 serde_json 的 [`Value`]，数字按 serde_json 对同一原文的解析结果。
    /// 供按 `Value` 定义的既有判定（如 `_timestamp` 的归一、`_search` 的请求字段）复用。
    pub fn to_value(&self) -> Value {
        match self {
            Json::Null => Value::Null,
            Json::Bool(b) => Value::Bool(*b),
            Json::Number(lit) => lit
                .parse::<serde_json::Number>()
                .map(Value::Number)
                // 原文已符合 JSON 数字文法，总能解析；保守起见不 panic。
                .unwrap_or(Value::Null),
            Json::String(s) => Value::String(s.clone()),
            Json::Array(items) => Value::Array(items.iter().map(Json::to_value).collect()),
            Json::Object(members) => Value::Object(
                members
                    .iter()
                    .map(|(k, v)| (k.clone(), v.to_value()))
                    .collect(),
            ),
        }
    }
}

/// 数字字面量按 IEEE 754 就近舍入（恰在正中取偶）所得是否超出 binary64 有限范围。
/// 标准库的十进制转浮点按精确值就近舍入，上溢得到无穷。
pub fn number_out_of_range(literal: &str) -> bool {
    !literal.parse::<f64>().is_ok_and(f64::is_finite)
}

/// `_json` 请求体的一个元素（REQ-AMB-ING-RECORD-ISOLATION）。
#[derive(Debug, Clone, PartialEq)]
pub enum Element {
    /// 不含缺陷①②④的元素；缺陷③由记录模型在取出字段生效取值后判定。
    Value(Json),
    /// 含取值缺陷①②④之一：字符串字面量（含键）里的非法 UTF-8 或未成对代理项，或嵌套深度超过 128。
    Defective,
}

/// 解析 `_json` 请求体并批次化：顶层是数组时每个成员一个元素，否则顶层的那一个值为唯一元素。
/// 把取值缺陷视为合法之后仍不符合 RFC 8259 文法、以字节序标记开头或字符串之外含非 ASCII 字节时为 `Err`（`invalid_json`）。
pub fn parse_ingest_body(bytes: &[u8]) -> Result<Vec<Element>, serde_json::Error> {
    let mut s = Scanner { bytes, pos: 0 };
    s.skip_ws();
    let mut elements = Vec::new();
    if s.eat(b'[') {
        s.skip_ws();
        if !s.eat(b']') {
            loop {
                elements.push(s.element()?);
                s.skip_ws();
                match s.next()? {
                    b',' => {}
                    b']' => break,
                    _ => return Err(syntax()),
                }
            }
        }
    } else {
        elements.push(s.element()?);
    }
    s.end()?;
    Ok(elements)
}

/// 把 `_search` 与 `_sent` 的请求体解析为 JSON 值；整体不是合法 JSON 即 `Err`（`invalid_json`）。
pub fn parse_body(bytes: &[u8]) -> Result<Value, serde_json::Error> {
    let json = parse_body_literal(bytes)?;
    if has_out_of_range_number(&json) {
        return Err(invalid("number out of range"));
    }
    Ok(json.to_value())
}

/// 把请求体当作单个值解析为保留数字原文的 [`Json`]：文法错误或含取值缺陷①②④（非法 UTF-8、未成对代理项、
/// 嵌套超过 128 层）即 `Err`；**不**检查数字范围（见 [`parse_body`]）。
pub fn parse_body_literal(bytes: &[u8]) -> Result<Json, serde_json::Error> {
    let mut s = Scanner { bytes, pos: 0 };
    let element = s.element()?;
    s.end()?;
    match element {
        Element::Value(json) => Ok(json),
        Element::Defective => Err(invalid(
            "invalid UTF-8, unpaired surrogate or nesting deeper than 128",
        )),
    }
}

/// 是否含按就近舍入超出 binary64 有限范围的数字（整数字面量同样按舍入结果判定）。
fn has_out_of_range_number(json: &Json) -> bool {
    let mut stack = vec![json];
    while let Some(v) = stack.pop() {
        match v {
            Json::Number(lit) if number_out_of_range(lit) => return true,
            Json::Array(items) => stack.extend(items),
            Json::Object(members) => stack.extend(members.iter().map(|(_, v)| v)),
            _ => {}
        }
    }
    false
}

fn invalid(msg: impl std::fmt::Display) -> serde_json::Error {
    <serde_json::Error as serde::de::Error>::custom(msg)
}

fn syntax() -> serde_json::Error {
    invalid("syntax error")
}

/// 正在构建的容器。
enum Frame {
    Array(Vec<Json>),
    Object {
        members: Vec<(String, Json)>,
        positions: HashMap<String, usize>,
        /// 下一个成员的键。
        key: String,
    },
}

impl Frame {
    fn new(object: bool) -> Frame {
        if object {
            Frame::Object {
                members: Vec::new(),
                positions: HashMap::new(),
                key: String::new(),
            }
        } else {
            Frame::Array(Vec::new())
        }
    }

    fn push(&mut self, value: Json) {
        match self {
            Frame::Array(items) => items.push(value),
            Frame::Object {
                members,
                positions,
                key,
            } => {
                let key = std::mem::take(key);
                // 重复键：取值取最后出现者，位置留在首次出现处。
                match positions.get(&key) {
                    Some(&i) => members[i].1 = value,
                    None => {
                        positions.insert(key.clone(), members.len());
                        members.push((key, value));
                    }
                }
            }
        }
    }

    fn finish(self) -> Json {
        match self {
            Frame::Array(items) => Json::Array(items),
            Frame::Object { members, .. } => Json::Object(members),
        }
    }
}

struct Scanner<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl Scanner<'_> {
    fn skip_ws(&mut self) {
        while matches!(self.bytes.get(self.pos), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn next(&mut self) -> Result<u8, serde_json::Error> {
        let b = self.peek().ok_or_else(syntax)?;
        self.pos += 1;
        Ok(b)
    }

    fn eat(&mut self, want: u8) -> bool {
        let hit = self.peek() == Some(want);
        if hit {
            self.pos += 1;
        }
        hit
    }

    /// 值之后只许有空白。
    fn end(&mut self) -> Result<(), serde_json::Error> {
        self.skip_ws();
        if self.pos == self.bytes.len() {
            Ok(())
        } else {
            Err(syntax())
        }
    }

    /// 扫描一个值（元素）。容器以显式栈处理；一旦发现取值缺陷即停止构建，只继续核对文法直到该值结束。
    fn element(&mut self) -> Result<Element, serde_json::Error> {
        // 已打开的容器（true 为对象）与对应的构建帧；`sound` 为 false 时不再构建，`frames` 为空。
        let mut open: Vec<bool> = Vec::new();
        let mut frames: Vec<Frame> = Vec::new();
        let mut sound = true;
        'value: loop {
            self.skip_ws();
            // 1. 读一个值；遇到非空容器的开头则压栈后回到 'value 读其第一个成员
            let mut value: Option<Json> = match self.next()? {
                c @ (b'[' | b'{') => {
                    let object = c == b'{';
                    open.push(object);
                    if open.len() > MAX_NESTING_DEPTH {
                        sound = false;
                        frames.clear();
                    }
                    if sound {
                        frames.push(Frame::new(object));
                    }
                    self.skip_ws();
                    if !self.eat(if object { b'}' } else { b']' }) {
                        if object {
                            self.key(&mut sound, &mut frames)?;
                        }
                        continue 'value;
                    }
                    open.pop();
                    frames.pop().map(Frame::finish)
                }
                b'"' => {
                    let s = self.string()?;
                    if s.is_none() {
                        sound = false;
                        frames.clear();
                    }
                    s.map(Json::String)
                }
                b'-' | b'0'..=b'9' => {
                    self.pos -= 1;
                    Some(Json::Number(self.number()?.to_owned()))
                }
                b't' => self.keyword(b"rue", Json::Bool(true))?,
                b'f' => self.keyword(b"alse", Json::Bool(false))?,
                b'n' => self.keyword(b"ull", Json::Null)?,
                _ => return Err(syntax()),
            };
            // 2. 一个值已完整：挂到外层容器，处理其后的逗号或闭括号（闭括号又完成一个值）
            loop {
                let Some(&object) = open.last() else {
                    return Ok(match (sound, value) {
                        (true, Some(json)) => Element::Value(json),
                        _ => Element::Defective,
                    });
                };
                if sound && let (Some(frame), Some(v)) = (frames.last_mut(), value.take()) {
                    frame.push(v);
                }
                self.skip_ws();
                match self.next()? {
                    b',' => {
                        if object {
                            self.key(&mut sound, &mut frames)?;
                        }
                        continue 'value;
                    }
                    b']' if !object => {}
                    b'}' if object => {}
                    _ => return Err(syntax()),
                }
                open.pop();
                value = if sound {
                    frames.pop().map(Frame::finish)
                } else {
                    None
                };
            }
        }
    }

    /// 读对象成员的键与其后的冒号；键含取值缺陷时停止构建。
    fn key(&mut self, sound: &mut bool, frames: &mut Vec<Frame>) -> Result<(), serde_json::Error> {
        self.skip_ws();
        if self.next()? != b'"' {
            return Err(syntax());
        }
        let key = self.string()?;
        self.skip_ws();
        if self.next()? != b':' {
            return Err(syntax());
        }
        match key {
            Some(k) if *sound => {
                if let Some(Frame::Object { key, .. }) = frames.last_mut() {
                    *key = k;
                }
            }
            Some(_) => {}
            None => {
                *sound = false;
                frames.clear();
            }
        }
        Ok(())
    }

    fn keyword(&mut self, rest: &[u8], value: Json) -> Result<Option<Json>, serde_json::Error> {
        if self.bytes[self.pos..].starts_with(rest) {
            self.pos += rest.len();
            Ok(Some(value))
        } else {
            Err(syntax())
        }
    }

    /// 数字：`-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?`，返回字面量原文。
    fn number(&mut self) -> Result<&str, serde_json::Error> {
        let start = self.pos;
        self.eat(b'-');
        match self.next()? {
            b'0' => {}
            b'1'..=b'9' => self.digits(),
            _ => return Err(syntax()),
        }
        if self.eat(b'.') {
            self.require_digits()?;
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.pos += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            self.require_digits()?;
        }
        // 数字只含 ASCII，切片必是合法 UTF-8。
        std::str::from_utf8(&self.bytes[start..self.pos]).map_err(|_| syntax())
    }

    fn digits(&mut self) {
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.pos += 1;
        }
    }

    fn require_digits(&mut self) -> Result<(), serde_json::Error> {
        if !matches!(self.peek(), Some(b'0'..=b'9')) {
            return Err(syntax());
        }
        self.digits();
        Ok(())
    }

    /// 开引号之后的字符串字面量，读到闭引号为止。文法错误（未转义的控制字符、非法转义、未闭合）为 `Err`；
    /// 含非法 UTF-8 字节或未成对代理项转义时为 `Ok(None)`（取值缺陷①②）。
    fn string(&mut self) -> Result<Option<String>, serde_json::Error> {
        let mut out: Vec<u8> = Vec::new();
        let mut defect = false;
        loop {
            let run = self.pos;
            while matches!(self.peek(), Some(b) if b >= 0x20 && b != b'"' && b != b'\\') {
                self.pos += 1;
            }
            out.extend_from_slice(&self.bytes[run..self.pos]);
            match self.next()? {
                b'"' => break,
                b'\\' => {}
                _ => return Err(syntax()),
            }
            let unit = match self.next()? {
                b'"' => '"',
                b'\\' => '\\',
                b'/' => '/',
                b'b' => '\u{8}',
                b'f' => '\u{c}',
                b'n' => '\n',
                b'r' => '\r',
                b't' => '\t',
                b'u' => {
                    let hi = self.hex4()?;
                    let code = match hi {
                        0xD800..=0xDBFF => match self.low_surrogate() {
                            Some(lo) => 0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00),
                            None => {
                                defect = true;
                                continue;
                            }
                        },
                        0xDC00..=0xDFFF => {
                            defect = true;
                            continue;
                        }
                        _ => hi,
                    };
                    char::from_u32(code).ok_or_else(syntax)?
                }
                _ => return Err(syntax()),
            };
            let mut buf = [0u8; 4];
            out.extend_from_slice(unit.encode_utf8(&mut buf).as_bytes());
        }
        if defect {
            return Ok(None);
        }
        Ok(String::from_utf8(out).ok())
    }

    fn hex4(&mut self) -> Result<u32, serde_json::Error> {
        let digits = self.bytes.get(self.pos..self.pos + 4).ok_or_else(syntax)?;
        if !digits.iter().all(u8::is_ascii_hexdigit) {
            return Err(syntax());
        }
        self.pos += 4;
        // 四个 ASCII 十六进制数字
        u32::from_str_radix(std::str::from_utf8(digits).map_err(|_| syntax())?, 16)
            .map_err(|_| syntax())
    }

    /// 高代理项之后紧跟 `\u` 低代理项时消费并返回它；否则不消费（其后的转义照常按文法读）。
    fn low_surrogate(&mut self) -> Option<u32> {
        let rest = self.bytes.get(self.pos..self.pos + 6)?;
        if &rest[..2] != b"\\u" || !rest[2..].iter().all(u8::is_ascii_hexdigit) {
            return None;
        }
        let lo = u32::from_str_radix(std::str::from_utf8(&rest[2..]).ok()?, 16).ok()?;
        if !(0xDC00..=0xDFFF).contains(&lo) {
            return None;
        }
        self.pos += 6;
        Some(lo)
    }
}

/// 协议整数字段的取值：只有整数形态且落在 int64 范围内的 JSON 数字返回 `Some`，
/// 其余（`1.0`、`1e2`、数字字符串、`null`、超出 int64 的整数等）返回 `None`，由调用方按类型不符处理。
pub fn protocol_integer(value: &Value) -> Option<i64> {
    // arbitrary_precision 下 `as_i64` 以 `str::parse::<i64>` 解析数字原文：带小数点或指数的原文
    // （`1.0`、`1e2`，后者原文被 serde_json 记作 `1e+2`）与超出 i64 的整数都解析失败，返回 None。
    value.as_i64()
}

/// 序列化为 JSON 字节，数字按 `_search` 的 `hits` 的写法（REQ-AMB-QRY-SEARCH-ENDPOINT）：Float64 写作含小数点或指数的
/// 最短往返形式，指数不带 `+`（`100.0`、`1e300`、`1e-7`）。serde_json 的浮点格式化把正指数写作 `1e+300`，这里在写出数字时
/// 去掉 `+`；整数不含指数，不受影响。桶 manifest 的 `column_stats` 引用同一写法（REQ-AMB-QRY-COLUMN-PRUNING）。
pub fn to_vec<T: serde::Serialize + ?Sized>(value: &T) -> Vec<u8> {
    let mut out = Vec::new();
    let mut ser = serde_json::Serializer::with_formatter(&mut out, NumberFormatter);
    value.serialize(&mut ser).expect("写入内存缓冲不会失败");
    out
}

struct NumberFormatter;

impl serde_json::ser::Formatter for NumberFormatter {
    fn write_number_str<W: ?Sized + std::io::Write>(
        &mut self,
        writer: &mut W,
        value: &str,
    ) -> std::io::Result<()> {
        match value.split_once("e+") {
            Some((mantissa, exponent)) => write!(writer, "{mantissa}e{exponent}"),
            None => writer.write_all(value.as_bytes()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_keys_last_wins() {
        let v = parse_body(br#"{"query":{"size":100000,"size":1}}"#).unwrap();
        assert_eq!(v["query"]["size"], 1);
        let v = parse_body(br#"{"query":{"size":1,"size":100000}}"#).unwrap();
        assert_eq!(v["query"]["size"], 100000);
    }

    #[test]
    fn protocol_integer_forms() {
        let cases: &[(&str, Option<i64>)] = &[
            ("10", Some(10)),
            ("-3", Some(-3)),
            ("0", Some(0)),
            ("9223372036854775807", Some(i64::MAX)),
            ("-9223372036854775808", Some(i64::MIN)),
            ("9223372036854775808", None),
            ("1.0", None),
            ("1e2", None),
            ("1E2", None),
            ("\"10\"", None),
            ("null", None),
            ("true", None),
        ];
        for (text, want) in cases {
            let v: Value = serde_json::from_str(text).unwrap();
            assert_eq!(protocol_integer(&v), *want, "{text}");
        }
    }

    #[test]
    fn literal_keeps_number_text_and_key_order() {
        let v = parse_body_literal(
            br#" { "b" : [1E+2, 1e2, -0, 0.10, 1.0] , "a" : 1 , "b" : -0.0e-1 } "#,
        )
        .unwrap();
        assert_eq!(
            v,
            Json::Object(vec![
                ("b".into(), Json::Number("-0.0e-1".into())),
                ("a".into(), Json::Number("1".into())),
            ])
        );
        let v =
            parse_body_literal(br#"[1E+2, 1e2, -0, 0.10, 1.0, 12345678901234567890123]"#).unwrap();
        let Json::Array(items) = v else {
            panic!("应为数组")
        };
        let lits: Vec<&str> = items
            .iter()
            .map(|i| match i {
                Json::Number(s) => s.as_str(),
                _ => panic!("应为数字"),
            })
            .collect();
        assert_eq!(
            lits,
            [
                "1E+2",
                "1e2",
                "-0",
                "0.10",
                "1.0",
                "12345678901234567890123"
            ]
        );
    }

    #[test]
    fn literal_decodes_strings() {
        let v = parse_body_literal(
            br#"["q\"b\\s\/", "\u0001\b\f\n\r\t\u001F", "\u00e9\ud83d\ude00", "\u00e9 plain"]"#,
        )
        .unwrap();
        assert_eq!(
            v,
            Json::Array(vec![
                Json::String("q\"b\\s/".into()),
                Json::String("\u{1}\u{8}\u{c}\n\r\t\u{1f}".into()),
                Json::String("é😀".into()),
                Json::String("é plain".into()),
            ])
        );
        let v = parse_body_literal("{\"k\":\"é\u{2028}中\"}".as_bytes()).unwrap();
        assert_eq!(
            v,
            Json::Object(vec![("k".into(), Json::String("é\u{2028}中".into()))])
        );
    }

    #[test]
    fn literal_rejects_syntax_and_defects() {
        let cases: &[&[u8]] = &[
            br#"["\ud800"]"#,
            b"\xEF\xBB\xBF[]",
            b"[\"\xFF\"]",
            br#"{"a":1,}"#,
            br#"[1] x"#,
            br#"01"#,
            br#"[1.]"#,
            br#"[-]"#,
            br#"[1e]"#,
            br#"["\x"]"#,
            b"[\"\x01\"]",
            br#"["\u12"]"#,
            br#"{"a" 1}"#,
            br#"{1:1}"#,
            br#"[tru]"#,
            br#"[1}"#,
            br#"{"a":1]"#,
            br#""#,
        ];
        for bytes in cases {
            assert!(parse_body(bytes).is_err());
            assert!(
                parse_body_literal(bytes).is_err(),
                "{}",
                String::from_utf8_lossy(bytes)
            );
        }
        for text in [
            "null",
            "true",
            "false",
            "\"x\"",
            "-1.5e-3",
            "[]",
            "{}",
            "[[],{}]",
            " [ 1 , 2 ] ",
            "0",
            "-0",
            "1E+2",
            "[1e400]",
        ] {
            assert!(parse_body_literal(text.as_bytes()).is_ok(), "{text}");
        }
    }

    #[test]
    fn nesting_depth_limit() {
        let nested = |n: usize| format!("{}1{}", "[".repeat(n), "]".repeat(n));
        assert!(parse_body(nested(128).as_bytes()).is_ok());
        assert!(parse_body(nested(129).as_bytes()).is_err());
        // 迭代扫描：极深的请求体不耗尽栈
        assert!(parse_body(nested(200_000).as_bytes()).is_err());
        // `_json`：顶层数组不计层，元素本身为第 1 层
        let body = format!("[{},{}]", nested(128), nested(129));
        let elements = parse_ingest_body(body.as_bytes()).unwrap();
        assert!(matches!(elements[0], Element::Value(_)));
        assert_eq!(elements[1], Element::Defective);
        let deep = format!("[{}]", nested(200_000));
        assert_eq!(
            parse_ingest_body(deep.as_bytes()).unwrap(),
            [Element::Defective]
        );
        // 过深之后的文法错误照样整体非法
        let broken = format!("[{}]", &nested(200)[..250]);
        assert!(parse_ingest_body(broken.as_bytes()).is_err());
    }

    #[test]
    fn ingest_body_defects_are_per_element() {
        let body = [
            br#"[{"m":"\ud800"},{"m":"ok"},{"m":""#.as_slice(),
            b"\xFF",
            br#""},{"\udc00":1},"\ud800x",{"t":["\ud800"]},{"m":"\ud83d\ude00"},{"m":"\ud800\u0041"}]"#,
        ]
        .concat();
        let elements = parse_ingest_body(&body).unwrap();
        let defective: Vec<bool> = elements.iter().map(|e| *e == Element::Defective).collect();
        assert_eq!(
            defective,
            [true, false, true, true, true, true, false, true]
        );
        assert_eq!(
            elements[6],
            Element::Value(Json::Object(vec![(
                "m".into(),
                Json::String("\u{1F600}".into())
            )]))
        );
        // 单个值的请求体即唯一元素；数字范围不在这里判定
        assert_eq!(
            parse_ingest_body(br#"{"b":-1e400}"#).unwrap(),
            [Element::Value(Json::Object(vec![(
                "b".into(),
                Json::Number("-1e400".into())
            )]))]
        );
        assert_eq!(parse_ingest_body(b" [ ] ").unwrap(), []);
        assert_eq!(
            parse_ingest_body(b"5").unwrap(),
            [Element::Value(Json::Number("5".into()))]
        );
        // 文法错误、字节序标记、字符串之外的非法字节：整体非法
        for bad in [
            br#"[{"a":1},{"b":1e400},{"c":]"#.as_slice(),
            b"\xEF\xBB\xBF[{\"a\":1}]",
            b"[{\"a\":1}\xFF]",
            b"",
            b"[\"\xFF",
        ] {
            assert!(
                parse_ingest_body(bad).is_err(),
                "{}",
                String::from_utf8_lossy(bad)
            );
        }
    }

    #[test]
    fn literal_to_value_matches_parse_body() {
        let text = br#"{"a":[1E2,-0,"x",null,true,{"b":1.5}],"c":{}}"#;
        assert_eq!(
            parse_body_literal(text).unwrap().to_value(),
            parse_body(text).unwrap()
        );
    }

    #[test]
    fn parse_body_number_range() {
        let cases: &[(&str, bool)] = &[
            (r#"[{"a":1e400}]"#, false),
            (r#"[{"a":-1e400}]"#, false),
            (r#"{"a":[1,{"b":1e309}]}"#, false),
            (r#"[{"a":1e308}]"#, true),
            (r#"[{"a":1e-400}]"#, true),
            (r#"[{"a":123456789012345678901234567890}]"#, true),
            (r#"[1.7976931348623158e308]"#, true),
            (r#"[1.7976931348623159e308]"#, false),
            (&format!("[1{}]", "0".repeat(400)), false),
        ];
        for (text, ok) in cases {
            assert_eq!(parse_body(text.as_bytes()).is_ok(), *ok, "{text}");
        }
    }
}

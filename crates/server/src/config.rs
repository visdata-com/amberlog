//! 配置装载与取值校验（REQ-AMB-CFG-STARTUP-V5 A 组）。
//!
//! 来源与优先级：环境变量 > 配置文件（TOML，仅在给出 `--config` 时读取）> 内置默认值。
//! 取值写法按块逐条自行判定，不经配置库的宽松类型转换；未知键一律报错。
//! 全部错误一并收集后返回，每条都指出键名（或配置文件路径），由调用方以退出码 2 退出。

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fmt;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use amberlog_storage::store::{Location, S3Options, ServerSideEncryption};

use crate::alerting::rule::{self, Rule};
use crate::alerting::webhook::{self, Target};
use crate::form::Form;
use crate::logging::{Format, Level};

const ENV_PREFIX: &str = "AMBERLOG_";

/// `retention.period` 的上限 `36500d`。
const RETENTION_PERIOD_MAX: Duration = Duration::from_secs(36_500 * 86_400);

/// `alerts.history_retention` 的下限 `1h`（上限同 `retention.period`，REQ-AMB-CFG-STARTUP-V8）。
const HISTORY_RETENTION_MIN: Duration = Duration::from_secs(3_600);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Bool,
    Int,
    Str,
}

/// 全部配置键及其 TOML 类型（块「配置项清单」与「取值的类型与格式通则」）。
const KEYS: &[(&str, Kind)] = &[
    ("server.http_addr", Kind::Str),
    ("server.shutdown_timeout", Kind::Str),
    ("server.tls.cert_file", Kind::Str),
    ("server.tls.key_file", Kind::Str),
    ("server.behind_tls_proxy", Kind::Bool),
    ("storage.prefix", Kind::Str),
    ("storage.commit_max_retries", Kind::Int),
    ("storage.delete_delay", Kind::Str),
    ("object_store.url", Kind::Str),
    ("object_store.s3.conditional_put", Kind::Str),
    ("object_store.s3.endpoint", Kind::Str),
    ("object_store.s3.region", Kind::Str),
    ("object_store.s3.access_key_id", Kind::Str),
    ("object_store.s3.secret_access_key", Kind::Str),
    ("object_store.s3.force_path_style", Kind::Bool),
    ("object_store.s3.ca_file", Kind::Str),
    ("object_store.s3.server_side_encryption", Kind::Str),
    ("object_store.s3.sse_kms_key_id", Kind::Str),
    ("wal.dir", Kind::Str),
    ("auth.enabled", Kind::Bool),
    ("auth.token", Kind::Str),
    ("ingest.max_body_bytes", Kind::Int),
    ("ingest.max_future", Kind::Str),
    ("ingest.max_past", Kind::Str),
    ("ingest.max_columns", Kind::Int),
    ("flush.interval", Kind::Str),
    ("flush.max_records", Kind::Int),
    ("ingest.max_backlog_memory_bytes", Kind::Int),
    ("wal.alert_bytes", Kind::Int),
    ("query.default_size", Kind::Int),
    ("query.max_size", Kind::Int),
    ("query.max_concurrency", Kind::Int),
    ("query.max_memory_bytes", Kind::Int),
    ("log.level", Kind::Str),
    ("log.format", Kind::Str),
    ("lifecycle.seal_grace", Kind::Str),
    ("lifecycle.lake_watermark", Kind::Str),
    ("lifecycle.lake_alert_after", Kind::Str),
    ("compaction.target_file_bytes", Kind::Int),
    ("hot_fsck.interval", Kind::Str),
    ("hot_fsck.min_orphan_age", Kind::Str),
    ("hot_retention.snapshots", Kind::Int),
    ("hot_retention.age", Kind::Str),
    ("retention.period", Kind::Str),
    ("retention.check_interval", Kind::Str),
    ("cold_compaction.check_interval", Kind::Str),
    ("cold_retention.snapshots", Kind::Int),
    ("cold_retention.age", Kind::Str),
    ("cold_retention.check_interval", Kind::Str),
    ("alerts.max_concurrency", Kind::Int),
    ("alerts.history_retention", Kind::Str),
    ("alerts.ca_file", Kind::Str),
    ("alerts.missed_collection.enabled", Kind::Bool),
    ("alerts.missed_collection.interval", Kind::Str),
    ("alerts.missed_collection.webhook_url", Kind::Str),
    ("reconciliation.grace", Kind::Str),
];

fn kind_of(key: &str) -> Option<Kind> {
    KEYS.iter().find(|(k, _)| *k == key).map(|(_, kind)| *kind)
}

fn is_table(key: &str) -> bool {
    KEYS.iter()
        .any(|(k, _)| k.len() > key.len() && k.starts_with(key) && k.as_bytes()[key.len()] == b'.')
}

fn env_name(key: &str) -> String {
    format!(
        "{ENV_PREFIX}{}",
        key.to_ascii_uppercase().replace('.', "__")
    )
}

#[derive(Debug, Clone, PartialEq)]
enum Raw {
    Bool(bool),
    Int(i64),
    Str(String),
}

/// 一条配置错误。`subject` 是键名、环境变量名或配置文件路径；子命令参数的错误（[`ConfigError::argument`]）为空，
/// 参数名写在 `message` 里。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError {
    pub subject: String,
    pub message: String,
}

impl ConfigError {
    /// 子命令参数取值的错误：`message` 自带参数名。
    pub fn argument(message: impl Into<String>) -> Self {
        err("", message)
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.subject.is_empty() {
            f.write_str(&self.message)
        } else {
            write!(f, "{}: {}", self.subject, self.message)
        }
    }
}

fn err(subject: impl Into<String>, message: impl Into<String>) -> ConfigError {
    ConfigError {
        subject: subject.into(),
        message: message.into(),
    }
}

/// 认证开关与口令。
#[derive(Clone, PartialEq, Eq)]
pub enum Auth {
    Enabled { token: String },
    Disabled,
}

/// 口令不进调试输出。
impl fmt::Debug for Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Auth::Enabled { .. } => f
                .debug_struct("Enabled")
                .field("token", &format_args!("***"))
                .finish(),
            Auth::Disabled => f.write_str("Disabled"),
        }
    }
}

/// 校验通过的配置。
#[derive(Debug, Clone)]
pub struct Config {
    pub http_addr: SocketAddr,
    /// `server.http_addr` 的配置取值原文（`plaintext_listen` 的字段）。
    pub http_addr_text: String,
    pub shutdown_timeout: Duration,
    /// `server.tls.cert_file` 与 `server.tls.key_file`（成对配置；REQ-AMB-ARCH-ENCRYPTION）。文件内容在配置一步另行读取校验。
    pub tls: Option<TlsFiles>,
    /// `server.behind_tls_proxy`：明文声明。
    pub behind_tls_proxy: bool,
    /// `object_store.s3.ca_file`（解析为绝对路径）；只在 `object_store.url` 为 `s3://` 时读取。
    pub ca_file: Option<CaFile>,
    pub storage_prefix: String,
    pub commit_max_retries: u64,
    /// `storage.delete_delay`：离开存活文件集的文件至少过这么久才物理删除（REQ-AMB-STO-AUDIT-INVARIANTS-V3）。
    pub delete_delay: Duration,
    /// `object_store.url` 的原文（`ready` 日志原样输出；userinfo、查询串与片段在校验时已被拒绝）。
    pub object_store_url: String,
    pub location: Location,
    pub s3: S3Options,
    /// `wal.dir`；只在 [`Form::requires_wal_dir`] 的形态中必填，其余形态缺省为 `None`（配置了也不使用）。
    pub wal_dir: Option<PathBuf>,
    /// 认证；`None` 只出现在不受理请求的形态（[`Form::requires_auth_token`] 为 false）中 `auth.enabled` 为 true 而未配口令时。
    pub auth: Option<Auth>,
    pub max_body_bytes: u64,
    pub max_future: Duration,
    pub max_past: Option<Duration>,
    pub flush_interval: Duration,
    pub flush_max_records: u64,
    /// `ingest.max_columns`，按 stream 计。
    pub max_columns: u64,
    /// `ingest.max_backlog_memory_bytes`，全局。
    pub max_backlog_memory_bytes: u64,
    /// `wal.alert_bytes`，全局。
    pub wal_alert_bytes: u64,
    pub query_default_size: u64,
    pub query_max_size: u64,
    /// `query.max_concurrency`：同时占用执行名额的 `_search` 上限（REQ-AMB-QRY-RESOURCE-LIMITS）。
    pub query_max_concurrency: u64,
    /// `query.max_memory_bytes`：单个 `_search` 由系统计量的内存上限（REQ-AMB-QRY-RESOURCE-LIMITS）。
    pub query_max_memory_bytes: u64,
    pub log_level: Level,
    pub log_format: Format,
    pub seal_grace: Duration,
    pub lake_watermark: Duration,
    /// `lifecycle.lake_alert_after`：入湖积压告警阈值（REQ-AMB-STO-LAKE-ENTRY）。
    pub lake_alert_after: Duration,
    pub compaction_target_file_bytes: u64,
    pub hot_fsck_interval: Duration,
    pub hot_fsck_min_orphan_age: Duration,
    /// `hot_retention.snapshots`：热层快照回收至少保留的快照个数（REQ-AMB-STO-SNAPSHOT-GC）。
    pub hot_retention_snapshots: u64,
    /// `hot_retention.age`：热层快照回收的保留时长（REQ-AMB-STO-SNAPSHOT-GC）。
    pub hot_retention_age: Duration,
    /// `retention.period`；缺省为不设。
    pub retention_period: Option<RetentionPeriod>,
    /// `retention.check_interval`：到期删除的执行间隔，只在配置了 `retention.period` 时起作用（REQ-AMB-STO-TTL）；
    /// 未配置保留期时照常校验。
    pub retention_check_interval: Duration,
    /// `cold_compaction.check_interval`：冷层合并的检查间隔（REQ-AMB-STO-COLD-COMPACTION）。
    pub cold_compaction_check_interval: Duration,
    /// `cold_retention.snapshots`、`cold_retention.age` 与 `cold_retention.check_interval`：冷层快照按个数与时长保留、
    /// 冷层保留的执行间隔（REQ-AMB-STO-COLD-RETENTION）。
    pub cold_retention_snapshots: u64,
    pub cold_retention_age: Duration,
    pub cold_retention_check_interval: Duration,
    /// `reconciliation.grace`：漏采判定的宽限期，整分钟，`1m`～`24h`（REQ-AMB-CFG-STARTUP-V8、
    /// REQ-AMB-GOV-MISSED-COLLECTION-TERM-GRACE）。
    pub reconciliation_grace: Duration,
    /// 索引提示（REQ-AMB-DM-INDEX-HINTS-V2）：数据集名 `default/<stream>` → 列名数组。启动时读入一次，决定写出数据文件时
    /// 哪些列带等值 bloom（REQ-AMB-QRY-EQUALITY-BLOOM）；不写入对象存储。
    pub index_hints: BTreeMap<String, Vec<String>>,
    /// 定时告警的配置（REQ-AMB-CFG-STARTUP-V8、REQ-AMB-ALR-RULES）：以任何子命令运行都校验，只在 `amberlog serve` 下起作用。
    pub alerts: Alerts,
}

/// `alerts.*` 配置项。
#[derive(Debug, Clone)]
pub struct Alerts {
    /// 启用的规则：`alerts.rules` 中 `enabled` 不为 false 者按配置中的先后，启用的内置规则 `missed_collection` 列在最后；
    /// `enabled` 为 false 的规则只校验、不在此列。
    pub rules: Vec<Arc<Rule>>,
    /// `alerts.max_concurrency`：告警名额。
    pub max_concurrency: u64,
    /// `alerts.history_retention`：评估记录的保留期（REQ-AMB-ALR-EVALUATIONS）。
    pub history_retention: Duration,
    /// `alerts.ca_file`：webhook 接收端追加信任的 CA 证书文件（REQ-AMB-ARCH-ENCRYPTION-V2）。
    pub ca_file: Option<CaFile>,
    /// `alerts.ca_file` 的内容：只在 `amberlog serve` 下由加密的校验读出（`startup/encryption.rs`）。
    pub ca_pem: Option<Vec<u8>>,
}

/// 入站 TLS 的证书链与私钥文件：配置取值原文（`tls_reload_failed` 的字段）与按工作目录解析后的路径。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsFiles {
    pub cert_file: String,
    pub key_file: String,
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
}

/// 追加信任的 CA 证书文件（`object_store.s3.ca_file` 或 `alerts.ca_file`）：配置取值原文与解析后的路径。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaFile {
    pub text: String,
    pub path: PathBuf,
}

/// `retention.period` 的取值：台账的 `retention` 列呈现配置值的原文（REQ-AMB-GOV-LEDGER），故与换算所得的时长一并保留。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionPeriod {
    pub text: String,
    pub period: Duration,
}

/// 装载配置。`form` 决定必填项（REQ-AMB-CFG-STARTUP-V7），其余识别与校验对各形态相同；
/// `env` 为进程环境（测试可注入），`cwd` 用于解析相对路径的 `wal.dir`。
pub fn load(
    form: Form,
    config_path: Option<&Path>,
    env: impl IntoIterator<Item = (OsString, OsString)>,
    cwd: &Path,
) -> Result<Config, Vec<ConfigError>> {
    let mut errors = Vec::new();
    let mut values: BTreeMap<String, Raw> = BTreeMap::new();
    let mut index_hints = BTreeMap::new();
    let mut rules = Vec::new();
    let mut builtin_headers = None;

    if let Some(path) = config_path {
        match read_file(path) {
            Ok(mut table) => {
                // `index_hints` 表之下的键是数据集名，不按配置键识别
                if let Some(hints) = table.remove(INDEX_HINTS) {
                    index_hints = parse_index_hints(&hints, &mut errors);
                }
                // `alerts.rules` 各元素之下的键是规则的字段，`alerts.missed_collection.webhook_headers` 之下的键是请求头名，
                // 都不按配置键识别（REQ-AMB-CFG-STARTUP-V8）
                let raw_rules = match table.get_mut("alerts") {
                    Some(toml::Value::Table(alerts)) => {
                        if let Some(toml::Value::Table(mc)) = alerts.get_mut("missed_collection") {
                            builtin_headers = mc.remove("webhook_headers");
                        }
                        alerts.remove("rules")
                    }
                    _ => None,
                };
                if let Some(raw) = raw_rules {
                    match rule::parse_rules(&raw) {
                        Ok(r) => rules = r,
                        Err(why) => errors.extend(why.into_iter().map(|w| err(ALERT_RULES, w))),
                    }
                }
                flatten(&table, "", &mut values, &mut errors)
            }
            Err(e) => return Err(vec![e]),
        }
    }
    read_env(env, &mut values, &mut errors);
    if !errors.is_empty() {
        return Err(errors);
    }
    if let Some(builtin) = missed_collection(&values, builtin_headers.as_ref(), &mut errors) {
        rules.push(Arc::new(builtin));
    }

    let cfg = Resolver {
        form,
        values: &values,
        errors: &mut errors,
    }
    .resolve(cwd);
    match cfg {
        Some(cfg) if errors.is_empty() => Ok(Config {
            index_hints,
            alerts: Alerts {
                rules,
                ..cfg.alerts
            },
            ..cfg
        }),
        _ => Err(errors),
    }
}

/// 内置规则的配置表（REQ-AMB-ALR-RULES F 组）：其校验错误指出这个名字。
const MISSED_COLLECTION: &str = "alerts.missed_collection";

/// 内置规则 `missed_collection`（REQ-AMB-ALR-RULES-BUILTIN-VALIDATION、-BUILTIN-AS-RULE；配置项见 REQ-AMB-CFG-STARTUP-V8）：
/// 启用时返回它作为一条启用的规则。`webhook_url` 与 `webhook_headers` 配了就校验，不论是否启用。
fn missed_collection(
    values: &BTreeMap<String, Raw>,
    headers: Option<&toml::Value>,
    errors: &mut Vec<ConfigError>,
) -> Option<Rule> {
    let enabled = matches!(
        values.get("alerts.missed_collection.enabled"),
        Some(Raw::Bool(true))
    );
    let interval_key = "alerts.missed_collection.interval";
    let interval = match values.get(interval_key) {
        Some(Raw::Str(text)) => match parse_duration(text) {
            Some(d) if rule::is_whole_minutes_within(d, Duration::from_secs(86_400)) => Some(d),
            _ => {
                errors.push(err(
                    interval_key,
                    "must be a duration of whole minutes between 1m and 24h",
                ));
                None
            }
        },
        _ => Some(Duration::from_secs(300)),
    };
    let url = match values.get("alerts.missed_collection.webhook_url") {
        Some(Raw::Str(text)) => match webhook::parse_url(text) {
            Ok(u) => Some(Ok(u)),
            Err(why) => {
                errors.push(err(MISSED_COLLECTION, why));
                Some(Err(()))
            }
        },
        _ => None,
    };
    let headers = match headers.map(webhook::parse_headers) {
        None => Some(Vec::new()),
        Some(Ok(h)) => Some(h),
        Some(Err(why)) => {
            errors.push(err(MISSED_COLLECTION, why));
            None
        }
    };
    if !enabled {
        return None;
    }
    let url = match url {
        Some(Ok(u)) => u,
        Some(Err(())) => return None,
        None => {
            errors.push(err(
                MISSED_COLLECTION,
                "webhook_url is required when enabled is true",
            ));
            return None;
        }
    };
    Some(rule::builtin(
        interval?,
        Target {
            url,
            headers: headers?,
        },
    ))
}

/// 告警规则的表数组（REQ-AMB-ALR-RULES）。以它为前缀的环境变量不在 [`KEYS`] 中，按不识别的配置键报错。
const ALERT_RULES: &str = "alerts.rules";

/// 配置文件中索引提示表的名字（REQ-AMB-DM-INDEX-HINTS）。以它为前缀的环境变量不在 [`KEYS`] 中，按不识别的配置键报错。
const INDEX_HINTS: &str = "index_hints";

/// 校验索引提示：须为表；每个键为 `default/<stream>`（恰一个 `/`、不作百分号解码，`<stream>` 合 REQ-AMB-DM-NAMESPACE 的正则）；
/// 每个值为字符串数组，元素非空且互不相等（可以为空数组）。错误逐键记下，指出 `index_hints` 与出错的键。
fn parse_index_hints(
    value: &toml::Value,
    errors: &mut Vec<ConfigError>,
) -> BTreeMap<String, Vec<String>> {
    let mut hints = BTreeMap::new();
    let Some(table) = value.as_table() else {
        errors.push(err(
            INDEX_HINTS,
            "expected a TOML table of dataset names to arrays of column names",
        ));
        return hints;
    };
    for (dataset, columns) in table {
        let subject = format!("{INDEX_HINTS}.\"{dataset}\"");
        let valid_name = dataset.split_once('/').is_some_and(|(org, stream)| {
            org == amberlog_common::dataset::DEFAULT_ORG
                && amberlog_common::dataset::is_valid_stream_name(stream)
        });
        if !valid_name {
            errors.push(err(
                subject,
                "expected a dataset name default/<stream> with <stream> matching ^[a-z0-9][a-z0-9_]{0,63}$",
            ));
            continue;
        }
        let names: Option<Vec<String>> = columns.as_array().and_then(|items| {
            items
                .iter()
                .map(|i| i.as_str().filter(|s| !s.is_empty()).map(str::to_owned))
                .collect()
        });
        match names {
            Some(names)
                if names
                    .iter()
                    .enumerate()
                    .all(|(i, n)| !names[..i].contains(n)) =>
            {
                hints.insert(dataset.clone(), names);
            }
            _ => errors.push(err(
                subject,
                "expected an array of distinct non-empty column-name strings",
            )),
        }
    }
    hints
}

fn read_file(path: &Path) -> Result<toml::Table, ConfigError> {
    let shown = path.display().to_string();
    let text =
        std::fs::read(path).map_err(|e| err(&shown, format!("cannot read config file: {e}")))?;
    let text = String::from_utf8(text)
        .map_err(|_| err(&shown, "config file is not valid TOML (not UTF-8)"))?;
    text.parse::<toml::Table>().map_err(|e| {
        err(
            &shown,
            format!("config file is not valid TOML: {}", e.message()),
        )
    })
}

fn flatten(
    table: &toml::Table,
    prefix: &str,
    out: &mut BTreeMap<String, Raw>,
    errors: &mut Vec<ConfigError>,
) {
    for (name, value) in table {
        let key = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}.{name}")
        };
        match (kind_of(&key), value) {
            (None, toml::Value::Table(t)) if is_table(&key) => flatten(t, &key, out, errors),
            (None, _) => errors.push(err(&key, "unknown configuration key")),
            (Some(Kind::Bool), toml::Value::Boolean(b)) => {
                out.insert(key, Raw::Bool(*b));
            }
            (Some(Kind::Int), toml::Value::Integer(i)) => {
                out.insert(key, Raw::Int(*i));
            }
            (Some(Kind::Str), toml::Value::String(s)) => {
                out.insert(key, Raw::Str(s.clone()));
            }
            (Some(kind), _) => {
                errors.push(err(&key, format!("expected a TOML {}", kind_name(kind))))
            }
        }
    }
}

fn kind_name(kind: Kind) -> &'static str {
    match kind {
        Kind::Bool => "boolean",
        Kind::Int => "integer",
        Kind::Str => "string",
    }
}

fn read_env(
    env: impl IntoIterator<Item = (OsString, OsString)>,
    out: &mut BTreeMap<String, Raw>,
    errors: &mut Vec<ConfigError>,
) {
    for (name, value) in env {
        let name = name.to_string_lossy().into_owned();
        if !name.starts_with(ENV_PREFIX) {
            continue;
        }
        let Some((key, kind)) = KEYS.iter().find(|(k, _)| env_name(k) == name) else {
            errors.push(err(
                &name,
                "unknown configuration key (environment variable)",
            ));
            continue;
        };
        let subject = format!("{name} ({key})");
        let Some(value) = value.to_str() else {
            errors.push(err(subject, "value is not valid UTF-8"));
            continue;
        };
        let raw = match kind {
            Kind::Str => Raw::Str(value.to_owned()),
            Kind::Bool => match value {
                "true" => Raw::Bool(true),
                "false" => Raw::Bool(false),
                _ => {
                    errors.push(err(subject, "expected `true` or `false`"));
                    continue;
                }
            },
            Kind::Int => match parse_env_int(value) {
                Some(i) => Raw::Int(i),
                None => {
                    errors.push(err(subject, "expected a decimal 64-bit signed integer"));
                    continue;
                }
            },
        };
        out.insert((*key).to_owned(), raw);
    }
}

/// 可带前导 `-` 的十进制整数，落在 i64 范围内；`+`、`_`、小数点与指数一律不收。
fn parse_env_int(s: &str) -> Option<i64> {
    let digits = s.strip_prefix('-').unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

struct Resolver<'a> {
    form: Form,
    values: &'a BTreeMap<String, Raw>,
    errors: &'a mut Vec<ConfigError>,
}

impl Resolver<'_> {
    fn fail(&mut self, key: &str, message: impl Into<String>) {
        self.errors.push(err(key, message));
    }

    fn str_opt(&self, key: &str) -> Option<&str> {
        match self.values.get(key) {
            Some(Raw::Str(s)) => Some(s),
            _ => None,
        }
    }

    fn str_or<'s>(&'s self, key: &str, default: &'s str) -> &'s str {
        self.str_opt(key).unwrap_or(default)
    }

    fn bool_or(&self, key: &str, default: bool) -> bool {
        match self.values.get(key) {
            Some(Raw::Bool(b)) => *b,
            _ => default,
        }
    }

    fn int_or(&self, key: &str, default: i64) -> i64 {
        match self.values.get(key) {
            Some(Raw::Int(i)) => *i,
            _ => default,
        }
    }

    /// 整数配置项，须不小于 `min`。
    fn int_at_least(&mut self, key: &str, default: i64, min: i64) -> Option<u64> {
        let v = self.int_or(key, default);
        if v < min {
            self.fail(key, format!("must be at least {min}"));
            return None;
        }
        Some(v as u64)
    }

    fn duration(&mut self, key: &str, text: &str) -> Option<Duration> {
        match parse_duration(text) {
            Some(d) => Some(d),
            None => {
                self.fail(
                    key,
                    "expected <digits><ms|s|m|h|d> not exceeding 2^63-1 microseconds",
                );
                None
            }
        }
    }

    fn duration_or(&mut self, key: &str, default: &str) -> Option<Duration> {
        let text = self.str_or(key, default).to_owned();
        self.duration(key, &text)
    }

    /// 时长类配置项，须大于 0。
    fn positive_duration_or(&mut self, key: &str, default: &str) -> Option<Duration> {
        match self.duration_or(key, default) {
            Some(d) if d.is_zero() => {
                self.fail(key, "must be greater than 0");
                None
            }
            other => other,
        }
    }

    /// 时长类配置项，换算为微秒后是 60 秒的整数倍，且不小于 `1m`、不大于 `24h`（REQ-AMB-CFG-STARTUP-V8）。
    fn whole_minutes_or(&mut self, key: &str, default: &str) -> Option<Duration> {
        let d = self.duration_or(key, default)?;
        let ok = d.as_micros() % 60_000_000 == 0
            && d >= Duration::from_secs(60)
            && d <= Duration::from_secs(24 * 3600);
        if !ok {
            self.fail(
                key,
                "must be a whole number of minutes, at least 1m and at most 24h",
            );
            return None;
        }
        Some(d)
    }

    fn resolve(mut self, cwd: &Path) -> Option<Config> {
        let http_addr_text = self.str_or("server.http_addr", "0.0.0.0:8800").to_owned();
        let http_addr = {
            let text = http_addr_text.clone();
            let parsed = parse_http_addr(&text);
            if parsed.is_none() {
                self.fail(
                    "server.http_addr",
                    "expected <IPv4 address>:<port> or [<IPv6 address>]:<port>",
                );
            }
            parsed
        };
        let shutdown_timeout = self.duration_or("server.shutdown_timeout", "10s");
        let (tls, behind_tls_proxy) = self.tls(cwd);
        let ca_file = self
            .path_opt("object_store.s3.ca_file", cwd)
            .map(|(text, path)| CaFile { text, path });

        let storage_prefix = {
            let p = self.str_or("storage.prefix", "amberlog").to_owned();
            if valid_prefix(&p) {
                Some(p)
            } else {
                self.fail(
                    "storage.prefix",
                    "must match ^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$",
                );
                None
            }
        };
        let commit_max_retries = self.int_at_least("storage.commit_max_retries", 8, 0);

        let (object_store_url, location) = match self.str_opt("object_store.url") {
            None => {
                self.fail("object_store.url", "required configuration key is missing");
                (None, None)
            }
            Some(url) => {
                let url = url.to_owned();
                match parse_object_store_url(&url) {
                    Ok(loc) => (Some(url), Some(loc)),
                    Err(why) => {
                        self.fail("object_store.url", why);
                        (None, None)
                    }
                }
            }
        };
        let s3 = self.s3_options(matches!(location, Some(Location::S3 { .. })));

        let wal_dir = match self.str_opt("wal.dir") {
            None | Some("") if self.form.requires_wal_dir() => {
                self.fail("wal.dir", "required configuration key is missing");
                None
            }
            None | Some("") => Some(None),
            Some(dir) => Some(Some(cwd.join(dir))),
        };

        let auth = {
            let enabled = self.bool_or("auth.enabled", true);
            let token = self
                .str_opt("auth.token")
                .filter(|t| !t.is_empty())
                .map(str::to_owned);
            if let Some(t) = &token
                && !valid_token(t)
            {
                self.fail("auth.token", "must match the RFC 6750 b64token syntax");
            }
            match (enabled, token) {
                (true, Some(token)) => Some(Some(Auth::Enabled { token })),
                (true, None) if self.form.requires_auth_token() => {
                    self.fail(
                        "auth.token",
                        "required configuration key is missing (auth.enabled is true)",
                    );
                    None
                }
                (true, None) => Some(None),
                (false, _) => Some(Some(Auth::Disabled)),
            }
        };

        let max_body_bytes = self.int_at_least("ingest.max_body_bytes", 33_554_432, 1);
        let max_future = self.duration_or("ingest.max_future", "24h");
        let max_past = match self.str_opt("ingest.max_past").map(str::to_owned) {
            None => Some(None),
            Some(text) => self.duration("ingest.max_past", &text).map(Some),
        };
        let max_columns = self.int_at_least("ingest.max_columns", 1000, 1);
        let flush_interval = self.positive_duration_or("flush.interval", "5s");
        let flush_max_records = self.int_at_least("flush.max_records", 100_000, 1);
        let max_backlog_memory_bytes =
            self.int_at_least("ingest.max_backlog_memory_bytes", 268_435_456, 1);
        let wal_alert_bytes = self.int_at_least("wal.alert_bytes", 8_589_934_592, 1);
        let query_default_size = self.int_at_least("query.default_size", 100, 0);
        let query_max_size = self.int_at_least("query.max_size", 10_000, 1);
        let query_max_concurrency = self.int_at_least("query.max_concurrency", 4, 1);
        let query_max_memory_bytes = self.int_at_least("query.max_memory_bytes", 536_870_912, 1);
        if let (Some(d), Some(m)) = (query_default_size, query_max_size)
            && d > m
        {
            self.errors
                .push(err("query.default_size", "must not exceed query.max_size"));
        }

        let log_level = match self.str_or("log.level", "info") {
            "error" => Some(Level::Error),
            "warn" => Some(Level::Warn),
            "info" => Some(Level::Info),
            "debug" => Some(Level::Debug),
            "trace" => Some(Level::Trace),
            _ => {
                self.fail(
                    "log.level",
                    "expected one of error, warn, info, debug, trace",
                );
                None
            }
        };
        let log_format = match self.str_or("log.format", "json") {
            "json" => Some(Format::Json),
            "text" => Some(Format::Text),
            _ => {
                self.fail("log.format", "expected json or text");
                None
            }
        };

        let seal_grace = self.positive_duration_or("lifecycle.seal_grace", "10m");
        let lake_watermark = self.positive_duration_or("lifecycle.lake_watermark", "72h");
        if let (Some(g), Some(w)) = (seal_grace, lake_watermark)
            && w <= g
        {
            self.fail(
                "lifecycle.lake_watermark",
                "must be greater than lifecycle.seal_grace",
            );
        }
        let lake_alert_after = self.positive_duration_or("lifecycle.lake_alert_after", "24h");
        let delete_delay = self.positive_duration_or("storage.delete_delay", "1h");
        let compaction_target_file_bytes =
            self.int_at_least("compaction.target_file_bytes", 134_217_728, 1);
        let hot_fsck_interval = self.positive_duration_or("hot_fsck.interval", "1h");
        let hot_fsck_min_orphan_age = self.positive_duration_or("hot_fsck.min_orphan_age", "1h");
        let hot_retention_snapshots = self.int_at_least("hot_retention.snapshots", 1000, 2);
        let hot_retention_age = self.positive_duration_or("hot_retention.age", "168h");
        let retention_period = match self.str_opt("retention.period").map(str::to_owned) {
            None => Some(None),
            Some(text) => match self.duration("retention.period", &text) {
                Some(d) if d.is_zero() || d > RETENTION_PERIOD_MAX => {
                    self.fail(
                        "retention.period",
                        "must be greater than 0 and not exceed 36500d",
                    );
                    None
                }
                other => other.map(|period| Some(RetentionPeriod { text, period })),
            },
        };
        if let (Some(Some(r)), Some(w)) = (&retention_period, lake_watermark)
            && r.period <= w
        {
            self.fail(
                "retention.period",
                "must be greater than lifecycle.lake_watermark",
            );
        }
        let retention_check_interval = self.positive_duration_or("retention.check_interval", "1h");
        let cold_compaction_check_interval =
            self.positive_duration_or("cold_compaction.check_interval", "24h");
        let cold_retention_snapshots = self.int_at_least("cold_retention.snapshots", 1, 1);
        let cold_retention_age = self.positive_duration_or("cold_retention.age", "168h");
        let cold_retention_check_interval =
            self.positive_duration_or("cold_retention.check_interval", "24h");
        let alerts_max_concurrency = self.int_at_least("alerts.max_concurrency", 1, 1);
        let history_retention = match self.duration_or("alerts.history_retention", "30d") {
            Some(d) if !(HISTORY_RETENTION_MIN..=RETENTION_PERIOD_MAX).contains(&d) => {
                self.fail(
                    "alerts.history_retention",
                    "must be at least 1h and not exceed 36500d",
                );
                None
            }
            other => other,
        };
        let alerts_ca_file = self
            .path_opt("alerts.ca_file", cwd)
            .map(|(text, path)| CaFile { text, path });
        let reconciliation_grace = self.whole_minutes_or("reconciliation.grace", "5m");

        Some(Config {
            http_addr: http_addr?,
            http_addr_text,
            shutdown_timeout: shutdown_timeout?,
            tls: tls?,
            behind_tls_proxy,
            ca_file,
            storage_prefix: storage_prefix?,
            commit_max_retries: commit_max_retries?,
            delete_delay: delete_delay?,
            object_store_url: object_store_url?,
            location: location?,
            s3: s3?,
            wal_dir: wal_dir?,
            auth: auth?,
            max_body_bytes: max_body_bytes?,
            max_future: max_future?,
            max_past: max_past?,
            flush_interval: flush_interval?,
            flush_max_records: flush_max_records?,
            max_columns: max_columns?,
            max_backlog_memory_bytes: max_backlog_memory_bytes?,
            wal_alert_bytes: wal_alert_bytes?,
            query_default_size: query_default_size?,
            query_max_size: query_max_size?,
            query_max_concurrency: query_max_concurrency?,
            query_max_memory_bytes: query_max_memory_bytes?,
            log_level: log_level?,
            log_format: log_format?,
            seal_grace: seal_grace?,
            lake_watermark: lake_watermark?,
            lake_alert_after: lake_alert_after?,
            compaction_target_file_bytes: compaction_target_file_bytes?,
            hot_fsck_interval: hot_fsck_interval?,
            hot_fsck_min_orphan_age: hot_fsck_min_orphan_age?,
            hot_retention_snapshots: hot_retention_snapshots?,
            hot_retention_age: hot_retention_age?,
            retention_period: retention_period?,
            retention_check_interval: retention_check_interval?,
            cold_compaction_check_interval: cold_compaction_check_interval?,
            cold_retention_snapshots: cold_retention_snapshots?,
            cold_retention_age: cold_retention_age?,
            cold_retention_check_interval: cold_retention_check_interval?,
            reconciliation_grace: reconciliation_grace?,
            index_hints: BTreeMap::new(),
            alerts: Alerts {
                rules: Vec::new(),
                max_concurrency: alerts_max_concurrency?,
                history_retention: history_retention?,
                ca_file: alerts_ca_file,
                ca_pem: None,
            },
        })
    }

    /// 路径类配置项：空字符串视同缺失，相对路径按工作目录解析（REQ-AMB-CFG-STARTUP-V7）。返回配置取值原文与解析后的路径。
    fn path_opt(&self, key: &str, cwd: &Path) -> Option<(String, PathBuf)> {
        self.str_opt(key)
            .filter(|p| !p.is_empty())
            .map(|p| (p.to_owned(), cwd.join(p)))
    }

    /// 入站 TLS 的两个文件与明文声明：证书与私钥成对；证书与明文声明不得同时配置（REQ-AMB-CFG-STARTUP-V7）。
    fn tls(&mut self, cwd: &Path) -> (Option<Option<TlsFiles>>, bool) {
        let behind_tls_proxy = self.bool_or("server.behind_tls_proxy", false);
        let cert = self.path_opt("server.tls.cert_file", cwd);
        let key = self.path_opt("server.tls.key_file", cwd);
        let files = match (cert, key) {
            (None, None) => Some(None),
            (Some(_), None) => {
                self.fail(
                    "server.tls.key_file",
                    "must be set together with server.tls.cert_file",
                );
                None
            }
            (None, Some(_)) => {
                self.fail(
                    "server.tls.cert_file",
                    "must be set together with server.tls.key_file",
                );
                None
            }
            (Some((cert_file, cert_path)), Some((key_file, key_path))) => Some(Some(TlsFiles {
                cert_file,
                key_file,
                cert_path,
                key_path,
            })),
        };
        if behind_tls_proxy
            && self
                .str_opt("server.tls.cert_file")
                .is_some_and(|p| !p.is_empty())
        {
            self.fail(
                "server.tls.cert_file",
                "must not be set when server.behind_tls_proxy is true",
            );
            return (None, behind_tls_proxy);
        }
        (files, behind_tls_proxy)
    }

    fn s3_options(&mut self, is_s3: bool) -> Option<S3Options> {
        let mut ok = true;
        match self.str_opt("object_store.s3.conditional_put") {
            // `etag` 是 `create_only` 的同义取值，保留以使既有配置照常可用
            Some("create_only" | "etag") => {}
            Some(_) => {
                self.fail(
                    "object_store.s3.conditional_put",
                    "expected create_only or etag",
                );
                ok = false;
            }
            None if is_s3 && self.form.requires_conditional_put() => {
                self.fail(
                    "object_store.s3.conditional_put",
                    "required configuration key is missing (object_store.url is s3://)",
                );
                ok = false;
            }
            None => {}
        }
        let endpoint = self.str_opt("object_store.s3.endpoint").map(str::to_owned);
        if let Some(e) = &endpoint
            && !valid_endpoint(e)
        {
            self.fail(
                "object_store.s3.endpoint",
                "expected an absolute URL starting with http:// or https://",
            );
            ok = false;
        }
        let region = self
            .str_or("object_store.s3.region", "us-east-1")
            .to_owned();
        if region.is_empty() {
            self.fail("object_store.s3.region", "must not be empty");
            ok = false;
        }
        let access_key_id = self
            .str_opt("object_store.s3.access_key_id")
            .map(str::to_owned);
        let secret_access_key = self
            .str_opt("object_store.s3.secret_access_key")
            .map(str::to_owned);
        match (&access_key_id, &secret_access_key) {
            (Some(_), None) => {
                self.fail(
                    "object_store.s3.secret_access_key",
                    "must be set together with object_store.s3.access_key_id",
                );
                ok = false;
            }
            (None, Some(_)) => {
                self.fail(
                    "object_store.s3.access_key_id",
                    "must be set together with object_store.s3.secret_access_key",
                );
                ok = false;
            }
            _ => {}
        }
        let force_path_style = self.bool_or("object_store.s3.force_path_style", false);
        let kms_key_id = self
            .str_opt("object_store.s3.sse_kms_key_id")
            .map(str::to_owned);
        if kms_key_id.as_deref() == Some("") {
            self.fail("object_store.s3.sse_kms_key_id", "must not be empty");
            ok = false;
        }
        let server_side_encryption = match self.str_opt("object_store.s3.server_side_encryption") {
            None => None,
            Some("sse_s3") => Some(ServerSideEncryption::SseS3),
            Some("sse_kms") => Some(ServerSideEncryption::SseKms {
                key_id: kms_key_id.clone().filter(|k| !k.is_empty()),
            }),
            Some(_) => {
                self.fail(
                    "object_store.s3.server_side_encryption",
                    "expected sse_s3 or sse_kms",
                );
                ok = false;
                None
            }
        };
        if kms_key_id.is_some()
            && !matches!(
                server_side_encryption,
                Some(ServerSideEncryption::SseKms { .. })
            )
        {
            self.fail(
                "object_store.s3.sse_kms_key_id",
                "may only be set when object_store.s3.server_side_encryption is sse_kms",
            );
            ok = false;
        }
        ok.then_some(S3Options {
            endpoint,
            region,
            access_key_id,
            secret_access_key,
            force_path_style,
            server_side_encryption,
            ca_pem: None,
        })
    }
}

/// `<整数><ms|s|m|h|d>`，整数只含十进制数字；换算为微秒不超过 2^63−1。
pub fn parse_duration(text: &str) -> Option<Duration> {
    let split = text.find(|c: char| !c.is_ascii_digit())?;
    let (digits, unit) = text.split_at(split);
    if digits.is_empty() {
        return None;
    }
    let per_unit: i64 = match unit {
        "ms" => 1_000,
        "s" => 1_000_000,
        "m" => 60_000_000,
        "h" => 3_600_000_000,
        "d" => 86_400_000_000,
        _ => return None,
    };
    let n: i64 = digits.parse().ok()?;
    let micros = n.checked_mul(per_unit)?;
    Some(Duration::from_micros(micros as u64))
}

/// `<IPv4>:<port>` 或 `[<IPv6>]:<port>`；不收主机名、IPv6 区域标识与带符号的端口。
fn parse_http_addr(text: &str) -> Option<SocketAddr> {
    let (host, port) = text.rsplit_once(':')?;
    if port.is_empty() || !port.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let port: u16 = port.parse().ok()?;
    if let Some(inner) = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
        let ip: Ipv6Addr = inner.parse().ok()?;
        Some(SocketAddr::V6(SocketAddrV6::new(ip, port, 0, 0)))
    } else {
        let ip: Ipv4Addr = host.parse().ok()?;
        Some(SocketAddr::from((ip, port)))
    }
}

fn valid_prefix(p: &str) -> bool {
    let b = p.as_bytes();
    !b.is_empty()
        && b.len() <= 64
        && b[0].is_ascii_alphanumeric()
        && b[1..]
            .iter()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b'-'))
}

/// RFC 6750 §2.1 `b64token`：`^[A-Za-z0-9._~+/-]+=*$`
fn valid_token(t: &str) -> bool {
    let body = t.trim_end_matches('=');
    !body.is_empty()
        && body.bytes().all(|c| {
            c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b'~' | b'+' | b'/' | b'-')
        })
}

fn valid_endpoint(e: &str) -> bool {
    let rest = e
        .strip_prefix("http://")
        .or_else(|| e.strip_prefix("https://"));
    matches!(rest, Some(r) if !r.is_empty() && !r.starts_with('/'))
}

fn is_unreserved(c: u8) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, b'-' | b'.' | b'_' | b'~')
}

fn is_sub_delim(c: u8) -> bool {
    matches!(
        c,
        b'!' | b'$' | b'&' | b'\'' | b'(' | b')' | b'*' | b'+' | b',' | b';' | b'='
    )
}

/// RFC 3986 `pchar` 中除百分号编码之外的字符。
fn is_pchar(c: u8) -> bool {
    is_unreserved(c) || is_sub_delim(c) || matches!(c, b':' | b'@')
}

/// 解析 `object_store.url`。先按块的各条自行校验，不交给会自动改写的 URL 库
/// （方案名转小写、消去点段、给空格补百分号编码都会掩盖本应报错的写法）。
fn parse_object_store_url(url: &str) -> Result<Location, String> {
    const SHAPE: &str = "expected file:///<absolute path> or s3://<bucket>[/<path>] (RFC 3986)";
    if url.contains(['?', '#']) {
        return Err(format!("{SHAPE}; query and fragment are not allowed"));
    }
    let (scheme, rest) = url.split_once("://").ok_or_else(|| SHAPE.to_owned())?;
    let (authority, path) = match rest.find('/') {
        Some(i) => rest.split_at(i),
        None => (rest, ""),
    };
    match scheme {
        "file" => {
            if !authority.is_empty() {
                return Err(format!("{SHAPE}; file:// must not have a host"));
            }
            let segments = decode_path(path)?;
            let mut dir = PathBuf::from("/");
            dir.extend(segments);
            Ok(Location::File { dir })
        }
        "s3" => {
            if authority.contains('@') {
                return Err(format!("{SHAPE}; userinfo is not allowed"));
            }
            if authority.is_empty()
                || !authority.bytes().all(is_unreserved)
                || authority == "."
                || authority == ".."
            {
                return Err(format!(
                    "{SHAPE}; bucket name must consist of ASCII letters, digits, '-', '.', '_', '~' and must not be '.' or '..'"
                ));
            }
            let segments = if path.is_empty() {
                Vec::new()
            } else {
                decode_path(path)?
            };
            Ok(Location::S3 {
                bucket: authority.to_owned(),
                path: segments,
            })
        }
        _ => Err(format!("{SHAPE}; unsupported scheme")),
    }
}

/// 校验并逐段解码绝对路径（以 `/` 开头）；末尾单个 `/` 不改变含义。
fn decode_path(path: &str) -> Result<Vec<String>, String> {
    let body = path.strip_prefix('/').ok_or("path must be absolute")?;
    let body = body.strip_suffix('/').unwrap_or(body);
    if body.is_empty() {
        return Ok(Vec::new());
    }
    body.split('/').map(decode_segment).collect()
}

fn decode_segment(seg: &str) -> Result<String, String> {
    if seg.is_empty() {
        return Err("path must not contain two adjacent '/'".into());
    }
    let bytes = seg.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'%' {
            let hex = bytes
                .get(i + 1..i + 3)
                .and_then(|h| std::str::from_utf8(h).ok())
                .and_then(|h| u8::from_str_radix(h, 16).ok())
                .ok_or("invalid percent-encoding in path")?;
            out.push(hex);
            i += 3;
        } else if is_pchar(c) {
            out.push(c);
            i += 1;
        } else {
            return Err("path contains a character that must be percent-encoded (RFC 3986)".into());
        }
    }
    let s = String::from_utf8(out).map_err(|_| "path does not decode to valid UTF-8")?;
    if s.contains(['/', '\0']) {
        return Err("a path segment decodes to '/' or NUL".into());
    }
    if s == "." || s == ".." {
        return Err("path must not contain '.' or '..' segments".into());
    }
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> Vec<(OsString, OsString)> {
        pairs
            .iter()
            .map(|(k, v)| (OsString::from(k), OsString::from(v)))
            .collect()
    }

    fn base_env() -> Vec<(OsString, OsString)> {
        env(&[
            ("AMBERLOG_OBJECT_STORE__URL", "file:///data"),
            ("AMBERLOG_WAL__DIR", "/wal"),
            ("AMBERLOG_AUTH__TOKEN", "secret"),
        ])
    }

    fn load_env(extra: &[(&str, &str)]) -> Result<Config, Vec<ConfigError>> {
        let mut e = base_env();
        e.extend(env(extra));
        load(Form::Serve, None, e, Path::new("/cwd"))
    }

    fn subjects(r: Result<Config, Vec<ConfigError>>) -> Vec<String> {
        r.unwrap_err().into_iter().map(|e| e.subject).collect()
    }

    #[test]
    fn debug_hides_secrets() {
        let c = load_env(&[
            ("AMBERLOG_AUTH__TOKEN", "tok-Very-Secret"),
            ("AMBERLOG_OBJECT_STORE__URL", "s3://bucket"),
            ("AMBERLOG_OBJECT_STORE__S3__CONDITIONAL_PUT", "etag"),
            ("AMBERLOG_OBJECT_STORE__S3__ACCESS_KEY_ID", "AKIDPLAIN"),
            (
                "AMBERLOG_OBJECT_STORE__S3__SECRET_ACCESS_KEY",
                "sk-Very-Secret",
            ),
        ])
        .unwrap();
        let out = format!("{c:?}");
        assert!(!out.contains("Very-Secret"), "{out}");
        assert!(out.contains("token: ***"), "{out}");
        assert!(out.contains("secret_access_key: Some(***)"), "{out}");
        assert_eq!(format!("{:?}", Auth::Disabled), "Disabled");
    }

    #[test]
    fn defaults() {
        let c = load_env(&[]).unwrap();
        assert_eq!(c.http_addr, "0.0.0.0:8800".parse().unwrap());
        assert_eq!(c.shutdown_timeout, Duration::from_secs(10));
        assert_eq!(c.storage_prefix, "amberlog");
        assert_eq!(c.commit_max_retries, 8);
        assert_eq!(c.max_body_bytes, 33_554_432);
        assert_eq!(c.max_future, Duration::from_secs(86_400));
        assert_eq!(c.max_past, None);
        assert_eq!(c.flush_interval, Duration::from_secs(5));
        assert_eq!(c.flush_max_records, 100_000);
        assert_eq!(c.max_columns, 1000);
        assert_eq!(c.max_backlog_memory_bytes, 268_435_456);
        assert_eq!(c.wal_alert_bytes, 8_589_934_592);
        assert_eq!((c.query_default_size, c.query_max_size), (100, 10_000));
        assert_eq!(
            (c.query_max_concurrency, c.query_max_memory_bytes),
            (4, 536_870_912)
        );
        assert_eq!(c.seal_grace, Duration::from_secs(600));
        assert_eq!(c.lake_watermark, Duration::from_secs(72 * 3600));
        assert_eq!(c.compaction_target_file_bytes, 134_217_728);
        assert_eq!(c.hot_fsck_interval, Duration::from_secs(3600));
        assert_eq!(c.hot_fsck_min_orphan_age, Duration::from_secs(3600));
        assert_eq!(c.hot_retention_snapshots, 1000);
        assert_eq!(c.hot_retention_age, Duration::from_secs(168 * 3600));
        assert_eq!(c.retention_period, None);
        assert_eq!(c.delete_delay, Duration::from_secs(3600));
        assert_eq!(c.lake_alert_after, Duration::from_secs(24 * 3600));
        assert_eq!(c.retention_check_interval, Duration::from_secs(3600));
        assert_eq!(
            c.cold_compaction_check_interval,
            Duration::from_secs(24 * 3600)
        );
        assert_eq!(c.cold_retention_snapshots, 1);
        assert_eq!(c.cold_retention_age, Duration::from_secs(168 * 3600));
        assert_eq!(
            c.cold_retention_check_interval,
            Duration::from_secs(24 * 3600)
        );
        assert_eq!(c.reconciliation_grace, Duration::from_secs(300));
        assert_eq!((c.log_level, c.log_format), (Level::Info, Format::Json));
        assert_eq!(c.s3.region, "us-east-1");
        assert!(!c.s3.force_path_style);
        assert_eq!(
            c.auth,
            Some(Auth::Enabled {
                token: "secret".into()
            })
        );
    }

    #[test]
    fn env_overrides_file_and_relative_wal_dir() {
        let dir = std::env::temp_dir().join(format!("amberlog-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("a.toml");
        std::fs::write(
            &file,
            "[server]\nhttp_addr = \"127.0.0.1:1\"\n[wal]\ndir = \"w\"\n[object_store]\nurl = \"file:///d\"\n[auth]\nenabled = false\n",
        )
        .unwrap();
        let c = load(
            Form::Serve,
            Some(&file),
            env(&[("AMBERLOG_SERVER__HTTP_ADDR", "127.0.0.1:2")]),
            Path::new("/cwd"),
        )
        .unwrap();
        assert_eq!(c.http_addr, "127.0.0.1:2".parse().unwrap());
        assert_eq!(c.wal_dir, Some(PathBuf::from("/cwd/w")));
        assert_eq!(c.auth, Some(Auth::Disabled));
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn env_value_forms() {
        for bad in ["True", "TRUE", "1", "0", "yes", "on", ""] {
            assert_eq!(
                subjects(load_env(&[("AMBERLOG_AUTH__ENABLED", bad)])),
                vec!["AMBERLOG_AUTH__ENABLED (auth.enabled)"],
                "{bad}"
            );
        }
        for bad in ["1_000", "+5", "1e3", "9223372036854775808", "1.0", ""] {
            assert!(
                load_env(&[("AMBERLOG_INGEST__MAX_BODY_BYTES", bad)]).is_err(),
                "{bad}"
            );
        }
        assert!(load_env(&[("AMBERLOG_INGEST__MAX_BODY_BYTES", "-5")]).is_err());
        assert_eq!(
            load_env(&[("AMBERLOG_INGEST__MAX_BODY_BYTES", "1024")])
                .unwrap()
                .max_body_bytes,
            1024
        );
    }

    #[test]
    fn env_names_must_match_exactly() {
        for name in [
            "AMBERLOG_SERVER_HTTP_ADDR",
            "AMBERLOG_server__http_addr",
            "AMBERLOG_SERVER__HTTP_ADDRS",
        ] {
            assert_eq!(subjects(load_env(&[(name, "127.0.0.1:1")])), vec![name]);
        }
    }

    #[test]
    fn empty_env_counts_as_set() {
        assert_eq!(
            subjects(load_env(&[("AMBERLOG_AUTH__TOKEN", "")])),
            vec!["auth.token"]
        );
        assert_eq!(
            subjects(load_env(&[("AMBERLOG_WAL__DIR", "")])),
            vec!["wal.dir"]
        );
    }

    /// `reconciliation.grace`：整分钟，`1m`～`24h`（REQ-AMB-CFG-STARTUP-V8）。
    #[test]
    fn reconciliation_grace_whole_minutes() {
        let grace = |v: &str| load_env(&[("AMBERLOG_RECONCILIATION__GRACE", v)]);
        assert_eq!(
            grace("60s").unwrap().reconciliation_grace,
            Duration::from_secs(60)
        );
        assert_eq!(
            grace("120000ms").unwrap().reconciliation_grace,
            Duration::from_secs(120)
        );
        assert_eq!(
            grace("24h").unwrap().reconciliation_grace,
            Duration::from_secs(86_400)
        );
        for bad in ["90s", "30s", "0s", "25h", "1441m", "5"] {
            assert_eq!(subjects(grace(bad)), ["reconciliation.grace"], "{bad}");
        }
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("150ms"), Some(Duration::from_millis(150)));
        assert_eq!(parse_duration("0s"), Some(Duration::ZERO));
        assert_eq!(parse_duration("2d"), Some(Duration::from_secs(172_800)));
        for bad in [
            "5",
            "-5s",
            "1.5s",
            "5 s",
            "1_0s",
            "5sec",
            "106751992d",
            "s",
            "+5s",
            "",
        ] {
            assert_eq!(parse_duration(bad), None, "{bad}");
        }
        assert!(parse_duration("106751991d").is_some());
    }

    #[test]
    fn http_addr_forms() {
        assert!(parse_http_addr("127.0.0.1:0").is_some());
        assert!(parse_http_addr("[::1]:8800").is_some());
        for bad in [
            "0.0.0.0",
            "0.0.0.0:65536",
            "[fe80::1%eth0]:8800",
            "[fe80::1%3]:8800",
            "localhost:8800",
            "0.0.0.0:+80",
            "::1:8800",
        ] {
            assert!(parse_http_addr(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn prefix_and_token_forms() {
        assert!(
            valid_prefix("amberlog") && valid_prefix("a.b_c-1") && valid_prefix(&"a".repeat(64))
        );
        for bad in ["..", "_x", "a/b", "", &"a".repeat(65)] {
            assert!(!valid_prefix(bad), "{bad}");
        }
        assert!(valid_token("abc") && valid_token("a.b~c+d/e-f=="));
        for bad in ["abc def", "abc=def", "=", "a\u{e9}"] {
            assert!(!valid_token(bad), "{bad}");
        }
    }

    #[test]
    fn object_store_url_forms() {
        let file = |p: &str| {
            Ok(Location::File {
                dir: PathBuf::from(p),
            })
        };
        assert_eq!(parse_object_store_url("file:///data"), file("/data"));
        assert_eq!(parse_object_store_url("file:///data/"), file("/data"));
        assert_eq!(parse_object_store_url("file:///data%20x"), file("/data x"));
        assert_eq!(
            parse_object_store_url("s3://My_Bucket/a%20b/c/"),
            Ok(Location::S3 {
                bucket: "My_Bucket".into(),
                path: vec!["a b".into(), "c".into()]
            })
        );
        assert_eq!(
            parse_object_store_url("s3://logs"),
            Ok(Location::S3 {
                bucket: "logs".into(),
                path: vec![]
            })
        );
        for bad in [
            "file://host/data",
            "file:///data?x=1",
            "file:///data#f",
            "file:///a/../b",
            "file:///a/%2E%2E/b",
            "file:///a%2Fb",
            "file:///a%00b",
            "file:///%FF",
            "file:///data x",
            "file:///数据",
            "file:///a//b",
            "file:///a%2",
            "s3://user@bucket",
            "s3://my$bucket",
            "s3://../logs",
            "s3://./logs",
            "s3://logs/a//b",
            "S3://logs",
            "FILE:///data",
            "gs://bucket/p",
            "s3:/bucket",
            "s3://",
        ] {
            assert!(parse_object_store_url(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn unknown_and_mistyped_file_keys() {
        let mut errors = Vec::new();
        let mut out = BTreeMap::new();
        let t: toml::Table = "bogus = 1\n[ingest]\nmax_body_byte = 1\nmax_body_bytes = \"1024\"\n[wal.dir]\nx = 1\n[auth]\nenabled = \"true\"\n"
            .parse()
            .unwrap();
        flatten(&t, "", &mut out, &mut errors);
        let mut s: Vec<_> = errors.into_iter().map(|e| e.subject).collect();
        s.sort();
        assert_eq!(
            s,
            vec![
                "auth.enabled",
                "bogus",
                "ingest.max_body_byte",
                "ingest.max_body_bytes",
                "wal.dir"
            ]
        );
    }

    /// 加密的六个配置项（REQ-AMB-CFG-STARTUP-V7 ③）：纯配置约束。
    #[test]
    fn encryption_keys() {
        let c = load_env(&[]).unwrap();
        assert_eq!((c.tls, c.behind_tls_proxy, c.ca_file), (None, false, None));
        assert_eq!(c.s3.server_side_encryption, None);
        let c = load_env(&[
            ("AMBERLOG_SERVER__TLS__CERT_FILE", "c.pem"),
            ("AMBERLOG_SERVER__TLS__KEY_FILE", "/k.pem"),
            ("AMBERLOG_OBJECT_STORE__S3__CA_FILE", "ca.pem"),
            (
                "AMBERLOG_OBJECT_STORE__S3__SERVER_SIDE_ENCRYPTION",
                "sse_kms",
            ),
            ("AMBERLOG_OBJECT_STORE__S3__SSE_KMS_KEY_ID", "k1"),
        ])
        .unwrap();
        assert_eq!(
            c.tls,
            Some(TlsFiles {
                cert_file: "c.pem".into(),
                key_file: "/k.pem".into(),
                cert_path: PathBuf::from("/cwd/c.pem"),
                key_path: PathBuf::from("/k.pem"),
            })
        );
        assert_eq!(c.ca_file.unwrap().path, PathBuf::from("/cwd/ca.pem"));
        assert_eq!(
            c.s3.server_side_encryption,
            Some(ServerSideEncryption::SseKms {
                key_id: Some("k1".into())
            })
        );
        // 三个路径项的空字符串视同缺失
        let c = load_env(&[
            ("AMBERLOG_SERVER__TLS__CERT_FILE", ""),
            ("AMBERLOG_SERVER__TLS__KEY_FILE", ""),
            ("AMBERLOG_OBJECT_STORE__S3__CA_FILE", ""),
            ("AMBERLOG_SERVER__BEHIND_TLS_PROXY", "true"),
        ])
        .unwrap();
        assert_eq!((c.tls, c.behind_tls_proxy, c.ca_file), (None, true, None));
        assert_eq!(
            load_env(&[(
                "AMBERLOG_OBJECT_STORE__S3__SERVER_SIDE_ENCRYPTION",
                "sse_s3"
            )])
            .unwrap()
            .s3
            .server_side_encryption,
            Some(ServerSideEncryption::SseS3)
        );
        for (pairs, want) in [
            (
                &[("AMBERLOG_SERVER__TLS__CERT_FILE", "c")][..],
                vec!["server.tls.key_file"],
            ),
            (
                &[("AMBERLOG_SERVER__TLS__KEY_FILE", "k")][..],
                vec!["server.tls.cert_file"],
            ),
            (
                &[
                    ("AMBERLOG_SERVER__TLS__CERT_FILE", "c"),
                    ("AMBERLOG_SERVER__TLS__KEY_FILE", "k"),
                    ("AMBERLOG_SERVER__BEHIND_TLS_PROXY", "true"),
                ][..],
                vec!["server.tls.cert_file"],
            ),
            (
                &[(
                    "AMBERLOG_OBJECT_STORE__S3__SERVER_SIDE_ENCRYPTION",
                    "SSE_S3",
                )][..],
                vec!["object_store.s3.server_side_encryption"],
            ),
            (
                &[
                    (
                        "AMBERLOG_OBJECT_STORE__S3__SERVER_SIDE_ENCRYPTION",
                        "sse_s3",
                    ),
                    ("AMBERLOG_OBJECT_STORE__S3__SSE_KMS_KEY_ID", "k1"),
                ][..],
                vec!["object_store.s3.sse_kms_key_id"],
            ),
            (
                &[("AMBERLOG_OBJECT_STORE__S3__SSE_KMS_KEY_ID", "k1")][..],
                vec!["object_store.s3.sse_kms_key_id"],
            ),
            (
                &[
                    (
                        "AMBERLOG_OBJECT_STORE__S3__SERVER_SIDE_ENCRYPTION",
                        "sse_kms",
                    ),
                    ("AMBERLOG_OBJECT_STORE__S3__SSE_KMS_KEY_ID", ""),
                ][..],
                vec!["object_store.s3.sse_kms_key_id"],
            ),
            (
                &[("AMBERLOG_SERVER__BEHIND_TLS_PROXY", "yes")][..],
                vec!["AMBERLOG_SERVER__BEHIND_TLS_PROXY (server.behind_tls_proxy)"],
            ),
        ] {
            assert_eq!(subjects(load_env(pairs)), want, "{pairs:?}");
        }
        // 证书与明文声明同时配置：stderr 指出两个键名
        let errs = load_env(&[
            ("AMBERLOG_SERVER__TLS__CERT_FILE", "c"),
            ("AMBERLOG_SERVER__TLS__KEY_FILE", "k"),
            ("AMBERLOG_SERVER__BEHIND_TLS_PROXY", "true"),
        ])
        .unwrap_err();
        assert!(errs[0].to_string().contains("server.behind_tls_proxy"));
        let errs = load_env(&[
            (
                "AMBERLOG_OBJECT_STORE__S3__SERVER_SIDE_ENCRYPTION",
                "sse_s3",
            ),
            ("AMBERLOG_OBJECT_STORE__S3__SSE_KMS_KEY_ID", "k1"),
        ])
        .unwrap_err();
        assert!(
            errs[0]
                .to_string()
                .contains("object_store.s3.server_side_encryption")
        );
    }

    /// 必填项按形态分开（REQ-AMB-CFG-STARTUP-V7）；其余校验各形态相同。
    #[test]
    fn required_keys_by_form() {
        let only = |pairs: &[(&str, &str)], form| load(form, None, env(pairs), Path::new("/cwd"));
        let s3 = [("AMBERLOG_OBJECT_STORE__URL", "s3://bucket")];
        assert_eq!(
            subjects(only(&s3, Form::Serve)),
            vec!["object_store.s3.conditional_put", "wal.dir", "auth.token"]
        );
        assert_eq!(subjects(only(&s3, Form::Catalog)), vec!["auth.token"]);
        assert_eq!(
            subjects(only(&s3, Form::Retire)),
            vec!["object_store.s3.conditional_put", "wal.dir"]
        );
        let c = only(&s3, Form::Evidence).unwrap();
        assert_eq!((c.wal_dir, c.auth), (None, None));
        for form in [Form::Serve, Form::Catalog, Form::Retire, Form::Evidence] {
            assert_eq!(
                subjects(only(&[], form)),
                if form.requires_wal_dir() {
                    vec!["object_store.url", "wal.dir"]
                } else {
                    vec!["object_store.url"]
                }
                .into_iter()
                .chain(form.requires_auth_token().then_some("auth.token"))
                .collect::<Vec<_>>(),
                "{form:?}"
            );
            // 用不到的配置项照常校验
            assert_eq!(
                subjects(only(
                    &[
                        ("AMBERLOG_OBJECT_STORE__URL", "file:///d"),
                        ("AMBERLOG_AUTH__ENABLED", "false"),
                        ("AMBERLOG_WAL__DIR", "w"),
                        ("AMBERLOG_SERVER__HTTP_ADDR", "x")
                    ],
                    form
                )),
                vec!["server.http_addr"],
                "{form:?}"
            );
        }
        let c = only(
            &[
                ("AMBERLOG_OBJECT_STORE__URL", "file:///d"),
                ("AMBERLOG_AUTH__ENABLED", "false"),
                ("AMBERLOG_WAL__DIR", ""),
            ],
            Form::Catalog,
        )
        .unwrap();
        assert_eq!((c.wal_dir, c.auth), (None, Some(Auth::Disabled)));
    }

    #[test]
    fn required_and_cross_field_rules() {
        assert_eq!(
            subjects(load(
                Form::Serve,
                None,
                env(&[("AMBERLOG_AUTH__TOKEN", "t")]),
                Path::new("/")
            )),
            vec!["object_store.url", "wal.dir"]
        );
        assert_eq!(
            subjects(load_env(&[("AMBERLOG_OBJECT_STORE__URL", "s3://bucket")])),
            vec!["object_store.s3.conditional_put"]
        );
        assert_eq!(
            subjects(load_env(&[(
                "AMBERLOG_OBJECT_STORE__S3__ACCESS_KEY_ID",
                "k"
            )])),
            vec!["object_store.s3.secret_access_key"]
        );
        assert_eq!(
            subjects(load_env(&[
                ("AMBERLOG_QUERY__DEFAULT_SIZE", "200"),
                ("AMBERLOG_QUERY__MAX_SIZE", "100")
            ])),
            vec!["query.default_size"]
        );
        for (k, v) in [
            ("AMBERLOG_FLUSH__INTERVAL", "0s"),
            ("AMBERLOG_FLUSH__MAX_RECORDS", "0"),
            ("AMBERLOG_QUERY__MAX_SIZE", "0"),
            ("AMBERLOG_STORAGE__COMMIT_MAX_RETRIES", "-1"),
            ("AMBERLOG_QUERY__DEFAULT_SIZE", "-1"),
            ("AMBERLOG_INGEST__MAX_COLUMNS", "0"),
            ("AMBERLOG_INGEST__MAX_BACKLOG_MEMORY_BYTES", "0"),
            ("AMBERLOG_WAL__ALERT_BYTES", "0"),
            ("AMBERLOG_COMPACTION__TARGET_FILE_BYTES", "0"),
            ("AMBERLOG_LIFECYCLE__SEAL_GRACE", "0s"),
            ("AMBERLOG_HOT_FSCK__INTERVAL", "0s"),
            ("AMBERLOG_HOT_FSCK__MIN_ORPHAN_AGE", "0s"),
            ("AMBERLOG_RETENTION__PERIOD", "0s"),
            ("AMBERLOG_RETENTION__PERIOD", "36501d"),
            ("AMBERLOG_STORAGE__DELETE_DELAY", "0s"),
            ("AMBERLOG_LIFECYCLE__LAKE_ALERT_AFTER", "0s"),
            ("AMBERLOG_RETENTION__CHECK_INTERVAL", "0s"),
            ("AMBERLOG_RETENTION__CHECK_INTERVAL", "-1h"),
        ] {
            assert!(load_env(&[(k, v)]).is_err(), "{k}={v}");
        }
        assert_eq!(
            load_env(&[("AMBERLOG_RETENTION__PERIOD", "36500d")])
                .unwrap()
                .retention_period,
            Some(RetentionPeriod {
                text: "36500d".into(),
                period: Duration::from_secs(36_500 * 86_400)
            })
        );
        assert_eq!(
            subjects(load_env(&[
                ("AMBERLOG_LIFECYCLE__SEAL_GRACE", "2h"),
                ("AMBERLOG_LIFECYCLE__LAKE_WATERMARK", "2h")
            ])),
            vec!["lifecycle.lake_watermark"]
        );
        assert_eq!(
            subjects(load_env(&[("AMBERLOG_RETENTION__PERIOD", "72h")])),
            vec!["retention.period"]
        );
        assert!(load_env(&[("AMBERLOG_RETENTION__PERIOD", "73h")]).is_ok());
        for v in ["create_only", "etag"] {
            assert!(
                load_env(&[
                    ("AMBERLOG_OBJECT_STORE__URL", "s3://bucket"),
                    ("AMBERLOG_OBJECT_STORE__S3__CONDITIONAL_PUT", v)
                ])
                .is_ok(),
                "{v}"
            );
        }
        assert_eq!(
            subjects(load_env(&[
                ("AMBERLOG_OBJECT_STORE__URL", "s3://bucket"),
                ("AMBERLOG_OBJECT_STORE__S3__CONDITIONAL_PUT", "none")
            ])),
            vec!["object_store.s3.conditional_put"]
        );
        assert!(
            load_env(&[
                ("AMBERLOG_STORAGE__COMMIT_MAX_RETRIES", "0"),
                ("AMBERLOG_QUERY__DEFAULT_SIZE", "0")
            ])
            .is_ok()
        );
    }

    #[test]
    fn missed_collection_keys() {
        let on = ("AMBERLOG_ALERTS__MISSED_COLLECTION__ENABLED", "true");
        let url = (
            "AMBERLOG_ALERTS__MISSED_COLLECTION__WEBHOOK_URL",
            "https://hooks.example/m",
        );
        assert!(load_env(&[]).unwrap().alerts.rules.is_empty());
        assert!(load_env(&[url]).unwrap().alerts.rules.is_empty());

        let c = load_env(&[on, url]).unwrap();
        let [r] = c.alerts.rules.as_slice() else {
            panic!("{:?}", c.alerts.rules);
        };
        assert_eq!(r.name, rule::MISSED_COLLECTION);
        assert_eq!(
            (r.interval, r.window),
            (Duration::from_secs(300), Duration::from_secs(300))
        );
        let c = load_env(&[
            on,
            url,
            ("AMBERLOG_ALERTS__MISSED_COLLECTION__INTERVAL", "24h"),
        ])
        .unwrap();
        assert_eq!(c.alerts.rules[0].window, Duration::from_secs(86_400));

        assert_eq!(subjects(load_env(&[on])), ["alerts.missed_collection"]);
        assert_eq!(
            subjects(load_env(&[(
                "AMBERLOG_ALERTS__MISSED_COLLECTION__WEBHOOK_URL",
                "ftp://h/x"
            )])),
            ["alerts.missed_collection"]
        );
        for bad in ["0m", "90s", "25h", "x"] {
            assert_eq!(
                subjects(load_env(&[
                    on,
                    url,
                    ("AMBERLOG_ALERTS__MISSED_COLLECTION__INTERVAL", bad)
                ])),
                ["alerts.missed_collection.interval"],
                "{bad}"
            );
        }
    }
}

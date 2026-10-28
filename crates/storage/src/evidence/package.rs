//! 证据包的成员与回执（REQ-AMB-STO-EVIDENCE-EXPORT D 组）：`evidence.json`、`receipt.json`、`files.jsonl`、
//! `check.json` 与 `SHA256SUMS` 的结构与写法，导出写出、查验读回共用。
//!
//! JSON 对象的成员按块所列的先后写出（`serde_json` 开了 `preserve_order`）；时刻一律为 UTC 微秒的 JSON 整数。

use std::collections::BTreeMap;

use serde_json::{Value, json};

use crate::chain::Digest;

/// 本版的证据格式版本（`evidence_format_version`）。
pub const EVIDENCE_FORMAT_VERSION: u64 = 1;

pub const EVIDENCE_JSON: &str = "evidence.json";
pub const RECEIPT_JSON: &str = "receipt.json";
pub const FILES_JSONL: &str = "files.jsonl";
pub const CHECK_JSON: &str = "check.json";
pub const SHA256SUMS: &str = "SHA256SUMS";
/// 元数据副本所在的目录（成员名为它后接对象相对 `<storage.prefix>/` 的路径）。
pub const OBJECTS_DIR: &str = "objects/";

/// 对一个段的引用：回执的 `retired_segment`、`cold_deletion_segment`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectRef {
    /// 相对数据集前缀。
    pub path: String,
    pub content_digest: Digest,
}

/// 回执的 `snapshot`：钉住的热层当前快照。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotHead {
    pub snapshot_id: u64,
    /// 相对数据集前缀（`manifest/<20 位编号>.json`）。
    pub path: String,
    pub content_digest: Digest,
}

/// 回执的 `cold`：钉住的冷层当前版本。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdHead {
    pub metadata_version: u64,
    /// 相对冷层表位置（`metadata/v<N>.metadata.json`）。
    pub path: String,
    pub content_digest: Digest,
}

/// 链头（REQ-AMB-STO-EVIDENCE-EXPORT-TERM-HEAD）：回执与查验报告的 `current` 共用这四个成员。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Heads {
    pub snapshot: SnapshotHead,
    pub retired_segment: Option<ObjectRef>,
    pub cold_deletion_segment: Option<ObjectRef>,
    pub cold: Option<ColdHead>,
}

impl Heads {
    /// `snapshot`、`retired_segment`、`cold_deletion_segment`、`cold` 四个成员（按此顺序）。
    pub fn members(&self) -> [(&'static str, Value); 4] {
        let r = |r: &Option<ObjectRef>| match r {
            Some(r) => json!({"path": r.path, "content_digest": r.content_digest.to_string()}),
            None => Value::Null,
        };
        [
            (
                "snapshot",
                json!({
                    "snapshot_id": self.snapshot.snapshot_id,
                    "path": self.snapshot.path,
                    "content_digest": self.snapshot.content_digest.to_string(),
                }),
            ),
            ("retired_segment", r(&self.retired_segment)),
            ("cold_deletion_segment", r(&self.cold_deletion_segment)),
            (
                "cold",
                match &self.cold {
                    Some(c) => json!({
                        "metadata_version": c.metadata_version,
                        "path": c.path,
                        "content_digest": c.content_digest.to_string(),
                    }),
                    None => Value::Null,
                },
            ),
        ]
    }
}

/// 回执（REQ-AMB-STO-EVIDENCE-EXPORT-RECEIPT）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Receipt {
    pub dataset: String,
    pub exported_at: i64,
    pub amberlog_version: String,
    pub heads: Heads,
}

impl Receipt {
    pub fn to_json(&self) -> Value {
        let mut m = serde_json::Map::new();
        m.insert(
            "evidence_format_version".into(),
            EVIDENCE_FORMAT_VERSION.into(),
        );
        m.insert("dataset".into(), self.dataset.clone().into());
        m.insert("exported_at".into(), self.exported_at.into());
        m.insert(
            "amberlog_version".into(),
            self.amberlog_version.clone().into(),
        );
        for (k, v) in self.heads.members() {
            m.insert(k.into(), v);
        }
        Value::Object(m)
    }

    /// `receipt.json` 的内容：不含换行的单行 JSON（stdout 输出同一份字节再加一个换行符，RECEIPT-STDOUT）。
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(&self.to_json()).expect("回执可序列化")
    }
}

/// `files.jsonl` 一行的 `tier`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Tier {
    Hot,
    Cold,
}

impl Tier {
    pub fn as_str(self) -> &'static str {
        match self {
            Tier::Hot => "hot",
            Tier::Cold => "cold",
        }
    }
}

/// `files.jsonl` 一行的 `deletion`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletionInfo {
    pub reason: &'static str,
    pub cutoff: Option<i64>,
    pub deleted_at: i64,
    pub removed_in: u64,
    /// 冷层行为该留痕条目的取值；热层行为 `None`（JSON `null`）。
    pub cold_removed_in: Option<i64>,
    pub handover_files: Vec<String>,
}

/// `files.jsonl` 的一行（REQ-AMB-STO-EVIDENCE-EXPORT-FILES-JSONL）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileLine {
    pub tier: Tier,
    /// 热层相对数据集前缀；冷层相对冷层表位置。
    pub path: String,
    pub content_digest: Digest,
    pub record_count: Option<u64>,
    pub size_bytes: Option<u64>,
    pub min_timestamp: Option<i64>,
    pub max_timestamp: Option<i64>,
    pub added_in: Option<u64>,
    /// 该行所依据的对象相对 `<storage.prefix>/` 的路径。
    pub source: String,
    /// `None` 即 `state` 为 `live`。
    pub deletion: Option<DeletionInfo>,
}

impl FileLine {
    pub fn is_live(&self) -> bool {
        self.deletion.is_none()
    }

    pub fn to_json(&self) -> Value {
        json!({
            "tier": self.tier.as_str(),
            "path": self.path,
            "state": if self.is_live() { "live" } else { "deleted" },
            "content_digest": self.content_digest.to_string(),
            "record_count": self.record_count,
            "size_bytes": self.size_bytes,
            "min_timestamp": self.min_timestamp,
            "max_timestamp": self.max_timestamp,
            "added_in": self.added_in,
            "source": self.source,
            "deletion": self.deletion.as_ref().map(|d| json!({
                "reason": d.reason,
                "cutoff": d.cutoff,
                "deleted_at": d.deleted_at,
                "removed_in": d.removed_in,
                "cold_removed_in": d.cold_removed_in,
                "handover_files": d.handover_files,
            })),
        })
    }
}

/// `files.jsonl` 的内容：按 `tier`（`hot` 在前）再按 `path` 的字节序排列，每行以换行符结尾。
pub fn files_jsonl(lines: &[FileLine]) -> Vec<u8> {
    let mut sorted: Vec<&FileLine> = lines.iter().collect();
    sorted.sort_by(|a, b| (a.tier, a.path.as_bytes()).cmp(&(b.tier, b.path.as_bytes())));
    let mut out = Vec::new();
    for l in sorted {
        serde_json::to_writer(&mut out, &l.to_json()).expect("可序列化");
        out.push(b'\n');
    }
    out
}

/// 核对结果或查验报告中一个数组的元素 `{"kind", "object", "detail"}`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    pub kind: &'static str,
    pub object: String,
    pub detail: String,
}

/// 一个 `kind` 闭集里的元素集合：按 `object` 再按 `kind` 的字节序排列，`kind` 与 `object` 都相同的只留一个（`detail` 合并）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Findings(BTreeMap<(String, &'static str), String>);

impl Findings {
    pub fn add(
        &mut self,
        kind: &'static str,
        object: impl Into<String>,
        detail: impl Into<String>,
    ) {
        let detail = detail.into();
        self.0
            .entry((object.into(), kind))
            .and_modify(|d| {
                if !d.split("; ").any(|x| x == detail) {
                    d.push_str("; ");
                    d.push_str(&detail);
                }
            })
            .or_insert(detail);
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn contains(&self, kind: &str, object: &str) -> bool {
        self.0.keys().any(|(o, k)| *k == kind && o == object)
    }

    pub fn iter(&self) -> impl Iterator<Item = Finding> + '_ {
        // 元组键按 (object, kind) 排序；两者都是字节序（`str` 的 `Ord` 即字节序）
        self.0.iter().map(|((object, kind), detail)| Finding {
            kind,
            object: object.clone(),
            detail: detail.clone(),
        })
    }

    pub fn to_json(&self) -> Value {
        Value::Array(
            self.iter()
                .map(|f| json!({"kind": f.kind, "object": f.object, "detail": f.detail}))
                .collect(),
        )
    }
}

/// 导出 `check.json` 的 `problems` 的 `kind` 闭集（REQ-AMB-STO-EVIDENCE-EXPORT-CHECK-JSON）。
pub mod problem {
    pub const MISSING: &str = "missing";
    pub const UNPARSEABLE: &str = "unparseable";
    pub const DIGEST_MISMATCH: &str = "digest_mismatch";
    pub const SIZE_MISMATCH: &str = "size_mismatch";
    pub const CHAIN_BROKEN: &str = "chain_broken";
    pub const NO_WRITE_RECORD: &str = "no_write_record";
    pub const UNTRACED_REMOVAL: &str = "untraced_removal";
    pub const LAKE_WITHOUT_HANDOVER: &str = "lake_without_handover";
    pub const LIVE_AFTER_REMOVAL: &str = "live_after_removal";
}

/// `transitions` 的 `kind`（导出三种；查验另加 `traced_still_present`）。
pub mod transition {
    pub const LEFT_LIVE: &str = "left_live";
    pub const HANDOVER_WINDOW: &str = "handover_window";
    pub const COLD_SEGMENT_PENDING: &str = "cold_segment_pending";
}

/// 导出 `notes` 的 `kind`。
pub mod note {
    pub const RETIREMENT_MISMATCH: &str = "retirement_mismatch";
    pub const RETIREMENT_UNREADABLE: &str = "retirement_unreadable";
}

/// 导出时的核对结果（`check.json`）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Check {
    pub problems: Findings,
    pub transitions: Findings,
    pub notes: Findings,
}

impl Check {
    pub fn to_bytes(&self) -> Vec<u8> {
        let v = json!({
            "result": if self.problems.is_empty() { "ok" } else { "problems_found" },
            "problems": self.problems.to_json(),
            "transitions": self.transitions.to_json(),
            "notes": self.notes.to_json(),
        });
        serde_json::to_vec(&v).expect("可序列化")
    }
}

/// `SHA256SUMS`：每个成员一行 `<64 位小写十六进制>  <成员名>`，按成员名的字节序排列（GNU `sha256sum` 的格式）。
pub fn sha256sums<'a>(members: impl IntoIterator<Item = (&'a str, &'a Digest)>) -> Vec<u8> {
    let mut lines: Vec<(&str, &Digest)> = members.into_iter().collect();
    lines.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    let mut out = String::new();
    for (name, d) in lines {
        out.push_str(&hex(d));
        out.push_str("  ");
        out.push_str(name);
        out.push('\n');
    }
    out.into_bytes()
}

/// 摘要的 64 位小写十六进制部分（去掉 `sha256:`）。
pub fn hex(d: &Digest) -> String {
    d.to_string()["sha256:".len()..].to_owned()
}

/// `<org>/<stream>` 的写法（REQ-AMB-STO-EVIDENCE-EXPORT-DATASET-ARG）：恰含一个 `/`，前段为 `default`，后段合 stream 的正则，
/// 按原样判定、不作百分号解码。导出判 `--dataset`、查验判证据包与回执的 `dataset` 都用它。
pub fn parse_dataset(value: &str) -> Option<amberlog_common::dataset::Dataset> {
    use amberlog_common::dataset::{DEFAULT_ORG, Dataset, is_valid_stream_name};
    let (org, stream) = value.split_once('/')?;
    (org == DEFAULT_ORG && !stream.contains('/') && is_valid_stream_name(stream))
        .then(|| Dataset::new(org, stream))
}

/// 证据包或回执的「头」：`evidence_format_version` 为不小于 1 的整数、`dataset` 合 [`parse_dataset`]。
fn head_ok(v: &Value) -> bool {
    v.is_object()
        && v["evidence_format_version"]
            .as_u64()
            .is_some_and(|n| n >= 1)
        && v["dataset"].as_str().and_then(parse_dataset).is_some()
}

/// 解开的证据包（REQ-AMB-STO-EVIDENCE-VERIFY-EVIDENCE-SHAPE 已核对）。
#[derive(Debug, Clone)]
pub struct Unpacked {
    /// 成员名 → 内容（同名成员只取第一个）。
    pub members: BTreeMap<String, Vec<u8>>,
    /// 全部普通文件成员的名字（按出现的先后，含重名）。
    pub names: Vec<String>,
    pub evidence: Value,
}

impl Unpacked {
    pub fn format_version(&self) -> u64 {
        self.evidence["evidence_format_version"]
            .as_u64()
            .unwrap_or_default()
    }

    pub fn dataset(&self) -> &str {
        self.evidence["dataset"].as_str().unwrap_or_default()
    }

    /// `receipt.json` 解析出的 JSON 值（不能解析为 `Null`）。
    pub fn receipt(&self) -> Value {
        self.members
            .get(RECEIPT_JSON)
            .and_then(|b| serde_json::from_slice(b).ok())
            .unwrap_or(Value::Null)
    }
}

/// 解开证据包；不是证据包时给出原因（EVIDENCE-SHAPE：不是 POSIX tar、缺五个成员之一、`evidence.json` 的头不合）。
pub fn unpack(bytes: &[u8]) -> Result<Unpacked, String> {
    let all = super::tar::read(bytes).map_err(|e| format!("not a POSIX tar file: {}", e.0))?;
    let mut members = BTreeMap::new();
    let mut names = Vec::new();
    for m in all {
        names.push(m.name.clone());
        members.entry(m.name).or_insert(m.data);
    }
    for want in [
        EVIDENCE_JSON,
        RECEIPT_JSON,
        FILES_JSONL,
        CHECK_JSON,
        SHA256SUMS,
    ] {
        if !members.contains_key(want) {
            return Err(format!("the package has no member {want}"));
        }
    }
    let evidence: Value = serde_json::from_slice(&members[EVIDENCE_JSON])
        .map_err(|e| format!("{EVIDENCE_JSON} is not JSON: {e}"))?;
    if !head_ok(&evidence) {
        return Err(format!(
            "{EVIDENCE_JSON} must be an object with an integer evidence_format_version of at least 1 and a dataset <org>/<stream>"
        ));
    }
    Ok(Unpacked {
        members,
        names,
        evidence,
    })
}

/// `--previous` 所指文件（PREVIOUS-KIND：按内容判定）。
#[derive(Debug, Clone)]
pub enum Previous {
    Package(Box<Unpacked>),
    Receipt(Value),
}

impl Previous {
    /// 前次锚点的回执。
    pub fn receipt(&self) -> Value {
        match self {
            Previous::Package(p) => p.receipt(),
            Previous::Receipt(r) => r.clone(),
        }
    }

    pub fn format_version(&self) -> u64 {
        match self {
            Previous::Package(p) => p.format_version(),
            Previous::Receipt(r) => r["evidence_format_version"].as_u64().unwrap_or_default(),
        }
    }

    pub fn dataset(&self) -> String {
        match self {
            Previous::Package(p) => p.dataset().to_owned(),
            Previous::Receipt(r) => r["dataset"].as_str().unwrap_or_default().to_owned(),
        }
    }
}

/// 判定 `--previous` 所指文件的种类：含 `evidence.json` 的 tar 按旧证据包，否则整个内容（可带一个末尾换行）是合格的
/// JSON 对象按旧回执；都不是时给出原因。
pub fn previous(bytes: &[u8]) -> Result<Previous, String> {
    if let Ok(all) = super::tar::read(bytes)
        && all.iter().any(|m| m.name == EVIDENCE_JSON)
    {
        return unpack(bytes).map(|p| Previous::Package(Box::new(p)));
    }
    let body = bytes.strip_suffix(b"\n").unwrap_or(bytes);
    match serde_json::from_slice::<Value>(body) {
        Ok(v) if head_ok(&v) => Ok(Previous::Receipt(v)),
        _ => Err("neither an evidence package nor a receipt".into()),
    }
}

/// 回执 JSON 中的链头；成员缺失或写法不合为 `None`。
pub fn heads_of(receipt: &Value) -> Option<Heads> {
    let digest = |v: &Value| v.as_str().and_then(Digest::parse);
    let obj = |v: &Value| -> Option<Option<ObjectRef>> {
        if v.is_null() {
            return Some(None);
        }
        Some(Some(ObjectRef {
            path: v["path"].as_str()?.to_owned(),
            content_digest: digest(&v["content_digest"])?,
        }))
    };
    let s = &receipt["snapshot"];
    let cold = &receipt["cold"];
    Some(Heads {
        snapshot: SnapshotHead {
            snapshot_id: s["snapshot_id"].as_u64()?,
            path: s["path"].as_str()?.to_owned(),
            content_digest: digest(&s["content_digest"])?,
        },
        retired_segment: obj(&receipt["retired_segment"])?,
        cold_deletion_segment: obj(&receipt["cold_deletion_segment"])?,
        cold: if cold.is_null() {
            None
        } else {
            Some(ColdHead {
                metadata_version: cold["metadata_version"].as_u64()?,
                path: cold["path"].as_str()?.to_owned(),
                content_digest: digest(&cold["content_digest"])?,
            })
        },
    })
}

/// `SHA256SUMS` 的各行（成员名 → 摘要的十六进制）；写法不合的行略过。
pub fn parse_sums(bytes: &[u8]) -> BTreeMap<String, String> {
    String::from_utf8_lossy(bytes)
        .lines()
        .filter_map(|l| l.split_once("  "))
        .map(|(h, n)| (n.to_owned(), h.to_owned()))
        .collect()
}

//! 查询类系统测试（`qry_search.rs`、`qry_plan_visibility.rs`）共用的部件：经 `_json` 摄取、`_search` 请求与结果、
//! 以测试进程自己的对象存储客户端读快照链（不经被测进程）。MinIO 台架上的实例与故障注入见 `common::s3`。

#![allow(dead_code)]

use std::time::{Duration, Instant};

use amberlog_common::dataset::Dataset;
use amberlog_storage::store::{Location, S3Options, Store};
use serde_json::{Value, json};

use crate::common::s3::{PREFIX, S3Instance};
use crate::common::{BEARER, Fixture, Resp, Server};

/// 覆盖所有测试数据的查询范围：`[0, 2100-01-01T00:00:00Z)`。
pub const ALL: (i64, i64) = (0, 4_102_444_800_000_000);

/// 2026-08-22T10:00:00Z（UTC 微秒）。
pub const H10: i64 = 1_787_392_800_000_000;
pub const MINUTE: i64 = 60_000_000;
pub const HOUR: i64 = 3_600_000_000;

/// 1970-01-01 第一分钟内的时刻 `us`（UTC 微秒）写成 RFC 3339 字符串：数值 `_timestamp` 按幅值判定单位，
/// 小于 1e11 的数按秒计，这么小的微秒值只能以字符串写入。
pub fn early(us: i64) -> String {
    assert!((0..MINUTE).contains(&us), "{us}");
    format!(
        "1970-01-01T00:00:{:02}.{:06}Z",
        us / 1_000_000,
        us % 1_000_000
    )
}

impl Server {
    /// `POST /api/default/{stream}/_json`。
    pub fn ingest(&self, stream: &str, records: &Value) -> Resp {
        self.ingest_raw(stream, records.to_string().as_bytes())
    }

    pub fn ingest_raw(&self, stream: &str, body: &[u8]) -> Resp {
        self.request(
            "POST",
            &format!("/api/default/{stream}/_json"),
            &[("Authorization", BEARER)],
            body,
        )
    }

    /// 摄取并断言 200。
    pub fn ingest_ok(&self, stream: &str, records: &Value) {
        let r = self.ingest(stream, records);
        assert_eq!(r.status, 200, "摄取 {stream}：{}", r.text());
    }

    /// `POST /api/default/_search`，请求体为 `body`。
    pub fn search(&self, body: &Value) -> Resp {
        self.search_raw(body.to_string().as_bytes())
    }

    pub fn search_raw(&self, body: &[u8]) -> Resp {
        self.request(
            "POST",
            "/api/default/_search",
            &[("Authorization", BEARER)],
            body,
        )
    }

    /// 以时间范围 [`ALL`] 查询 `sql`。
    pub fn sql(&self, sql: &str) -> Resp {
        self.search(&query(sql, ALL, &json!({})))
    }

    /// 以时间范围 [`ALL`] 查询 `sql` 并断言 200，返回响应体。
    pub fn sql_ok(&self, sql: &str) -> Value {
        let r = self.sql(sql);
        assert_eq!(r.status, 200, "{sql}：{}", r.text());
        r.json()
    }

    /// 以时间范围 [`ALL`] 查询 `sql`，断言错误信封的状态码与 `reason`。
    pub fn sql_err(&self, sql: &str, status: u16, reason: &str) {
        let r = self.sql(sql);
        assert_eq!(
            (r.status, r.json()["reason"].as_str().map(str::to_owned)),
            (status, Some(reason.to_owned())),
            "{sql}：{}",
            r.text()
        );
        r.assert_envelope(status, reason);
    }
}

/// `{"query":{"sql":…,"start_time":…,"end_time":…,<extra 的各键>}}`。
pub fn query(sql: &str, (start, end): (i64, i64), extra: &Value) -> Value {
    let mut q = json!({"sql": sql, "start_time": start, "end_time": end});
    if let Some(extra) = extra.as_object() {
        for (k, v) in extra {
            q[k] = v.clone();
        }
    }
    json!({ "query": q })
}

/// 响应体的 `hits`。
pub fn hits(body: &Value) -> Vec<Value> {
    body["hits"].as_array().expect("hits 是数组").clone()
}

/// 各 hit 中列 `col` 的值（没有该键记为 `null`），按 hit 顺序。
pub fn column(body: &Value, col: &str) -> Vec<Value> {
    hits(body)
        .iter()
        .map(|h| h.get(col).cloned().unwrap_or(Value::Null))
        .collect()
}

/// 反复执行 `f` 直到返回 `Some`（至多 `limit`），超时 panic 并带上 `what`。
pub fn eventually<T>(limit: Duration, what: &str, mut f: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + limit;
    loop {
        if let Some(v) = f() {
            return v;
        }
        assert!(Instant::now() < deadline, "{limit:?} 内未等到：{what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

// ---------------------------------------------------------------------------
// 快照链（测试进程自己的对象存储客户端，不经被测进程）
// ---------------------------------------------------------------------------

/// 与被测实例同一位置的对象存储客户端。
pub struct Chain {
    store: Store,
    rt: tokio::runtime::Runtime,
}

impl Chain {
    pub fn file(f: &Fixture) -> Chain {
        let store = Store::open(
            &Location::File { dir: f.store_dir() },
            &S3Options::default(),
            PREFIX,
        )
        .unwrap();
        Chain::from_store(store)
    }

    /// MinIO 台架上的实例（root 凭据，直连台架）。
    pub fn s3(inst: &S3Instance) -> Chain {
        Chain::from_store(inst.store())
    }

    fn from_store(store: Store) -> Chain {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        Chain { store, rt }
    }

    /// 数据集前缀下 `rel` 的对象内容（JSON）；不存在为 `None`。
    pub fn json(&self, stream: &str, rel: &str) -> Option<Value> {
        let ds = Dataset::new("default", stream);
        let path = self.store.object_path(&ds, rel);
        let got = self.rt.block_on(self.store.read(&path)).unwrap()?;
        Some(serde_json::from_slice(&got.bytes).unwrap())
    }

    /// 当前快照（按找链尾规则确定）；尚无快照为 `None`。
    pub fn current_snapshot(&self, stream: &str) -> Option<Value> {
        // 找链尾（REQ-AMB-STO-MANIFEST-COMMIT-V3）：自提示所指编号起向后找到最后一个存在的快照；提示不存在时自 1 起
        let mut id = self
            .json(stream, "manifest/_current")
            .and_then(|v| v["snapshot_id"].as_u64());
        loop {
            let next = id.map_or(1, |i| i + 1);
            if self
                .json(stream, &format!("manifest/{next:020}.json"))
                .is_none()
            {
                break;
            }
            id = Some(next);
        }
        self.json(stream, &format!("manifest/{:020}.json", id?))
    }

    /// 等到当前快照的 `summary.record_count` 等于 `n`（至多 30 秒），返回该快照。
    pub fn wait_record_count(&self, stream: &str, n: u64) -> Value {
        eventually(
            Duration::from_secs(30),
            &format!("{stream} 的已提交记录数为 {n}"),
            || {
                self.current_snapshot(stream)
                    .filter(|s| s["summary"]["record_count"] == n)
            },
        )
    }

    /// 快照 `snapshot` 所引用的全部数据文件条目（桶 manifest 的 `files`），按桶与条目顺序。
    pub fn files(&self, stream: &str, snapshot: &Value) -> Vec<Value> {
        let mut out = Vec::new();
        for b in snapshot["buckets"].as_array().unwrap() {
            let m = crate::common::store::restore_with(
                |rel| {
                    self.json(stream, rel)
                        .expect("快照引用的桶 manifest 的清单片存在")
                },
                b["manifest"].as_str().unwrap(),
            );
            out.extend(m["files"].as_array().unwrap().iter().cloned());
        }
        out
    }
}

//! 冷层表的表目录与对象路径（REQ-AMB-STO-COLD-TABLE「表的位置」「数据文件」「元数据文件与提示」）。
//!
//! 表目录为根前缀之下的 `_iceberg/<org>/<stream>/`，只含 `metadata/` 与 `data/`。本模块的相对路径一律相对表目录
//! （交接记录 `files[].path` 与摘要 `amberlog.file_digests` 的键就按这个基准写），写进清单的 `file_path` 则是
//! 表位置 URI 加相对路径。

use amberlog_common::dataset::Dataset;
use object_store::path::Path;

use crate::layout::{HourBucket, is_object_id, new_object_id};
use crate::store::Location;

/// 冷层前缀（根前缀之下）。
pub const ICEBERG_DIR: &str = "_iceberg";

/// 提示文件相对表目录的路径。
pub const HINT: &str = "metadata/version-hint.text";

/// 分区字段名（ADR-0041 第 3 条：由 Amberlog 定，合法的 Avro 名字）。
pub const PARTITION_FIELD: &str = "_timestamp_day";

/// 根前缀在对象存储里的键与外部引擎所用的 URI。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdLayout {
    root: Path,
    /// 根前缀的 URI，末尾不带 `/`：`s3://<bucket>/<键>` 或 `file://<绝对路径>`。
    root_uri: String,
}

impl ColdLayout {
    /// `root` 是对象存储客户端里的根前缀（[`crate::store::Store::root`]），`location` 是 `object_store.url` 所指的位置。
    pub fn new(location: &Location, root: Path) -> Self {
        let root_uri = match location {
            // 对象存储客户端以 `Path` 的字符串为键，外部引擎按 URI 的路径部分取同一个键
            Location::S3 { bucket, .. } => format!("s3://{bucket}/{}", root.as_ref()),
            Location::File { dir } => {
                let dir = dir.to_string_lossy();
                format!(
                    "file://{}/{}",
                    percent_encode_path(dir.trim_end_matches('/')),
                    percent_encode_path(root.as_ref())
                )
            }
        };
        ColdLayout { root, root_uri }
    }

    /// 表目录的键：`<root>/_iceberg/<org>/<stream>`。
    pub fn table_dir(&self, dataset: &Dataset) -> Path {
        self.root
            .clone()
            .join(ICEBERG_DIR)
            .join(dataset.org.as_str())
            .join(dataset.stream.as_str())
    }

    /// 冷层前缀的键：`<root>/_iceberg`。
    pub fn iceberg_dir(&self) -> Path {
        self.root.clone().join(ICEBERG_DIR)
    }

    /// 表元数据的 `location`（REQ-AMB-STO-COLD-TABLE-LOCATION）：末尾不带 `/`。
    pub fn table_location(&self, dataset: &Dataset) -> String {
        format!(
            "{}/{ICEBERG_DIR}/{}/{}",
            self.root_uri, dataset.org, dataset.stream
        )
    }

    /// 表目录下相对路径 `rel` 的键。
    pub fn object_path(&self, dataset: &Dataset, rel: &str) -> Path {
        rel.split('/')
            .fold(self.table_dir(dataset), |path, part| path.join(part))
    }

    /// 表目录下相对路径 `rel` 的 URI（写进清单与清单列表的位置）。
    pub fn object_uri(&self, dataset: &Dataset, rel: &str) -> String {
        format!("{}/{rel}", self.table_location(dataset))
    }

    /// 若 `uri` 在该表的表位置之下，返回相对表目录的路径。
    pub fn rel_of_uri<'a>(&self, dataset: &Dataset, uri: &'a str) -> Option<&'a str> {
        let rest = uri.strip_prefix(&self.table_location(dataset))?;
        rest.strip_prefix('/').filter(|r| !r.is_empty())
    }

    /// 若对象键 `path` 在该表的表目录之下，返回相对表目录的路径。
    pub fn rel_of_path(&self, dataset: &Dataset, path: &Path) -> Option<String> {
        let dir = self.table_dir(dataset);
        let rest: Vec<_> = path
            .prefix_match(&dir)?
            .map(|p| p.as_ref().to_owned())
            .collect();
        (!rest.is_empty()).then(|| rest.join("/"))
    }
}

/// 版本 `n` 的元数据文件相对表目录的路径：`metadata/v<n>.metadata.json`（不带前导零）。
pub fn metadata_path(n: u64) -> String {
    format!("metadata/v{n}.metadata.json")
}

/// 若 `rel`（相对表目录）是某个 `v<N>.metadata.json`（N 自 1 起、不带前导零），返回 N。
pub fn parse_metadata_path(rel: &str) -> Option<u64> {
    let n = rel
        .strip_prefix("metadata/v")?
        .strip_suffix(".metadata.json")?;
    if n.is_empty() || n.starts_with('0') || !n.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    n.parse().ok()
}

/// 清单与清单列表相对表目录的路径：`metadata/<26 字符小写 ULID>-<用途>.avro`。
/// 每次生成都取新的标识，已写出的清单与清单列表不会被覆盖（REQ-AMB-STO-COLD-TABLE-IMMUTABLE）。
pub fn new_avro_path(kind: &str) -> String {
    format!("metadata/{}-{kind}.avro", new_object_id())
}

/// UTC 日（自 1970-01-01 起的天数）。
pub fn day_of(ts_micros: i64) -> i32 {
    ts_micros.div_euclid(86_400_000_000) as i32
}

/// UTC 日的 `YYYY-MM-DD` 写法。
pub fn day_string(day: i32) -> String {
    // 小时桶写作 `YYYYMMDDHH`（年份在本系统的时间戳范围内恒为 4 位）
    let hour = HourBucket::of(i64::from(day) * 86_400_000_000).to_string();
    format!("{}-{}-{}", &hour[0..4], &hour[4..6], &hour[6..8])
}

/// 新冷层数据文件相对表目录的路径（REQ-AMB-STO-COLD-TABLE-DATA-PATH）：
/// `data/_timestamp_day=<YYYY-MM-DD>/<file_id>.parquet`。
pub fn new_data_file_path(day: i32) -> String {
    format!(
        "data/{PARTITION_FIELD}={}/{}.parquet",
        day_string(day),
        new_object_id()
    )
}

/// 若 `rel`（相对表目录）符合冷层数据文件路径模板，返回其分区日的写法。
pub fn parse_data_file_path(rel: &str) -> Option<&str> {
    let rest = rel.strip_prefix("data/")?.strip_prefix(PARTITION_FIELD)?;
    let (day, name) = rest.strip_prefix('=')?.split_once('/')?;
    let id = name.strip_suffix(".parquet")?;
    (day.len() == 10 && is_object_id(id)).then_some(day)
}

/// 按 RFC 3986 对路径做百分号编码：`/` 与非保留字符原样保留。
fn percent_encode_path(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn app() -> Dataset {
        Dataset::new("default", "app")
    }

    #[test]
    fn s3_location_has_no_endpoint_and_no_trailing_slash() {
        let location = Location::S3 {
            bucket: "bucket-a".into(),
            path: vec!["base".into()],
        };
        let layout = ColdLayout::new(&location, Path::from_iter(["base", "amberlog"]));
        assert_eq!(
            layout.table_location(&app()),
            "s3://bucket-a/base/amberlog/_iceberg/default/app"
        );
        assert_eq!(
            layout.table_dir(&app()).as_ref(),
            "base/amberlog/_iceberg/default/app"
        );
    }

    #[test]
    fn file_location_is_absolute_file_url() {
        let location = Location::File {
            dir: "/tmp/my store".into(),
        };
        let layout = ColdLayout::new(&location, Path::from("amberlog"));
        assert_eq!(
            layout.table_location(&app()),
            "file:///tmp/my%20store/amberlog/_iceberg/default/app"
        );
    }

    #[test]
    fn uris_and_paths_round_trip() {
        let location = Location::S3 {
            bucket: "b".into(),
            path: vec![],
        };
        let layout = ColdLayout::new(&location, Path::from("amberlog"));
        let rel = new_data_file_path(day_of(0));
        let uri = layout.object_uri(&app(), &rel);
        assert_eq!(layout.rel_of_uri(&app(), &uri), Some(rel.as_str()));
        let path = layout.object_path(&app(), &rel);
        assert_eq!(
            layout.rel_of_path(&app(), &path).as_deref(),
            Some(rel.as_str())
        );
        assert_eq!(
            layout.rel_of_uri(&Dataset::new("default", "web"), &uri),
            None
        );
    }

    #[test]
    fn metadata_names() {
        assert_eq!(metadata_path(12), "metadata/v12.metadata.json");
        assert_eq!(parse_metadata_path("metadata/v12.metadata.json"), Some(12));
        assert_eq!(parse_metadata_path("metadata/v012.metadata.json"), None);
        assert_eq!(parse_metadata_path("metadata/v0.metadata.json"), None);
        assert_eq!(parse_metadata_path("metadata/v.metadata.json"), None);
        assert_eq!(parse_metadata_path("metadata/version-hint.text"), None);
    }

    #[test]
    fn data_paths_are_day_partitioned() {
        // 1969-12-31T23:59:59.999999Z 属前一日
        assert_eq!(day_string(day_of(-1)), "1969-12-31");
        assert_eq!(day_string(day_of(0)), "1970-01-01");
        // 2026-08-22T10:59:59Z
        let rel = new_data_file_path(day_of(1_787_396_399_000_000));
        assert!(rel.starts_with("data/_timestamp_day=2026-08-22/"), "{rel}");
        assert_eq!(parse_data_file_path(&rel), Some("2026-08-22"));
        assert_eq!(parse_data_file_path("data/x.parquet"), None);
    }
}

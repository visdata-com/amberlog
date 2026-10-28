//! 数据集前缀之下的对象路径（REQ-AMB-STO-HOT-LAYOUT-V2「前缀布局」「数据文件」「快照文件与当前指针」「桶 manifest」）。
//!
//! 本模块的路径一律相对数据集前缀 `<storage.prefix>/<org>/<stream>/`（快照 `buckets[].manifest` 与
//! 桶 manifest `files[].path` 就按这个基准写），拼上前缀后才是对象存储里的键，见 [`crate::store::Store::dataset_prefix`]。

use std::fmt;

use crate::chain::{SegmentId, SnapshotId};

/// `_current` 相对数据集前缀的路径。
pub const CURRENT: &str = "manifest/_current";

/// 快照所在的子前缀。
pub const MANIFEST_DIR: &str = "manifest";

/// 快照 `snapshot_id` 相对数据集前缀的路径：`manifest/<零填充 20 位>.json`。
pub fn snapshot_path(id: SnapshotId) -> String {
    format!("{MANIFEST_DIR}/{:020}.json", id.0)
}

/// 若 `path`（相对数据集前缀）符合快照路径模板，返回其快照号。编号须自 1 起，所以全零不算。
pub fn parse_snapshot_path(path: &str) -> Option<SnapshotId> {
    let name = path.strip_prefix("manifest/")?.strip_suffix(".json")?;
    if name.len() != 20 || !name.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    match name.parse::<u64>() {
        Ok(0) | Err(_) => None,
        Ok(id) => Some(SnapshotId(id)),
    }
}

/// 留痕归档段 `segment_id` 相对数据集前缀的路径：`manifest/retired/<零填充 20 位>.json`（REQ-AMB-STO-HOT-LAYOUT-V4）。
pub fn retired_segment_path(id: SegmentId) -> String {
    format!("{MANIFEST_DIR}/retired/{:020}.json", id.0)
}

/// 冷层变更段 `segment_id` 相对数据集前缀的路径：`manifest/cold-deletions/<零填充 20 位>.json`（REQ-AMB-STO-HOT-LAYOUT-V4）。
pub fn cold_change_segment_path(id: SegmentId) -> String {
    format!("{MANIFEST_DIR}/cold-deletions/{:020}.json", id.0)
}

/// 若 `path`（相对数据集前缀）符合留痕归档段路径模板，返回其段号。编号须自 1 起，所以全零不算。
pub fn parse_retired_segment_path(path: &str) -> Option<SegmentId> {
    parse_segment(path.strip_prefix("manifest/retired/")?)
}

/// 若 `path`（相对数据集前缀）符合冷层变更段路径模板，返回其段号。编号须自 1 起，所以全零不算。
pub fn parse_cold_change_segment_path(path: &str) -> Option<SegmentId> {
    parse_segment(path.strip_prefix("manifest/cold-deletions/")?)
}

fn parse_segment(file: &str) -> Option<SegmentId> {
    let name = file.strip_suffix(".json")?;
    if name.len() != 20 || !name.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    match name.parse::<u64>() {
        Ok(0) | Err(_) => None,
        Ok(id) => Some(SegmentId(id)),
    }
}

/// 冷层表目录（`<storage.prefix>/_iceberg/<org>/<stream>/`，REQ-AMB-STO-COLD-TABLE）在根前缀之下的第一段。
pub const COLD_ROOT: &str = "_iceberg";

/// 桶 manifest 相对数据集前缀的路径：`manifest/buckets/<YYYYMMDDHH>-<manifest_id>.json`。
pub fn bucket_manifest_path(bucket: HourBucket, manifest_id: &str) -> String {
    format!("{MANIFEST_DIR}/buckets/{bucket}-{manifest_id}.json")
}

/// 数据文件相对数据集前缀的路径：`data/<YYYY>/<MM>/<DD>/<HH>/<file_id>.parquet`。
pub fn data_file_path(bucket: HourBucket, file_id: &str) -> String {
    let (y, m, d, h) = bucket.civil();
    format!("data/{y:04}/{m:02}/{d:02}/{h:02}/{file_id}.parquet")
}

/// 若 `path`（相对数据集前缀）符合数据文件路径模板，返回其路径所示小时桶。
pub fn parse_data_file_path(path: &str) -> Option<HourBucket> {
    let rest = path.strip_prefix("data/")?;
    let mut parts = rest.split('/');
    let (y, m, d, h, name) = (
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next()?,
        parts.next()?,
    );
    if parts.next().is_some() || y.len() != 4 || [m, d, h].iter().any(|p| p.len() != 2) {
        return None;
    }
    let bucket = HourBucket::parse(&format!("{y}{m}{d}{h}"))?;
    let id = name.strip_suffix(".parquet")?;
    is_object_id(id).then_some(bucket)
}

/// 若 `path`（相对数据集前缀）符合桶 manifest 路径模板，返回其小时桶。
pub fn parse_bucket_manifest_path(path: &str) -> Option<HourBucket> {
    let name = path
        .strip_prefix("manifest/buckets/")?
        .strip_suffix(".json")?;
    let (label, id) = name.split_once('-')?;
    let bucket = HourBucket::parse(label)?;
    is_object_id(id).then_some(bucket)
}

/// 一个 UTC 小时桶，以该小时起点（自 Unix epoch 起的 UTC 微秒）表示。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HourBucket(i64);

const MICROS_PER_HOUR: i64 = 3_600_000_000;

impl HourBucket {
    /// `_timestamp`（UTC 微秒）所在的小时桶。
    pub fn of(timestamp_us: i64) -> HourBucket {
        HourBucket(timestamp_us.div_euclid(MICROS_PER_HOUR) * MICROS_PER_HOUR)
    }

    /// 该小时的起点，UTC 微秒。
    pub fn start_us(self) -> i64 {
        self.0
    }

    /// 该小时内最后一个微秒。
    pub fn end_us(self) -> i64 {
        self.0 + MICROS_PER_HOUR - 1
    }

    /// 由 `YYYYMMDDHH` 解析；不是合法的年月日时为 `None`。
    pub fn parse(label: &str) -> Option<HourBucket> {
        if label.len() != 10 || !label.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let y: i64 = label[0..4].parse().ok()?;
        let m: u32 = label[4..6].parse().ok()?;
        let d: u32 = label[6..8].parse().ok()?;
        let h: i64 = label[8..10].parse().ok()?;
        if !(1..=12).contains(&m) || d == 0 || d > days_in_month(y, m) || h > 23 {
            return None;
        }
        let days = days_from_civil(y, m, d);
        Some(HourBucket((days * 24 + h) * MICROS_PER_HOUR))
    }

    /// (年, 月, 日, 时)
    fn civil(self) -> (i64, u32, u32, i64) {
        let hours = self.0.div_euclid(MICROS_PER_HOUR);
        let (y, m, d) = civil_from_days(hours.div_euclid(24));
        (y, m, d, hours.rem_euclid(24))
    }
}

/// `YYYYMMDDHH`：快照 `buckets[].bucket` 与桶 manifest `bucket` 的写法。
impl fmt::Display for HourBucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (y, m, d, h) = self.civil();
        write!(f, "{y:04}{m:02}{d:02}{h:02}")
    }
}

/// 新的对象标识：26 字符小写 ULID（Crockford base32；48 位毫秒时间戳 + 80 位随机数）。
/// 用于 `<file_id>` 与 `<manifest_id>`。
pub fn new_object_id() -> String {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis())
        & ((1 << 48) - 1);
    let random: u128 = rand::random::<u128>() & ((1 << 80) - 1);
    encode_ulid(millis << 80 | random)
}

/// 26 字符小写 Crockford base32（不含 i、l、o、u）。
fn encode_ulid(value: u128) -> String {
    const ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";
    (0..26)
        .rev()
        .map(|i| ALPHABET[((value >> (i * 5)) & 0x1f) as usize] as char)
        .collect()
}

/// `s` 是否为 26 字符小写 ULID。
pub fn is_object_id(s: &str) -> bool {
    s.len() == 26
        && s.as_bytes()[0] <= b'7'
        && s
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'h' | b'j' | b'k' | b'm' | b'n' | b'p'..=b't' | b'v'..=b'z'))
}

// 公历与自 1970-01-01 起的天数互换（Howard Hinnant 的 days_from_civil / civil_from_days）。

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let m = i64::from(m);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

fn days_in_month(y: i64, m: u32) -> u32 {
    match m {
        2 if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2026-08-22T10:15:00Z
    const T: i64 = 1_787_393_700_000_000;

    #[test]
    fn hour_bucket_label_and_paths() {
        let b = HourBucket::of(T);
        assert_eq!(b.to_string(), "2026082210");
        assert_eq!(HourBucket::parse("2026082210"), Some(b));
        assert_eq!(b.start_us(), 1_787_392_800_000_000);
        assert_eq!(HourBucket::of(b.end_us()), b);
        assert_eq!(HourBucket::of(b.end_us() + 1).to_string(), "2026082211");
        assert_eq!(
            data_file_path(b, "01j9zzzzzzzzzzzzzzzzzzzzzz"),
            "data/2026/08/22/10/01j9zzzzzzzzzzzzzzzzzzzzzz.parquet"
        );
        assert_eq!(
            bucket_manifest_path(b, "01j9zzzzzzzzzzzzzzzzzzzzzz"),
            "manifest/buckets/2026082210-01j9zzzzzzzzzzzzzzzzzzzzzz.json"
        );
        // 1970 之前与闰日
        assert_eq!(HourBucket::of(-1).to_string(), "1969123123");
        assert!(HourBucket::parse("2024022900").is_some());
        assert!(HourBucket::parse("2026022900").is_none());
        assert!(HourBucket::parse("2026082224").is_none());
        assert!(HourBucket::parse("202608221").is_none());
    }

    #[test]
    fn data_and_bucket_manifest_paths_parse() {
        let b = HourBucket::of(T);
        let id = new_object_id();
        assert_eq!(parse_data_file_path(&data_file_path(b, &id)), Some(b));
        assert_eq!(
            parse_bucket_manifest_path(&bucket_manifest_path(b, &id)),
            Some(b)
        );
        for bad in [
            "data/2026/08/22/10/x.parquet",
            "data/2026/08/22/24/01j9zzzzzzzzzzzzzzzzzzzzzz.parquet",
            "data/2026/08/22/10/01j9zzzzzzzzzzzzzzzzzzzzzz.json",
            "data/2026/08/22/10/a/01j9zzzzzzzzzzzzzzzzzzzzzz.parquet",
            "data/2026/8/22/10/01j9zzzzzzzzzzzzzzzzzzzzzz.parquet",
        ] {
            assert_eq!(parse_data_file_path(bad), None, "{bad}");
        }
        for bad in [
            "manifest/buckets/2026082210.json",
            "manifest/buckets/2026082210-x.json",
            "manifest/buckets/2026082299-01j9zzzzzzzzzzzzzzzzzzzzzz.json",
            "manifest/00000000000000000001.json",
        ] {
            assert_eq!(parse_bucket_manifest_path(bad), None, "{bad}");
        }
    }

    #[test]
    fn snapshot_paths_are_zero_padded() {
        assert_eq!(
            snapshot_path(SnapshotId(1)),
            "manifest/00000000000000000001.json"
        );
        assert_eq!(
            parse_snapshot_path("manifest/00000000000000000042.json"),
            Some(SnapshotId(42))
        );
        for bad in [
            "manifest/00000000000000000000.json",
            "manifest/0000000000000000001.json",
            "manifest/_current",
            "manifest/buckets/00000000000000000001.json",
            "manifest/0000000000000000000a.json",
        ] {
            assert_eq!(parse_snapshot_path(bad), None, "{bad}");
        }
    }

    #[test]
    fn object_ids_are_lowercase_ulids() {
        let a = new_object_id();
        let b = new_object_id();
        assert_ne!(a, b);
        for id in [&a, &b] {
            assert!(is_object_id(id), "{id}");
            assert_eq!(id.len(), 26);
        }
        assert_eq!(encode_ulid(0), "0".repeat(26));
        assert_eq!(encode_ulid(u128::MAX), format!("7{}", "z".repeat(25)));
        assert!(!is_object_id(&"0".repeat(25)));
        assert!(!is_object_id("0123456789abcdefghjkmnpqrI"));
        assert!(!is_object_id("0123456789abcdefghjkmnpqru"));
    }

    #[test]
    fn segment_paths_roundtrip() {
        assert_eq!(
            retired_segment_path(SegmentId(1)),
            "manifest/retired/00000000000000000001.json"
        );
        assert_eq!(
            cold_change_segment_path(SegmentId(2)),
            "manifest/cold-deletions/00000000000000000002.json"
        );
        for id in [1, 2, 99_999] {
            let id = SegmentId(id);
            assert_eq!(
                parse_retired_segment_path(&retired_segment_path(id)),
                Some(id)
            );
            assert_eq!(
                parse_cold_change_segment_path(&cold_change_segment_path(id)),
                Some(id)
            );
            assert_eq!(
                parse_cold_change_segment_path(&retired_segment_path(id)),
                None
            );
        }
        for bad in [
            "manifest/retired/00000000000000000000.json",
            "manifest/retired/1.json",
            "manifest/retired/0000000000000000000a.json",
            "manifest/cold-deletions/00000000000000000001",
            "manifest/00000000000000000001.json",
        ] {
            assert_eq!(parse_retired_segment_path(bad), None, "{bad}");
            assert_eq!(parse_cold_change_segment_path(bad), None, "{bad}");
        }
    }
}

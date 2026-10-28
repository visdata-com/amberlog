//! 冷层孤儿清理的一次（一个数据集）（REQ-AMB-STO-COLD-TABLE「冷层孤儿」）。
//!
//! 冷层孤儿是表目录 `data/` 与 `metadata/` 之下、既不是 `v<N>.metadata.json` 也不是提示文件、且不被冷层当前版本中
//! 任一快照引用的对象（-ORPHAN）。一次清理依次：
//! 1. 记下开始时刻，列出表目录下的对象（只有此时已在的对象才可能被删）；
//! 2. 取本进程在途冷层提交已上传的对象（-ORPHAN-INFLIGHT）；
//! 3. 确定冷层当前版本，读出它全部快照引用的对象。
//!
//! 先列举、再取在途、最后读引用：列举时已在、此后被新版本引用的对象，取在途时要么仍在途，要么所在的提交已写成、
//! 读引用时就在引用集里，所以不会删掉任一时刻冷层当前版本所引用的对象（-ORPHAN-LIVE）。读清单列表或清单得到不存在、
//! 无法解析或不可访问时放弃本数据集的本次清理、不删除任何对象，也不停用（-ORPHAN-ABORT）。
//! 删除的是上传完成时刻（对象的最后修改时刻，与热层巡检同口径）早于开始时刻减 `hot_fsck.min_orphan_age` 的孤儿，
//! 直接删除，不记留痕（-ORPHAN-CLEAN）。

use std::time::Duration;

use amberlog_common::dataset::Dataset;

use super::commit::InFlight;
use super::current;
use super::error::ColdError;
use super::layout::{self, ColdLayout};
use super::read;
use crate::fault;
use crate::store::Store;

/// 一次清理的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cleaned {
    /// 尚无冷层表：没有可清理的。
    NoTable,
    /// 删除了这些对象（相对表目录）。
    Deleted(Vec<String>),
}

/// 清理数据集 `dataset` 的冷层孤儿。调用方负责只对存在、未停用、已有冷层表的数据集调用，并在停机过程中不再开始新的一次
/// （REQ-AMB-STO-COLD-TABLE-SHUTDOWN、-DISABLED）；`still_enabled` 在删除之前再问一次（判定之后数据集可能已停用）。
pub async fn clean(
    store: &Store,
    layout: &ColdLayout,
    inflight: &InFlight,
    dataset: &Dataset,
    min_orphan_age: Duration,
    still_enabled: impl Fn() -> bool,
) -> Result<Cleaned, ColdError> {
    let started_us = now_us();
    let cutoff = started_us.saturating_sub(min_orphan_age.as_micros() as i64);
    let listed = store.list(&layout.table_dir(dataset)).await?;
    let in_flight = inflight.snapshot(dataset);
    let cur = current::current(store, layout, dataset).await?;
    if cur.version == 0 {
        return Ok(Cleaned::NoTable);
    }
    let referenced = read::referenced(store, layout, dataset, cur.version).await?;

    let mut orphans = Vec::new();
    for o in listed {
        let Some(rel) = layout.rel_of_path(dataset, &o.location) else {
            continue;
        };
        if !is_orphan_candidate(&rel) || referenced.contains(&rel) || in_flight.contains(&rel) {
            continue;
        }
        if o.last_modified.timestamp_micros() < cutoff {
            orphans.push((rel, o.location));
        }
    }
    fault::gate("cold_orphan.before_delete").await;
    let mut deleted = Vec::new();
    for (rel, path) in orphans {
        if !still_enabled() {
            break;
        }
        store.delete(&path).await?;
        deleted.push(rel);
    }
    Ok(Cleaned::Deleted(deleted))
}

/// `rel` 在 `data/` 或 `metadata/` 之下，且既不是 `v<N>.metadata.json` 也不是提示文件。
fn is_orphan_candidate(rel: &str) -> bool {
    (rel.starts_with("data/") || rel.starts_with("metadata/"))
        && rel != layout::HINT
        && layout::parse_metadata_path(rel).is_none()
}

fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as i64)
}

/// 测试用：一个对象集合的相对路径。
#[cfg(test)]
pub(super) fn rels(c: &Cleaned) -> std::collections::HashSet<String> {
    match c {
        Cleaned::NoTable => Default::default(),
        Cleaned::Deleted(v) => v.iter().cloned().collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidates() {
        assert!(is_orphan_candidate(
            "data/_timestamp_day=2026-08-22/x.parquet"
        ));
        assert!(is_orphan_candidate("metadata/x-m0.avro"));
        assert!(is_orphan_candidate("metadata/v01.metadata.json"));
        assert!(!is_orphan_candidate("metadata/v3.metadata.json"));
        assert!(!is_orphan_candidate("metadata/version-hint.text"));
        assert!(!is_orphan_candidate("other/x"));
    }
}

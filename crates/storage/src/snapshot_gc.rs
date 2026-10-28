//! 热层快照回收（REQ-AMB-STO-SNAPSHOT-GC）：只保留快照链上以当前快照结尾的连续一段。
//!
//! 不另设周期工作：由热层巡检（[`crate::hot_fsck::HotFsck`]）在同一周期、同一数据集上先于巡检调用，待回收结束（含放弃
//! 与中止）再巡检（BEFORE-FSCK）。一次回收：
//! 1. 记下开始时刻，读提示（不存在按 0 计、本次不删任何快照），按找链尾确定当前快照，列举一次 `manifest/`；
//! 2. 自当前快照沿 `parent_snapshot_id` 回溯，定出**保留起点**（[`retain_start`]）：「当前快照编号减
//!    `hot_retention.snapshots` 加 1」与「遇到的第一个 `created_at` 早于开始时刻减 `hot_retention.age` 的快照编号（回溯到
//!    链底仍未遇到时取链底编号）」两者中较小者，且不大于提示所记编号。读不全、或回溯到链底仍未遇到早于保留时长的快照、
//!    链底不是创世快照而列举所见有编号小于链底的快照，即放弃本次回收（INCOMPLETE）；
//! 3. 读在用快照登记（[`crate::in_use::InUseSnapshots::oldest`]），不删编号不小于其中最小者的快照；
//! 4. 按编号从小到大删除列举所见、编号小于保留起点的快照文件；与保留起点之间至少缺一个编号的是**重现快照**。
//!
//! 回收只删快照文件，不写任何对象，不经提交器；读路径不登记停用状态（NO-DISABLE）。对象存储不可访问即放弃本次，已删的
//! 不恢复。定出的保留起点交给同一周期的巡检判「断档即放弃」（REQ-AMB-STO-HOT-FSCK-V3）。

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use amberlog_common::dataset::Dataset;

use crate::chain::SnapshotId;
use crate::fault;
use crate::format::{self, SnapshotContent};
use crate::hot_fsck::FsckEvents;
use crate::in_use::InUseSnapshots;
use crate::layout;
use crate::store::Store;

/// 热层快照的保留设置。
#[derive(Debug, Clone, Copy)]
pub struct RetentionConfig {
    /// `hot_retention.snapshots`（不小于 2）
    pub snapshots: u64,
    /// `hot_retention.age`
    pub age: Duration,
}

/// 回收或巡检放弃本次的原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Abandon {
    /// 对象存储不可访问、停机或提交失败（不输出 warn 日志）。
    Silent,
    /// 读不全或断档（输出 warn 日志）：原因的英文短语，不承诺稳定。
    Incomplete(&'static str),
}

/// 一次定出的保留起点及其依据。
#[derive(Debug, Clone)]
pub(crate) struct Retention {
    /// 保留起点 R：编号小于它的快照文件都可回收。
    pub start: SnapshotId,
    /// 同一次列举所见的快照编号。
    pub listed: BTreeSet<SnapshotId>,
}

/// 列举数据集 `manifest/` 下（不含子前缀）符合快照路径模板的对象编号。
pub(crate) async fn list_snapshots(
    store: &Store,
    dataset: &Dataset,
) -> Result<BTreeSet<SnapshotId>, Abandon> {
    let dir = store.dataset_prefix(dataset).join(layout::MANIFEST_DIR);
    let (objects, _) = store.list_level(&dir).await.map_err(|_| Abandon::Silent)?;
    Ok(objects
        .iter()
        .filter_map(|o| o.location.filename())
        .filter_map(|name| layout::parse_snapshot_path(&format!("{}/{name}", layout::MANIFEST_DIR)))
        .collect())
}

/// 读提示：不存在为 `None`；内容无法解析即放弃。
pub(crate) async fn read_hint(
    store: &Store,
    dataset: &Dataset,
) -> Result<Option<SnapshotId>, Abandon> {
    match store
        .read(&store.object_path(dataset, layout::CURRENT))
        .await
        .map_err(|_| Abandon::Silent)?
    {
        None => Ok(None),
        Some(f) => format::decode_current(&f.bytes)
            .map(Some)
            .map_err(|_| Abandon::Incomplete("the hint is unparseable")),
    }
}

/// 找链尾（REQ-AMB-STO-MANIFEST-COMMIT-V4）：从 `hint` + 1 起逐个探测，不登记停用状态。尚无任何快照为 `None`。
pub(crate) async fn find_tail(
    store: &Store,
    dataset: &Dataset,
    hint: Option<SnapshotId>,
) -> Result<Option<SnapshotId>, Abandon> {
    let mut current = hint;
    loop {
        let next = SnapshotId(current.map_or(1, |c| c.0 + 1));
        let exists = store
            .exists(&store.object_path(dataset, &layout::snapshot_path(next)))
            .await
            .map_err(|_| Abandon::Silent)?;
        if !exists {
            return Ok(current);
        }
        current = Some(next);
    }
}

/// 快照在回溯中要用到的两项：父编号与 `created_at`。
pub(crate) type Link = (Option<SnapshotId>, i64);

/// 读快照 `id` 的 [`Link`]：不存在为 `None`；内容无法解析或格式版本不兼容即放弃（不登记停用）。
pub(crate) async fn read_link(
    store: &Store,
    dataset: &Dataset,
    id: SnapshotId,
) -> Result<Option<Link>, Abandon> {
    let Some(f) = store
        .read(&store.object_path(dataset, &layout::snapshot_path(id)))
        .await
        .map_err(|_| Abandon::Silent)?
    else {
        return Ok(None);
    };
    match format::decode_snapshot(&f.bytes, dataset, id) {
        Ok(SnapshotContent::Supported(s)) => Ok(Some((s.parent_snapshot_id, s.created_at))),
        _ => Err(Abandon::Incomplete(
            "a snapshot on the chain is unparseable",
        )),
    }
}

/// 快照写出后不可变，回溯所读的 [`Link`] 按（数据集、编号）跨次缓存；某快照是否仍存在以同一次列举为准。
#[derive(Default)]
pub(crate) struct Links(Mutex<HashMap<Dataset, BTreeMap<SnapshotId, Link>>>);

impl Links {
    fn get(&self, dataset: &Dataset, id: SnapshotId) -> Option<Link> {
        self.lock().get(dataset).and_then(|m| m.get(&id).copied())
    }

    fn put(&self, dataset: &Dataset, id: SnapshotId, link: Link) {
        self.lock()
            .entry(dataset.clone())
            .or_default()
            .insert(id, link);
    }

    /// 编号小于 `below` 的不再需要。
    fn prune(&self, dataset: &Dataset, below: SnapshotId) {
        if let Some(m) = self.lock().get_mut(dataset) {
            *m = m.split_off(&below);
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<Dataset, BTreeMap<SnapshotId, Link>>> {
        self.0.lock().expect("快照回溯缓存锁未中毒")
    }

    /// 列举所见的快照 `id` 的 [`Link`]：未列举到为 `None`。
    pub(crate) async fn link(
        &self,
        store: &Store,
        dataset: &Dataset,
        listed: &BTreeSet<SnapshotId>,
        id: SnapshotId,
    ) -> Result<Option<Link>, Abandon> {
        if !listed.contains(&id) {
            return Ok(None);
        }
        if let Some(l) = self.get(dataset, id) {
            return Ok(Some(l));
        }
        let link = read_link(store, dataset, id).await?;
        if let Some(l) = link {
            self.put(dataset, id, l);
        }
        Ok(link)
    }
}

/// 定出数据集 `dataset` 本次的保留起点（RETAIN-START、TERM-BOTTOM、INCOMPLETE）；`now` 为开始时刻。巡检在同一周期回收
/// 未定出时以同一段代码自行确定。
pub(crate) async fn retain_start(
    store: &Store,
    links: &Links,
    dataset: &Dataset,
    config: RetentionConfig,
    now: i64,
) -> Result<Retention, Abandon> {
    let hint = read_hint(store, dataset).await?;
    let tail = find_tail(store, dataset, hint)
        .await?
        .ok_or(Abandon::Incomplete("no current snapshot"))?;
    let listed = list_snapshots(store, dataset).await?;
    let horizon = now.saturating_sub(micros(config.age));

    // 自当前快照回溯，直到遇到早于保留时长的快照，或到链底
    let mut id = tail;
    let (older, bottom, genesis) = loop {
        let Some((parent, created_at)) = links.link(store, dataset, &listed, id).await? else {
            // 当前快照，或列举之后才不见的父快照
            return Err(Abandon::Incomplete(
                "a snapshot on the chain does not exist",
            ));
        };
        if created_at < horizon {
            break (Some(id), id, false);
        }
        match parent {
            None => break (None, id, true),
            Some(p) if listed.contains(&p) => id = p,
            Some(_) => break (None, id, false),
        }
    };
    let by_age = match older {
        Some(o) => o,
        None => {
            if !genesis && listed.range(..bottom).next().is_some() {
                return Err(Abandon::Incomplete(
                    "snapshots exist below the chain bottom",
                ));
            }
            bottom
        }
    };
    let by_count = SnapshotId((tail.0 + 1).saturating_sub(config.snapshots));
    let start = by_count.min(by_age).min(hint.unwrap_or(SnapshotId(0)));
    links.prune(dataset, start);
    Ok(Retention { start, listed })
}

/// 对数据集 `dataset` 执行一次快照回收，返回定出的保留起点（定出之前放弃为 `None`）。`stopping` 为真时不再发出新的
/// 删除请求。
pub(crate) async fn collect(
    store: &Store,
    links: &Links,
    in_use: &InUseSnapshots,
    dataset: &Dataset,
    config: RetentionConfig,
    events: &dyn FsckEvents,
    stopping: &(dyn Fn() -> bool + Sync),
) -> Option<SnapshotId> {
    let now = crate::lifecycle::now_us();
    let retention = match retain_start(store, links, dataset, config, now).await {
        Ok(r) => r,
        Err(Abandon::Incomplete(reason)) => {
            events.abandoned(dataset, "snapshot_gc", reason);
            return None;
        }
        Err(Abandon::Silent) => return None,
    };
    let start = retention.start;
    fault::gate("snapshot_gc.before_delete").await;
    // 在定出保留起点之后读在用登记（见 [`InUseSnapshots`]）
    let limit = in_use.oldest(dataset).map_or(start, |o| o.min(start));
    for &id in retention.listed.range(..limit) {
        if stopping() {
            break;
        }
        let resurrected = (id.0 + 1..start.0).any(|k| !retention.listed.contains(&SnapshotId(k)));
        match store
            .delete(&store.object_path(dataset, &layout::snapshot_path(id)))
            .await
        {
            Ok(true) => events.snapshot_deleted(dataset, id, resurrected),
            Ok(false) => {}
            Err(_) => break,
        }
        fault::pause("snapshot_gc.after_each_delete");
    }
    Some(start)
}

fn micros(d: Duration) -> i64 {
    i64::try_from(d.as_micros()).unwrap_or(i64::MAX)
}

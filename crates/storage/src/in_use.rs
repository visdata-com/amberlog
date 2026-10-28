//! 在用快照登记：本进程的查询与后台任务正在使用哪些热层快照（REQ-AMB-STO-SNAPSHOT-GC-IN-USE）。
//!
//! 这是一份进程内共享状态，不是调用边（与 [`crate::gate::DatasetGates`] 同类）：
//! 读者（查询、治理视图与桶生命周期的各后台任务）在定下要读哪个快照时登记，读完注销；快照回收只读这张表，
//! 不向读者询问。登记随 [`SnapshotPin`] 的生命周期：放下即注销。
//!
//! **先登记、后定快照**。读者按找链尾定下快照之前先以 [`InUseSnapshots::begin`] 登记一个下界（取本进程此刻已知的
//! 最大编号，见 [`crate::reader::StoreChain`] 的 `ChainReader::reading`），定下后以 [`SnapshotPin::settle`] 改为确切编号。
//! 找链尾所得不小于登记时的已知最大编号，所以登记中的下界总不大于读者最终所用的快照。快照回收在定出保留起点之后读
//! [`InUseSnapshots::oldest`]，不删编号不小于它的快照：此前已登记的读者受下界保护；此后才登记的读者，找链尾所得不小于
//! 回收开始时的当前快照，而保留起点不大于它，回收本来就不删。
//!
//! 直接取本进程缓存的最新快照的后台任务（无需找链尾），以 [`InUseSnapshots::pin`] 登记确切编号；取缓存与登记之间
//! 不得有 `.await`（见 [`crate::reader::StoreChain::pin_latest`]）。

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use amberlog_common::dataset::Dataset;

use crate::chain::SnapshotId;

/// 进程内的在用快照登记表，全进程一份（随 [`crate::reader::StoreChain`] 创建）。
#[derive(Debug, Default)]
pub struct InUseSnapshots {
    /// 每个数据集：登记号 → 该登记所保护的最小编号。
    held: Mutex<HashMap<Dataset, BTreeMap<u64, SnapshotId>>>,
    next: AtomicU64,
}

impl InUseSnapshots {
    pub fn new() -> Self {
        InUseSnapshots::default()
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<Dataset, BTreeMap<u64, SnapshotId>>> {
        self.held.lock().expect("在用快照登记锁未中毒")
    }

    /// 读者将要按找链尾定下 `dataset` 的快照：登记下界 `floor`（本进程此刻已知的最大编号；不知道时为 `None`，
    /// 按编号 0 计，即保护全部快照），定下后以 [`SnapshotPin::settle`] 改为确切编号。
    pub fn begin(self: &Arc<Self>, dataset: &Dataset, floor: Option<SnapshotId>) -> SnapshotPin {
        self.register(dataset, floor.unwrap_or(SnapshotId(0)))
    }

    /// 读者已定下要读 `dataset` 的快照 `id`：登记它。
    pub fn pin(self: &Arc<Self>, dataset: &Dataset, id: SnapshotId) -> SnapshotPin {
        self.register(dataset, id)
    }

    fn register(self: &Arc<Self>, dataset: &Dataset, id: SnapshotId) -> SnapshotPin {
        let ticket = self.next.fetch_add(1, Ordering::Relaxed);
        self.lock()
            .entry(dataset.clone())
            .or_default()
            .insert(ticket, id);
        SnapshotPin {
            held: Some((Arc::clone(self), dataset.clone(), ticket)),
        }
    }

    /// `dataset` 现有登记所保护的最小编号；没有登记为 `None`。快照回收不删编号不小于它的快照。
    pub fn oldest(&self, dataset: &Dataset) -> Option<SnapshotId> {
        self.lock()
            .get(dataset)
            .and_then(|pins| pins.values().min().copied())
    }

    fn settle(&self, dataset: &Dataset, ticket: u64, id: SnapshotId) {
        if let Some(slot) = self
            .lock()
            .get_mut(dataset)
            .and_then(|pins| pins.get_mut(&ticket))
        {
            *slot = id;
        }
    }

    fn release(&self, dataset: &Dataset, ticket: u64) {
        let mut held = self.lock();
        if let Some(pins) = held.get_mut(dataset) {
            pins.remove(&ticket);
            if pins.is_empty() {
                held.remove(dataset);
            }
        }
    }
}

/// 一次登记；放下即注销。不属于任何登记表的（[`SnapshotPin::detached`]）什么也不保护，供测试替身与不经对象存储的读取。
#[derive(Debug)]
#[must_use = "放下即注销：要保护快照，须持有到读完其所引用的对象"]
pub struct SnapshotPin {
    held: Option<(Arc<InUseSnapshots>, Dataset, u64)>,
}

impl SnapshotPin {
    /// 不登记的空登记。
    pub fn detached() -> Self {
        SnapshotPin { held: None }
    }

    /// 读者已定下快照 `id`（找链尾所得）：登记改为它。
    pub fn settle(&mut self, id: SnapshotId) {
        if let Some((table, dataset, ticket)) = &self.held {
            table.settle(dataset, *ticket, id);
        }
    }
}

impl Drop for SnapshotPin {
    fn drop(&mut self) {
        if let Some((table, dataset, ticket)) = self.held.take() {
            table.release(&dataset, ticket);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn app() -> Dataset {
        Dataset::new("default", "app")
    }

    #[test]
    fn oldest_is_the_minimum_of_live_pins() {
        let t = Arc::new(InUseSnapshots::new());
        assert_eq!(t.oldest(&app()), None);
        let a = t.pin(&app(), SnapshotId(7));
        let b = t.pin(&app(), SnapshotId(5));
        let other = t.pin(&Dataset::new("default", "web"), SnapshotId(1));
        assert_eq!(t.oldest(&app()), Some(SnapshotId(5)));
        drop(b);
        assert_eq!(t.oldest(&app()), Some(SnapshotId(7)));
        drop(a);
        assert_eq!(t.oldest(&app()), None);
        drop(other);
        assert!(t.lock().is_empty());
    }

    #[test]
    fn begin_protects_from_the_floor_until_settled() {
        let t = Arc::new(InUseSnapshots::new());
        let mut p = t.begin(&app(), None);
        assert_eq!(t.oldest(&app()), Some(SnapshotId(0)));
        p.settle(SnapshotId(9));
        assert_eq!(t.oldest(&app()), Some(SnapshotId(9)));
        let mut q = t.begin(&app(), Some(SnapshotId(4)));
        assert_eq!(t.oldest(&app()), Some(SnapshotId(4)));
        q.settle(SnapshotId(10));
        assert_eq!(t.oldest(&app()), Some(SnapshotId(9)));
        drop(p);
        drop(q);
        assert_eq!(t.oldest(&app()), None);
    }

    #[test]
    fn detached_pin_protects_nothing() {
        let mut p = SnapshotPin::detached();
        p.settle(SnapshotId(3));
        drop(p);
    }
}

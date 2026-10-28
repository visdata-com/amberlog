//! stream 存在性登记表（REQ-AMB-DM-NAMESPACE）。
//!
//! 「stream 是否存在」以三者之并为准：开始受理 `/api/` 请求之前在对象存储中找到的、WAL 重放恢复出其存在的、
//! 本进程自动创建的。三处来源各自往这张表里登记，自动创建的「尚不存在」与查询的 `stream_not_found`
//! 都只查这张表，不在请求路径上列举对象存储。表里同时记着每个 stream 的存在持久到了哪一步，
//! 供摄取判断应答前是否还要先持久化其存在、供查询判断是否只以内存作答（ADR-0021 第 3 条②）。
//!
//! 进程内共享状态，不是调用边；只进不退，直到进程重启。

use std::collections::HashMap;
use std::sync::RwLock;

use amberlog_common::dataset::Dataset;

/// 一个 stream 的存在持久到了哪一步。取值顺序即先后：登记只会往后走。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Persistence {
    /// 存在尚未持久化：自动创建时 WAL 写不进去、以 5xx 应答，只在本进程内成立，
    /// 不经此后的 WAL 重放恢复。
    Unpersisted,
    /// 存在已写入本地 WAL 并 fsync，创世快照尚未提交成功。
    LocalWal,
    /// 在对象存储中存在（启动时找到，或本进程已提交其创世快照）。
    Stored,
}

/// 全部已存在的 stream。
#[derive(Debug, Default)]
pub struct DatasetRegistry {
    datasets: RwLock<HashMap<Dataset, Persistence>>,
}

impl DatasetRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// 该 stream 是否存在（三者之并）。
    pub fn contains(&self, dataset: &Dataset) -> bool {
        self.persistence(dataset).is_some()
    }

    /// 该 stream 的存在持久到了哪一步；不存在为 `None`。
    pub fn persistence(&self, dataset: &Dataset) -> Option<Persistence> {
        self.datasets
            .read()
            .expect("登记表锁未中毒")
            .get(dataset)
            .copied()
    }

    /// 登记 `dataset` 的存在已持久到 `persistence`（不存在则加入）。只进不退：已登记到更靠后的一步时不变。
    /// 返回登记之后的取值。
    pub fn record(&self, dataset: &Dataset, persistence: Persistence) -> Persistence {
        let mut datasets = self.datasets.write().expect("登记表锁未中毒");
        let entry = datasets.entry(dataset.clone()).or_insert(persistence);
        *entry = (*entry).max(persistence);
        *entry
    }

    /// 全部已存在的 stream，按标识排序。
    pub fn datasets(&self) -> Vec<Dataset> {
        let mut all: Vec<Dataset> = self
            .datasets
            .read()
            .expect("登记表锁未中毒")
            .keys()
            .cloned()
            .collect();
        all.sort();
        all
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_is_union_and_only_advances() {
        let r = DatasetRegistry::new();
        let app = Dataset::new("default", "app");
        let web = Dataset::new("default", "web");
        assert!(!r.contains(&app));

        // 自动创建时 WAL 写不进去：本进程内存在
        assert_eq!(
            r.record(&app, Persistence::Unpersisted),
            Persistence::Unpersisted
        );
        assert!(r.contains(&app));
        // 此后写入 WAL 成功、再提交了创世快照
        r.record(&app, Persistence::LocalWal);
        assert_eq!(r.record(&app, Persistence::Stored), Persistence::Stored);
        // 不回退
        assert_eq!(r.record(&app, Persistence::LocalWal), Persistence::Stored);

        // 启动时在对象存储中找到
        r.record(&web, Persistence::Stored);
        assert_eq!(r.datasets(), vec![app, web]);
    }
}

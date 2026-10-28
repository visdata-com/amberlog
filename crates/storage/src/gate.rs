//! 数据集闸门状态：按数据集记录的两种停用状态（REQ-AMB-STO-HOT-LAYOUT-V2「格式版本不兼容情形」「manifest 不可读情形」）。
//!
//! 这是一份进程内共享状态，不是调用边：读到对象存储的各方（查询、快照提交、
//! 生命周期编排的启动读取）写入，接入面的状态闸门、摄取、查询、物化与快照提交读取。一经进入即保持到进程重启，
//! 不以此刻对象存储中的状态为条件。
//!
//! 数据集首次进入停用状态时调用一次观察者（稳定日志事件 `dataset_disabled`，REQ-AMB-CFG-STARTUP-V4），并记下首先进入的
//! 状态（`amberlog_dataset_disabled` 的 `state` 取先读到者，REQ-AMB-OBS-HEALTH-METRICS-V3）。

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use amberlog_common::dataset::Dataset;
use amberlog_common::error::Reason;

/// 一个数据集的状态。取值顺序即报告的先后：两种停用状态同时成立时报格式版本不兼容情形。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum DatasetState {
    #[default]
    Normal,
    /// manifest 不可读情形：`_json`、`_sent` 与 `_search` 一律 500 `internal`。
    ManifestUnreadable,
    /// 格式版本不兼容情形：`_json`、`_sent` 与 `_search` 一律 503 `unsupported_format_version`。
    IncompatibleFormat,
}

impl DatasetState {
    /// 停用状态对应的应答 `reason`；`Normal` 为 `None`。
    pub fn reason(self) -> Option<Reason> {
        match self {
            DatasetState::Normal => None,
            DatasetState::ManifestUnreadable => Some(Reason::Internal),
            DatasetState::IncompatibleFormat => Some(Reason::UnsupportedFormatVersion),
        }
    }

    /// 停用状态在 `dataset_disabled` 事件与 `amberlog_dataset_disabled` 中的 `state` 取值；`Normal` 为 `None`。
    pub fn label(self) -> Option<&'static str> {
        match self {
            DatasetState::Normal => None,
            DatasetState::ManifestUnreadable => Some("manifest_unreadable"),
            DatasetState::IncompatibleFormat => Some("format_version"),
        }
    }
}

/// 数据集首次进入停用状态时的观察者。
pub type DisabledObserver = Arc<dyn Fn(&Dataset, DatasetState) + Send + Sync>;

/// 全部数据集的闸门状态。
#[derive(Default)]
pub struct DatasetGates {
    states: RwLock<HashMap<Dataset, State>>,
    observer: Option<DisabledObserver>,
}

#[derive(Clone, Copy)]
struct State {
    now: DatasetState,
    /// 首先进入的停用状态。
    first: DatasetState,
}

impl std::fmt::Debug for DatasetGates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatasetGates").finish_non_exhaustive()
    }
}

impl DatasetGates {
    pub fn new() -> Self {
        Self::default()
    }

    /// 数据集首次进入停用状态时调用 `observer`（每个数据集至多一次）。
    pub fn with_observer(observer: DisabledObserver) -> Self {
        DatasetGates {
            states: RwLock::default(),
            observer: Some(observer),
        }
    }

    pub fn state(&self, dataset: &Dataset) -> DatasetState {
        self.states
            .read()
            .expect("闸门状态锁未中毒")
            .get(dataset)
            .map_or(DatasetState::Normal, |s| s.now)
    }

    /// 使数据集进入 `state`。只进不退：已处于更靠后的状态时不变。
    pub fn enter(&self, dataset: &Dataset, state: DatasetState) {
        if state == DatasetState::Normal {
            return;
        }
        let first = {
            let mut states = self.states.write().expect("闸门状态锁未中毒");
            match states.get_mut(dataset) {
                Some(current) => {
                    current.now = current.now.max(state);
                    false
                }
                None => {
                    states.insert(
                        dataset.clone(),
                        State {
                            now: state,
                            first: state,
                        },
                    );
                    true
                }
            }
        };
        if first && let Some(observe) = &self.observer {
            observe(dataset, state);
        }
    }

    /// 数据集首先进入的停用状态；未停用为 [`DatasetState::Normal`]（台账的 `status` 取先读到者，REQ-AMB-GOV-LEDGER）。
    pub fn first_state(&self, dataset: &Dataset) -> DatasetState {
        self.states
            .read()
            .expect("闸门状态锁未中毒")
            .get(dataset)
            .map_or(DatasetState::Normal, |s| s.first)
    }

    /// 已进入停用状态的数据集及其首先进入的状态，按标识排序。
    pub fn disabled(&self) -> Vec<(Dataset, DatasetState)> {
        let mut all: Vec<(Dataset, DatasetState)> = self
            .states
            .read()
            .expect("闸门状态锁未中毒")
            .iter()
            .map(|(d, s)| (d.clone(), s.first))
            .collect();
        all.sort();
        all
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gate_is_sticky_and_incompatible_wins() {
        let gates = DatasetGates::new();
        let a = Dataset::new("default", "a");
        let b = Dataset::new("default", "b");
        assert_eq!(gates.state(&a), DatasetState::Normal);

        gates.enter(&a, DatasetState::ManifestUnreadable);
        gates.enter(&a, DatasetState::Normal);
        assert_eq!(gates.state(&a), DatasetState::ManifestUnreadable);

        gates.enter(&a, DatasetState::IncompatibleFormat);
        gates.enter(&a, DatasetState::ManifestUnreadable);
        assert_eq!(gates.state(&a), DatasetState::IncompatibleFormat);
        assert_eq!(
            gates.state(&a).reason(),
            Some(Reason::UnsupportedFormatVersion)
        );

        assert_eq!(gates.state(&b), DatasetState::Normal, "不牵连其他数据集");
        assert_eq!(
            gates.disabled(),
            vec![(a, DatasetState::ManifestUnreadable)],
            "按首先进入的状态报告"
        );
    }

    #[test]
    fn observer_fires_once_per_dataset() {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let s = Arc::clone(&seen);
        let gates = DatasetGates::with_observer(Arc::new(move |d, st| {
            s.lock().unwrap().push((d.clone(), st))
        }));
        let a = Dataset::new("default", "a");
        gates.enter(&a, DatasetState::Normal);
        gates.enter(&a, DatasetState::IncompatibleFormat);
        gates.enter(&a, DatasetState::ManifestUnreadable);
        gates.enter(&a, DatasetState::IncompatibleFormat);
        assert_eq!(
            *seen.lock().unwrap(),
            vec![(a.clone(), DatasetState::IncompatibleFormat)]
        );
        assert_eq!(
            DatasetState::IncompatibleFormat.label(),
            Some("format_version")
        );
        assert_eq!(
            DatasetState::ManifestUnreadable.label(),
            Some("manifest_unreadable")
        );
    }
}

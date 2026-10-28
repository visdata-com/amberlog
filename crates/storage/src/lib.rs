//! 耐久缓冲（WAL）、可见集（Memtable）、物化（Flush）与快照提交（Commit）。
//!
//! 供其他 crate 使用的接口：[`wal::Wal`]、[`memtable::MemtableView`]、[`commit::Committer`]、[`chain::ChainReader`]
//! 与进程内共享的 [`gate::DatasetGates`]；另有热层的四种生命周期提交 [`commit::HotCommits`]、冷层表的读写
//! [`cold::ColdReader`] 与 [`cold::ColdWriter`]、桶生命周期的周期工作 [`maintenance::BucketTask`]，其中 TTL 到期删除在 [`ttl`]。
//! 另有进程内共享的在用快照登记 [`in_use::InUseSnapshots`]。
//! 测试替身在 [`testing`]（`testing` feature）。

use std::future::Future;
use std::pin::Pin;

pub mod archive;
pub mod audit;
pub mod bloom;
pub mod chain;
pub mod cold;
pub mod cold_compaction;
pub mod cold_retention;
pub mod commit;
pub mod committer;
pub mod compaction;
pub mod evidence;
pub mod fault;
pub mod flush;
pub mod flusher;
pub mod format;
pub mod gate;
pub mod hot_fsck;
pub mod in_use;
pub mod lake;
pub mod layout;
pub mod lifecycle;
pub mod maintenance;
pub mod memory;
pub mod memtable;
pub mod objects;
pub mod purge;
pub mod reader;
pub mod recovery;
pub mod registry;
pub mod retire;
pub mod scan;
pub mod schema;
pub mod snapshot_gc;
pub mod stats;
pub mod store;
#[cfg(any(test, feature = "testing"))]
pub mod testing;
pub mod ttl;
pub mod wal;

/// 接口里的异步方法返回的 future：装箱以便各接口能以 `Arc<dyn …>` 在真实实现与测试替身之间替换。
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

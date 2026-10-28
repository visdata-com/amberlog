//! 进程内测试共用：真实的 FileWal 与可见集，快照提交用替身；「重启」= 丢掉全部内存状态、以同一 `wal.dir` 重放。

#![allow(dead_code)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use amberlog_common::dataset::Dataset;
use amberlog_ingest::pipeline::Pipeline;
use amberlog_ingest::timestamp::TimestampWindow;
use amberlog_storage::gate::DatasetGates;
use amberlog_storage::memtable::{Memtable, MemtableView};
use amberlog_storage::recovery::{Parts, recover};
use amberlog_storage::registry::DatasetRegistry;
use amberlog_storage::testing::FakeCommitter;
use amberlog_storage::wal::{FileWal, Wal};
use arrow::array::{Array, StringArray};

pub const NOW_US: i64 = 1_790_000_000_000_000;

pub struct TempDir(pub PathBuf);

impl TempDir {
    pub fn new(tag: &str) -> TempDir {
        static N: AtomicU32 = AtomicU32::new(0);
        let p = std::env::temp_dir().join(format!(
            "amberlog-{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// 一个「进程」：以 `wal_dir` 打开并重放 WAL，把重放结果写回新的可见集，再建摄取管道。
pub struct Process {
    pub memtable: Arc<Memtable>,
    pub pipeline: Pipeline,
}

impl Process {
    pub async fn start(wal_dir: &Path) -> Process {
        let wal = Arc::new(FileWal::open(wal_dir));
        let mut entries = Vec::new();
        wal.replay(&mut |p, e| entries.push((p, e))).unwrap();
        let memtable = Arc::new(Memtable::new());
        let committer = Arc::new(FakeCommitter::new());
        let gates = DatasetGates::new();
        let registry = Arc::new(DatasetRegistry::new());
        recover(
            entries,
            &[],
            Parts {
                wal: wal.as_ref(),
                committer: committer.as_ref(),
                memtable: memtable.as_ref(),
                writer: memtable.as_ref(),
                gates: &gates,
                registry: &registry,
                max_columns: 1000,
            },
        )
        .await;
        let pipeline = Pipeline::new(
            wal,
            memtable.clone(),
            memtable.clone(),
            committer,
            registry,
            TimestampWindow::default(),
            HashMap::new(),
        );
        Process { memtable, pipeline }
    }

    /// 可见集中该数据集某个 Utf8 列的全部非空取值（相当于未提交任何快照时 `SELECT <column> FROM <stream>`）。
    pub fn strings(&self, dataset: &Dataset, column: &str) -> Vec<String> {
        let mut out = Vec::new();
        for batch in self.memtable.candidates(dataset).participating(None) {
            if let Some(col) = batch.column_by_name(column) {
                let col = col.as_any().downcast_ref::<StringArray>().unwrap();
                for i in 0..col.len() {
                    if col.is_valid(i) {
                        out.push(col.value(i).to_owned());
                    }
                }
            }
        }
        out
    }
}

pub fn ds(stream: &str) -> Dataset {
    Dataset::new("default", stream)
}

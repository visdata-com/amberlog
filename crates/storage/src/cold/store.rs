//! [`ColdReader`]、[`ColdWriter`] 的对象存储实现，与冷层孤儿清理的周期工作（[`ColdOrphans`]）。
//!
//! 返回 [`ColdWriteError::Unavailable`] 时进度按数据集保留：调用方以同一请求再调（入湖以 (桶, H, 各输入的摘要)、
//! 冷层删除以 (cutoff, paths) 认作同一次提交），沿用已写好的冷层数据文件与同号、逐字节相同的元数据续做
//! （REQ-AMB-STO-COLD-TABLE-SAME-CONTENT）；请求变了即放弃旧进度（其在途登记随之撤销）、按新提交处理。

use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use amberlog_common::dataset::Dataset;
use amberlog_common::schema::ColumnType;
use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use iceberg::spec::{PrimitiveType, Type};

use super::commit::{Change, Committer, OPERATION, Progress, Select};
use super::error::ColdError;
use super::lake::{self, Rewritten};
use super::layout::ColdLayout;
use super::{
    ColdAppend, ColdBatches, ColdDataFile, ColdDelete, ColdDeleted, ColdExpire, ColdExpired,
    ColdField, ColdOperation, ColdReadError, ColdReader, ColdReplace, ColdReplaced, ColdScope,
    ColdSnapshot, ColdState, ColdTable, ColdUpkeep, ColdWriteError, ColdWriter, compact, current,
    orphan, read,
};
use crate::BoxFuture;
use crate::bloom::{self, FileBlooms, Pick, StoreObject};
use crate::chain::{ColdFileEntry, Digest, HandoverDraft, SnapshotId};
use crate::fault;
use crate::gate::{DatasetGates, DatasetState};
use crate::layout::HourBucket;
use crate::maintenance::BucketTask;
use crate::registry::{DatasetRegistry, Persistence};
use crate::store::Store;

/// 入湖的一次冷层提交在各轮调用之间保留的内容。
struct PendingAppend {
    key: (HourBucket, SnapshotId, Vec<Digest>),
    rewritten: Rewritten,
    progress: Progress,
}

/// TTL 的一次冷层提交在各轮调用之间保留的内容。
struct PendingDelete {
    key: (i64, Vec<String>),
    progress: Progress,
}

/// 冷层合并的一次冷层提交在各轮调用之间保留的内容。
struct PendingReplace {
    key: Vec<(String, Digest)>,
    rewritten: Rewritten,
    progress: Progress,
}

/// 按数据集保留的进度，每个数据集至多一份。
struct Pending<T>(Mutex<HashMap<Dataset, T>>);

impl<T> Default for Pending<T> {
    fn default() -> Self {
        Pending(Mutex::default())
    }
}

impl<T> Pending<T> {
    fn take(&self, dataset: &Dataset) -> Option<T> {
        self.0.lock().expect("进度表未中毒").remove(dataset)
    }

    fn keep(&self, dataset: &Dataset, p: T) {
        self.0
            .lock()
            .expect("进度表未中毒")
            .insert(dataset.clone(), p);
    }
}

/// 冷层表的对象存储实现：读取与 `_iceberg/` 的唯一写者。
pub struct StoreCold {
    committer: Committer,
    /// `compaction.target_file_bytes`：入湖写出的每个冷层数据文件不超过它（REQ-AMB-STO-LAKE-ENTRY-SIZE）。
    target_file_bytes: u64,
    /// 入湖与 TTL 的进度分开保留：一方的调用不丢掉另一方结果不明的提交（否则入湖重发时会把已写成的提交再追加一次）。
    appends: Pending<PendingAppend>,
    deletes: Pending<PendingDelete>,
    /// 冷层合并与冷层过期提交的进度同样各自保留。
    replaces: Pending<PendingReplace>,
    expires: Pending<Progress>,
}

impl StoreCold {
    /// `max_retries` 为 `storage.commit_max_retries`，`target_file_bytes` 为 `compaction.target_file_bytes`。
    pub fn new(
        store: Store,
        gates: Arc<DatasetGates>,
        max_retries: u32,
        target_file_bytes: u64,
    ) -> StoreCold {
        let layout = ColdLayout::new(&store.location, store.root.clone());
        StoreCold {
            committer: Committer::new(store, layout, gates, max_retries),
            target_file_bytes,
            appends: Pending::default(),
            deletes: Pending::default(),
            replaces: Pending::default(),
            expires: Pending::default(),
        }
    }

    pub fn layout(&self) -> &ColdLayout {
        &self.committer.layout
    }

    #[cfg(test)]
    pub(super) fn committer_for_test(&self) -> &Committer {
        &self.committer
    }

    async fn append_inner(
        &self,
        dataset: &Dataset,
        request: ColdAppend,
    ) -> Result<HandoverDraft, ColdWriteError> {
        let c = &self.committer;
        let _guard = c.lock(dataset).await;
        let state = c.gates.state(dataset);
        if state != DatasetState::Normal {
            return Err(ColdWriteError::Disabled(state));
        }
        let key = (
            request.bucket,
            request.hot_snapshot_id,
            request.inputs.iter().map(|b| Digest::of(b)).collect(),
        );
        let (rewritten, mut progress) = match self.appends.take(dataset) {
            Some(PendingAppend {
                key: k,
                rewritten,
                progress,
            }) if k == key => (rewritten, progress),
            _ => {
                let rewritten = lake::rewrite(
                    c,
                    dataset,
                    &request.schema,
                    &request.inputs,
                    self.target_file_bytes,
                    &request.bloom,
                )
                .await
                .map_err(|e| self.lake_error(dataset, e))?;
                (rewritten, Progress::new(dataset, c.inflight()))
            }
        };
        let result = lake::commit(
            c,
            dataset,
            request.bucket,
            request.hot_snapshot_id,
            &request.schema,
            &rewritten,
            &mut progress,
        )
        .await;
        match result {
            Ok(done) => Ok(HandoverDraft {
                table_location: c.layout.table_location(dataset),
                metadata_version: done.version,
                iceberg_snapshot_id: done.snapshot_id,
                files: rewritten
                    .files()
                    .into_iter()
                    .map(|f| ColdFileEntry {
                        path: f.path,
                        min_timestamp: f.min_timestamp,
                        max_timestamp: f.max_timestamp,
                        record_count: f.record_count,
                        size_bytes: f.size_bytes,
                        content_digest: f.digest,
                    })
                    .collect(),
            }),
            Err(ColdError::Unavailable(e)) => {
                self.appends.keep(
                    dataset,
                    PendingAppend {
                        key,
                        rewritten,
                        progress,
                    },
                );
                Err(ColdWriteError::Unavailable(e.0))
            }
            Err(e) => Err(self.lake_error(dataset, e)),
        }
    }

    /// 入湖读取冷层当前版本所需的冷层对象得到不存在或无法解析：数据集进入 manifest 不可读情形
    /// （REQ-AMB-STO-COLD-TABLE-READ-BROKEN）。
    fn lake_error(&self, dataset: &Dataset, e: ColdError) -> ColdWriteError {
        match e {
            ColdError::Broken(_) | ColdError::Missing(_) => {
                self.committer
                    .gates
                    .enter(dataset, DatasetState::ManifestUnreadable);
                ColdWriteError::Disabled(DatasetState::ManifestUnreadable)
            }
            e => write_error(e),
        }
    }

    async fn delete_inner(
        &self,
        dataset: &Dataset,
        request: ColdDelete,
    ) -> Result<ColdDeleted, ColdWriteError> {
        let c = &self.committer;
        let _guard = c.lock(dataset).await;
        let key = (request.cutoff, request.paths.clone());
        let mut progress = match self.deletes.take(dataset) {
            Some(PendingDelete { key: k, progress }) if k == key => progress,
            _ => Progress::new(dataset, c.inflight()),
        };
        let change = Change::Delete {
            paths: request.paths.iter().cloned().collect(),
            summary: [
                (OPERATION.to_owned(), "ttl".to_owned()),
                ("amberlog.ttl_cutoff".to_owned(), request.cutoff.to_string()),
            ]
            .into(),
        };
        match c.commit(dataset, &change, &mut progress).await {
            Ok(done) => Ok(ColdDeleted {
                metadata_version: done.version,
                iceberg_snapshot_id: done.snapshot_id,
                removed: done.removed,
            }),
            Err(ColdError::Unavailable(e)) => {
                self.deletes.keep(dataset, PendingDelete { key, progress });
                Err(ColdWriteError::Unavailable(e.0))
            }
            // TTL 的读取不使数据集停用（REQ-AMB-STO-COLD-TABLE-READ-BROKEN、REQ-AMB-STO-TTL）
            Err(e) => Err(write_error(e)),
        }
    }
}

impl StoreCold {
    async fn replace_inner(
        &self,
        dataset: &Dataset,
        request: ColdReplace,
    ) -> Result<ColdReplaced, ColdWriteError> {
        let c = &self.committer;
        let state = c.gates.state(dataset);
        if state != DatasetState::Normal {
            return Err(ColdWriteError::Disabled(state));
        }
        let key: Vec<(String, Digest)> = request
            .inputs
            .iter()
            .map(|(p, b)| (p.clone(), Digest::of(b)))
            .collect();
        // 重写与上传不持锁：合并在上传之后暂停时，同一数据集的入湖与 TTL 的冷层提交照常进行（-RECHECK 的前提）
        let (rewritten, mut progress) = match self.replaces.take(dataset) {
            Some(PendingReplace {
                key: k,
                rewritten,
                progress,
            }) if k == key => (rewritten, progress),
            _ => {
                let rewritten = compact::rewrite(
                    c,
                    dataset,
                    &request.inputs,
                    self.target_file_bytes,
                    &request.bloom,
                )
                .await
                .map_err(|e| self.lake_error(dataset, e))?;
                (rewritten, Progress::new(dataset, c.inflight()))
            }
        };
        let uploaded = compact::upload(c, &rewritten, &mut progress).await;
        let result = match uploaded {
            Ok(()) => {
                fault::gate("cold_compaction.after_upload").await;
                let _guard = c.lock(dataset).await;
                compact::commit(c, dataset, &request.inputs, &rewritten, &mut progress).await
            }
            Err(e) => Err(e),
        };
        match result {
            Ok(done) => Ok(ColdReplaced {
                metadata_version: done.version,
                iceberg_snapshot_id: done.snapshot_id,
                outputs: rewritten
                    .files()
                    .into_iter()
                    .map(|f| ColdFileEntry {
                        path: f.path,
                        min_timestamp: f.min_timestamp,
                        max_timestamp: f.max_timestamp,
                        record_count: f.record_count,
                        size_bytes: f.size_bytes,
                        content_digest: f.digest,
                    })
                    .collect(),
            }),
            Err(ColdError::Unavailable(e)) => {
                self.replaces.keep(
                    dataset,
                    PendingReplace {
                        key,
                        rewritten,
                        progress,
                    },
                );
                Err(ColdWriteError::Unavailable(e.0))
            }
            // 有输入已不存活：丢下进度，已上传的合并产出随之撤出在途，留给冷层孤儿清理
            Err(e) => Err(self.lake_error(dataset, e)),
        }
    }

    async fn expire_inner(
        &self,
        dataset: &Dataset,
        request: ColdExpire,
    ) -> Result<Option<ColdExpired>, ColdWriteError> {
        let c = &self.committer;
        let _guard = c.lock(dataset).await;
        let mut progress = self
            .expires
            .take(dataset)
            .unwrap_or_else(|| Progress::new(dataset, c.inflight()));
        let change = Change::Expire {
            select: Select(request.select),
        };
        match c.commit(dataset, &change, &mut progress).await {
            Ok(done) => Ok(Some(ColdExpired {
                metadata_version: done.version,
                removed: done.expired,
            })),
            Err(ColdError::Stale) => Ok(None),
            Err(ColdError::Unavailable(e)) => {
                self.expires.keep(dataset, progress);
                Err(ColdWriteError::Unavailable(e.0))
            }
            // 冷层保留的读取不使数据集停用（REQ-AMB-STO-COLD-TABLE-V2）
            Err(e) => Err(write_error(e)),
        }
    }
}

impl ColdUpkeep for StoreCold {
    fn replace<'a>(
        &'a self,
        dataset: &'a Dataset,
        request: ColdReplace,
    ) -> BoxFuture<'a, Result<ColdReplaced, ColdWriteError>> {
        Box::pin(self.replace_inner(dataset, request))
    }

    fn expire<'a>(
        &'a self,
        dataset: &'a Dataset,
        request: ColdExpire,
    ) -> BoxFuture<'a, Result<Option<ColdExpired>, ColdWriteError>> {
        Box::pin(self.expire_inner(dataset, request))
    }
}

fn write_error(e: ColdError) -> ColdWriteError {
    match e {
        ColdError::Unavailable(e) => ColdWriteError::Unavailable(e.0),
        ColdError::Broken(m) | ColdError::Missing(m) | ColdError::Invalid(m) => {
            ColdWriteError::Integrity(m)
        }
        ColdError::Exhausted => ColdWriteError::Exhausted,
        ColdError::Disabled(state) => ColdWriteError::Disabled(state),
        ColdError::Stale => ColdWriteError::Stale,
        ColdError::Rewrite(m) => ColdWriteError::Rewrite(m),
    }
}

fn read_error(e: ColdError) -> ColdReadError {
    match e {
        ColdError::Unavailable(e) => ColdReadError::Unavailable(e.0),
        ColdError::Missing(m) => ColdReadError::NotFound(m),
        other => ColdReadError::Integrity(other.to_string()),
    }
}

impl ColdWriter for StoreCold {
    fn append<'a>(
        &'a self,
        dataset: &'a Dataset,
        request: ColdAppend,
    ) -> BoxFuture<'a, Result<HandoverDraft, ColdWriteError>> {
        Box::pin(self.append_inner(dataset, request))
    }

    fn delete<'a>(
        &'a self,
        dataset: &'a Dataset,
        request: ColdDelete,
    ) -> BoxFuture<'a, Result<ColdDeleted, ColdWriteError>> {
        Box::pin(self.delete_inner(dataset, request))
    }

    fn in_flight(&self, dataset: &Dataset) -> HashSet<String> {
        self.committer.inflight().snapshot(dataset)
    }
}

pub(super) fn cold_table(view: read::TableView) -> ColdTable {
    ColdTable {
        location: view.location,
        metadata_version: view.version,
        current_snapshot_id: view.current_snapshot_id,
        fields: view
            .fields
            .iter()
            .map(|f| ColdField {
                field_id: f.id as u32,
                name: f.name.clone(),
                column_type: match f.field_type.as_ref() {
                    Type::Primitive(PrimitiveType::Double) => ColumnType::Float64,
                    Type::Primitive(PrimitiveType::Boolean) => ColumnType::Boolean,
                    Type::Primitive(PrimitiveType::String) => ColumnType::Utf8,
                    // `long` 与 `_timestamp` 的 `timestamptz`：热层都是 Int64
                    _ => ColumnType::Int64,
                },
            })
            .collect(),
        snapshots: view
            .snapshots
            .into_iter()
            .map(|s| ColdSnapshot {
                snapshot_id: s.snapshot_id,
                parent_snapshot_id: s.parent_snapshot_id,
                timestamp_ms: s.timestamp_ms,
                operation: match s.operation {
                    read::Operation::Lake {
                        bucket,
                        hot_snapshot_id,
                        file_digests,
                    } => ColdOperation::Lake {
                        bucket,
                        hot_snapshot_id,
                        file_digests,
                    },
                    read::Operation::Ttl { cutoff } => ColdOperation::Ttl { cutoff },
                    read::Operation::Compaction { file_digests } => {
                        ColdOperation::Compaction { file_digests }
                    }
                    read::Operation::Other => ColdOperation::Other,
                },
            })
            .collect(),
        files: view.files.into_iter().map(cold_file).collect(),
    }
}

fn cold_file(f: read::LiveFile) -> ColdDataFile {
    ColdDataFile {
        path: f.path,
        added_snapshot_id: f.added_snapshot_id,
        record_count: f.record_count,
        size_bytes: f.size_bytes,
        min_timestamp: f.min_timestamp,
        max_timestamp: f.max_timestamp,
        day: f.day,
    }
}

impl ColdReader for StoreCold {
    fn table<'a>(
        &'a self,
        dataset: &'a Dataset,
        scope: ColdScope,
    ) -> BoxFuture<'a, Result<ColdState, ColdReadError>> {
        Box::pin(async move {
            Ok(match self.version(dataset).await? {
                None => ColdState::NoTable,
                Some(version) => ColdState::Table(self.table_at(dataset, version, scope).await?),
            })
        })
    }

    fn version<'a>(
        &'a self,
        dataset: &'a Dataset,
    ) -> BoxFuture<'a, Result<Option<u64>, ColdReadError>> {
        Box::pin(async move {
            let c = &self.committer;
            let (version, _) = current::version(&c.store, &c.layout, dataset)
                .await
                .map_err(read_error)?;
            Ok((version > 0).then_some(version))
        })
    }

    fn table_at<'a>(
        &'a self,
        dataset: &'a Dataset,
        version: u64,
        scope: ColdScope,
    ) -> BoxFuture<'a, Result<Arc<ColdTable>, ColdReadError>> {
        Box::pin(async move {
            let days = match scope {
                ColdScope::All => None,
                ColdScope::Days(r) => Some(r),
            };
            let c = &self.committer;
            let view = read::table_at(&c.store, &c.layout, dataset, version, days)
                .await
                .map_err(read_error)?;
            Ok(Arc::new(cold_table(view)))
        })
    }

    fn snapshot_files<'a>(
        &'a self,
        dataset: &'a Dataset,
        table: &'a ColdTable,
        snapshot_id: i64,
    ) -> BoxFuture<'a, Result<Vec<ColdDataFile>, ColdReadError>> {
        Box::pin(async move {
            let c = &self.committer;
            let files = read::snapshot_files(
                &c.store,
                &c.layout,
                dataset,
                table.metadata_version,
                snapshot_id,
            )
            .await
            .map_err(read_error)?;
            Ok(files.into_iter().map(cold_file).collect())
        })
    }

    fn snapshot_references<'a>(
        &'a self,
        dataset: &'a Dataset,
        table: &'a ColdTable,
        snapshot_id: i64,
    ) -> BoxFuture<'a, Result<HashSet<String>, ColdReadError>> {
        Box::pin(async move {
            let c = &self.committer;
            read::snapshot_references(
                &c.store,
                &c.layout,
                dataset,
                table.metadata_version,
                snapshot_id,
            )
            .await
            .map_err(read_error)
        })
    }

    fn referenced<'a>(
        &'a self,
        dataset: &'a Dataset,
        table: &'a ColdTable,
    ) -> BoxFuture<'a, Result<HashSet<String>, ColdReadError>> {
        Box::pin(async move {
            let c = &self.committer;
            read::referenced(&c.store, &c.layout, dataset, table.metadata_version)
                .await
                .map_err(read_error)
        })
    }

    fn file_bytes<'a>(
        &'a self,
        dataset: &'a Dataset,
        path: &'a str,
    ) -> BoxFuture<'a, Result<Bytes, ColdReadError>> {
        Box::pin(async move {
            let c = &self.committer;
            read::file_bytes(&c.store, &c.layout, dataset, path)
                .await
                .map_err(read_error)
        })
    }

    fn blooms<'a>(
        &'a self,
        dataset: &'a Dataset,
        file: &'a ColdDataFile,
        picks: &'a [Pick],
    ) -> BoxFuture<'a, Option<FileBlooms>> {
        Box::pin(async move {
            let c = &self.committer;
            let object = StoreObject {
                store: &c.store,
                path: c.layout.object_path(dataset, &file.path),
            };
            bloom::probe(&object, file.size_bytes, picks).await
        })
    }

    fn read_file<'a>(
        &'a self,
        dataset: &'a Dataset,
        _table: &'a ColdTable,
        file: &'a ColdDataFile,
        target: SchemaRef,
        time: Range<i64>,
    ) -> BoxFuture<'a, Result<ColdBatches, ColdReadError>> {
        Box::pin(async move {
            let c = &self.committer;
            let batches = read::read_file(&c.store, &c.layout, dataset, &file.path, target, time)
                .await
                .map_err(read_error)?;
            Ok(Box::new(batches.map(|b| b.map_err(read_error))) as ColdBatches)
        })
    }
}

/// 冷层孤儿清理（REQ-AMB-STO-COLD-TABLE-ORPHAN-CLEAN）：自开始受理 `/api/` 请求起每经过 `hot_fsck.interval`，
/// 对在对象存储中存在、未停用、已有冷层表的每个数据集做一次 [`orphan::clean`]。停机过程中不开始新的一轮
/// （-SHUTDOWN）；某数据集的一次遇不可访问或读不到清单时只放弃它的这一次（-ORPHAN-ABORT）。
pub struct ColdOrphans {
    cold: Arc<StoreCold>,
    registry: Arc<DatasetRegistry>,
    interval: Duration,
    min_orphan_age: Duration,
    stopping: AtomicBool,
}

impl ColdOrphans {
    /// `interval` 为 `hot_fsck.interval`，`min_orphan_age` 为 `hot_fsck.min_orphan_age`。
    pub fn new(
        cold: Arc<StoreCold>,
        registry: Arc<DatasetRegistry>,
        interval: Duration,
        min_orphan_age: Duration,
    ) -> ColdOrphans {
        ColdOrphans {
            cold,
            registry,
            interval,
            min_orphan_age,
            stopping: AtomicBool::new(false),
        }
    }

    async fn run(&self) {
        let c = &self.cold.committer;
        for dataset in self.registry.datasets() {
            if self.stopping.load(Ordering::SeqCst) {
                return;
            }
            let enabled = || c.gates.state(&dataset) == DatasetState::Normal;
            if self.registry.persistence(&dataset) != Some(Persistence::Stored) || !enabled() {
                continue;
            }
            // 失败（不可访问、读不到清单）只放弃本数据集的这一次，不停用
            let _ = orphan::clean(
                &c.store,
                &c.layout,
                c.inflight(),
                &dataset,
                self.min_orphan_age,
                enabled,
            )
            .await;
        }
    }
}

impl BucketTask for ColdOrphans {
    fn period(&self) -> Duration {
        self.interval
    }

    fn run_once(&self) -> BoxFuture<'_, ()> {
        Box::pin(self.run())
    }

    fn stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
    }
}

//! 冷层提交（REQ-AMB-STO-COLD-TABLE「提交」）：生成新版本的清单、清单列表与元数据，以「仅当不存在时创建」写出
//! `metadata/v<N+1>.metadata.json`，写成即生效。
//!
//! 一次提交的步骤（同一数据集在进程内经同一把锁依次进行，REQ-AMB-STO-COLD-TABLE-SERIAL）：
//! 1. 确定冷层当前版本 N（[`super::current`]），以它为基础生成本次提交：新清单与新清单列表（文件名取新的 ULID，已写出的
//!    不覆盖），以及新元数据的完整字节（同一编号下只生成一次，各次尝试逐字节相同，-SAME-CONTENT）。
//! 2. 上传本次的清单与清单列表（数据文件由调用方先行上传，-COMMIT-ORDER）。
//! 3. N ≥ 1 而提示缺失、无法解析或小于 N 时先补写提示为 N，写失败照常继续（-HINT-BEFORE、-HINT-FAIL）。
//! 4. 以「仅当不存在时创建」写 `v<N+1>.metadata.json`。得到「已存在」时读回比对完整字节的 SHA-256：相同即已完成
//!    （-EXISTS-SAME）；不同即真冲突，下一次尝试以新的当前版本为基础重新生成、换号（-EXISTS-CONFLICT）；读回遇不可访问
//!    则保持原编号与原字节，下一次尝试再判。
//! 5. 提示以普通覆盖写更新为 N+1，写失败不影响提交（-HINT-AFTER、-HINT-FAIL）。
//!
//! 重试与热层提交同口径（REQ-AMB-STO-MANIFEST-COMMIT-V3）：真冲突按指数退避（起始 50 毫秒、每次翻倍、单次上限 2 秒）
//! 重试至多 `storage.commit_max_retries` 次，耗尽（各次尝试都未遇到对象存储不可访问）即数据集进入 manifest 不可读情形
//! （-EXHAUST）。某次尝试遇到对象存储不可访问即结束本轮、返回 [`ColdError::Unavailable`]：进度（含结果不明时的同一份
//! 元数据字节）留在调用方的 [`Progress`] 里，调用方以同一请求再调时续做。
//!
//! 在途对象：一次提交（含其各次尝试）上传的数据文件、清单与清单列表自上传之前起登记在 [`InFlight`] 中，直到持有它们的
//! [`Progress`] 被丢弃；冷层孤儿清理不删在途对象（-ORPHAN-INFLIGHT）。

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use amberlog_common::dataset::Dataset;
use bytes::Bytes;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataFile, ManifestFile, ManifestListWriter, ManifestStatus, ManifestWriterBuilder, Operation,
    Schema, Snapshot, Summary, TableMetadata, TableMetadataBuilder,
};
use object_store::{ObjectStoreExt, PutPayload};
use sha2::{Digest as _, Sha256};

use super::current::{self, Current};
use super::error::{ColdError, invalid};
use super::layout::{self, ColdLayout};
use super::manifests;
use super::names;
use crate::fault;
use crate::gate::{DatasetGates, DatasetState};
use crate::objects::Created;
use crate::schema::SchemaColumn;
use crate::store::{Store, Unavailable};

/// 冷层快照摘要中 `amberlog.operation` 的属性名（REQ-AMB-STO-COLD-TABLE-SUMMARY-OP）。
pub const OPERATION: &str = "amberlog.operation";

/// 一次冷层提交对冷层表数据文件集合的改变。
#[derive(Debug, Clone)]
pub enum Change {
    /// 加入新的数据文件（已上传），冷层 schema 按 `schema` 追加（REQ-AMB-STO-COLD-TABLE-COVER、-APPEND）。
    Append {
        schema: Vec<SchemaColumn>,
        files: Vec<DataFile>,
        /// 快照摘要中 `amberlog.*` 各属性（含 `amberlog.operation`）。
        summary: BTreeMap<String, String>,
    },
    /// 把这些冷层数据文件（相对表目录）中仍存活的整个标为已删除（REQ-AMB-STO-COLD-TABLE-NO-ROW-DELETE）。
    Delete {
        paths: HashSet<String>,
        summary: BTreeMap<String, String>,
    },
    /// 冷层合并（REQ-AMB-STO-COLD-COMPACTION-ONE-COMMIT）：把 `inputs`（相对表目录）整个标为已删除、加入 `files`（已上传），
    /// 其余条目保留原快照号与序号（-KEEP-ENTRIES）；标准 `operation` 为 `replace`（REQ-AMB-STO-COLD-TABLE-V2）。
    /// 任一输入在所基于的版本中已不存活即 [`ColdError::Stale`]（-RECHECK）。冷层 schema 不变。
    Replace {
        inputs: HashSet<String>,
        files: Vec<DataFile>,
        summary: BTreeMap<String, String>,
    },
    /// 冷层过期提交（REQ-AMB-STO-COLD-RETENTION）：每次生成都以所基于的版本调用 `select`，从元数据中移除选出的快照，
    /// 不新增快照；选出为空（或都已不在）即 [`ColdError::Stale`]。
    Expire { select: Select },
}

/// [`Change::Expire`] 的判定回调。
#[derive(Clone)]
pub struct Select(pub Arc<dyn super::ExpireSelect>);

impl std::fmt::Debug for Select {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Select(..)")
    }
}

/// 一次冷层提交的结果。
#[derive(Debug, Clone)]
pub struct Committed {
    /// 写出的版本 N+1。
    pub version: u64,
    /// 新增的 Iceberg 快照号。
    pub snapshot_id: i64,
    pub metadata: TableMetadata,
    /// `Change::Delete`、`Change::Replace` 时实际标为已删除的文件（相对表目录）。
    pub removed: Vec<String>,
    /// `Change::Expire` 时移除的快照号。
    pub expired: Vec<i64>,
}

/// 本进程正在进行的冷层提交已上传、尚未被已写出版本引用的对象（相对表目录），按数据集。
#[derive(Default)]
pub struct InFlight {
    paths: Mutex<HashMap<(Dataset, String), usize>>,
}

impl InFlight {
    fn add(&self, dataset: &Dataset, rel: &str) {
        *self
            .paths
            .lock()
            .expect("在途表锁未中毒")
            .entry((dataset.clone(), rel.to_owned()))
            .or_default() += 1;
    }

    fn remove(&self, dataset: &Dataset, rel: &str) {
        let mut paths = self.paths.lock().expect("在途表锁未中毒");
        let key = (dataset.clone(), rel.to_owned());
        if let Some(n) = paths.get_mut(&key) {
            *n -= 1;
            if *n == 0 {
                paths.remove(&key);
            }
        }
    }

    /// 数据集 `dataset` 此刻的在途对象。
    pub fn snapshot(&self, dataset: &Dataset) -> HashSet<String> {
        self.paths
            .lock()
            .expect("在途表锁未中毒")
            .keys()
            .filter(|(d, _)| d == dataset)
            .map(|(_, p)| p.clone())
            .collect()
    }
}

/// 一次冷层提交在各次尝试与各轮调用之间保留的进度。丢弃即放弃这次提交（其在途登记随之撤销）。
pub struct Progress {
    dataset: Dataset,
    inflight: Arc<InFlight>,
    registered: Vec<String>,
    uploaded: HashSet<String>,
    prepared: Option<Prepared>,
}

impl Progress {
    pub fn new(dataset: &Dataset, inflight: &Arc<InFlight>) -> Self {
        Progress {
            dataset: dataset.clone(),
            inflight: Arc::clone(inflight),
            registered: Vec::new(),
            uploaded: HashSet::new(),
            prepared: None,
        }
    }

    fn register(&mut self, rel: &str) {
        self.inflight.add(&self.dataset, rel);
        self.registered.push(rel.to_owned());
    }

    /// 以「仅当不存在时创建」上传本次提交要引用的对象 `rel`（相对表目录）：先登记为在途，已上传过的跳过。
    /// 已存在只可能是本次提交此前一次结果不明的上传（文件名是新生成的 ULID）。
    pub async fn upload(
        &mut self,
        store: &Store,
        layout: &ColdLayout,
        rel: &str,
        bytes: Bytes,
    ) -> Result<(), Unavailable> {
        if self.uploaded.contains(rel) {
            return Ok(());
        }
        if !self.registered.iter().any(|r| r == rel) {
            self.register(rel);
        }
        store
            .create(&layout.object_path(&self.dataset, rel), bytes)
            .await?;
        self.uploaded.insert(rel.to_owned());
        fault::pause("cold_commit.after_each_put");
        Ok(())
    }
}

impl Drop for Progress {
    fn drop(&mut self) {
        for rel in &self.registered {
            self.inflight.remove(&self.dataset, rel);
        }
    }
}

/// 一个编号下已生成的提交内容。
pub(super) struct Prepared {
    version: u64,
    /// 生成所据的冷层当前版本（N）的提示是否要补写。
    hint_lags: bool,
    bytes: Bytes,
    digest: [u8; 32],
    metadata: TableMetadata,
    snapshot_id: i64,
    uploads: Vec<(String, Bytes)>,
    removed: Vec<String>,
    expired: Vec<i64>,
}

enum Fail {
    /// 本次尝试未完成；`unavailable` 为真表示遇到对象存储不可访问。
    Retry {
        unavailable: bool,
    },
    Fatal(ColdError),
}

impl From<Unavailable> for Fail {
    fn from(_: Unavailable) -> Fail {
        Fail::Retry { unavailable: true }
    }
}

impl From<ColdError> for Fail {
    fn from(e: ColdError) -> Fail {
        match e {
            ColdError::Unavailable(_) => Fail::Retry { unavailable: true },
            e => Fail::Fatal(e),
        }
    }
}

fn backoff(attempt: u32) -> Duration {
    let ms = 50u64.saturating_mul(1 << (attempt - 1).min(16));
    Duration::from_millis(ms.min(2_000))
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

/// 冷层提交器：持有对象存储、布局、停用闸门与每数据集的锁。
pub struct Committer {
    pub store: Store,
    pub layout: ColdLayout,
    pub gates: Arc<DatasetGates>,
    max_retries: u32,
    inflight: Arc<InFlight>,
    locks: Mutex<HashMap<Dataset, Arc<tokio::sync::Mutex<()>>>>,
    /// 测试在「生成内容之后、创建元数据文件之前」插入别人的写入。
    #[cfg(test)]
    before_create: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl Committer {
    pub fn new(
        store: Store,
        layout: ColdLayout,
        gates: Arc<DatasetGates>,
        max_retries: u32,
    ) -> Self {
        Committer {
            store,
            layout,
            gates,
            max_retries,
            inflight: Arc::default(),
            locks: Mutex::default(),
            #[cfg(test)]
            before_create: Mutex::default(),
        }
    }

    pub fn inflight(&self) -> &Arc<InFlight> {
        &self.inflight
    }

    /// 同一数据集的冷层提交在进程内依次进行：调用方在整次提交（含上传数据文件）期间持有它。
    pub async fn lock(&self, dataset: &Dataset) -> tokio::sync::OwnedMutexGuard<()> {
        let m = Arc::clone(
            self.locks
                .lock()
                .expect("冷层提交锁表未中毒")
                .entry(dataset.clone())
                .or_default(),
        );
        m.lock_owned().await
    }

    /// 做一次冷层提交。调用方须已持有 [`Self::lock`]。
    ///
    /// 数据集处于停用状态时不写任何对象（REQ-AMB-STO-COLD-TABLE-DISABLED），返回 [`ColdError::Disabled`]。
    /// `Change::Delete` 所列文件都已不存活、`Change::Replace` 有输入已不存活、`Change::Expire` 没有可移除的快照时
    /// 返回 [`ColdError::Stale`]。
    pub async fn commit(
        &self,
        dataset: &Dataset,
        change: &Change,
        progress: &mut Progress,
    ) -> Result<Committed, ColdError> {
        for attempt in 0..=self.max_retries {
            if attempt > 0 {
                tokio::time::sleep(backoff(attempt)).await;
            }
            match self.attempt(dataset, change, progress).await {
                Ok(c) => return Ok(c),
                Err(Fail::Fatal(e)) => return Err(e),
                Err(Fail::Retry { unavailable: true }) => {
                    return Err(ColdError::Unavailable(Unavailable(
                        "object store is not accessible during the cold commit".into(),
                    )));
                }
                Err(Fail::Retry { unavailable: false }) => {}
            }
        }
        // 各次尝试都没遇到不可访问（遇到即已返回）
        self.gates.enter(dataset, DatasetState::ManifestUnreadable);
        Err(ColdError::Exhausted)
    }

    async fn attempt(
        &self,
        dataset: &Dataset,
        change: &Change,
        progress: &mut Progress,
    ) -> Result<Committed, Fail> {
        let state = self.gates.state(dataset);
        if state != DatasetState::Normal {
            return Err(Fail::Fatal(ColdError::Disabled(state)));
        }
        let cur = current::current(&self.store, &self.layout, dataset).await?;

        // 上一次尝试已定下的编号：当前版本已到达或越过它，且它确为本次所写，即已成功；
        // 当前版本仍是它的前一个则保持同号、用同一份字节；否则（被他人占用）以新的当前版本重来。
        if let Some(p) = progress.prepared.take() {
            if cur.version >= p.version {
                if self.written_by_us(dataset, &p).await? {
                    return Ok(self.succeed(dataset, p).await);
                }
            } else if cur.version + 1 == p.version {
                progress.prepared = Some(p);
            }
        }
        if progress.prepared.is_none() {
            progress.prepared = Some(self.prepare(dataset, change, &cur).await?);
        }
        let p = progress.prepared.as_ref().expect("刚定下");
        let uploads = p.uploads.clone();
        let (version, hint_lags) = (p.version, p.hint_lags);
        for (rel, bytes) in uploads {
            progress
                .upload(&self.store, &self.layout, &rel, bytes)
                .await?;
        }
        if hint_lags {
            self.write_hint(dataset, version - 1).await;
        }

        fault::gate("cold_commit.before_metadata_put").await;
        #[cfg(test)]
        if let Some(f) = self.before_create.lock().expect("未中毒").take() {
            f();
        }
        let p = progress.prepared.as_ref().expect("刚定下");
        let path = self
            .layout
            .object_path(dataset, &layout::metadata_path(p.version));
        match self.store.create(&path, p.bytes.clone()).await? {
            Created::Done => {}
            Created::AlreadyExists => {
                if !self.written_by_us(dataset, p).await? {
                    // 真冲突：下一次尝试以新的当前版本为基础重新生成、换号
                    progress.prepared = None;
                    return Err(Fail::Retry { unavailable: false });
                }
            }
        }
        fault::pause("cold_commit.after_each_put");
        fault::pause("cold_commit.after_metadata_put");
        let p = progress.prepared.take().expect("刚写出");
        Ok(self.succeed(dataset, p).await)
    }

    /// 编号 `p.version` 的元数据文件是否就是 `p` 的字节（读回比对完整字节的 SHA-256）。
    async fn written_by_us(&self, dataset: &Dataset, p: &Prepared) -> Result<bool, Unavailable> {
        let path = self
            .layout
            .object_path(dataset, &layout::metadata_path(p.version));
        Ok(match self.store.read(&path).await? {
            Some(f) => sha256(&f.bytes) == p.digest,
            // 「已存在」之后又读到不存在：不可能由本系统造成（版本文件永不删除），按冲突处置
            None => false,
        })
    }

    async fn succeed(&self, dataset: &Dataset, p: Prepared) -> Committed {
        self.write_hint(dataset, p.version).await;
        Committed {
            version: p.version,
            snapshot_id: p.snapshot_id,
            metadata: p.metadata,
            removed: p.removed,
            expired: p.expired,
        }
    }

    /// 以普通覆盖写把提示更新为 `n`；失败不影响提交（REQ-AMB-STO-COLD-TABLE-HINT-FAIL）。
    async fn write_hint(&self, dataset: &Dataset, n: u64) {
        if let Some(dir) = &self.store.local_dir
            && !dir.is_dir()
        {
            return;
        }
        let path = self.layout.object_path(dataset, layout::HINT);
        if self
            .store
            .inner
            .put(&path, PutPayload::from(n.to_string().into_bytes()))
            .await
            .is_ok()
        {
            fault::pause("cold_commit.after_each_put");
        }
    }

    /// 以冷层当前版本 `cur` 为基础生成编号 N+1 的提交内容。
    async fn prepare(
        &self,
        dataset: &Dataset,
        change: &Change,
        cur: &Current,
    ) -> Result<Prepared, ColdError> {
        let base = match &cur.metadata {
            Some((_, m)) => m.clone(),
            None => match change {
                Change::Append { schema, .. } => {
                    new_table(&self.layout.table_location(dataset), schema)?
                }
                _ => return Err(ColdError::Stale),
            },
        };
        if let Change::Expire { select } = change {
            return self.prepare_expire(dataset, select, cur, base).await;
        }
        let mut used: HashSet<i64> = base.snapshots().map(|s| s.snapshot_id()).collect();
        let snapshot_id = loop {
            let id = rand::random::<i64>() & i64::MAX;
            if id != 0 && used.insert(id) {
                break id;
            }
        };
        let parent = base.current_snapshot().cloned();
        let sequence_number = base.last_sequence_number() + 1;
        let parent_manifests = match &parent {
            Some(s) => manifests::manifest_list(&self.store, &self.layout, dataset, s).await?,
            None => Vec::new(),
        };

        let file_io = FileIO::new_with_memory();
        let mut uploads = Vec::new();
        let mut list: Vec<ManifestFile> = Vec::new();
        let mut removed = Vec::new();
        let (operation, summary, schema) = match change {
            Change::Append {
                schema,
                files,
                summary,
            } => {
                let fields = names::evolve(Some(base.current_schema()), schema);
                let schema = if fields.len() == base.current_schema().as_struct().fields().len() {
                    None
                } else {
                    let id = base
                        .schemas_iter()
                        .map(|s| s.schema_id())
                        .max()
                        .unwrap_or(0)
                        + 1;
                    Some(
                        Schema::builder()
                            .with_schema_id(id)
                            .with_fields(fields)
                            .build()
                            .map_err(invalid)?,
                    )
                };
                let manifest_schema = match &schema {
                    Some(s) => Arc::new(s.clone()),
                    None => Arc::clone(base.current_schema()),
                };
                let rel = layout::new_avro_path("m0");
                let uri = self.layout.object_uri(dataset, &rel);
                let mut writer = ManifestWriterBuilder::new(
                    file_io.new_output(&uri).map_err(invalid)?,
                    Some(snapshot_id),
                    manifest_schema,
                    base.default_partition_spec().as_ref().clone(),
                )
                .build_v2_data();
                for f in files {
                    writer
                        .add_file(f.clone(), sequence_number)
                        .map_err(invalid)?;
                }
                list.push(writer.write_manifest_file().await.map_err(invalid)?);
                uploads.push((rel, read_back(&file_io, &uri).await?));
                list.extend(parent_manifests);
                (Operation::Append, summary, schema)
            }
            Change::Delete { paths, summary } => {
                self.remove_entries(
                    dataset,
                    &base,
                    snapshot_id,
                    &file_io,
                    parent_manifests,
                    paths,
                    &mut list,
                    &mut uploads,
                    &mut removed,
                )
                .await?;
                if removed.is_empty() {
                    return Err(ColdError::Stale);
                }
                removed.sort();
                (Operation::Delete, summary, None)
            }
            Change::Replace {
                inputs,
                files,
                summary,
            } => {
                let rel = layout::new_avro_path("m0");
                let uri = self.layout.object_uri(dataset, &rel);
                let mut writer = ManifestWriterBuilder::new(
                    file_io.new_output(&uri).map_err(invalid)?,
                    Some(snapshot_id),
                    Arc::clone(base.current_schema()),
                    base.default_partition_spec().as_ref().clone(),
                )
                .build_v2_data();
                for f in files {
                    writer
                        .add_file(f.clone(), sequence_number)
                        .map_err(invalid)?;
                }
                list.push(writer.write_manifest_file().await.map_err(invalid)?);
                uploads.push((rel, read_back(&file_io, &uri).await?));
                self.remove_entries(
                    dataset,
                    &base,
                    snapshot_id,
                    &file_io,
                    parent_manifests,
                    inputs,
                    &mut list,
                    &mut uploads,
                    &mut removed,
                )
                .await?;
                if removed.len() != inputs.len() {
                    return Err(ColdError::Stale);
                }
                removed.sort();
                (Operation::Replace, summary, None)
            }
            Change::Expire { .. } => unreachable!("上面已单独生成"),
        };

        let list_rel = layout::new_avro_path("snap");
        let list_uri = self.layout.object_uri(dataset, &list_rel);
        let mut list_writer = ManifestListWriter::v2(
            file_io
                .new_output(&list_uri)
                .map_err(invalid)?
                .writer()
                .await
                .map_err(invalid)?,
            snapshot_id,
            parent.as_ref().map(|s| s.snapshot_id()),
            sequence_number,
        );
        list_writer
            .add_manifests(list.into_iter())
            .map_err(invalid)?;
        list_writer.close().await.map_err(invalid)?;
        uploads.push((list_rel, read_back(&file_io, &list_uri).await?));

        let schema_id = schema
            .as_ref()
            .map_or(base.current_schema_id(), |s| s.schema_id());
        let snapshot = Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_parent_snapshot_id(parent.as_ref().map(|s| s.snapshot_id()))
            .with_sequence_number(sequence_number)
            .with_timestamp_ms(now_ms().max(base.last_updated_ms()))
            .with_manifest_list(list_uri)
            .with_summary(Summary {
                operation,
                additional_properties: summary.clone().into_iter().collect(),
            })
            .with_schema_id(schema_id)
            .build();
        let previous = (cur.version > 0).then(|| {
            self.layout
                .object_uri(dataset, &layout::metadata_path(cur.version))
        });
        let mut builder = TableMetadataBuilder::new_from_metadata(base, previous);
        if let Some(schema) = schema {
            builder = builder
                .add_schema(schema)
                .map_err(invalid)?
                .set_current_schema(TableMetadataBuilder::LAST_ADDED)
                .map_err(invalid)?;
        }
        let metadata = builder
            .set_branch_snapshot(snapshot, "main")
            .map_err(invalid)?
            .build()
            .map_err(invalid)?
            .metadata;
        let bytes = Bytes::from(serde_json::to_vec(&metadata).map_err(invalid)?);
        Ok(Prepared {
            version: cur.version + 1,
            hint_lags: cur.version >= 1 && cur.hint.is_none_or(|h| h < cur.version),
            digest: sha256(&bytes),
            bytes,
            metadata,
            snapshot_id,
            uploads,
            removed,
            expired: Vec::new(),
        })
    }

    /// 改写 `parent_manifests` 中含 `paths` 里存活条目的清单：这些条目标为已删除（记入 `removed`），其余条目保留原快照号
    /// 与序号，上一个快照已标为已删除的条目不再带入（REQ-AMB-STO-COLD-TABLE-V2）；不含的清单原样沿用。
    #[allow(clippy::too_many_arguments)]
    async fn remove_entries(
        &self,
        dataset: &Dataset,
        base: &TableMetadata,
        snapshot_id: i64,
        file_io: &FileIO,
        parent_manifests: Vec<ManifestFile>,
        paths: &HashSet<String>,
        list: &mut Vec<ManifestFile>,
        uploads: &mut Vec<(String, Bytes)>,
        removed: &mut Vec<String>,
    ) -> Result<(), ColdError> {
        for file in parent_manifests {
            let entries = manifests::entries(&self.store, &self.layout, dataset, &file).await?;
            let hit = entries.iter().any(|e| {
                e.is_alive()
                    && self
                        .layout
                        .rel_of_uri(dataset, e.file_path())
                        .is_some_and(|r| paths.contains(r))
            });
            if !hit {
                list.push(file);
                continue;
            }
            let rel = layout::new_avro_path("m0");
            let uri = self.layout.object_uri(dataset, &rel);
            let mut writer = ManifestWriterBuilder::new(
                file_io.new_output(&uri).map_err(invalid)?,
                Some(snapshot_id),
                Arc::clone(base.current_schema()),
                base.default_partition_spec().as_ref().clone(),
            )
            .build_v2_data();
            for e in entries {
                let seq = e.sequence_number.unwrap_or(file.sequence_number);
                let file_seq = e.file_sequence_number.or(Some(seq));
                match e.status {
                    // 上一个快照标为已删除的条目不再沿用（Iceberg 改写清单的通常做法）
                    ManifestStatus::Deleted => {}
                    _ => {
                        let target = self
                            .layout
                            .rel_of_uri(dataset, e.file_path())
                            .filter(|r| paths.contains(*r))
                            .map(str::to_owned);
                        match target {
                            Some(rel) => {
                                writer
                                    .add_delete_file(e.data_file, seq, file_seq)
                                    .map_err(invalid)?;
                                removed.push(rel);
                            }
                            None => writer
                                .add_existing_file(
                                    e.data_file,
                                    e.snapshot_id.unwrap_or(file.added_snapshot_id),
                                    seq,
                                    file_seq,
                                )
                                .map_err(invalid)?,
                        }
                    }
                }
            }
            list.push(writer.write_manifest_file().await.map_err(invalid)?);
            uploads.push((rel, read_back(file_io, &uri).await?));
        }
        Ok(())
    }

    /// 冷层过期提交的生成：以所基于的版本 `cur`（元数据 `base`）调用 `select`，从元数据中移除选出的快照。不新增快照，
    /// 不上传任何对象；`snapshot-log` 由 iceberg-rust 的元数据构建器去掉指向已不存在快照的条目及其之前的全部条目，
    /// `metadata-log` 按缺省至多 100 条（REQ-AMB-STO-COLD-TABLE-V2）。`select` 读取失败按读冷层失败返回，不停用。
    async fn prepare_expire(
        &self,
        dataset: &Dataset,
        select: &Select,
        cur: &Current,
        base: TableMetadata,
    ) -> Result<Prepared, ColdError> {
        let view =
            super::read::table_at(&self.store, &self.layout, dataset, cur.version, None).await?;
        let table = super::store::cold_table(view);
        let chosen = select
            .0
            .select(dataset, &table)
            .await
            .map_err(|e| match e {
                super::ColdReadError::Unavailable(m) => ColdError::Unavailable(Unavailable(m)),
                super::ColdReadError::Integrity(m) | super::ColdReadError::NotFound(m) => {
                    ColdError::Broken(m)
                }
            })?;
        let current = base.current_snapshot_id();
        let mut expired: Vec<i64> = chosen
            .into_iter()
            .filter(|id| Some(*id) != current && base.snapshot_by_id(*id).is_some())
            .collect();
        expired.sort_unstable();
        expired.dedup();
        if expired.is_empty() {
            return Err(ColdError::Stale);
        }
        fault::gate("cold_retention.before_commit").await;
        let previous = self
            .layout
            .object_uri(dataset, &layout::metadata_path(cur.version));
        let metadata = TableMetadataBuilder::new_from_metadata(base, Some(previous))
            .remove_snapshots(&expired)
            .build()
            .map_err(invalid)?
            .metadata;
        let bytes = Bytes::from(serde_json::to_vec(&metadata).map_err(invalid)?);
        Ok(Prepared {
            version: cur.version + 1,
            hint_lags: cur.version >= 1 && cur.hint.is_none_or(|h| h < cur.version),
            digest: sha256(&bytes),
            snapshot_id: metadata.current_snapshot_id().unwrap_or_default(),
            bytes,
            metadata,
            uploads: Vec::new(),
            removed: Vec::new(),
            expired,
        })
    }
}

async fn read_back(file_io: &FileIO, uri: &str) -> Result<Bytes, ColdError> {
    file_io
        .new_input(uri)
        .map_err(invalid)?
        .read()
        .await
        .map_err(invalid)
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// 新表的第一个基础版本（不写出，只作为第一次冷层提交的基础）：格式第 2 版，分区规格恰含 `_timestamp_day`
/// （`day(_timestamp)`，REQ-AMB-STO-COLD-TABLE-PARTITION），不排序，没有表属性（-NO-NAME-PROPS），
/// schema 各列按字段编号从小到大（-APPEND）。字段编号沿用热层（-FIELD-ID）：不经 `TableMetadataBuilder::new`，
/// 它会重排字段编号。
fn new_table(location: &str, hot: &[SchemaColumn]) -> Result<TableMetadata, ColdError> {
    let fields = names::evolve(None, hot);
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(fields)
        .build()
        .map_err(invalid)?;
    let uuid = {
        let b = rand::random::<u128>().to_be_bytes();
        let mut b = b;
        b[6] = (b[6] & 0x0f) | 0x40;
        b[8] = (b[8] & 0x3f) | 0x80;
        let h: String = b.iter().map(|x| format!("{x:02x}")).collect();
        format!(
            "{}-{}-{}-{}-{}",
            &h[0..8],
            &h[8..12],
            &h[12..16],
            &h[16..20],
            &h[20..32]
        )
    };
    let template = serde_json::json!({
        "format-version": 2,
        "table-uuid": uuid,
        "location": location,
        "last-sequence-number": 0,
        "last-updated-ms": now_ms(),
        "last-column-id": schema.highest_field_id(),
        "current-schema-id": 0,
        "schemas": [schema],
        "default-spec-id": 0,
        "partition-specs": [{
            "spec-id": 0,
            "fields": [{
                "name": layout::PARTITION_FIELD,
                "transform": "day",
                "source-id": crate::schema::TIMESTAMP_FIELD_ID,
                "field-id": 1000
            }]
        }],
        "last-partition-id": 1000,
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "properties": {},
        "snapshots": [],
        "snapshot-log": [],
        "metadata-log": [],
        "refs": {}
    });
    serde_json::from_value(template).map_err(invalid)
}

#[cfg(test)]
pub(super) type PreparedForTest = Prepared;

#[cfg(test)]
impl Prepared {
    pub(super) fn bytes(&self) -> &Bytes {
        &self.bytes
    }
}

#[cfg(test)]
impl Progress {
    /// 模拟上一轮已在某编号下生成了内容。
    pub(super) fn set_prepared_for_test(&mut self, p: Prepared) {
        self.prepared = Some(p);
    }
}

#[cfg(test)]
impl Committer {
    pub(super) async fn prepare_for_test(
        &self,
        dataset: &Dataset,
        change: &Change,
        cur: &Current,
    ) -> Result<Prepared, ColdError> {
        self.prepare(dataset, change, cur).await
    }

    pub(super) fn before_create_for_test(&self, f: impl FnOnce() + Send + 'static) {
        *self.before_create.lock().expect("未中毒") = Some(Box::new(f));
    }
}

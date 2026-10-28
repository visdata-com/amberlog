//! 对象级读写与条件写（REQ-AMB-STO-HOT-LAYOUT-V2「对象状态的判定」、REQ-AMB-STO-MANIFEST-COMMIT「条件写语义」）。
//!
//! 每个操作的结果按块分类：对象不存在、对象存储不可访问（[`Unavailable`]）、条件写的前提不满足，其余为成功。
//! 内容能否解析由调用方按对象种类判定（[`crate::format`]）。
//!
//! 条件写两种：
//! - 「仅当不存在时创建」（快照与两种段；数据文件与桶 manifest 也这样写，以防覆盖）：两类后端都用 `PutMode::Create`。
//!   提示 `_current` 以普通覆盖写更新（[`Store::put`]，REQ-AMB-STO-MANIFEST-COMMIT-V3）。
//! - 「版本一致才替换」（计数对象）：`s3://` 用 `PutMode::Update` 带读取时的 ETag；`file://` 的本地后端对
//!   `PutMode::Update` 返回未实现，由这里自建——进程内加锁，读出当前内容与期望比对，一致才以
//!   本地后端的原子写（写临时文件后改名）替换。期望「尚不存在」时两类后端都用 `PutMode::Create`。

use std::ops::Range;
use std::sync::LazyLock;

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};

use crate::store::{Store, Unavailable};

/// 读到的对象：完整字节与用于「版本一致才替换」的版本。
#[derive(Debug, Clone)]
pub struct Fetched {
    pub bytes: Bytes,
    pub version: Version,
}

/// 对象的版本：`s3://` 为 ETag（及版本号），`file://` 为读到的完整内容本身。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Version {
    Tag(UpdateVersion),
    Content(Bytes),
}

/// 「仅当不存在时创建」的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Created {
    Done,
    /// 目标已存在，未写入。
    AlreadyExists,
}

/// 「版本一致才替换」的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Swapped {
    Done,
    /// 当前版本与期望不一致（含期望尚不存在而对象已存在、期望存在而对象已不见），未写入。
    Conflict,
}

/// `file://` 下「版本一致才替换」的进程内锁（只有计数对象这样更新，一把锁够用）。
static LOCAL_SWAP: LazyLock<tokio::sync::Mutex<()>> = LazyLock::new(|| tokio::sync::Mutex::new(()));

impl Store {
    /// 数据集前缀下相对路径 `rel` 对应的键。
    pub fn object_path(&self, dataset: &amberlog_common::dataset::Dataset, rel: &str) -> Path {
        let mut path = self.dataset_prefix(dataset);
        for part in rel.split('/') {
            path = path.join(part);
        }
        path
    }

    /// 读取整个对象；对象不存在为 `Ok(None)`。
    pub async fn read(&self, path: &Path) -> Result<Option<Fetched>, Unavailable> {
        let got = match self.inner.get(path).await {
            Ok(got) => got,
            Err(object_store::Error::NotFound { .. }) => {
                self.check_local_dir()?;
                return Ok(None);
            }
            Err(e) => return Err(unavailable("read", path, e)),
        };
        let tag = UpdateVersion {
            e_tag: got.meta.e_tag.clone(),
            version: got.meta.version.clone(),
        };
        let bytes = match got.bytes().await {
            Ok(bytes) => bytes,
            Err(object_store::Error::NotFound { .. }) => {
                self.check_local_dir()?;
                return Ok(None);
            }
            Err(e) => return Err(unavailable("read", path, e)),
        };
        let version = match self.local_dir {
            Some(_) => Version::Content(bytes.clone()),
            None => Version::Tag(tag),
        };
        Ok(Some(Fetched { bytes, version }))
    }

    /// 读对象的一段字节 `[range.start, range.end)`（等值 bloom 的判定读尾部元数据与 bloom，REQ-AMB-QRY-EQUALITY-BLOOM）。
    /// 对象不存在、范围越过对象末尾、对象存储不可访问或读到的字节数不符都为 `None`：判定读不到即不下结论。
    pub async fn read_range(&self, path: &Path, range: Range<u64>) -> Option<Bytes> {
        let want = range.end.checked_sub(range.start)?;
        let got = self.inner.get_range(path, range).await.ok()?;
        (got.len() as u64 == want).then_some(got)
    }

    /// 探测（读取对象元数据）：对象存在为 `Ok(true)`，不存在为 `Ok(false)`（找链尾按编号逐个探测快照，
    /// REQ-AMB-STO-MANIFEST-COMMIT-V3）。
    pub async fn exists(&self, path: &Path) -> Result<bool, Unavailable> {
        match self.inner.head(path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => {
                self.check_local_dir()?;
                Ok(false)
            }
            Err(e) => Err(unavailable("probe", path, e)),
        }
    }

    /// 普通覆盖写（提示 `_current`，REQ-AMB-STO-MANIFEST-COMMIT-V3「提示 MUST 以普通覆盖写更新」）。
    pub async fn put(&self, path: &Path, bytes: Bytes) -> Result<(), Unavailable> {
        self.check_local_dir()?;
        self.inner
            .put(path, PutPayload::from(bytes))
            .await
            .map(|_| ())
            .map_err(|e| unavailable("write", path, e))
    }

    /// 「仅当不存在时创建」。
    pub async fn create(&self, path: &Path, bytes: Bytes) -> Result<Created, Unavailable> {
        // 本地后端写入时会补建路径上缺的目录，连 `file://` 所指的目录本身也会被建出来
        self.check_local_dir()?;
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        match self
            .inner
            .put_opts(path, PutPayload::from(bytes), opts)
            .await
        {
            Ok(_) => Ok(Created::Done),
            Err(object_store::Error::AlreadyExists { .. }) => Ok(Created::AlreadyExists),
            Err(e) => Err(unavailable("create", path, e)),
        }
    }

    /// 「版本一致才替换」：`expected` 为 `None` 表示期望对象尚不存在。
    /// 返回 `Err` 时写入的结果不明（可能已写入），调用方须重读后再判。
    pub async fn swap(
        &self,
        path: &Path,
        expected: Option<&Version>,
        bytes: Bytes,
    ) -> Result<Swapped, Unavailable> {
        let Some(expected) = expected else {
            return Ok(match self.create(path, bytes).await? {
                Created::Done => Swapped::Done,
                Created::AlreadyExists => Swapped::Conflict,
            });
        };
        match expected {
            Version::Tag(tag) => {
                let opts = PutOptions {
                    mode: PutMode::Update(tag.clone()),
                    ..Default::default()
                };
                match self
                    .inner
                    .put_opts(path, PutPayload::from(bytes), opts)
                    .await
                {
                    Ok(_) => Ok(Swapped::Done),
                    Err(
                        object_store::Error::Precondition { .. }
                        | object_store::Error::NotFound { .. },
                    ) => Ok(Swapped::Conflict),
                    Err(e) => Err(unavailable("conditional update", path, e)),
                }
            }
            Version::Content(want) => {
                let _guard = LOCAL_SWAP.lock().await;
                match self.read(path).await? {
                    Some(now) if now.bytes == *want => {}
                    _ => return Ok(Swapped::Conflict),
                }
                // 本地后端的覆盖写先写临时文件再改名，读者看不到半截内容
                self.inner
                    .put(path, PutPayload::from(bytes))
                    .await
                    .map_err(|e| unavailable("conditional update", path, e))?;
                Ok(Swapped::Done)
            }
        }
    }

    /// 删除对象。对象已不存在为 `Ok(false)`。
    pub async fn delete(&self, path: &Path) -> Result<bool, Unavailable> {
        match self.inner.delete(path).await {
            Ok(()) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(unavailable("delete", path, e)),
        }
    }

    /// 列出 `prefix` 下的全部对象（递归）。前缀下没有对象为空，不属不可访问。
    pub async fn list(&self, prefix: &Path) -> Result<Vec<ObjectMeta>, Unavailable> {
        let all = self
            .inner
            .list(Some(prefix))
            .try_collect::<Vec<_>>()
            .await
            .map_err(|e| unavailable("list", prefix, e))?;
        self.check_local_dir()?;
        Ok(all)
    }

    /// 列出 `prefix` 下一级：(对象, 子前缀)。
    pub async fn list_level(
        &self,
        prefix: &Path,
    ) -> Result<(Vec<ObjectMeta>, Vec<Path>), Unavailable> {
        let r = self
            .inner
            .list_with_delimiter(Some(prefix))
            .await
            .map_err(|e| unavailable("list", prefix, e))?;
        self.check_local_dir()?;
        Ok((r.objects, r.common_prefixes))
    }

    /// `file://` 所指目录不存在属对象存储不可访问（本地后端对其下的键只会报「不存在」，写入时则会把它建出来）。
    fn check_local_dir(&self) -> Result<(), Unavailable> {
        match &self.local_dir {
            Some(dir) if !dir.is_dir() => Err(Unavailable(format!(
                "object store directory {} does not exist",
                dir.display()
            ))),
            _ => Ok(()),
        }
    }
}

fn unavailable(op: &str, path: &Path, e: object_store::Error) -> Unavailable {
    Unavailable(format!("object store {op} of {path} failed: {e}"))
}

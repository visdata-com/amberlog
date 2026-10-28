//! 目录只是桶的投影（REQ-AMB-STO-COLD-CATALOG B、D、E、H 组；ADR-0039 第 7 条）：命名空间是 org，表是 stream，
//! 一切从 `<storage.prefix>/_iceberg/` 现读，每个请求都重新列举与确定冷层当前版本，不跨请求缓存（FRESH）。
//! 不读热层，读到坏元数据也不使数据集停用（NO-DISABLE）：这里只调用冷层的只读接口，不经冷层读写实例。

use amberlog_common::dataset::{DEFAULT_ORG, Dataset, is_valid_stream_name};
use amberlog_storage::cold::current;
use amberlog_storage::cold::error::ColdError;
use amberlog_storage::cold::layout::ColdLayout;
use amberlog_storage::store::{Store, Unavailable};
use serde_json::Value;

/// 读表的结果。
pub struct Loaded {
    pub metadata_location: String,
    pub metadata: Value,
}

/// 投影的读取失败：对象存储不可访问（503），或所读的元数据文件不存在、内容无法解析（500）。
#[derive(Debug)]
pub enum ReadError {
    Unavailable(String),
    Broken(String),
}

impl From<Unavailable> for ReadError {
    fn from(e: Unavailable) -> Self {
        ReadError::Unavailable(e.0)
    }
}

impl From<ColdError> for ReadError {
    fn from(e: ColdError) -> Self {
        match e {
            ColdError::Unavailable(u) => ReadError::Unavailable(u.0),
            other => ReadError::Broken(other.to_string()),
        }
    }
}

pub struct Projection {
    store: Store,
    layout: ColdLayout,
}

impl Projection {
    pub fn new(store: Store) -> Self {
        let layout = ColdLayout::new(&store.location, store.root.clone());
        Projection { store, layout }
    }

    /// `_iceberg/`（`org` 为 `None`）或 `_iceberg/<org>/` 之下一级子前缀的名字。
    async fn children(&self, org: Option<&str>) -> Result<Vec<String>, ReadError> {
        let mut dir = self.layout.iceberg_dir();
        if let Some(org) = org {
            dir = dir.join(org);
        }
        let (_, prefixes) = self.store.list_level(&dir).await?;
        Ok(prefixes
            .iter()
            .filter_map(|p| p.filename().map(str::to_owned))
            .collect())
    }

    /// 数据集的冷层当前版本；0 即该表不存在（REQ-AMB-STO-COLD-CATALOG-TABLE-EXISTS）。
    pub async fn version(&self, dataset: &Dataset) -> Result<u64, ReadError> {
        Ok(current::version(&self.store, &self.layout, dataset)
            .await?
            .0)
    }

    /// 命名空间 `org` 之下存在的表，按名字字节序升序（REQ-AMB-STO-COLD-CATALOG-LIST-TABLES）。`first` 为真时找到一张即停
    /// （只判定命名空间是否存在，NS-EXISTS）。
    pub async fn tables(&self, org: &str, first: bool) -> Result<Vec<String>, ReadError> {
        let mut names = self.children(Some(org)).await?;
        names.retain(|n| is_valid_stream_name(n));
        names.sort();
        let mut found = Vec::new();
        for stream in names {
            if self.version(&Dataset::new(org, stream.as_str())).await? > 0 {
                found.push(stream);
                if first {
                    break;
                }
            }
        }
        Ok(found)
    }

    /// 命名空间 `org` 是否存在（REQ-AMB-STO-COLD-CATALOG-NS-EXISTS）。
    pub async fn namespace_exists(&self, org: &str) -> Result<bool, ReadError> {
        Ok(!self.tables(org, true).await?.is_empty())
    }

    /// 存在的命名空间，按名字字节序升序（REQ-AMB-STO-COLD-CATALOG-LIST-NS）：候选名取自 `_iceberg/` 之下的一级子前缀，
    /// 只有 REQ-AMB-DM-NAMESPACE 接受的 org 才可能存在。
    pub async fn namespaces(&self) -> Result<Vec<String>, ReadError> {
        let mut names = self.children(None).await?;
        names.retain(|n| n == DEFAULT_ORG);
        names.sort();
        let mut found = Vec::new();
        for org in names {
            if self.namespace_exists(&org).await? {
                found.push(org);
            }
        }
        Ok(found)
    }

    /// 读版本 `n`（已确定的冷层当前版本）的元数据（REQ-AMB-STO-COLD-CATALOG-LOAD-LOCATION、-LOAD-METADATA）：文件内容按
    /// 通用 JSON 原样内联（不经 Iceberg 模型再序列化），`metadata-location` 取其中 `location` 后接
    /// `/metadata/v<N>.metadata.json`。内容无法解析的界定按 REQ-AMB-STO-COLD-TABLE（[`current::parse_metadata`]）。
    pub async fn load(&self, dataset: &Dataset, n: u64) -> Result<Loaded, ReadError> {
        let (bytes, _) = current::read_metadata(&self.store, &self.layout, dataset, n).await?;
        let metadata: Value = serde_json::from_slice(&bytes)
            .map_err(|e| ReadError::Broken(format!("v{n}.metadata.json: {e}")))?;
        let location = metadata
            .get("location")
            .and_then(Value::as_str)
            .ok_or_else(|| ReadError::Broken(format!("v{n}.metadata.json has no location")))?;
        Ok(Loaded {
            metadata_location: format!("{location}/metadata/v{n}.metadata.json"),
            metadata,
        })
    }
}

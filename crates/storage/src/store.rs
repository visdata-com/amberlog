//! 对象存储的构造与启动探测（REQ-AMB-CFG-STARTUP-V2「`wal.dir` 与对象存储的校验」）。
//!
//! `object_store.url` 的文法校验由配置装载完成，这里只接收已校验、已解码的位置。
//! 对象存储不可访问的判定口径见 REQ-AMB-STO-HOT-LAYOUT；本模块的错误一律归入该情形，由启动流程映射为退出码 3。

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use amberlog_common::dataset::Dataset;

use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey, S3ConditionalPut};
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{
    BackoffConfig, Certificate, ClientOptions, ObjectStore, ObjectStoreExt, PutMode, PutOptions,
    PutPayload, RetryConfig,
};

/// 已校验的对象存储位置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Location {
    /// `file:///…`：本地目录（已解码的绝对路径）。
    File { dir: std::path::PathBuf },
    /// `s3://<bucket>[/<path>]`：存储桶名原样，路径已逐段解码。
    S3 { bucket: String, path: Vec<String> },
}

/// `object_store.s3.*` 的取值。调试输出不带 `secret_access_key` 的原文。
#[derive(Clone, Default)]
pub struct S3Options {
    pub endpoint: Option<String>,
    pub region: String,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub force_path_style: bool,
    /// 服务端加密（`object_store.s3.server_side_encryption` 与 `sse_kms_key_id`，REQ-AMB-ARCH-ENCRYPTION）：
    /// 由本层对每个写入请求统一加头。
    pub server_side_encryption: Option<ServerSideEncryption>,
    /// `object_store.s3.ca_file` 的内容（PEM，已在配置一步校验）：追加到系统根证书之上，用于验证对象存储端点的证书。
    pub ca_pem: Option<Vec<u8>>,
}

impl fmt::Debug for S3Options {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Options")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("access_key_id", &self.access_key_id)
            .field(
                "secret_access_key",
                &self.secret_access_key.as_ref().map(|_| format_args!("***")),
            )
            .field("force_path_style", &self.force_path_style)
            .field("server_side_encryption", &self.server_side_encryption)
            .field("ca_pem", &self.ca_pem)
            .finish()
    }
}

/// 服务端加密方式。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerSideEncryption {
    /// `sse_s3`：`x-amz-server-side-encryption: AES256`。
    SseS3,
    /// `sse_kms`：`x-amz-server-side-encryption: aws:kms`，配置了密钥编号时另带它。
    SseKms { key_id: Option<String> },
}

/// 打开后的对象存储与本系统的根前缀（`[<s3 路径>/]<storage.prefix>`）。
#[derive(Clone)]
pub struct Store {
    pub inner: Arc<dyn ObjectStore>,
    pub root: Path,
    /// 打开时所用的位置（冷层表的 `location` 由它与根前缀算出，REQ-AMB-STO-COLD-TABLE）。
    pub location: Location,
    /// `file://` 时为其目录：该目录不存在属对象存储不可访问，而不是对象不存在（见 [`crate::objects`]）。
    pub(crate) local_dir: Option<std::path::PathBuf>,
}

impl fmt::Debug for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Store").field("root", &self.root).finish()
    }
}

/// S3 客户端的连接选项：超时，以及 `object_store.s3.ca_file` 中的证书——追加到系统根证书之上，不替换
/// （REQ-AMB-ARCH-ENCRYPTION-OUT-CA-TRUST；也作用于凭据提供方端点的客户端）。证书始终验证，不提供跳过验证的选项。
fn client_options(s3: &S3Options) -> Result<ClientOptions, Unavailable> {
    let mut options = ClientOptions::new()
        .with_connect_timeout(Duration::from_secs(5))
        .with_timeout(Duration::from_secs(30));
    if let Some(pem) = &s3.ca_pem {
        // 内容已在配置一步校验过
        let certs = Certificate::from_pem_bundle(pem)
            .map_err(|e| unavailable("cannot use object_store.s3.ca_file", e))?;
        for cert in certs {
            options = options.with_root_certificate(cert);
        }
    }
    Ok(options)
}

/// 服务端加密的 object_store 配置键与取值。
fn sse_config(sse: Option<&ServerSideEncryption>) -> Vec<(AmazonS3ConfigKey, String)> {
    let key = |name: &str| {
        name.parse::<AmazonS3ConfigKey>()
            .expect("object_store SSE key")
    };
    match sse {
        None => Vec::new(),
        Some(ServerSideEncryption::SseS3) => {
            vec![(key("aws_server_side_encryption"), "AES256".to_owned())]
        }
        Some(ServerSideEncryption::SseKms { key_id }) => {
            let mut out = vec![(key("aws_server_side_encryption"), "aws:kms".to_owned())];
            if let Some(id) = key_id {
                out.push((key("aws_sse_kms_key_id"), id.clone()));
            }
            out
        }
    }
}

/// 对象存储不可访问（REQ-AMB-STO-HOT-LAYOUT）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unavailable(pub String);

impl fmt::Display for Unavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Unavailable {}

fn unavailable(what: &str, e: impl fmt::Display) -> Unavailable {
    Unavailable(format!("{what}: {e}"))
}

impl Store {
    /// 构造对象存储客户端。`file://` 所指目录不存在即不可访问；S3 的可达性要到探测时才知道。
    pub fn open(location: &Location, s3: &S3Options, prefix: &str) -> Result<Store, Unavailable> {
        let local_dir = match location {
            Location::File { dir } => Some(dir.clone()),
            Location::S3 { .. } => None,
        };
        let (inner, mut parts): (Arc<dyn ObjectStore>, Vec<String>) = match location {
            Location::File { dir } => {
                let fs = LocalFileSystem::new_with_prefix(dir)
                    .map_err(|e| unavailable("object store directory is not accessible", e))?;
                (Arc::new(fs), Vec::new())
            }
            Location::S3 { bucket, path } => {
                // 凭据缺省时走标准 AWS_* 环境变量与凭据链，显式配置覆盖之；其余 AWS_* 不生效（见 [`CREDENTIAL_ENV`]）。
                let mut b = credential_env(std::env::vars_os())
                    .into_iter()
                    .fold(AmazonS3Builder::new(), |b, (key, value)| {
                        b.with_config(key, value)
                    })
                    .with_bucket_name(bucket)
                    .with_region(&s3.region)
                    .with_virtual_hosted_style_request(!s3.force_path_style)
                    .with_conditional_put(S3ConditionalPut::ETagMatch)
                    .with_retry(startup_retry())
                    .with_client_options(client_options(s3)?);
                // 服务端加密头由本层对每个写入请求统一加（REQ-AMB-ARCH-ENCRYPTION）：object_store 只在 PutObject
                // （含条件写入）、CreateMultipartUpload 等写入请求上带它，读取请求不带；SSE-C 不配置，也不从环境变量取
                for (key, value) in sse_config(s3.server_side_encryption.as_ref()) {
                    b = b.with_config(key, value);
                }
                if let Some(endpoint) = &s3.endpoint {
                    // object_store 在虚拟主机风格下把 endpoint 当作已含桶名的地址使用，
                    // 所以由这里把桶名放进主机名：`http://minio:9000` → `http://<bucket>.minio:9000`。
                    let endpoint = if s3.force_path_style {
                        endpoint.clone()
                    } else {
                        if endpoint_host_is_ip(endpoint) {
                            // 桶名无法放进 IP 地址；不拦下的话 object_store 会在签名时 panic。
                            return Err(Unavailable(
                                "virtual-hosted-style S3 requests need a DNS host name in object_store.s3.endpoint; \
                                 set object_store.s3.force_path_style = true to use an IP address endpoint"
                                    .into(),
                            ));
                        }
                        virtual_hosted_endpoint(endpoint, bucket)
                    };
                    b = b
                        .with_allow_http(endpoint.starts_with("http://"))
                        .with_endpoint(endpoint);
                }
                if let (Some(id), Some(secret)) = (&s3.access_key_id, &s3.secret_access_key) {
                    b = b.with_access_key_id(id).with_secret_access_key(secret);
                }
                let s3 = b
                    .build()
                    .map_err(|e| unavailable("cannot build S3 client", e))?;
                (Arc::new(s3), path.clone())
            }
        };
        parts.push(prefix.to_owned());
        let root = Path::from_iter(parts);
        #[cfg(feature = "fault-injection")]
        let inner = crate::fault::store::FaultyStore::wrap(inner, &root);
        Ok(Store {
            inner,
            root,
            location: location.clone(),
            local_dir,
        })
    }

    /// 数据集前缀 `<root>/<org>/<stream>`：该数据集的数据文件、快照链与计数对象都在其下（REQ-AMB-STO-HOT-LAYOUT-V2）。
    pub fn dataset_prefix(&self, dataset: &Dataset) -> Path {
        self.root
            .clone()
            .join(dataset.org.as_str())
            .join(dataset.stream.as_str())
    }

    /// 数据集的冷层表目录 `<root>/_iceberg/<org>/<stream>`（REQ-AMB-STO-COLD-TABLE）：冷层写入是其下对象的唯一写者，
    /// 跨层查询与桶生命周期直接读它。
    pub fn cold_table_prefix(&self, dataset: &Dataset) -> Path {
        self.root
            .clone()
            .join(crate::layout::COLD_ROOT)
            .join(dataset.org.as_str())
            .join(dataset.stream.as_str())
    }

    /// `<root>/_system/<name>`
    pub fn system_path(&self, name: &str) -> Path {
        self.root.clone().join("_system").join(name)
    }

    /// `<root>/_system/<rel>`，`rel` 按 `/` 分段。
    fn system_key(&self, rel: &str) -> Path {
        rel.split('/')
            .fold(self.root.clone().join("_system"), |p, part| p.join(part))
    }

    /// 读取 `_system/` 之下相对路径为 `rel` 的整个对象；不存在为 `Ok(None)`（评估记录对象，REQ-AMB-ALR-EVALUATIONS）。
    pub async fn system_read(&self, rel: &str) -> Result<Option<bytes::Bytes>, Unavailable> {
        Ok(self.read(&self.system_key(rel)).await?.map(|f| f.bytes))
    }

    /// 以普通覆盖写（不带条件头）写出 `_system/` 之下相对路径为 `rel` 的对象。
    pub async fn system_put(&self, rel: &str, bytes: Vec<u8>) -> Result<(), Unavailable> {
        self.put(&self.system_key(rel), bytes.into()).await
    }

    /// 删除 `_system/` 之下相对路径为 `rel` 的对象；已不存在为 `Ok(false)`。
    pub async fn system_delete(&self, rel: &str) -> Result<bool, Unavailable> {
        self.delete(&self.system_key(rel)).await
    }

    /// 列出 `_system/` 之下前缀 `rel` 之下的全部对象（递归，按路径段匹配前缀），返回各对象相对 `_system/` 的路径。
    pub async fn system_list(&self, rel: &str) -> Result<Vec<String>, Unavailable> {
        let system = self.root.clone().join("_system");
        Ok(self
            .list(&self.system_key(rel))
            .await?
            .into_iter()
            .filter_map(|meta| {
                let parts: Vec<String> = meta
                    .location
                    .prefix_match(&system)?
                    .map(|p| p.as_ref().to_owned())
                    .collect();
                Some(parts.join("/"))
            })
            .collect())
    }

    /// 启动探测：在 `_system/` 下完成一次写入、读取与条件写探测（REQ-AMB-CFG-STARTUP-V5）。
    pub async fn probe(&self) -> Result<(), ProbeError> {
        self.probe_readyz().await.map_err(ProbeError::Unavailable)?;
        self.probe_create_only().await
    }

    /// 就绪探测：对固定对象 `<root>/_system/readyz-probe` 写入并读回，每次覆盖同一对象
    /// （REQ-AMB-OBS-HEALTH-METRICS）。
    pub async fn probe_readyz(&self) -> Result<(), Unavailable> {
        let probe = self.system_path("readyz-probe");
        let body = format!("{}-{}", std::process::id(), unique_suffix());
        self.inner
            .put(&probe, PutPayload::from(body.clone()))
            .await
            .map_err(|e| unavailable("object store write probe failed", e))?;
        let read = self
            .inner
            .get(&probe)
            .await
            .map_err(|e| unavailable("object store read probe failed", e))?
            .bytes()
            .await
            .map_err(|e| unavailable("object store read probe failed", e))?;
        if read.as_ref() != body.as_bytes() {
            return Err(Unavailable(
                "object store read probe returned different content".into(),
            ));
        }
        Ok(())
    }

    /// 只读探测（REQ-AMB-STO-COLD-CATALOG-TERM-RO-PROBE）：对 `<root>/_iceberg/` 以 `/` 为分隔符列举一次，不写任何对象。
    /// 列举结果为空也算成功；`file://` 所指目录不存在属对象存储不可访问。只读目录模式的启动探测与就绪探测都用它。
    pub async fn probe_readonly(&self) -> Result<(), Unavailable> {
        self.list_level(&self.root.clone().join(crate::layout::COLD_ROOT))
            .await
            .map(|_| ())
    }

    /// 条件写探测（REQ-AMB-CFG-STARTUP-V5）：对 `_system/` 下一个每次启动新取的键先后两次「仅当不存在时创建」，
    /// 第一次须成功，第二次须以「已存在」失败。第二次写入成功（存储静默忽略了 `If-None-Match`），或任一次得到
    /// HTTP 400 或 501，即存储不支持本系统的提交协议（[`ProbeError::Unsupported`]）；其余失败属对象存储不可访问。
    /// 探测写出的键不清理（块不作承诺），也就不需要删除权限。
    async fn probe_create_only(&self) -> Result<(), ProbeError> {
        const WHAT: &str = "object store conditional write probe failed";
        let probe = self.system_path(&format!(
            "create-only-probe-{}",
            crate::layout::new_object_id()
        ));
        let create = || PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };
        let refused = |e: &object_store::Error| matches!(http_status(e), Some(400 | 501));
        match self
            .inner
            .put_opts(&probe, PutPayload::from_static(b"1"), create())
            .await
        {
            Ok(_) => {}
            Err(e) if refused(&e) => {
                return Err(ProbeError::Unsupported(format!(
                    "object store refused a create-if-absent write: {e}"
                )));
            }
            Err(e) => return Err(ProbeError::Unavailable(unavailable(WHAT, e))),
        }
        match self
            .inner
            .put_opts(&probe, PutPayload::from_static(b"2"), create())
            .await
        {
            Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
            Ok(_) => Err(ProbeError::Unsupported(
                "object store does not honor create-if-absent writes (a second create of the same key succeeded)"
                    .into(),
            )),
            Err(e) if refused(&e) => Err(ProbeError::Unsupported(format!(
                "object store refused a create-if-absent write: {e}"
            ))),
            Err(e) => Err(ProbeError::Unavailable(unavailable(WHAT, e))),
        }
    }
}

/// 启动探测的失败。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeError {
    /// 对象存储不可访问（REQ-AMB-STO-HOT-LAYOUT）。
    Unavailable(Unavailable),
    /// 对象存储不支持本系统的提交协议所用的「仅当不存在时创建」（REQ-AMB-CFG-STARTUP-V5，退出码 2）。
    Unsupported(String),
}

impl fmt::Display for ProbeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProbeError::Unavailable(u) => write!(f, "{u}"),
            ProbeError::Unsupported(m) => f.write_str(m),
        }
    }
}

/// 对象存储请求失败所得的 HTTP 状态码。object_store 不对外给出它（一般错误的来源是其内部的重试错误），只能从错误链
/// 的文本 `non-2xx status code: <状态码>` 取；故障注入层的错误文本用同一格式。取不到为 `None`。
pub(crate) fn http_status(e: &object_store::Error) -> Option<u16> {
    const MARK: &str = "non-2xx status code: ";
    let mut cur: Option<&dyn std::error::Error> = Some(e);
    while let Some(err) = cur {
        let text = err.to_string();
        if let Some(i) = text.find(MARK) {
            let digits: String = text[i + MARK.len()..]
                .chars()
                .take_while(char::is_ascii_digit)
                .collect();
            if let Ok(code) = digits.parse() {
                return Some(code);
            }
        }
        cur = err.source();
    }
    None
}

/// S3 客户端从环境读取的变量：只有凭据与凭据链一类（REQ-AMB-CFG-STARTUP-V4 的密钥配置项「缺省走标准 `AWS_*` 环境变量与凭据链」），
/// 取 object_store 0.13.2 的 `AmazonS3Builder::from_env` 识别、且只用于获取凭据的那些键：静态密钥与会话令牌（含 object_store 认的别名
/// `AWS_TOKEN`），凭据链的 Web 身份（如 EKS 的 IRSA，含其 STS 端点）、容器凭据（ECS、EKS Pod Identity）与实例元数据服务各项。
/// 其余 `AWS_*`（S3 端点、区域、服务端加密、校验和、跳过签名、请求方付费、S3 Express、标签、复制等）一概不读：它们会改变对象怎样
/// 写进客户的桶，只能经 `AMBERLOG_` 配置项设定（配置用环境变量只认该前缀）。名字按标准写法（全大写）逐字匹配。
const CREDENTIAL_ENV: [(&str, AmazonS3ConfigKey); 13] = [
    ("AWS_ACCESS_KEY_ID", AmazonS3ConfigKey::AccessKeyId),
    ("AWS_SECRET_ACCESS_KEY", AmazonS3ConfigKey::SecretAccessKey),
    ("AWS_SESSION_TOKEN", AmazonS3ConfigKey::Token),
    ("AWS_TOKEN", AmazonS3ConfigKey::Token),
    (
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        AmazonS3ConfigKey::WebIdentityTokenFile,
    ),
    ("AWS_ROLE_ARN", AmazonS3ConfigKey::RoleArn),
    ("AWS_ROLE_SESSION_NAME", AmazonS3ConfigKey::RoleSessionName),
    ("AWS_ENDPOINT_URL_STS", AmazonS3ConfigKey::StsEndpoint),
    (
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        AmazonS3ConfigKey::ContainerCredentialsRelativeUri,
    ),
    (
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        AmazonS3ConfigKey::ContainerCredentialsFullUri,
    ),
    (
        "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
        AmazonS3ConfigKey::ContainerAuthorizationTokenFile,
    ),
    ("AWS_METADATA_ENDPOINT", AmazonS3ConfigKey::MetadataEndpoint),
    ("AWS_IMDSV1_FALLBACK", AmazonS3ConfigKey::ImdsV1Fallback),
];

/// 从环境变量中挑出 [`CREDENTIAL_ENV`] 所列的（名字逐字匹配、取值须为合法 UTF-8）。
fn credential_env(
    vars: impl IntoIterator<Item = (std::ffi::OsString, std::ffi::OsString)>,
) -> Vec<(AmazonS3ConfigKey, String)> {
    vars.into_iter()
        .filter_map(|(name, value)| {
            let (_, key) = CREDENTIAL_ENV.iter().find(|(n, _)| name == *n)?;
            Some((*key, value.into_string().ok()?))
        })
        .collect()
}

fn endpoint_host_is_ip(endpoint: &str) -> bool {
    let rest = endpoint.split_once("://").map_or(endpoint, |(_, r)| r);
    let authority = rest.split('/').next().unwrap_or(rest);
    if authority.starts_with('[') {
        return true;
    }
    let host = authority.rsplit_once(':').map_or(authority, |(h, _)| h);
    host.parse::<std::net::Ipv4Addr>().is_ok()
}

fn virtual_hosted_endpoint(endpoint: &str, bucket: &str) -> String {
    match endpoint.split_once("://") {
        Some((scheme, rest)) => format!("{scheme}://{bucket}.{rest}"),
        None => endpoint.to_owned(),
    }
}

/// 启动期的重试上限：不可达的端点应在几十秒内判定失败，而不是按库默认的三分钟。
fn startup_retry() -> RetryConfig {
    RetryConfig {
        backoff: BackoffConfig::default(),
        max_retries: 3,
        retry_timeout: Duration::from_secs(20),
    }
}

fn unique_suffix() -> String {
    use std::hash::BuildHasher;
    let h = std::collections::hash_map::RandomState::new().hash_one(std::process::id());
    format!("{h:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn s3_options_debug_hides_secret() {
        let o = S3Options {
            access_key_id: Some("AKIDPLAIN".into()),
            secret_access_key: Some("sk-Very-Secret".into()),
            ..S3Options::default()
        };
        let out = format!("{o:?}");
        assert!(!out.contains("Very-Secret"), "{out}");
        assert!(out.contains("secret_access_key: Some(***)"), "{out}");
        assert!(out.contains("AKIDPLAIN"), "{out}");
        assert!(format!("{:?}", S3Options::default()).contains("secret_access_key: None"));
    }

    #[tokio::test]
    async fn file_store_probe_writes_under_system_prefix() {
        let dir = tempdir();
        let store = Store::open(
            &Location::File { dir: dir.clone() },
            &S3Options::default(),
            "amberlog",
        )
        .unwrap();
        // 连续三次启动探测：就绪探测对象固定为一个；条件写探测每次取新键、不删除
        for round in 1..=3 {
            store.probe().await.unwrap();
            let system = dir.join("amberlog/_system");
            let names: Vec<_> = std::fs::read_dir(&system)
                .unwrap()
                .map(|e| e.unwrap().file_name().into_string().unwrap())
                .collect();
            assert_eq!(names.iter().filter(|n| *n == "readyz-probe").count(), 1);
            assert_eq!(
                names
                    .iter()
                    .filter(|n| n.starts_with("create-only-probe-"))
                    .count(),
                round
            );
            assert_eq!(names.len(), round + 1, "{names:?}");
        }
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn http_status_from_error_text() {
        let generic = |m: &str| object_store::Error::Generic {
            store: "S3",
            source: m.to_owned().into(),
        };
        assert_eq!(
            http_status(&generic(
                "Error performing PUT x in 1ms - Server returned non-2xx status code: 501 Not Implemented: "
            )),
            Some(501)
        );
        assert_eq!(http_status(&generic("connection refused")), None);
    }

    #[test]
    fn virtual_hosted_endpoint_puts_bucket_in_host() {
        assert_eq!(
            virtual_hosted_endpoint("http://minio:9000", "logs"),
            "http://logs.minio:9000"
        );
        assert_eq!(
            virtual_hosted_endpoint("https://s3.example.com", "My_Bucket"),
            "https://My_Bucket.s3.example.com"
        );
        assert!(endpoint_host_is_ip("http://127.0.0.1:9000"));
        assert!(endpoint_host_is_ip("http://[::1]:9000/x"));
        assert!(!endpoint_host_is_ip("http://minio:9000"));
        let s3 = S3Options {
            endpoint: Some("http://127.0.0.1:9000".into()),
            region: "us-east-1".into(),
            ..Default::default()
        };
        let loc = Location::S3 {
            bucket: "logs".into(),
            path: vec![],
        };
        assert!(Store::open(&loc, &s3, "amberlog").is_err());
    }

    #[test]
    fn only_credential_env_is_read() {
        let vars = [
            ("AWS_ACCESS_KEY_ID", "id"),
            ("AWS_SECRET_ACCESS_KEY", "secret"),
            ("AWS_SESSION_TOKEN", "token"),
            ("AWS_ROLE_ARN", "arn"),
            ("AWS_ENDPOINT", "http://elsewhere"),
            ("AWS_ENDPOINT_URL", "http://elsewhere"),
            ("AWS_REGION", "eu-west-1"),
            ("AWS_SERVER_SIDE_ENCRYPTION", "sse:kms"),
            ("AWS_SSE_CUSTOMER_KEY_BASE64", "a2V5"),
            ("AWS_SKIP_SIGNATURE", "true"),
            ("AWS_TOKEN", "alias"),
            ("AWS_ENDPOINT_URL_STS", "http://sts"),
            ("AWS_METADATA_ENDPOINT", "http://imds"),
            ("AWS_ENDPOINT_URL_S3", "http://elsewhere"),
            ("AWS_S3_EXPRESS", "true"),
            ("AWS_DISABLE_TAGGING", "true"),
            ("aws_access_key_id", "lowercase-not-read"),
            ("AMBERLOG_X", "y"),
        ]
        .map(|(k, v)| (k.into(), v.into()));
        let got: Vec<(String, String)> = credential_env(vars)
            .into_iter()
            .map(|(k, v)| (k.as_ref().to_owned(), v))
            .collect();
        assert_eq!(
            got,
            [
                ("aws_access_key_id", "id"),
                ("aws_secret_access_key", "secret"),
                ("aws_session_token", "token"),
                ("aws_role_arn", "arn"),
                ("aws_session_token", "alias"),
                ("aws_endpoint_url_sts", "http://sts"),
                ("aws_metadata_endpoint", "http://imds"),
            ]
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
        );
    }

    #[test]
    fn missing_file_dir_is_unavailable() {
        let dir = tempdir().join("missing");
        let r = Store::open(&Location::File { dir }, &S3Options::default(), "amberlog");
        assert!(r.is_err());
    }

    fn tempdir() -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("amberlog-store-{}", unique_suffix()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }
}

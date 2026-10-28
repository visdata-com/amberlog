//! 对象存储请求的故障注入（`fault-injection` feature，只供测试用）。
//!
//! 环境变量 `TESTBED_FAULT_STORE` 给出一个控制文件的路径。设了它，[`crate::store::Store::open`] 就在对象存储客户端外
//! 包一层 [`FaultyStore`]：每个请求发出前读一次控制文件，按其中的规则决定照常转发、不转发而以某个 HTTP 状态失败、转发后
//! 把应答换成失败，或把「仅当不存在时创建」当作普通写入转发。控制文件不存在或为空时一律照常转发，测试在运行中写入、
//! 改写或删除它即可施加或撤除注入，不必重启进程（同 `TESTBED_FAULT_WAL` 的做法）。不开该 feature 的构建里没有这层。
//!
//! 控制文件每行一条规则，`#` 起为注释，空行忽略：
//!
//! ```text
//! <操作> <路径前缀> <效果> [skip=<n>] [times=<n>]
//! ```
//!
//! - 操作：`get`（读取）、`head`（探测，读取对象元数据）、`put`（普通覆盖写，含分段上传与复制、改名的目标）、
//!   `create`（「仅当不存在时创建」）、`delete`、`list`（含按层列举），`write` 是 `put` 与 `create` 两者，`*` 是全部；
//!   可用逗号连写几种，如 `get,head`。
//! - 路径前缀：相对根前缀 `<storage.prefix>/` 的对象键（列举为所列前缀后加 `/`）以它开头即命中，如 `_iceberg/`、
//!   `default/app/manifest/_current`；`*` 命中一切。
//! - 效果：
//!   - `<状态码>`（如 `403`、`501`、`503`）：不转发，以该状态的错误失败；
//!   - `after-<状态码>`：照常转发，再把应答换成该状态的错误（不论转发的结果；只用于 `put` 与 `create` 的单次写入，
//!     分段上传按 `<状态码>` 处理）；
//!   - `ignore-condition`：只对 `create` 有效，按普通覆盖写转发并应答成功，即存储静默忽略 `If-None-Match` 请求头；
//!   - `stall`：请求挂起、不转发也不返回，直到控制文件中不再有这一行规则（删去或改写），再照常转发
//!     （REQ-AMB-STO-MANIFEST-COMMIT-V5 的测试用它挂住一个数据集的提交：如 `write default/app/ stall`
//!     挂住 `app` 数据集前缀之下的写入）。挂起的是发请求的异步任务，不占线程，也不挡其他请求。
//! - `skip=<n>`：命中本规则的前 n 个请求照常转发，不计入下面的次数；`times=<n>`：此后至多对 n 个请求生效。
//!   计数按规则行的原文记在进程内，改写控制文件时保留同一行就接着计。
//!
//! 请求记录：环境变量 `TESTBED_FAULT_STORE_LOG` 给出一个文件时，每个经过本层的请求（读取、探测、`get_ranges`、写入、
//! 删除流中的逐个删除、各种列举、复制与改名的目标；不论是否命中规则）都在其末尾追加一行 `<操作> <相对根前缀的对象键>`
//! （列举记所列前缀），同一把锁下整行写入，并发请求的行不交错。供测试断言某前缀之下没有收到某类请求
//! （REQ-AMB-QRY-CROSS-TIER 场景「停用的数据集查询不读冷层」）。`TESTBED_FAULT_STORE` 与 `TESTBED_FAULT_STORE_LOG`
//! 任设其一即包上本层：只设后者时只记录、不注入。
//!
//! 状态码换成 object_store 的错误时与它对 S3 应答的归类一致：404 为对象不存在，403 为没有权限，401 为未认证，
//! 409 为已存在，412 为前提不满足，其余（含 501、503）为一般错误。按 REQ-AMB-STO-HOT-LAYOUT 的分类，除 404 与 `create`
//! 的 409、412 之外都属对象存储不可访问。

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result,
};

/// 控制文件路径所在的环境变量。
pub const CONTROL_ENV: &str = "TESTBED_FAULT_STORE";

/// 请求记录文件路径所在的环境变量。
pub const LOG_ENV: &str = "TESTBED_FAULT_STORE_LOG";

/// 请求的种类（规则里的「操作」）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Get,
    Head,
    Put,
    Create,
    Delete,
    List,
}

impl Op {
    fn name(self) -> &'static str {
        match self {
            Op::Get => "get",
            Op::Head => "head",
            Op::Put => "put",
            Op::Create => "create",
            Op::Delete => "delete",
            Op::List => "list",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Effect {
    Fail(u16),
    After(u16),
    IgnoreCondition,
    Stall,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Rule {
    line: String,
    ops: Vec<Op>,
    /// `None`：命中一切。
    prefix: Option<String>,
    effect: Effect,
    skip: u64,
    times: Option<u64>,
}

/// 解析控制文件的内容；写错的行忽略（测试自己写的文件，写错时注入不生效，测试会因此失败而暴露）。
fn parse_rules(text: &str) -> Vec<Rule> {
    text.lines().filter_map(parse_rule).collect()
}

fn parse_rule(raw: &str) -> Option<Rule> {
    let line = raw.split('#').next().unwrap_or("").trim();
    let mut words = line.split_whitespace();
    let ops_word = words.next()?;
    let mut ops = Vec::new();
    for op in ops_word.split(',') {
        match op {
            "get" => ops.push(Op::Get),
            "head" => ops.push(Op::Head),
            "put" => ops.push(Op::Put),
            "create" => ops.push(Op::Create),
            "write" => ops.extend([Op::Put, Op::Create]),
            "delete" => ops.push(Op::Delete),
            "list" => ops.push(Op::List),
            "*" => ops.extend([Op::Get, Op::Head, Op::Put, Op::Create, Op::Delete, Op::List]),
            _ => return None,
        }
    }
    let prefix = match words.next()? {
        "*" => None,
        p => Some(p.to_owned()),
    };
    let effect = match words.next()? {
        "ignore-condition" => Effect::IgnoreCondition,
        "stall" => Effect::Stall,
        e => match e.strip_prefix("after-") {
            Some(code) => Effect::After(code.parse().ok()?),
            None => Effect::Fail(e.parse().ok()?),
        },
    };
    let (mut skip, mut times) = (0, None);
    for w in words {
        match w.split_once('=') {
            Some(("skip", n)) => skip = n.parse().ok()?,
            Some(("times", n)) => times = Some(n.parse().ok()?),
            _ => return None,
        }
    }
    Some(Rule {
        line: line.to_owned(),
        ops,
        prefix,
        effect,
        skip,
        times,
    })
}

/// 以 HTTP 状态 `status` 失败的错误（归类同 object_store 对 S3 应答的处理）。
fn injected(status: u16, op: Op, path: &str) -> object_store::Error {
    // 文本格式同 object_store 对 S3 非 2xx 应答的错误（[`crate::store`] 由此取状态码）
    let source = format!(
        "Server returned non-2xx status code: {status} (fault injection, {})",
        op.name()
    )
    .into();
    let path = path.to_owned();
    match status {
        404 => object_store::Error::NotFound { path, source },
        409 => object_store::Error::AlreadyExists { path, source },
        412 => object_store::Error::Precondition { path, source },
        403 => object_store::Error::PermissionDenied { path, source },
        401 => object_store::Error::Unauthenticated { path, source },
        _ => object_store::Error::Generic {
            store: "fault-injection",
            source,
        },
    }
}

/// 按控制文件注入故障的对象存储包装层。
pub struct FaultyStore {
    inner: Arc<dyn ObjectStore>,
    control: Arc<Control>,
}

/// 控制文件与命中计数（删除请求的流要在 `'static` 的闭包里判定，故单独共享）。
struct Control {
    /// 根前缀（`[<s3 路径>/]<storage.prefix>`）；规则的路径前缀相对于它。
    root: String,
    /// 控制文件（`TESTBED_FAULT_STORE`）；没设时不注入。
    file: Option<std::path::PathBuf>,
    /// 规则行原文 → 已命中的请求数。
    hits: Mutex<HashMap<String, u64>>,
    /// 请求记录文件（`TESTBED_FAULT_STORE_LOG`），写入时持锁。
    log: Mutex<Option<std::path::PathBuf>>,
}

impl FaultyStore {
    /// 设了 `TESTBED_FAULT_STORE` 或 `TESTBED_FAULT_STORE_LOG` 时把 `inner` 包起来，否则原样返回。
    pub fn wrap(inner: Arc<dyn ObjectStore>, root: &Path) -> Arc<dyn ObjectStore> {
        let file = std::env::var_os(CONTROL_ENV).map(Into::into);
        let log = std::env::var_os(LOG_ENV).map(Into::into);
        if file.is_none() && log.is_none() {
            return inner;
        }
        Arc::new(FaultyStore::new(inner, root, file, log))
    }

    fn new(
        inner: Arc<dyn ObjectStore>,
        root: &Path,
        file: Option<std::path::PathBuf>,
        log: Option<std::path::PathBuf>,
    ) -> FaultyStore {
        FaultyStore {
            inner,
            control: Arc::new(Control {
                root: root.as_ref().to_owned(),
                file,
                hits: Mutex::default(),
                log: Mutex::new(log),
            }),
        }
    }

    async fn effect(&self, op: Op, path: &str) -> Option<Effect> {
        self.control.effect(op, path).await
    }

    async fn check(&self, op: Op, path: &Path) -> Result<()> {
        self.control.check(op, path).await
    }
}

impl Control {
    fn relative<'a>(&self, path: &'a str) -> &'a str {
        if self.root.is_empty() {
            return path;
        }
        path.strip_prefix(self.root.as_str())
            .and_then(|rest| rest.strip_prefix('/').or(rest.is_empty().then_some("")))
            .unwrap_or(path)
    }

    /// 本次请求要施加的效果；没有命中的规则为 `None`。命中 `stall` 时先挂起到该行规则不在控制文件中，再照常转发（`None`）。
    async fn effect(&self, op: Op, path: &str) -> Option<Effect> {
        let rel = self.relative(path);
        self.record(op, rel);
        let (effect, line) = self.hit(op, rel)?;
        if effect != Effect::Stall {
            return Some(effect);
        }
        while self.rule_present(&line) {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        None
    }

    /// 控制文件此刻仍有原文为 `line` 的规则行。控制文件读不出（如测试正在改写）时按仍在处理，下一轮再读。
    fn rule_present(&self, line: &str) -> bool {
        let Some(file) = self.file.as_ref() else {
            return false;
        };
        match std::fs::read_to_string(file) {
            Ok(text) => parse_rules(&text).iter().any(|r| r.line == line),
            Err(e) => e.kind() != std::io::ErrorKind::NotFound,
        }
    }

    /// 命中的规则：(效果, 规则行原文)。
    fn hit(&self, op: Op, rel: &str) -> Option<(Effect, String)> {
        let text = std::fs::read_to_string(self.file.as_ref()?).ok()?;
        let mut hits = self.hits.lock().expect("故障注入计数锁未中毒");
        for rule in parse_rules(&text) {
            if !rule.ops.contains(&op) {
                continue;
            }
            // 列举所给的前缀不带结尾的 `/`：按带上它比较，`default/app/` 也命中对 `default/app` 的列举
            let listed = (op == Op::List).then(|| format!("{rel}/"));
            let matches =
                |p: &str| rel.starts_with(p) || listed.as_deref().is_some_and(|l| l.starts_with(p));
            if rule.prefix.as_deref().is_some_and(|p| !matches(p)) {
                continue;
            }
            if rule.effect == Effect::IgnoreCondition && op != Op::Create {
                continue;
            }
            let n = hits.entry(rule.line.clone()).or_default();
            *n += 1;
            if *n <= rule.skip {
                return None;
            }
            if rule.times.is_some_and(|t| *n - rule.skip > t) {
                continue;
            }
            return Some((rule.effect, rule.line));
        }
        None
    }

    /// 在请求记录文件末尾追加一行（持锁，整行一次写入）。
    fn record(&self, op: Op, rel: &str) {
        use std::io::Write;
        let log = self.log.lock().expect("请求记录锁未中毒");
        let Some(log) = log.as_ref() else {
            return;
        };
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log)
        {
            let _ = f.write_all(format!("{} {rel}\n", op.name()).as_bytes());
        }
    }

    /// 读取、探测、删除、列举与分段上传：效果为 `<状态码>` 或 `after-<状态码>` 时都不转发、以该状态失败。
    async fn check(&self, op: Op, path: &Path) -> Result<()> {
        match self.effect(op, path.as_ref()).await {
            Some(Effect::Fail(status) | Effect::After(status)) => {
                Err(injected(status, op, path.as_ref()))
            }
            _ => Ok(()),
        }
    }
}

impl FaultyStore {
    /// 判定一次列举（可能挂起）之后再开始 `list` 给出的流。
    fn checked_list(
        &self,
        prefix: Option<Path>,
        list: impl FnOnce() -> BoxStream<'static, Result<ObjectMeta>> + Send + 'static,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let control = Arc::clone(&self.control);
        stream::once(async move {
            let at = prefix.unwrap_or_default();
            match control.check(Op::List, &at).await {
                Ok(()) => list(),
                Err(e) => stream::once(async move { Err(e) }).boxed(),
            }
        })
        .flatten()
        .boxed()
    }
}

impl fmt::Display for FaultyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FaultyStore({})", self.inner)
    }
}

impl fmt::Debug for FaultyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FaultyStore")
            .field("root", &self.control.root)
            .field("control", &self.control.file)
            .finish()
    }
}

#[async_trait]
impl ObjectStore for FaultyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        mut opts: PutOptions,
    ) -> Result<PutResult> {
        let op = match opts.mode {
            PutMode::Create => Op::Create,
            PutMode::Overwrite | PutMode::Update(_) => Op::Put,
        };
        match self.effect(op, location.as_ref()).await {
            None | Some(Effect::Stall) => self.inner.put_opts(location, payload, opts).await,
            Some(Effect::Fail(status)) => Err(injected(status, op, location.as_ref())),
            Some(Effect::After(status)) => {
                let _ = self.inner.put_opts(location, payload, opts).await;
                Err(injected(status, op, location.as_ref()))
            }
            Some(Effect::IgnoreCondition) => {
                opts.mode = PutMode::Overwrite;
                self.inner.put_opts(location, payload, opts).await
            }
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.check(Op::Put, location).await?;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let op = if options.head { Op::Head } else { Op::Get };
        self.check(op, location).await?;
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        self.check(Op::Get, location).await?;
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        // 逐个判定、逐个删除：被注入的路径以错误结束，其余照常删除（测试规模，不在乎失去批量删除）
        let inner = Arc::clone(&self.inner);
        let control = Arc::clone(&self.control);
        locations
            .then(move |item| {
                let inner = Arc::clone(&inner);
                let control = Arc::clone(&control);
                async move {
                    let path = item?;
                    control.check(Op::Delete, &path).await?;
                    inner.delete(&path).await?;
                    Ok(path)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.cloned();
        self.checked_list(prefix.clone(), move || inner.list(prefix.as_ref()))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let inner = Arc::clone(&self.inner);
        let (prefix, offset) = (prefix.cloned(), offset.clone());
        self.checked_list(prefix.clone(), move || {
            inner.list_with_offset(prefix.as_ref(), &offset)
        })
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let at = prefix.cloned().unwrap_or_default();
        self.check(Op::List, &at).await?;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.check(Op::Put, to).await?;
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.check(Op::Put, to).await?;
        self.inner.rename_opts(from, to, options).await
    }
}

#[cfg(test)]
mod tests;

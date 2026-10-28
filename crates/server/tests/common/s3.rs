//! 接在 MinIO 台架（`tools/testbed.sh`）上的被测实例，以及两种故障注入。
//!
//! - 「对象存储已停止」：被测进程经本进程里的 TCP 转发（[`Relay`]）连 MinIO。[`Relay::stop`] 关掉监听并断开在途连接，
//!   此后被测进程的连接被拒；[`Relay::resume`] 在同一端口重新转发。被测进程读写的始终是真实 MinIO，台架本身不停，
//!   同一测试二进制里并行的其他测试不受影响。
//! - 「对象存储拒绝读取（403）」：[`S3Instance::with_app_user`] 让被测进程用台架的受限用户（`TESTBED_S3_APP_*`），
//!   其策略拒绝读取带标签 `testbed-deny=1` 的对象；[`S3Instance::deny_reads`] 以 root 凭据给某前缀下的现有对象
//!   打上该标签（以同样的字节重新写入并带标签），此后被测进程对它们的 GET 与 HEAD 得到真实的 403。
//!
//! 用法：
//! ```ignore
//! let inst = S3Instance::new();             // 或 S3Instance::with_app_user()
//! let server = inst.start(&[("flush.max_records", Some("1"))]);
//! inst.relay.stop();                        // 对象存储已停止
//! inst.relay.resume();                      // 恢复
//! inst.deny_reads("default/app/");          // amberlog/default/app/ 下现有对象此后读取得 403
//! let store = inst.store();                 // 测试进程自己的客户端（root 凭据，直连台架，不经转发）
//! ```
//! 每个实例在台架桶里用一个独有的路径（`s3://<桶>/<路径>`），`storage.prefix` 取缺省的 `amberlog`。

use std::io::{ErrorKind, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use amberlog_storage::store::{Location, S3Options, Store};
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutOptions, TagSet};

use super::{Fixture, Kv, Server};

/// 被测进程的 `storage.prefix`（缺省值）。
pub const PREFIX: &str = "amberlog";

/// 台架环境变量；缺失时 panic 并提示怎样起台架。
pub fn testbed(k: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| {
        panic!("{k} 未设置：这个测试要用 MinIO 台架，先 eval \"$(tools/testbed.sh <标签> up)\"")
    })
}

/// 一个接在 MinIO 台架前的被测实例：`s3://<台架桶>/<本实例独有路径>`，端点指向本实例的 [`Relay`]。
pub struct S3Instance {
    pub fixture: Fixture,
    pub relay: Relay,
    path: String,
    /// 被测进程所用的凭据。
    credentials: (String, String),
}

impl Default for S3Instance {
    fn default() -> Self {
        Self::new()
    }
}

impl S3Instance {
    /// 被测进程用台架的 root 凭据。
    pub fn new() -> S3Instance {
        S3Instance::with_credentials(
            testbed("TESTBED_S3_ACCESS_KEY_ID"),
            testbed("TESTBED_S3_SECRET_ACCESS_KEY"),
        )
    }

    /// 被测进程用台架的受限用户（读不了带 `testbed-deny=1` 标签的对象），供 [`S3Instance::deny_reads`] 使用。
    pub fn with_app_user() -> S3Instance {
        S3Instance::with_credentials(
            testbed("TESTBED_S3_APP_ACCESS_KEY_ID"),
            testbed("TESTBED_S3_APP_SECRET_ACCESS_KEY"),
        )
    }

    fn with_credentials(id: String, secret: String) -> S3Instance {
        static N: AtomicU32 = AtomicU32::new(0);
        let endpoint = testbed("TESTBED_S3_ENDPOINT");
        let upstream = endpoint
            .strip_prefix("http://")
            .expect("台架端点是 http://")
            .trim_end_matches('/');
        let upstream: SocketAddr = std::net::ToSocketAddrs::to_socket_addrs(upstream)
            .unwrap()
            .next()
            .unwrap();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        S3Instance {
            fixture: Fixture::new(),
            relay: Relay::start(upstream),
            path: format!(
                "it-{}-{}-{nanos}",
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed)
            ),
            credentials: (id, secret),
        }
    }

    /// 本实例的对象存储配置（TOML 取值），外加 `extra`。
    pub fn config<'a>(&'a self, extra: &[Kv<'a>]) -> Vec<(&'a str, Option<String>)> {
        let mut kv: Vec<(&str, Option<String>)> = vec![
            (
                "object_store.url",
                Some(format!(
                    "\"s3://{}/{}\"",
                    testbed("TESTBED_S3_BUCKET"),
                    self.path
                )),
            ),
            ("object_store.s3.conditional_put", Some("\"etag\"".into())),
            (
                "object_store.s3.endpoint",
                Some(format!("\"http://{}\"", self.relay.addr)),
            ),
            (
                "object_store.s3.access_key_id",
                Some(format!("\"{}\"", self.credentials.0)),
            ),
            (
                "object_store.s3.secret_access_key",
                Some(format!("\"{}\"", self.credentials.1)),
            ),
            ("object_store.s3.force_path_style", Some("true".into())),
        ];
        kv.extend(extra.iter().map(|(k, v)| (*k, v.map(str::to_owned))));
        kv
    }

    /// 以本实例的对象存储配置外加 `extra` 启动，等到 `ready`。
    pub fn start(&self, extra: &[Kv]) -> Server {
        let owned = self.config(extra);
        let kv: Vec<Kv> = owned.iter().map(|(k, v)| (*k, v.as_deref())).collect();
        self.fixture.start(&kv)
    }

    /// 测试进程自己的对象存储客户端：root 凭据、直连台架（不经转发），根前缀与被测进程相同（`<路径>/amberlog`）。
    pub fn store(&self) -> Store {
        self.store_as(
            testbed("TESTBED_S3_ACCESS_KEY_ID"),
            testbed("TESTBED_S3_SECRET_ACCESS_KEY"),
        )
    }

    /// 同 [`S3Instance::store`]，但用被测进程的凭据：用来确认故障注入对被测进程确实生效。
    pub fn store_as_instance(&self) -> Store {
        self.store_as(self.credentials.0.clone(), self.credentials.1.clone())
    }

    fn store_as(&self, id: String, secret: String) -> Store {
        Store::open(
            &Location::S3 {
                bucket: testbed("TESTBED_S3_BUCKET"),
                path: vec![self.path.clone()],
            },
            &S3Options {
                endpoint: Some(testbed("TESTBED_S3_ENDPOINT")),
                region: "us-east-1".into(),
                access_key_id: Some(id),
                secret_access_key: Some(secret),
                force_path_style: true,
                ..S3Options::default()
            },
            PREFIX,
        )
        .unwrap()
    }

    /// 等到数据集 `default/<stream>` 编号最大的快照的 `summary.record_count` 等于 `n`（至多 30 秒）。
    pub fn wait_committed(&self, stream: &str, n: u64) {
        let store = self.store();
        let dir = Path::from(format!("{}/default/{stream}/manifest", store.root));
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let count = rt.block_on(async {
                let objects: Vec<_> = store.inner.list(Some(&dir)).try_collect().await.ok()?;
                let last = objects
                    .iter()
                    .filter(|m| {
                        m.location
                            .filename()
                            .and_then(|f| f.strip_suffix(".json"))
                            .is_some_and(|id| {
                                id.len() == 20 && id.bytes().all(|b| b.is_ascii_digit())
                            })
                    })
                    .max_by(|a, b| a.location.cmp(&b.location))?;
                let bytes = store
                    .inner
                    .get(&last.location)
                    .await
                    .ok()?
                    .bytes()
                    .await
                    .ok()?;
                let v: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
                v["summary"]["record_count"].as_u64()
            });
            if count == Some(n) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "30 秒内 {stream} 的已提交记录数未到 {n}（{count:?}）"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// 给 `amberlog/<rel>` 下的现有对象打上 `testbed-deny=1`（以同样的字节重新写入并带标签），返回打了标签的对象数。
    /// 被测进程须以 [`S3Instance::with_app_user`] 启动，此后它对这些对象的读取得到 403；之后新写的对象不受影响。
    pub fn deny_reads(&self, rel: &str) -> usize {
        let store = self.store();
        let prefix = Path::from(format!("{}/{}", store.root, rel.trim_matches('/')));
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let objects: Vec<_> = store.inner.list(Some(&prefix)).try_collect().await.unwrap();
            for meta in &objects {
                let bytes = store
                    .inner
                    .get(&meta.location)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
                let mut tags = TagSet::default();
                tags.push("testbed-deny", "1");
                let opts = PutOptions {
                    tags,
                    ..PutOptions::default()
                };
                store
                    .inner
                    .put_opts(&meta.location, bytes.into(), opts)
                    .await
                    .unwrap();
            }
            objects.len()
        })
    }
}

/// 本进程里的 TCP 转发：`addr` 上接受的连接逐字节转给 `upstream`。
pub struct Relay {
    pub addr: SocketAddr,
    shared: Arc<RelayShared>,
}

struct RelayShared {
    upstream: SocketAddr,
    /// 是否在转发；置为 false 后监听随即关闭。
    up: AtomicBool,
    /// 转发线程退出。
    quit: AtomicBool,
    /// 在途连接（两端各一个句柄），停止时全部断开。
    conns: Mutex<Vec<TcpStream>>,
    /// 台架发回被测进程的全部字节（[`Relay::statuses`] 从中取各响应的状态码）。
    replies: Arc<Mutex<Vec<u8>>>,
}

impl Relay {
    pub fn start(upstream: SocketAddr) -> Relay {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let shared = Arc::new(RelayShared {
            upstream,
            up: AtomicBool::new(true),
            quit: AtomicBool::new(false),
            conns: Mutex::new(Vec::new()),
            replies: Arc::new(Mutex::new(Vec::new())),
        });
        let s = Arc::clone(&shared);
        std::thread::spawn(move || accept_loop(s, addr, Some(listener)));
        Relay { addr, shared }
    }

    /// 停止转发：关掉监听（此后连接被拒）并断开全部在途连接。
    pub fn stop(&self) {
        self.shared.up.store(false, Ordering::SeqCst);
        for c in self.shared.conns.lock().unwrap().drain(..) {
            let _ = c.shutdown(Shutdown::Both);
        }
        // 等转发线程关掉监听
        let deadline = Instant::now() + Duration::from_secs(5);
        while TcpStream::connect(self.addr).is_ok() {
            assert!(Instant::now() < deadline, "转发端未关闭监听");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// 经本转发的台架响应的状态码（按到达先后）：台架对被测进程请求的作答记录。
    pub fn statuses(&self) -> Vec<u16> {
        let replies = self.shared.replies.lock().unwrap();
        let mut out = Vec::new();
        for (i, w) in replies.windows(12).enumerate() {
            let line_start = i == 0 || replies[i - 1] == b'\n';
            if line_start && w.starts_with(b"HTTP/1.1 ") && w[9..12].iter().all(u8::is_ascii_digit)
            {
                out.push(std::str::from_utf8(&w[9..12]).unwrap().parse().unwrap());
            }
        }
        out
    }

    /// 恢复转发（同一端口）。
    pub fn resume(&self) {
        self.shared.up.store(true, Ordering::SeqCst);
        let deadline = Instant::now() + Duration::from_secs(5);
        while TcpStream::connect(self.addr).is_err() {
            assert!(Instant::now() < deadline, "转发端未恢复监听");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.shared.quit.store(true, Ordering::SeqCst);
        self.shared.up.store(false, Ordering::SeqCst);
        for c in self.shared.conns.lock().unwrap().drain(..) {
            let _ = c.shutdown(Shutdown::Both);
        }
    }
}

fn accept_loop(s: Arc<RelayShared>, addr: SocketAddr, mut listener: Option<TcpListener>) {
    while !s.quit.load(Ordering::SeqCst) {
        let up = s.up.load(Ordering::SeqCst);
        match (&listener, up) {
            (Some(_), false) => listener = None,
            (None, true) => match TcpListener::bind(addr) {
                Ok(l) => listener = Some(l),
                Err(_) => std::thread::sleep(Duration::from_millis(10)),
            },
            _ => {}
        }
        let Some(l) = &listener else {
            std::thread::sleep(Duration::from_millis(10));
            continue;
        };
        l.set_nonblocking(true).unwrap();
        match l.accept() {
            Ok((client, _)) => {
                client.set_nonblocking(false).unwrap();
                let Ok(server) = TcpStream::connect(s.upstream) else {
                    continue;
                };
                {
                    let mut conns = s.conns.lock().unwrap();
                    conns.push(client.try_clone().unwrap());
                    conns.push(server.try_clone().unwrap());
                }
                pipe(
                    client.try_clone().unwrap(),
                    server.try_clone().unwrap(),
                    None,
                );
                pipe(server, client, Some(Arc::clone(&s.replies)));
            }
            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(_) => {}
        }
    }
}

/// 把 `from` 读到的字节写给 `to`，直到任一端关闭；`record` 给出时同时追加进去。
fn pipe(mut from: TcpStream, mut to: TcpStream, record: Option<Arc<Mutex<Vec<u8>>>>) {
    std::thread::spawn(move || {
        let mut buf = [0u8; 16 * 1024];
        loop {
            match from.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if let Some(r) = &record {
                        r.lock().unwrap().extend_from_slice(&buf[..n]);
                    }
                    if to.write_all(&buf[..n]).is_err() {
                        break;
                    }
                }
            }
        }
        let _ = to.shutdown(Shutdown::Write);
    });
}

impl S3Instance {
    /// 本实例在台架桶里的路径（`s3://<桶>/<路径>` 的 `<路径>`）。
    pub fn path(&self) -> &str {
        &self.path
    }

    /// 本实例根前缀 `<路径>/amberlog/` 之下 `rel` 的 ARN 资源名（会话策略用）。
    pub fn arn(&self, rel: &str) -> String {
        format!(
            "arn:aws:s3:::{}/{}/{PREFIX}/{rel}",
            testbed("TESTBED_S3_BUCKET"),
            self.path
        )
    }

    /// 本实例根前缀 `<路径>/amberlog/` 之下 `rel` 的列举前缀（会话策略中 `s3:prefix` 条件用）。
    pub fn list_prefix(&self, rel: &str) -> String {
        format!("{}/{PREFIX}/{rel}", self.path)
    }

    /// 只授予 `<storage.prefix>/<rel>` 之下读取与列举的会话策略（[`session_credentials`] 用）；只读目录模式的只读凭据
    /// 取 `rel` 为 `_iceberg/`。
    pub fn read_only_policy(&self, rel: &str) -> serde_json::Value {
        serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": [self.arn(&format!("{rel}*"))]},
                {
                    "Effect": "Allow",
                    "Action": ["s3:ListBucket"],
                    "Resource": [format!("arn:aws:s3:::{}", testbed("TESTBED_S3_BUCKET"))],
                    "Condition": {"StringLike": {"s3:prefix": [self.list_prefix(&format!("{rel}*"))]}},
                },
            ],
        })
    }

    /// 只读目录模式进程（`amberlog catalog`）的命令：对象存储配置同本实例，但不在配置中写凭据，改由 `envs`
    /// （如 [`session_credentials`] 的 `AWS_*`）给出；外加 `extra`。
    pub fn catalog(&self, extra: &[Kv], envs: &[(&str, String)]) -> std::process::Command {
        let mut kv = self.config(&[
            ("object_store.s3.access_key_id", None),
            ("object_store.s3.secret_access_key", None),
        ]);
        kv.extend(extra.iter().map(|(k, v)| (*k, v.map(str::to_owned))));
        let kv: Vec<Kv> = kv.iter().map(|(k, v)| (*k, v.as_deref())).collect();
        let mut cmd = self.fixture.catalog(&kv);
        for (k, v) in envs {
            cmd.env(k, v);
        }
        cmd
    }
}

/// 以台架受限用户（`TESTBED_S3_APP_*`，其自身策略允许 `s3:*`）向 MinIO 的 STS 发 AssumeRole，带会话策略 `policy`，
/// 取得一组临时凭据：其权限是受限用户策略与会话策略的交集，即 `policy` 所授。返回给被测进程的环境变量
/// （`AWS_ACCESS_KEY_ID`、`AWS_SECRET_ACCESS_KEY`、`AWS_SESSION_TOKEN`）。请求按 SigV4 自签（服务名 `sts`）。
pub fn session_credentials(policy: &serde_json::Value) -> Vec<(&'static str, String)> {
    let endpoint = testbed("TESTBED_S3_ENDPOINT");
    let host = endpoint
        .strip_prefix("http://")
        .expect("台架端点是 http://")
        .trim_end_matches('/');
    let addr: SocketAddr = std::net::ToSocketAddrs::to_socket_addrs(host)
        .unwrap()
        .next()
        .unwrap();
    let id = testbed("TESTBED_S3_APP_ACCESS_KEY_ID");
    let secret = testbed("TESTBED_S3_APP_SECRET_ACCESS_KEY");
    let body = format!(
        "Action=AssumeRole&Version=2011-06-15&DurationSeconds=3600&Policy={}",
        form_encode(&policy.to_string())
    );
    let (date, stamp) = amz_now();
    let scope = format!("{date}/us-east-1/sts/aws4_request");
    let content_type = "application/x-www-form-urlencoded";
    // `common::http` 以解析后的地址写 Host，签名按同一取值
    let canonical = format!(
        "POST\n/\n\ncontent-type:{content_type}\nhost:{addr}\nx-amz-date:{stamp}\n\ncontent-type;host;x-amz-date\n{}",
        hex(&sha256(body.as_bytes()))
    );
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{}",
        hex(&sha256(canonical.as_bytes()))
    );
    let mut key = hmac(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    for part in ["us-east-1", "sts", "aws4_request"] {
        key = hmac(&key, part.as_bytes());
    }
    let signature = hex(&hmac(&key, to_sign.as_bytes()));
    let auth = format!(
        "AWS4-HMAC-SHA256 Credential={id}/{scope}, SignedHeaders=content-type;host;x-amz-date, Signature={signature}"
    );
    let r = super::http(
        addr,
        "POST",
        "/",
        &[
            ("Content-Type", content_type),
            ("X-Amz-Date", &stamp),
            ("Authorization", &auth),
        ],
        body.as_bytes(),
    );
    assert_eq!(r.status, 200, "STS AssumeRole 失败：{}", r.text());
    let xml = r.text();
    let field = |name: &str| -> String {
        let open = format!("<{name}>");
        let start = xml
            .find(&open)
            .unwrap_or_else(|| panic!("STS 应答缺 {name}：{xml}"))
            + open.len();
        let end = start + xml[start..].find('<').unwrap();
        xml[start..end].to_owned()
    };
    vec![
        ("AWS_ACCESS_KEY_ID", field("AccessKeyId")),
        ("AWS_SECRET_ACCESS_KEY", field("SecretAccessKey")),
        ("AWS_SESSION_TOKEN", field("SessionToken")),
    ]
}

fn sha256(bytes: &[u8]) -> Vec<u8> {
    use sha2::Digest;
    sha2::Sha256::digest(bytes).to_vec()
}

/// HMAC-SHA256（RFC 2104）。
fn hmac(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let mut k = if key.len() > 64 {
        sha256(key)
    } else {
        key.to_vec()
    };
    k.resize(64, 0);
    let pad = |b: u8| k.iter().map(|x| x ^ b).collect::<Vec<u8>>();
    let mut inner = pad(0x36);
    inner.extend_from_slice(msg);
    let mut outer = pad(0x5c);
    outer.extend_from_slice(&sha256(&inner));
    sha256(&outer)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// `application/x-www-form-urlencoded` 的取值编码（非保留字符原样，其余 `%XX`）。
fn form_encode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// 此刻的 (`YYYYMMDD`, `YYYYMMDDTHHMMSSZ`)，UTC。
fn amz_now() -> (String, String) {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // 由自 1970-01-01 起的天数求公历日期（Howard Hinnant 的 civil_from_days）
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    let date = format!("{y:04}{m:02}{d:02}");
    let stamp = format!(
        "{date}T{:02}{:02}{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    );
    (date, stamp)
}

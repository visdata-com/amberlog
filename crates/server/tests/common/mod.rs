//! 系统测试共用的台架：临时目录、合法配置、以子进程运行的 `amberlog`、原始 HTTP/1.1 客户端。
//!
//! 客户端直接写字节，便于发未经规范化的请求目标（如 `/%61pi/…`）、重复的请求头与 `HEAD`。

#![allow(dead_code)]

pub mod alerting;
pub mod api;
pub mod browser;
pub mod evidence;
pub mod gov;
pub mod lake;
pub mod missed;
pub mod retire;
pub mod s3;
pub mod s3log;
pub mod store;
pub mod tls;
pub mod ui;
pub mod webhook;

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub struct TempDir(PathBuf);

impl TempDir {
    pub fn new() -> TempDir {
        static N: AtomicU32 = AtomicU32::new(0);
        let p = std::env::temp_dir().join(format!(
            "amberlog-it-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        // 权限类场景可能留下不可写目录，先恢复再删
        let _ = Command::new("chmod")
            .arg("-R")
            .arg("u+rwx")
            .arg(&self.0)
            .status();
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn set_mode(path: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

/// 不继承测试进程环境的 `amberlog` 命令。
pub fn amberlog(cwd: &Path) -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_amberlog"));
    c.env_clear().current_dir(cwd);
    c
}

/// 配置键与 TOML 取值（`None` 表示删去该键）。
pub type Kv<'a> = (&'a str, Option<&'a str>);

pub const TOKEN: &str = "s3cret";

/// 一组合法配置：`file://` 对象存储、`wal.dir`、口令 `s3cret`，监听回环地址的随机端口。
pub struct Fixture {
    dir: TempDir,
}

impl Default for Fixture {
    fn default() -> Self {
        Self::new()
    }
}

impl Fixture {
    pub fn new() -> Fixture {
        let dir = TempDir::new();
        std::fs::create_dir_all(dir.path().join("store")).unwrap();
        Fixture { dir }
    }

    pub fn root(&self) -> &Path {
        self.dir.path()
    }

    pub fn store_dir(&self) -> PathBuf {
        self.root().join("store")
    }

    pub fn wal_dir(&self) -> PathBuf {
        self.root().join("wal")
    }

    pub fn store_url(&self) -> String {
        format!("file://{}", self.store_dir().display())
    }

    /// 合法配置（TOML），`overrides` 中出现的键替换或新增，取值为 `None` 的键删去。
    pub fn toml(&self, overrides: &[Kv]) -> String {
        let mut entries: Vec<(String, String)> = vec![
            ("server.http_addr".into(), "\"127.0.0.1:0\"".into()),
            (
                "object_store.url".into(),
                format!("\"{}\"", self.store_url()),
            ),
            (
                "wal.dir".into(),
                format!("\"{}\"", self.wal_dir().display()),
            ),
            ("auth.token".into(), format!("\"{TOKEN}\"")),
        ];
        for (k, v) in overrides {
            entries.retain(|(e, _)| e != k);
            if let Some(v) = v {
                entries.push(((*k).into(), (*v).into()));
            }
        }
        entries
            .iter()
            .map(|(k, v)| format!("{k} = {v}\n"))
            .collect()
    }

    pub fn write_config(&self, overrides: &[Kv]) -> PathBuf {
        let p = self.root().join("amberlog.toml");
        std::fs::write(&p, self.toml(overrides)).unwrap();
        p
    }

    /// `amberlog serve --config <写好的配置>`。
    pub fn serve(&self, overrides: &[Kv]) -> Command {
        let path = self.write_config(overrides);
        let mut c = amberlog(self.root());
        c.arg("serve").arg("--config").arg(path);
        c
    }

    /// 以给定配置启动并等到 `ready` 事件。
    pub fn start(&self, overrides: &[Kv]) -> Server {
        Server::start(self.serve(overrides))
    }

    /// `amberlog catalog --config <写好的配置>`（只读目录模式，REQ-AMB-STO-COLD-CATALOG）：配置写在 `catalog.toml`，
    /// 与同一夹具上的写入实例各用各的文件。
    pub fn catalog(&self, overrides: &[Kv]) -> Command {
        let p = self.root().join("catalog.toml");
        std::fs::write(&p, self.toml(overrides)).unwrap();
        let mut c = amberlog(self.root());
        c.arg("catalog").arg("--config").arg(p);
        c
    }
}

/// 以子进程运行的 `amberlog`；stderr 逐行收集，进程随本值析构而终止。
pub struct Server {
    child: Child,
    lines: Arc<Mutex<Vec<String>>>,
    out: Arc<Mutex<Vec<String>>>,
    pub addr: SocketAddr,
}

fn collect(stream: impl Read + Send + 'static) -> Arc<Mutex<Vec<String>>> {
    let lines = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&lines);
    std::thread::spawn(move || {
        for line in BufReader::new(stream).lines() {
            let Ok(line) = line else { break };
            sink.lock().unwrap().push(line);
        }
    });
    lines
}

impl Server {
    /// 启动并读 stderr 直到出现 `ready` 事件（json 或 text 格式），从中取实际监听地址。
    pub fn start(mut cmd: Command) -> Server {
        let mut child = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("启动 amberlog");
        let lines = collect(child.stderr.take().unwrap());
        let out = collect(child.stdout.take().unwrap());
        let mut server = Server {
            child,
            lines,
            out,
            addr: "0.0.0.0:0".parse().unwrap(),
        };
        let line = server
            .wait_line(|l| l.contains("\"event\":\"ready\"") || l.contains("event=ready"))
            .unwrap_or_else(|| panic!("未等到 ready 事件，stderr：\n{:#?}", server.stderr_lines()));
        let addr = match serde_json::from_str::<serde_json::Value>(&line) {
            Ok(v) => v["http_addr"].as_str().unwrap().to_owned(),
            Err(_) => line
                .split_whitespace()
                .find_map(|w| w.strip_prefix("http_addr="))
                .unwrap()
                .to_owned(),
        };
        let mut addr: SocketAddr = addr.parse().unwrap();
        if addr.ip().is_unspecified() {
            addr.set_ip("127.0.0.1".parse().unwrap());
        }
        server.addr = addr;
        server
    }

    pub fn stdout_lines(&self) -> Vec<String> {
        self.out.lock().unwrap().clone()
    }

    pub fn stderr_lines(&self) -> Vec<String> {
        self.lines.lock().unwrap().clone()
    }

    /// stderr 中能解析为 JSON 的行。
    pub fn json_lines(&self) -> Vec<serde_json::Value> {
        self.stderr_lines()
            .iter()
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect()
    }

    /// 等待（至多 30 秒）第一条满足条件的 stderr 行。
    pub fn wait_line(&self, pred: impl Fn(&str) -> bool) -> Option<String> {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if let Some(l) = self.stderr_lines().into_iter().find(|l| pred(l)) {
                return Some(l);
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        None
    }

    pub fn get(&self, path: &str) -> Resp {
        http(self.addr, "GET", path, &[], b"")
    }

    pub fn get_auth(&self, path: &str) -> Resp {
        http(self.addr, "GET", path, &[("Authorization", BEARER)], b"")
    }

    pub fn request(&self, method: &str, path: &str, headers: &[(&str, &str)], body: &[u8]) -> Resp {
        http(self.addr, method, path, headers, body)
    }

    /// 反复 `GET /readyz` 直到状态码为 `want`（至多 `limit`）。
    pub fn wait_readyz(&self, want: u16, limit: Duration) -> Resp {
        let deadline = Instant::now() + limit;
        loop {
            let r = self.get("/readyz");
            if r.status == want || Instant::now() >= deadline {
                return r;
            }
            std::thread::sleep(Duration::from_millis(250));
        }
    }
}

impl Server {
    /// 被测进程的 pid（读 `/proc/<pid>/status` 等）。
    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// 同 [`Server::request`]，但请求体由另一线程发送、本线程同时读应答：服务端可不读完请求体就应答并关闭连接，
    /// 发送端的写错误不算失败（REQ-AMB-ING-BODY-ENCODING「可不读完请求体」）。
    pub fn request_streaming(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: Vec<u8>,
    ) -> Resp {
        let mut s = TcpStream::connect(self.addr).expect("连接 amberlog");
        s.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
        let mut req = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nContent-Length: {}\r\n",
            self.addr,
            body.len()
        );
        for (k, v) in headers {
            req.push_str(&format!("{k}: {v}\r\n"));
        }
        req.push_str("\r\n");
        let mut writer = s.try_clone().unwrap();
        let sender = std::thread::spawn(move || {
            let _ = writer
                .write_all(req.as_bytes())
                .and_then(|_| writer.write_all(&body));
        });
        let mut raw = Vec::new();
        let mut buf = [0u8; 64 * 1024];
        loop {
            match s.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => raw.extend_from_slice(&buf[..n]),
                // 连接被对端重置：已读到的应答照样解析
                Err(_) => break,
            }
        }
        let _ = s.shutdown(std::net::Shutdown::Both);
        let _ = sender.join();
        parse_response(&raw)
    }

    /// 同 [`Server::request_streaming`]，但不带 `Content-Length`、以 `Transfer-Encoding: chunked` 分块（每块 64 KiB）发送
    /// 请求体：服务端只能边收边计数，不能按声明的长度不读即拒。
    pub fn request_chunked(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: Vec<u8>,
    ) -> Resp {
        let mut s = TcpStream::connect(self.addr).expect("连接 amberlog");
        s.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
        let mut req = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n",
            self.addr
        );
        for (k, v) in headers {
            req.push_str(&format!("{k}: {v}\r\n"));
        }
        req.push_str("\r\n");
        let mut writer = s.try_clone().unwrap();
        let sender = std::thread::spawn(move || {
            let mut send = || -> std::io::Result<()> {
                writer.write_all(req.as_bytes())?;
                for chunk in body.chunks(64 * 1024) {
                    writer.write_all(format!("{:x}\r\n", chunk.len()).as_bytes())?;
                    writer.write_all(chunk)?;
                    writer.write_all(b"\r\n")?;
                }
                writer.write_all(b"0\r\n\r\n")
            };
            let _ = send();
        });
        let mut raw = Vec::new();
        let mut buf = [0u8; 64 * 1024];
        loop {
            match s.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => raw.extend_from_slice(&buf[..n]),
                // 连接被对端重置：已读到的应答照样解析
                Err(_) => break,
            }
        }
        let _ = s.shutdown(std::net::Shutdown::Both);
        let _ = sender.join();
        parse_response(&raw)
    }

    /// 发 SIGKILL 并等进程退出（崩溃类场景）。
    pub fn sigkill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// 等文件出现（至多 `limit`），用于等 `TESTBED_FAULT_MARKER` 暂停点标记。
pub fn wait_file(path: &Path, limit: Duration) -> bool {
    let deadline = Instant::now() + limit;
    while Instant::now() < deadline {
        if path.exists() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    path.exists()
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub const BEARER: &str = "Bearer s3cret";

/// 一次 HTTP/1.1 应答。
#[derive(Debug)]
pub struct Resp {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Resp {
    /// 按名取头（不区分大小写），多行时取第一行。
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| {
            panic!(
                "响应体不是 JSON（{e}）：{}",
                String::from_utf8_lossy(&self.body)
            )
        })
    }

    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    /// `Allow` 头按逗号拆分、去空白后的方法集合。
    pub fn allow(&self) -> std::collections::BTreeSet<String> {
        self.header("allow")
            .expect("应带 Allow")
            .split(',')
            .map(|m| m.trim().to_owned())
            .collect()
    }

    /// 断言是错误信封：状态码、`Content-Type`、`code` 与状态码相同、`reason` 为给定值、带 `x-request-id`。
    pub fn assert_envelope(&self, status: u16, reason: &str) -> serde_json::Value {
        assert_eq!(self.status, status, "{self:?}");
        assert_eq!(self.header("content-type"), Some("application/json"));
        assert!(self.header("x-request-id").is_some());
        let v = self.json();
        assert_eq!(v["code"], status);
        assert_eq!(v["reason"], reason);
        assert!(v["message"].is_string());
        v
    }
}

/// 发一个 HTTP/1.1 请求（`Connection: close`），读到连接关闭为止（读超时 30 秒）。
pub fn http(
    addr: SocketAddr,
    method: &str,
    target: &str,
    headers: &[(&str, &str)],
    body: &[u8],
) -> Resp {
    http_within(addr, method, target, headers, body, Duration::from_secs(30))
}

/// 同 [`http`]，读超时为 `read_timeout`（请求会被暂停点拦住较久时用）。
pub fn http_within(
    addr: SocketAddr,
    method: &str,
    target: &str,
    headers: &[(&str, &str)],
    body: &[u8],
    read_timeout: Duration,
) -> Resp {
    let mut s = TcpStream::connect(addr).expect("连接 amberlog");
    s.set_read_timeout(Some(read_timeout)).unwrap();
    let mut req = format!("{method} {target} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n");
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    if !body.is_empty() || method == "POST" {
        req.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    req.push_str("\r\n");
    s.write_all(req.as_bytes()).unwrap();
    s.write_all(body).unwrap();
    let mut raw = Vec::new();
    s.read_to_end(&mut raw).unwrap();
    parse_response(&raw)
}

pub fn parse_response(raw: &[u8]) -> Resp {
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("完整的响应头");
    let head = String::from_utf8_lossy(&raw[..split]).into_owned();
    let mut rest = raw[split + 4..].to_vec();
    let mut lines = head.split("\r\n");
    let status: u16 = lines
        .next()
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap();
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_owned(), v.trim().to_owned()))
        .collect();
    let chunked = headers.iter().any(|(k, v)| {
        k.eq_ignore_ascii_case("transfer-encoding") && v.eq_ignore_ascii_case("chunked")
    });
    if chunked {
        rest = dechunk(&rest);
    }
    Resp {
        status,
        headers,
        body: rest,
    }
}

fn dechunk(mut data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(eol) = data.windows(2).position(|w| w == b"\r\n") {
        let size_text = String::from_utf8_lossy(&data[..eol]);
        let size = usize::from_str_radix(size_text.split(';').next().unwrap().trim(), 16).unwrap();
        data = &data[eol + 2..];
        if size == 0 {
            break;
        }
        out.extend_from_slice(&data[..size]);
        data = &data[size + 2..];
    }
    out
}

impl Server {
    /// 启动但不等 `ready`（开始监听之前的场景）；`addr` 未知，保持为 `0.0.0.0:0`。
    pub fn spawn(mut cmd: Command) -> Server {
        let mut child = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("启动 amberlog");
        let lines = collect(child.stderr.take().unwrap());
        let out = collect(child.stdout.take().unwrap());
        Server {
            child,
            lines,
            out,
            addr: "0.0.0.0:0".parse().unwrap(),
        }
    }

    /// 发信号（`TERM`、`INT`……），不等进程退出。
    pub fn signal(&self, name: &str) {
        let status = Command::new("kill")
            .arg(format!("-{name}"))
            .arg(self.child.id().to_string())
            .status()
            .expect("执行 kill");
        assert!(status.success(), "kill -{name} 失败");
    }

    /// 等进程退出（至多 `limit`），返回退出码；被信号杀死时为 `Some(None)`，超时为 `None`。
    pub fn wait_exit(&mut self, limit: Duration) -> Option<Option<i32>> {
        let deadline = Instant::now() + limit;
        loop {
            if let Some(status) = self.child.try_wait().expect("查询子进程状态") {
                return Some(status.code());
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

/// 测试自己占住的回环端口，用来断言被测进程没有开始监听（「进程退出前没有在 `server.http_addr` 上建立监听」一类断言）。
///
/// 被测进程的 `server.http_addr` 指向这个端口：它一旦走到监听这一步，绑定必然失败，以退出码 4 退出、stderr 含
/// `cannot listen on`（同 `cfg_listen_failure_exit4`）。所以进程以场景要的其他退出码退出，或停在监听之前的暂停点，
/// 就说明它没有走到监听。不用 [`listening`] 探测「端口上有没有人监听」：被测进程不监听时端口空着，同一测试二进制里
/// 并行的其他测试可能拿到这个端口并监听，探测会误报。
pub struct HeldPort(std::net::TcpListener);

impl HeldPort {
    pub fn new() -> HeldPort {
        HeldPort(std::net::TcpListener::bind("127.0.0.1:0").unwrap())
    }

    pub fn port(&self) -> u16 {
        self.0.local_addr().unwrap().port()
    }

    /// `server.http_addr` 的 TOML 取值。
    pub fn addr(&self) -> String {
        format!("\"127.0.0.1:{}\"", self.port())
    }

    /// 断言被测进程（已退出，`code` 为其退出码）没有试过在本端口上监听。
    pub fn assert_never_listened(&self, code: Option<i32>, server: &Server) {
        let stderr = server.stderr_lines();
        assert_ne!(code, Some(4), "被测进程试图监听：{stderr:#?}");
        assert!(
            !stderr.iter().any(|l| l.contains("cannot listen on")),
            "被测进程试图监听：{stderr:#?}"
        );
    }
}

/// 取一个此刻空闲的回环端口。取号之后、被测进程绑定之前它可能被别人占走；要断言被测进程没有开始监听时用 [`HeldPort`]。
pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// 该端口上此刻有没有监听。
pub fn listening(port: u16) -> bool {
    TcpStream::connect(("127.0.0.1", port)).is_ok()
}

/// 等文件 `path` 的内容（去掉首尾空白）变为 `want`（至多 `limit`）。
pub fn wait_file_content(path: &Path, want: &str, limit: Duration) -> bool {
    let deadline = Instant::now() + limit;
    while Instant::now() < deadline {
        if std::fs::read_to_string(path).is_ok_and(|s| s.trim() == want) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    false
}

/// 让子进程能写的单个文件不超过 `bytes` 字节（RLIMIT_FSIZE），并忽略 SIGXFSZ：超限的写入以 EFBIG 失败而不是终止进程。
pub fn limit_file_size(cmd: &mut Command, bytes: u64) {
    use std::os::unix::process::CommandExt;
    // SAFETY: 闭包在 fork 之后、exec 之前运行，只调用异步信号安全的 setrlimit 与 signal。
    unsafe {
        cmd.pre_exec(move || {
            let lim = libc::rlimit {
                rlim_cur: bytes,
                rlim_max: bytes,
            };
            if libc::setrlimit(libc::RLIMIT_FSIZE, &lim) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
            Ok(())
        });
    }
}

/// 子进程可同时打开的文件数（RLIMIT_NOFILE）的软上限与硬上限。
pub fn limit_open_files(cmd: &mut Command, soft: u64, hard: u64) {
    use std::os::unix::process::CommandExt;
    // SAFETY: 闭包在 fork 之后、exec 之前运行，只调用异步信号安全的 setrlimit。
    unsafe {
        cmd.pre_exec(move || {
            let lim = libc::rlimit {
                rlim_cur: soft,
                rlim_max: hard,
            };
            if libc::setrlimit(libc::RLIMIT_NOFILE, &lim) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

/// `/proc/<pid>/limits` 中「Max open files」一行的软上限与硬上限。
pub fn open_file_limits(pid: u32) -> (u64, u64) {
    let text = std::fs::read_to_string(format!("/proc/{pid}/limits")).unwrap();
    let line = text
        .lines()
        .find(|l| l.starts_with("Max open files"))
        .unwrap_or_else(|| panic!("{text}"));
    let mut n = line["Max open files".len()..]
        .split_whitespace()
        .map(|w| w.parse().unwrap());
    (n.next().unwrap(), n.next().unwrap())
}

/// 被测进程此刻打开的文件描述符数。
pub fn open_fds(pid: u32) -> usize {
    std::fs::read_dir(format!("/proc/{pid}/fd")).map_or(0, |d| d.count())
}

/// 被测进程此刻打开的套接字描述符数（`/proc/<pid>/fd` 中链接到 `socket:[…]` 的项）。
pub fn open_sockets(pid: u32) -> usize {
    std::fs::read_dir(format!("/proc/{pid}/fd")).map_or(0, |d| {
        d.filter_map(Result::ok)
            .filter_map(|e| std::fs::read_link(e.path()).ok())
            .filter(|l| l.to_string_lossy().starts_with("socket:"))
            .count()
    })
}

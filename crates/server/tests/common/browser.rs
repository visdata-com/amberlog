//! 检索界面（REQ-AMB-UI-CONSOLE、REQ-AMB-UI-SEARCH、REQ-AMB-UI-GOVERNANCE）系统测试用的无头浏览器：以子进程启动
//! chrome-headless-shell（`tools/chrome.sh`），经 Chrome DevTools Protocol（WebSocket 上的 JSON 消息）驱动页面。
//!
//! 只用到协议里很小的一部分，自己收发，不另引 CDP 客户端库：
//! - `Target.getTargets` / `Target.attachToTarget`（flatten）接管启动时打开的页面，此后的命令都带 `sessionId`；
//!   `Target.createTarget` 在同一浏览器（同一份本地存储）里另开标签页（[`Browser::new_tab`]、[`Browser::reopen`]）；
//! - `Network.enable` 后记下页面发出的每个请求（`Network.requestWillBeSent`：URL、方法、请求头、请求体），以及页面建立的
//!   WebSocket 连接（`Network.webSocketCreated`，记为方法 `WEBSOCKET` 的请求）；
//! - `Fetch.enable` 拦住请求（`Fetch.requestPaused`）：匹配 [`Browser::answer_where`] 的立即改答（`Fetch.fulfillRequest`），
//!   匹配 [`Browser::hold`] 的留给测试逐个放行（`Fetch.continueRequest`）或改答（[`Browser::fulfill`]），其余立即放行；
//! - `Emulation.setTimezoneOverride` 改浏览器时区，`Emulation.setEmulatedMedia` 改 `prefers-color-scheme`；
//! - `Runtime.evaluate` 执行脚本、读页面文字、存储与计算样式，填输入框、点按钮也经它完成。
//!
//! 同步收发：发出一条命令后一直读到它的应答为止，其间到达的事件存起来，被拦住的请求在这时按规则放行或改答（所以测试只要在
//! 等待中照常求值，改答就会发生）。被测进程停止后浏览器与页面照常可用。

use std::collections::HashSet;
use std::io::{BufRead, BufReader};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket};

use super::TempDir;

/// `tools/chrome.sh` 的缺省下载位置（版本与脚本一致）。
const DEFAULT_CHROME: &str = ".cache/amberlog/chrome-headless-shell/154.0.8037.92/chrome-headless-shell-linux64/chrome-headless-shell";

fn chrome_path() -> String {
    if let Ok(p) = std::env::var("TESTBED_CHROME") {
        return p;
    }
    let home = std::env::var("HOME").unwrap_or_default();
    let p = format!("{home}/{DEFAULT_CHROME}");
    assert!(
        std::path::Path::new(&p).exists(),
        "找不到 chrome-headless-shell（{p}）：这个测试要用无头浏览器，先在运行测试的机器上执行 tools/chrome.sh，\
         或以 TESTBED_CHROME 指定浏览器"
    );
    p
}

/// 页面发出的一个请求。
#[derive(Debug, Clone)]
pub struct Request {
    /// `Network` 域的请求号（等应答收完用）。
    pub id: String,
    pub url: String,
    pub method: String,
    pub headers: Value,
    pub body: Option<String>,
}

impl Request {
    /// 请求 URL 的路径（不含查询串）。
    pub fn path(&self) -> &str {
        let rest = self
            .url
            .split_once("://")
            .map_or(self.url.as_str(), |(_, r)| r);
        let path = rest.find('/').map_or("/", |i| &rest[i..]);
        path.split(['?', '#']).next().unwrap_or(path)
    }

    /// 请求头（名称不区分大小写）。
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .as_object()?
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .and_then(|(_, v)| v.as_str())
    }

    /// 请求体按 JSON 解析。
    pub fn json(&self) -> Value {
        serde_json::from_str(self.body.as_deref().expect("请求有请求体")).expect("请求体是 JSON")
    }
}

/// `Network.requestWillBeSent` 或 `Fetch.requestPaused` 中的 `request`（`id` 为 `Network` 域的请求号）。
fn request_of(id: &str, r: &Value) -> Request {
    Request {
        id: id.to_owned(),
        url: r["url"].as_str().unwrap_or_default().to_owned(),
        method: r["method"].as_str().unwrap_or_default().to_owned(),
        headers: r["headers"].clone(),
        body: r["postData"].as_str().map(str::to_owned),
    }
}

/// 被 [`Browser::hold`] 拦住的一个请求。
#[derive(Debug, Clone)]
pub struct Held {
    /// `Fetch` 域的请求号（放行用）。
    pub id: String,
    /// 所在标签页的会话（放行与改答要发到同一会话）。
    session: String,
    /// `Network` 域的请求号（等应答收完用）。
    pub network_id: String,
    pub request: Request,
}

/// 改答规则：请求满足 `pred` 时以这份应答作答。
struct Answer {
    pred: Box<dyn Fn(&Request) -> bool>,
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}

/// 一个无头浏览器进程与其中的当前页面。
pub struct Browser {
    child: Child,
    ws: WebSocket<MaybeTlsStream<TcpStream>>,
    /// 当前页面的会话与目标号。
    session: String,
    target: String,
    next_id: u64,
    events: Vec<Value>,
    answers: Vec<Answer>,
    /// [`Browser::hold`] 的 URL 模式。
    hold: Option<String>,
    /// 已由台架自己放行或改答的 `Fetch` 请求号（不算拦住）。
    handled: HashSet<String>,
    /// 新开的标签页也照用的时区与外观。
    timezone: Option<String>,
    color_scheme: Option<String>,
    _profile: TempDir,
}

impl Browser {
    pub fn launch() -> Browser {
        let profile = TempDir::new();
        let mut child = Command::new(chrome_path())
            .args([
                "--headless",
                "--remote-debugging-port=0",
                "--no-first-run",
                "--no-default-browser-check",
                // 容器等受限环境里常没有可用的用户命名空间沙箱；只打开本机的被测页面
                "--no-sandbox",
                "--disable-gpu",
                "--disable-dev-shm-usage",
                // 不找桌面会话的密钥环取 Cookie 加密密钥：桌面上密钥环不应答时，页面发出第一个请求后浏览器就挂住
                "--password-store=basic",
                // 多个测试并行时不让页面因「在后台」而被降速
                "--disable-background-timer-throttling",
                "--disable-renderer-backgrounding",
                "--disable-backgrounding-occluded-windows",
            ])
            .arg(format!("--user-data-dir={}", profile.path().display()))
            .arg("about:blank")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|e| panic!("启动 chrome-headless-shell 失败：{e}"));
        // 浏览器在 stderr 打出 `DevTools listening on ws://…`
        let mut lines = BufReader::new(child.stderr.take().unwrap()).lines();
        let url = loop {
            match lines.next() {
                Some(Ok(l)) => {
                    if let Some(u) = l.strip_prefix("DevTools listening on ") {
                        break u.trim().to_owned();
                    }
                }
                _ => panic!("chrome-headless-shell 未给出调试地址就退出了"),
            }
        };
        // 此后的输出丢弃，免得管道写满挂住浏览器
        std::thread::spawn(move || for _ in lines {});
        let (ws, _) = tungstenite::connect(&url).expect("连接浏览器调试端口");
        if let MaybeTlsStream::Plain(s) = ws.get_ref() {
            s.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
        }
        let mut b = Browser {
            child,
            ws,
            session: String::new(),
            target: String::new(),
            next_id: 0,
            events: Vec::new(),
            answers: Vec::new(),
            hold: None,
            handled: HashSet::new(),
            timezone: None,
            color_scheme: None,
            _profile: profile,
        };
        // 接管启动时打开的那个页面（不另开标签页）
        let targets = b.call_browser("Target.getTargets", json!({}));
        let page = targets["targetInfos"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["type"] == "page")
            .expect("浏览器有一个页面")["targetId"]
            .as_str()
            .unwrap()
            .to_owned();
        b.attach(&page);
        b
    }

    /// 接管目标 `target` 为当前页面，打开所需的域，并照用已设的时区、外观与拦截。
    fn attach(&mut self, target: &str) {
        let attached = self.call_browser(
            "Target.attachToTarget",
            json!({"targetId": target, "flatten": true}),
        );
        self.session = attached["sessionId"].as_str().unwrap().to_owned();
        self.target = target.to_owned();
        self.call("Network.enable", json!({}));
        self.call("Page.enable", json!({}));
        if let Some(tz) = self.timezone.clone() {
            self.call("Emulation.setTimezoneOverride", json!({"timezoneId": tz}));
        }
        if let Some(cs) = self.color_scheme.clone() {
            self.emulate_color_scheme(&cs);
        }
        if self.hold.is_some() || !self.answers.is_empty() {
            self.enable_fetch();
        }
    }

    /// 在同一浏览器里新开一个标签页并以它为当前页面，打开 `url`（原标签页留着）。本地存储与原标签页共用，会话存储不共用。
    pub fn new_tab(&mut self, url: &str) {
        let created = self.call_browser("Target.createTarget", json!({"url": "about:blank"}));
        let target = created["targetId"].as_str().unwrap().to_owned();
        self.attach(&target);
        self.open(url);
    }

    /// 关闭当前页面，在同一浏览器里新开一个标签页打开 `url`（「关闭页面后重新打开」）。
    pub fn reopen(&mut self, url: &str) {
        let old = self.target.clone();
        let created = self.call_browser("Target.createTarget", json!({"url": "about:blank"}));
        let target = created["targetId"].as_str().unwrap().to_owned();
        self.call_browser("Target.closeTarget", json!({"targetId": old}));
        self.attach(&target);
        self.open(url);
    }

    /// 浏览器的时区（`Emulation.setTimezoneOverride`），此后新开的标签页照用。在 [`Browser::open`] 之前调。
    pub fn set_timezone(&mut self, tz: &str) {
        self.timezone = Some(tz.to_owned());
        self.call("Emulation.setTimezoneOverride", json!({"timezoneId": tz}));
    }

    /// 浏览器的 `prefers-color-scheme`（`light` 或 `dark`），此后新开的标签页照用。在 [`Browser::open`] 之前调。
    pub fn set_color_scheme(&mut self, scheme: &str) {
        self.color_scheme = Some(scheme.to_owned());
        self.emulate_color_scheme(scheme);
    }

    fn emulate_color_scheme(&mut self, scheme: &str) {
        self.call(
            "Emulation.setEmulatedMedia",
            json!({"features": [{"name": "prefers-color-scheme", "value": scheme}]}),
        );
    }

    fn send(&mut self, msg: Value) -> u64 {
        self.next_id += 1;
        let mut msg = msg;
        msg["id"] = json!(self.next_id);
        // 系统调用被信号打断（EINTR：测试进程同时管着浏览器与被测进程，子进程退出等信号随时会到）时，帧已进写缓冲，
        // 接着把缓冲写完即可；读的一侧同理重读（已收到的部分留在 tungstenite 的缓冲里）
        let mut r = self.ws.send(Message::text(msg.to_string()));
        while let Err(tungstenite::Error::Io(e)) = &r
            && e.kind() == std::io::ErrorKind::Interrupted
        {
            r = self.ws.flush();
        }
        r.expect("向浏览器发送命令");
        self.next_id
    }

    fn wait_reply(&mut self, id: u64, method: &str) -> Value {
        loop {
            let msg = match self.ws.read() {
                Ok(m) => m,
                Err(tungstenite::Error::Io(e)) if e.kind() == std::io::ErrorKind::Interrupted => {
                    continue;
                }
                Err(e) => panic!("等 {method} 的应答时读浏览器的消息失败：{e}"),
            };
            let Message::Text(text) = msg else { continue };
            let v: Value = serde_json::from_str(text.as_str()).expect("浏览器消息是 JSON");
            if v["id"] == json!(id) {
                if let Some(e) = v.get("error") {
                    panic!("{method} 失败：{e}");
                }
                return v["result"].clone();
            }
            if v.get("method").is_some() {
                if v["method"] == "Fetch.requestPaused" {
                    self.on_paused(&v);
                }
                self.events.push(v);
            }
        }
    }

    /// 被拦住的请求：匹配改答规则的立即改答，匹配 [`Browser::hold`] 的留着，其余立即放行。只发命令、不等应答
    /// （应答到达时按未知编号丢弃）。
    fn on_paused(&mut self, v: &Value) {
        let id = v["params"]["requestId"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let session = v["sessionId"].as_str().unwrap_or_default().to_owned();
        let req = request_of(
            v["params"]["networkId"].as_str().unwrap_or_default(),
            &v["params"]["request"],
        );
        if let Some(a) = self.answers.iter().find(|a| (a.pred)(&req)) {
            let msg = fulfill_message(&id, a.status, &a.headers, &a.body);
            self.handled.insert(id);
            self.send(
                json!({"method": "Fetch.fulfillRequest", "params": msg, "sessionId": session}),
            );
        } else if self.hold.as_deref().is_some_and(|p| glob(p, &req.url)) {
            // 留给测试放行
        } else {
            self.handled.insert(id.clone());
            self.send(json!({"method": "Fetch.continueRequest", "params": {"requestId": id}, "sessionId": session}));
        }
    }

    fn call_browser(&mut self, method: &str, params: Value) -> Value {
        let id = self.send(json!({"method": method, "params": params}));
        self.wait_reply(id, method)
    }

    /// 在当前页面的会话上执行一条命令。
    pub fn call(&mut self, method: &str, params: Value) -> Value {
        let session = self.session.clone();
        self.call_in(&session, method, params)
    }

    fn call_in(&mut self, session: &str, method: &str, params: Value) -> Value {
        let id = self.send(json!({"method": method, "params": params, "sessionId": session}));
        self.wait_reply(id, method)
    }

    /// 打开 `url` 并等到页面加载完成。
    pub fn open(&mut self, url: &str) {
        self.call("Page.navigate", json!({"url": url}));
        // 多个测试并行时浏览器起得慢，给足时间；先确认已离开 about:blank
        assert!(
            self.wait_until(
                &format!(
                    "location.href === {} && document.readyState === 'complete'",
                    json!(url)
                ),
                Duration::from_secs(30)
            ),
            "{url} 30 秒内未加载完成：{}",
            self.eval("location.href + ' ' + document.readyState")
        );
    }

    /// 在页面里求值表达式 `js`（Promise 取其结果），返回 JSON 值。
    pub fn eval(&mut self, js: &str) -> Value {
        let r = self.call(
            "Runtime.evaluate",
            json!({"expression": js, "awaitPromise": true, "returnByValue": true}),
        );
        if let Some(e) = r.get("exceptionDetails") {
            panic!("页面脚本出错：{js}\n{e}");
        }
        r["result"]["value"].clone()
    }

    /// 反复求值 `js` 直到为 `true`（至多 `limit`）。
    pub fn wait_until(&mut self, js: &str, limit: Duration) -> bool {
        let deadline = Instant::now() + limit;
        loop {
            if self.eval(js) == json!(true) {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// 到此刻为止页面发出的全部请求，按发出顺序。WebSocket 连接（`Network.webSocketCreated`）也算一个请求，
    /// 方法记为 `WEBSOCKET`，没有请求头与请求体。
    pub fn requests(&mut self) -> Vec<Request> {
        // 一条空命令：其应答之前的事件都已收下
        self.call("Runtime.evaluate", json!({"expression": "0"}));
        self.events
            .iter()
            .filter_map(|e| match e["method"].as_str() {
                Some("Network.requestWillBeSent") => Some(request_of(
                    e["params"]["requestId"].as_str().unwrap_or_default(),
                    &e["params"]["request"],
                )),
                Some("Network.webSocketCreated") => Some(Request {
                    id: e["params"]["requestId"]
                        .as_str()
                        .unwrap_or_default()
                        .to_owned(),
                    url: e["params"]["url"].as_str().unwrap_or_default().to_owned(),
                    method: "WEBSOCKET".to_owned(),
                    headers: json!({}),
                    body: None,
                }),
                _ => None,
            })
            .collect()
    }

    // ---- 拦住与放行请求（造出晚到的应答） ----

    fn enable_fetch(&mut self) {
        self.call(
            "Fetch.enable",
            json!({"patterns": [{"urlPattern": "*", "requestStage": "Request"}]}),
        );
    }

    /// 此后 URL 匹配 `url_pattern`（`*` 为通配）的请求都先被拦住，直到 [`Browser::release`] 或 [`Browser::fulfill`]
    /// （同时匹配改答规则的照规则改答，不拦住）。
    pub fn hold(&mut self, url_pattern: &str) {
        self.hold = Some(url_pattern.to_owned());
        self.enable_fetch();
    }

    /// 此后满足 `pred` 的请求一律以 HTTP `status`、响应头 `headers` 与响应体 `body` 改答（不到达被测进程）；
    /// 被改答的请求照样记进 [`Browser::requests`]。先加的规则先匹配。
    pub fn answer_where(
        &mut self,
        pred: impl Fn(&Request) -> bool + 'static,
        status: u16,
        headers: &[(&str, &str)],
        body: &str,
    ) {
        self.answers.push(Answer {
            pred: Box::new(pred),
            status,
            headers: headers
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
            body: body.to_owned(),
        });
        self.enable_fetch();
    }

    /// 此后路径为 `path` 的请求一律改答（见 [`Browser::answer_where`]）。
    pub fn answer_all(&mut self, path: &str, status: u16, headers: &[(&str, &str)], body: &str) {
        let path = path.to_owned();
        self.answer_where(move |r| r.path() == path, status, headers, body);
    }

    /// 到此刻为止被 [`Browser::hold`] 拦住过的全部请求（含已放行的），按拦住的先后。
    pub fn held(&mut self) -> Vec<Held> {
        self.call("Runtime.evaluate", json!({"expression": "0"}));
        self.events
            .iter()
            .filter(|e| e["method"] == "Fetch.requestPaused")
            .filter(|e| {
                !self
                    .handled
                    .contains(e["params"]["requestId"].as_str().unwrap_or_default())
            })
            .map(|e| Held {
                id: e["params"]["requestId"].as_str().unwrap().to_owned(),
                session: e["sessionId"].as_str().unwrap_or_default().to_owned(),
                network_id: e["params"]["networkId"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                request: request_of(
                    e["params"]["networkId"].as_str().unwrap_or_default(),
                    &e["params"]["request"],
                ),
            })
            .collect()
    }

    /// 等被拦住过的请求累计达到 `n` 个（至多 `limit`），返回全部。
    pub fn wait_held(&mut self, n: usize, limit: Duration) -> Vec<Held> {
        let deadline = Instant::now() + limit;
        loop {
            let h = self.held();
            if h.len() >= n {
                return h;
            }
            assert!(
                Instant::now() < deadline,
                "{limit:?} 内只拦住 {} 个请求，要 {n} 个：{h:?}",
                h.len()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// 放行一个被拦住的请求。
    pub fn release(&mut self, h: &Held) {
        self.call_in(
            &h.session,
            "Fetch.continueRequest",
            json!({"requestId": h.id}),
        );
    }

    /// 以 HTTP `status`、响应头 `headers` 与响应体 `body` 改答一个被拦住的请求。
    pub fn fulfill(&mut self, h: &Held, status: u16, headers: &[(&str, &str)], body: &str) {
        let headers: Vec<(String, String)> = headers
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        let msg = fulfill_message(&h.id, status, &headers, body);
        self.call_in(&h.session, "Fetch.fulfillRequest", msg);
    }

    /// 等被拦住的请求 `h` 的应答体收完（`Network.loadingFinished` 或 `Network.loadingFailed`；至多 `limit`）。
    pub fn wait_loaded(&mut self, h: &Held, limit: Duration) -> bool {
        let id = h.network_id.clone();
        self.wait_network_done(&id, limit)
    }

    /// 等请求 `r` 的应答体收完或请求失败（至多 `limit`）。
    pub fn wait_finished(&mut self, r: &Request, limit: Duration) -> bool {
        self.wait_network_done(&r.id, limit)
    }

    fn wait_network_done(&mut self, network_id: &str, limit: Duration) -> bool {
        let deadline = Instant::now() + limit;
        loop {
            self.call("Runtime.evaluate", json!({"expression": "0"}));
            let done = self.events.iter().any(|e| {
                (e["method"] == "Network.loadingFinished" || e["method"] == "Network.loadingFailed")
                    && e["params"]["requestId"] == json!(network_id)
            });
            if done {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// 除 `/ui` 之外的请求。
    pub fn requests_besides_ui(&mut self) -> Vec<Request> {
        self.requests()
            .into_iter()
            .filter(|r| r.path() != "/ui")
            .collect()
    }

    // ---- 页面操作（按元素 id） ----

    /// 给输入框填值并触发 `input` 事件。
    pub fn fill(&mut self, id: &str, value: &str) {
        self.eval(&format!(
            "(() => {{ const e = document.getElementById({id}); e.value = {value}; \
             e.dispatchEvent(new Event('input', {{bubbles: true}})); }})()",
            id = json!(id),
            value = json!(value)
        ));
    }

    /// 点击元素。
    pub fn click(&mut self, id: &str) {
        self.eval(&format!("document.getElementById({}).click()", json!(id)));
    }

    /// 点击选择器 `selector` 选中的第一个元素。
    pub fn click_sel(&mut self, selector: &str) {
        self.eval(&format!(
            "document.querySelector({}).click()",
            json!(selector)
        ));
    }

    /// 下拉框选值并触发 `change` 事件。
    pub fn select(&mut self, id: &str, value: &str) {
        self.eval(&format!(
            "(() => {{ const e = document.getElementById({id}); e.value = {value}; \
             e.dispatchEvent(new Event('change', {{bubbles: true}})); }})()",
            id = json!(id),
            value = json!(value)
        ));
    }

    /// 让元素失去焦点（触发 `blur` 事件）。
    pub fn blur(&mut self, id: &str) {
        self.eval(&format!(
            "(() => {{ const e = document.getElementById({}); e.focus(); e.blur(); }})()",
            json!(id)
        ));
    }

    /// 在元素上按下一个键（`keydown`，如 `Enter`）。
    pub fn press(&mut self, id: &str, key: &str) {
        self.eval(&format!(
            "document.getElementById({}).dispatchEvent(new KeyboardEvent('keydown', {{key: {}, bubbles: true}}))",
            json!(id),
            json!(key)
        ));
    }

    /// 选择器 `selector` 有没有选中元素。
    pub fn exists(&mut self, selector: &str) -> bool {
        self.eval(&format!(
            "document.querySelector({}) !== null",
            json!(selector)
        )) == json!(true)
    }

    /// 元素的值（输入框、下拉框）。
    pub fn value(&mut self, id: &str) -> String {
        self.eval(&format!("document.getElementById({}).value", json!(id)))
            .as_str()
            .unwrap_or_default()
            .to_owned()
    }

    /// 点击文字为 `label` 的标签按钮（三屏切换）。
    pub fn tab(&mut self, label: &str) {
        self.eval(&format!(
            "[...document.querySelectorAll('nav button')].find(b => b.textContent === {}).click()",
            json!(label)
        ));
    }

    /// 元素的可见文字。
    pub fn text(&mut self, id: &str) -> String {
        self.eval(&format!("document.getElementById({}).innerText", json!(id)))
            .as_str()
            .unwrap_or_default()
            .to_owned()
    }

    /// 等元素 `id` 的可见文字含 `needle`（至多 `limit`）。
    pub fn wait_text(&mut self, id: &str, needle: &str, limit: Duration) -> bool {
        self.wait_until(
            &format!(
                "document.getElementById({}).innerText.includes({})",
                json!(id),
                json!(needle)
            ),
            limit,
        )
    }

    /// 结果区 `id` 里表格的各行（含表头行），每行为各格文字。
    pub fn table(&mut self, id: &str) -> Vec<Vec<String>> {
        let v = self.eval(&format!(
            "[...document.querySelectorAll('#' + {} + ' tr')].map(tr => [...tr.children].map(c => c.innerText))",
            json!(id)
        ));
        serde_json::from_value(v).unwrap()
    }
}

/// `Fetch.fulfillRequest` 的参数。
fn fulfill_message(id: &str, status: u16, headers: &[(String, String)], body: &str) -> Value {
    let headers: Vec<Value> = headers
        .iter()
        .map(|(k, v)| json!({"name": k, "value": v}))
        .collect();
    json!({"requestId": id, "responseCode": status, "responseHeaders": headers, "body": base64(body.as_bytes())})
}

/// 标准 Base64（`Fetch.fulfillRequest` 的响应体要这样编码）。
fn base64(data: &[u8]) -> String {
    const ABC: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for c in data.chunks(3) {
        let n = (u32::from(c[0]) << 16)
            | (u32::from(*c.get(1).unwrap_or(&0)) << 8)
            | u32::from(*c.get(2).unwrap_or(&0));
        for (i, shift) in [18, 12, 6, 0].into_iter().enumerate() {
            out.push(if i <= c.len() {
                ABC[(n >> shift & 63) as usize] as char
            } else {
                '='
            });
        }
    }
    out
}

/// `*` 为通配的模式匹配。
fn glob(pattern: &str, text: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == text;
    }
    let (first, last) = (parts[0], parts[parts.len() - 1]);
    if !text.starts_with(first) || text.len() < first.len() + last.len() || !text.ends_with(last) {
        return false;
    }
    let mut rest = &text[first.len()..text.len() - last.len()];
    for p in &parts[1..parts.len() - 1] {
        match rest.find(p) {
            Some(i) => rest = &rest[i + p.len()..],
            None => return false,
        }
    }
    true
}

impl Drop for Browser {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

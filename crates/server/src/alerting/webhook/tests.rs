use super::*;

#[test]
#[should_panic(expected = "REQ-AMB-ALR-WEBHOOK-HEADERS")]
fn header_value_rejects_control_bytes() {
    HeaderValue::new("a\r\nX-Injected: 1".into());
}

#[test]
fn header_value_is_not_printed() {
    let v = HeaderValue::new("Bearer s3cret".into());
    assert_eq!(format!("{v:?} {v}"), "<redacted> <redacted>");
    assert_eq!(v.expose(), "Bearer s3cret");
}

#[test]
fn url_accepts_absolute_http_and_https() {
    let u = parse_url("https://127.0.0.1:8443/hook/a?team=ops").unwrap();
    assert!(u.is_https());
    assert_eq!(
        (
            u.host.as_str(),
            u.port,
            u.authority.as_str(),
            u.target.as_str()
        ),
        (
            "127.0.0.1",
            Some(8443),
            "127.0.0.1:8443",
            "/hook/a?team=ops"
        )
    );
    let u = parse_url("http://hooks.example.com").unwrap();
    assert_eq!((u.port, u.target.as_str()), (Some(80), "/"));
    let u = parse_url("https://hooks.example.com?k=1").unwrap();
    assert_eq!((u.port, u.target.as_str()), (Some(443), "/?k=1"));
    let u = parse_url("http://[::1]:9000/x").unwrap();
    assert_eq!(
        (u.host.as_str(), u.authority.as_str()),
        ("::1", "[::1]:9000")
    );
    let u = parse_url("http://h:/x").unwrap();
    assert_eq!(u.port, Some(80));
    // 端口写法合乎 RFC 3986 但超出范围：照常接受，每次尝试按连接失败处置
    assert_eq!(parse_url("http://h:99999/").unwrap().port, None);
    assert!(parse_url("http://h/a%20b?x=%2F&y=1").is_ok());
}

#[test]
fn url_rejections_do_not_echo_the_url() {
    for bad in [
        "ftp://127.0.0.1/hook",
        "HTTPS://127.0.0.1/hook",
        "/hook",
        "https://user:pass@127.0.0.1/hook",
        "https://127.0.0.1/hook#x",
        "https:///hook",
        "https://:443/hook",
        "http://h/a b",
        "http://h/a\r\nX: y",
        "http://h/%zz",
        "http://h/%4",
        "http://[::1/x",
        "http://[not-ip]/x",
        "http://h:8x/",
        "http://hé/",
    ] {
        let err = parse_url(bad).unwrap_err();
        assert!(err.starts_with("webhook_url"), "{bad}: {err}");
        assert!(
            !err.contains("pass") && !err.contains("/hook") && !err.contains("127.0"),
            "{bad}: {err}"
        );
    }
    let err = parse_url("https://user:pass@127.0.0.1/hook").unwrap_err();
    assert!(err.contains("webhook_headers"), "{err}");
}

#[test]
fn shown_url_strips_query() {
    assert_eq!(shown_url("http://h:9/hook?key=abc"), "http://h:9/hook");
    assert_eq!(shown_url("http://h:9/hook"), "http://h:9/hook");
    let u = parse_url("http://127.0.0.1:9/x?key=k-secret-42").unwrap();
    assert_eq!(u.shown(), "http://127.0.0.1:9/x");
    assert!(!format!("{u:?}").contains("k-secret-42"));
}

fn headers(text: &str) -> Result<Vec<(String, HeaderValue)>, String> {
    let v: toml::Value = toml::from_str(&format!("h = {text}")).unwrap();
    parse_headers(&v["h"])
}

#[test]
fn headers_keep_name_case_and_order() {
    let h = headers(
        r#"{ "Authorization" = "Bearer s3cr3t", "X-Team" = "ops", "x-API-key" = "a	b c" }"#,
    )
    .unwrap();
    let names: Vec<&str> = h.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(names, ["Authorization", "X-Team", "x-API-key"]);
    assert_eq!(h[2].1.expose(), "a\tb c");
}

#[test]
fn header_rejections_do_not_echo_values() {
    for bad in [
        r#"{ "Content-Type" = "text/plain" }"#,
        r#"{ "X Token" = "a" }"#,
        r#"{ "X-Token" = "" }"#,
        r#"{ "X-Token" = "a", "x-token" = "b" }"#,
        r#"{ "X-Token" = "a\nb" }"#,
        r#"{ "X-Token" = "a\rb" }"#,
        r#"{ "X-Token" = " a" }"#,
        r#"{ "X-Token" = "a	" }"#,
        r#"{ "X-Token" = "é" }"#,
        r#"{ "X-Token" = 1 }"#,
        r#"{ "host" = "x" }"#,
        r#"{ "TRANSFER-ENCODING" = "x" }"#,
        r#"{ "" = "x" }"#,
        r#""not a table""#,
    ] {
        let err = headers(bad).unwrap_err();
        assert!(err.starts_with("webhook_headers"), "{bad}: {err}");
        assert!(
            !err.contains("text/plain") && !err.contains("a\nb"),
            "{bad}: {err}"
        );
    }
}

#[test]
fn status_line_parsing() {
    assert_eq!(parse_status(b"HTTP/1.1 200 OK\r\n\r\n").unwrap(), 200);
    assert_eq!(parse_status(b"HTTP/1.0 503\r\n\r\n").unwrap(), 503);
    assert_eq!(parse_status(b"HTTP/1.1 600 X\r\n\r\n").unwrap(), 600);
    assert_eq!(
        parse_status(b"HTTP/1.1 302 Found\r\nLocation: /x\r\n\r\n").unwrap(),
        302
    );
    for bad in [
        &b"HTTP/2 200 OK\r\n"[..],
        b"HTTP/1.1 20 OK\r\n",
        b"garbage\r\n",
        b"HTTP/1.1 2000\r\n",
        b"HTTP/1.1 000 X\r\n",
        b"HTTP/1.1 099\r\n",
    ] {
        assert!(
            parse_status(bad).is_err(),
            "{}",
            String::from_utf8_lossy(bad)
        );
    }
}

#[test]
fn request_head_preserves_header_names() {
    let target = Target {
        url: parse_url("http://127.0.0.1:9/hook/a?team=ops").unwrap(),
        headers: headers(r#"{ "X-API-Key" = "k", "authorization" = "Bearer t" }"#).unwrap(),
    };
    let head = String::from_utf8(request_head(&target, 42)).unwrap();
    assert_eq!(
        head,
        format!(
            "POST /hook/a?team=ops HTTP/1.1\r\nHost: 127.0.0.1:9\r\nContent-Type: application/json\r\n\
             User-Agent: amberlog/{SEMVER}\r\nX-API-Key: k\r\nauthorization: Bearer t\r\nContent-Length: 42\r\n\r\n"
        )
    );
}

async fn exchange_with(response: &[u8]) -> io::Result<u16> {
    let (client, mut server) = tokio::io::duplex(1 << 20);
    let response = response.to_vec();
    let srv = tokio::spawn(async move {
        let mut buf = vec![0u8; 1024];
        let _ = server.read(&mut buf).await;
        let _ = server.write_all(&response).await;
        // 关闭写端：客户端读到 EOF
        drop(server);
    });
    let got = exchange(client, b"POST / HTTP/1.1\r\n\r\n", b"").await;
    srv.await.unwrap();
    got
}

#[tokio::test]
async fn exchange_reads_only_the_final_head() {
    assert_eq!(
        exchange_with(b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 102 Processing\r\n\r\nHTTP/1.1 204 No Content\r\n\r\n")
            .await
            .unwrap(),
        204
    );
    // 响应体不读，响应头之后的字节不影响结果
    assert_eq!(
        exchange_with(b"HTTP/1.1 202 Accepted\r\nContent-Length: 8\r\n\r\nnot json")
            .await
            .unwrap(),
        202
    );
    // 只收到 1xx 或半个响应头就被关闭：连接失败
    assert!(
        exchange_with(b"HTTP/1.1 100 Continue\r\n\r\n")
            .await
            .is_err()
    );
    assert!(exchange_with(b"HTTP/1.1 200 OK\r\nX: ").await.is_err());
    assert!(exchange_with(b"").await.is_err());
    // 响应头过大：按连接失败
    let mut big = b"HTTP/1.1 200 OK\r\n".to_vec();
    big.extend(std::iter::repeat_n(b'a', MAX_RESPONSE_HEAD + 10));
    assert!(exchange_with(&big).await.is_err());
}

/// 进程内的接收端：按到达先后依次回 `replies[i]`（状态码、回应前的延迟），超出的回最后一个；
/// 记下每个请求的原字节与到达时刻。
struct Peer {
    url: String,
    seen: Arc<std::sync::Mutex<Vec<Seen>>>,
}

/// 到达时刻与请求的原字节。
type Seen = (Instant, Vec<u8>);

async fn peer(replies: Vec<(u16, Duration)>) -> Peer {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/hook?k=v", listener.local_addr().unwrap());
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let log = Arc::clone(&seen);
    tokio::spawn(async move {
        let mut n = 0;
        loop {
            let (mut s, _) = listener.accept().await.unwrap();
            let (status, delay) = replies[n.min(replies.len() - 1)];
            n += 1;
            let log = Arc::clone(&log);
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut chunk = [0u8; 4096];
                // 读到请求体读完（请求头里有 Content-Length）
                loop {
                    let k = s.read(&mut chunk).await.unwrap_or(0);
                    if k == 0 {
                        return;
                    }
                    buf.extend_from_slice(&chunk[..k]);
                    if let Some(end) = find_head_end(&buf, 0) {
                        let head = String::from_utf8_lossy(&buf[..end]).to_ascii_lowercase();
                        let len: usize = head
                            .lines()
                            .find_map(|l| l.strip_prefix("content-length: "))
                            .unwrap()
                            .trim()
                            .parse()
                            .unwrap();
                        if buf.len() >= end + len {
                            break;
                        }
                    }
                }
                log.lock().unwrap().push((Instant::now(), buf));
                tokio::time::sleep(delay).await;
                let _ = s
                    .write_all(
                        format!("HTTP/1.1 {status} X\r\nContent-Length: 0\r\n\r\n").as_bytes(),
                    )
                    .await;
            });
        }
    });
    Peer { url, seen }
}

fn notice(url: &str, interval: Duration, scheduled_at: i64) -> Notice {
    use crate::alerting::rule::{CmpOp, Condition, Rule, Source};
    Notice {
        rule: Arc::new(Rule {
            name: "r1".into(),
            org: "default".into(),
            interval,
            window: Duration::from_secs(300),
            source: Source::Sql("SELECT k FROM app".into()),
            condition: Condition::Rows {
                op: CmpOp::Gt,
                n: 0,
            },
            webhook: Target {
                url: parse_url(url).unwrap(),
                headers: headers(r#"{ "X-Api-KEY" = "s3cr3t" }"#).unwrap(),
            },
        }),
        scheduled_at,
        matched_rows: 1,
        truncated: false,
        rows: vec![Hit(vec![("k".into(), Value::from(1))])],
    }
}

fn outcome(result: DeliveryResult, attempts: u32, last_status: Option<u16>) -> DeliveryOutcome {
    DeliveryOutcome {
        result,
        attempts,
        last_status,
    }
}

const MINUTE: Duration = Duration::from_secs(60);

#[tokio::test]
async fn delivered_first_time_with_exact_request() {
    let p = peer(vec![(202, Duration::ZERO)]).await;
    let n = notice(&p.url, MINUTE, now_us());
    let (_trigger, stop) = Stop::pair();
    let got = Webhook::new(None).deliver(n.clone(), stop).await;
    assert_eq!(got, outcome(DeliveryResult::Delivered, 1, Some(202)));
    let seen = p.seen.lock().unwrap();
    let raw = String::from_utf8(seen[0].1.clone()).unwrap();
    let (head, body) = raw.split_once("\r\n\r\n").unwrap();
    assert!(head.starts_with("POST /hook?k=v HTTP/1.1\r\n"), "{head}");
    assert!(head.contains("\r\nX-Api-KEY: s3cr3t\r\n"), "{head}");
    assert!(
        head.contains(&format!("\r\nContent-Length: {}", body.len())),
        "{head}"
    );
    let v: Value = serde_json::from_str(body).unwrap();
    assert_eq!(v["evaluation_id"], format!("r1-{}", n.scheduled_at));
    assert_eq!(v["rows"], serde_json::json!([{"k": 1}]));
    // 十个字段按块列出的次序
    let at: Vec<usize> = [
        "version",
        "rule",
        "evaluation_id",
        "scheduled_at",
        "window_start",
        "window_end",
        "matched_rows",
        "truncated",
        "rows",
        "sql",
    ]
    .iter()
    .map(|k| body.find(&format!("\"{k}\":")).unwrap())
    .collect();
    assert!(at.windows(2).all(|w| w[0] < w[1]), "{body}");
    assert_eq!(v.as_object().unwrap().len(), 10);
}

#[test]
fn body_times_and_id() {
    let n = notice("http://h/x", MINUTE, 1_792_130_400_000_000);
    let v: Value = serde_json::from_slice(&request_body(&n)).unwrap();
    assert_eq!(v["evaluation_id"], "r1-1792130400000000");
    assert_eq!(v["scheduled_at"], "2026-10-16T06:00:00.000000Z");
    assert_eq!(v["window_end"], "2026-10-16T06:00:00.000000Z");
    assert_eq!(v["window_start"], "2026-10-16T05:55:00.000000Z");
    assert_eq!(v["version"], 1);
    assert_eq!(v["sql"], "SELECT k FROM app");
}

#[tokio::test]
async fn retries_retryable_with_same_body_then_delivers() {
    let p = peer(vec![
        (503, Duration::ZERO),
        (429, Duration::ZERO),
        (204, Duration::ZERO),
    ])
    .await;
    let (_trigger, stop) = Stop::pair();
    let got = Webhook::new(None)
        .deliver(notice(&p.url, MINUTE, now_us()), stop)
        .await;
    assert_eq!(
        got,
        outcome(DeliveryResult::DeliveredAfterRetry, 3, Some(204))
    );
    let seen = p.seen.lock().unwrap();
    assert_eq!(seen.len(), 3);
    assert!(
        seen.iter().all(|(_, raw)| *raw == seen[0].1),
        "各次尝试逐字节相同"
    );
    // 间隔 1、2 秒（允许调度误差）
    let gaps: Vec<Duration> = seen.windows(2).map(|w| w[1].0 - w[0].0).collect();
    assert!(
        gaps[0] >= Duration::from_secs(1) && gaps[0] < Duration::from_millis(1500),
        "{gaps:?}"
    );
    assert!(
        gaps[1] >= Duration::from_secs(2) && gaps[1] < Duration::from_millis(2500),
        "{gaps:?}"
    );
}

#[tokio::test]
async fn not_retryable_statuses_abandon_after_one_attempt() {
    for status in [401u16, 302, 404, 301, 600, 999] {
        let p = peer(vec![(status, Duration::ZERO)]).await;
        let (_trigger, stop) = Stop::pair();
        let got = Webhook::new(None)
            .deliver(notice(&p.url, MINUTE, now_us()), stop)
            .await;
        assert_eq!(got, outcome(DeliveryResult::Abandoned, 1, Some(status)));
        assert_eq!(p.seen.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn too_close_to_deadline_sends_nothing() {
    let p = peer(vec![(200, Duration::ZERO)]).await;
    let (_trigger, stop) = Stop::pair();
    // 截止时刻在 0.5 秒之后
    let n = notice(&p.url, MINUTE, now_us() - 60_000_000 + 500_000);
    let got = Webhook::new(None).deliver(n, stop).await;
    assert_eq!(got, outcome(DeliveryResult::Abandoned, 0, None));
    assert!(p.seen.lock().unwrap().is_empty());
}

#[tokio::test]
async fn deadline_aborts_the_attempt_in_flight() {
    let p = peer(vec![(200, Duration::from_secs(30))]).await;
    let (_trigger, stop) = Stop::pair();
    let start = Instant::now();
    // 截止时刻在 3 秒之后：第一次尝试等不到响应，在截止时刻被中止
    let got = Webhook::new(None)
        .deliver(notice(&p.url, Duration::from_secs(3), now_us()), stop)
        .await;
    assert_eq!(got, outcome(DeliveryResult::Abandoned, 1, None));
    let took = start.elapsed();
    assert!(
        took >= Duration::from_millis(2900) && took < Duration::from_secs(4),
        "{took:?}"
    );
}

#[tokio::test]
async fn connection_failures_retry_until_five_attempts() {
    // 绑定而不监听的端口：连接被拒。测试期间一直持有，并行的其他测试绑不上它（不设 SO_REUSEADDR）；
    // 先绑定再释放的端口会被它们拿去监听
    let refusing = tokio::net::TcpSocket::new_v4().unwrap();
    refusing.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let port = refusing.local_addr().unwrap().port();
    let (_trigger, stop) = Stop::pair();
    let start = Instant::now();
    let got = Webhook::new(None)
        .deliver(
            notice(&format!("http://127.0.0.1:{port}/x"), MINUTE, now_us()),
            stop,
        )
        .await;
    assert_eq!(got, outcome(DeliveryResult::Abandoned, 5, None));
    // 1 + 2 + 4 + 8 秒。上限留到 25 秒：机器满载时实测到过 19.9 秒；多重试一次至少还要再等 16 秒，仍会超过上限
    let took = start.elapsed();
    assert!(
        took >= Duration::from_secs(15) && took < Duration::from_secs(25),
        "{took:?}"
    );
}

#[tokio::test]
async fn backoff_past_deadline_gives_up() {
    let p = peer(vec![(500, Duration::ZERO)]).await;
    let (_trigger, stop) = Stop::pair();
    // 截止时刻在 2.5 秒之后：第一次失败后等 1 秒发第二次，第二次失败后要等 2 秒，结束时刻晚于截止时刻，放弃
    let got = Webhook::new(None)
        .deliver(notice(&p.url, Duration::from_millis(2500), now_us()), stop)
        .await;
    assert_eq!(got, outcome(DeliveryResult::Abandoned, 2, Some(500)));
}

#[tokio::test]
async fn stop_abandons_in_flight_and_during_backoff() {
    // 进行中的尝试：停机即中止，连接关闭
    let p = peer(vec![(200, Duration::from_secs(30))]).await;
    let (trigger, stop) = Stop::pair();
    let w = Webhook::new(None);
    let n = notice(&p.url, MINUTE, now_us());
    let run = w.deliver(n, stop);
    let started = Instant::now();
    let (got, ()) = tokio::join!(run, async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        trigger.stop();
    });
    assert_eq!(got, outcome(DeliveryResult::Abandoned, 1, None));
    assert!(started.elapsed() < Duration::from_secs(2));

    // 退避期间：停机即放弃，不再发尝试
    let p = peer(vec![(503, Duration::ZERO)]).await;
    let (trigger, stop) = Stop::pair();
    let n = notice(&p.url, MINUTE, now_us());
    let (got, ()) = tokio::join!(w.deliver(n, stop), async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        trigger.stop();
    });
    assert_eq!(got, outcome(DeliveryResult::Abandoned, 1, Some(503)));
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(p.seen.lock().unwrap().len(), 1);

    // 已置位：不发尝试
    let (trigger, stop) = Stop::pair();
    trigger.stop();
    let got = w.deliver(notice(&p.url, MINUTE, now_us()), stop).await;
    assert_eq!(got, outcome(DeliveryResult::Abandoned, 0, None));
}

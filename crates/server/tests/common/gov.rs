//! 治理视图（REQ-AMB-GOV-LEDGER、REQ-AMB-GOV-RECONCILIATION）的系统测试共用部件：以测试台架直接写入或改坏
//! 计数对象，按给定时间范围查询系统表。
//!
//! 测试写入的计数对象取被测进程不会计入的过去某小时（被测进程只为有计数贡献的小时写出对象，贡献按接收时刻归入
//! 当前小时），数据集以空的 `_sent` 上报建立（不产生任何计数）。

use serde_json::{Value, json};

use super::api::MINUTE_US;
use super::{BEARER, Fixture, Resp, Server};

/// 计数对象的一个 `windows` 元素。
pub fn entry(w: i64, received: u64, durable: u64, sent: Value) -> Value {
    json!({"window_start": w, "window_end": w + MINUTE_US, "received": received, "durable": durable, "sent": sent})
}

/// 计数对象的内容（`format_version` 1）。
pub fn counter_object(stream: &str, windows: &[Value]) -> Value {
    json!({"format_version": 1, "dataset": format!("default/{stream}"), "windows": windows})
}

/// 在 `file://` 对象存储中写入 `stream` 在 `hour` 所在小时的计数对象（覆盖已有的）。
pub fn write_counters(fx: &Fixture, stream: &str, hour: i64, windows: &[Value]) {
    write_counter_bytes(
        fx,
        stream,
        hour,
        counter_object(stream, windows).to_string().as_bytes(),
    );
}

/// 以任意字节写入计数对象（改坏用）。
pub fn write_counter_bytes(fx: &Fixture, stream: &str, hour: i64, bytes: &[u8]) {
    let p = super::api::counter_file(fx, stream, hour);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(p, bytes).unwrap();
}

/// 以时间范围 `[start, end)`、`size` 为 10000 查询 `sql`，返回应答（不论状态码）。
pub fn query(server: &Server, sql: &str, (start, end): (i64, i64)) -> Resp {
    let body = json!({"query": {"sql": sql, "start_time": start, "end_time": end, "size": 10000}});
    server.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        body.to_string().as_bytes(),
    )
}

/// 同 [`query`]，断言 200，返回 `hits`。
pub fn query_ok(server: &Server, sql: &str, range: (i64, i64)) -> Vec<Value> {
    let r = query(server, sql, range);
    assert_eq!(r.status, 200, "{sql}：{}", r.text());
    r.json()["hits"].as_array().unwrap().clone()
}

/// 以 `flush.interval` 为 `1h` 启动，依次摄取各批次后优雅停机（停机时 flush 并提交）。
pub fn committed(fx: &Fixture, batches: &[(&str, &str)]) {
    let mut server = fx.start(&[super::api::FLUSH_1H]);
    for (stream, body) in batches {
        super::api::ingest_ok(&server, stream, body);
    }
    stop(&mut server);
}

/// SIGTERM 并等进程以 0 退出。
pub fn stop(server: &mut Server) {
    server.signal("TERM");
    assert_eq!(
        server.wait_exit(std::time::Duration::from_secs(30)),
        Some(Some(0)),
        "{:#?}",
        server.stderr_lines()
    );
}

/// `_current` 所指快照中第一个桶的桶 manifest 的相对路径。
pub fn first_bucket_manifest(fx: &Fixture, stream: &str) -> String {
    fx.dataset(stream).head()["buckets"][0]["manifest"]
        .as_str()
        .unwrap()
        .to_owned()
}

/// UTC 微秒 `us` 的 RFC 3339 写法（`YYYY-MM-DDTHH:MM:SS.ffffffZ`）：数值 `_timestamp` 按幅值判定单位，
/// 远未来的微秒值会被当作纳秒，以字符串写入才不受影响。
pub fn rfc3339(us: i64) -> String {
    let days = us.div_euclid(86_400_000_000);
    let rem = us.rem_euclid(86_400_000_000);
    // civil_from_days（Howard Hinnant）
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    let secs = rem / 1_000_000;
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:06}Z",
        secs / 3600,
        secs / 60 % 60,
        secs % 60,
        rem % 1_000_000
    )
}

use std::path::PathBuf;

use amberlog_storage::store::{Location, S3Options};
use amberlog_storage::testing::FakeWal;

use super::*;

/// 当前小时的起点：写出后早于上一个小时的对象会移出内存，测试用当前小时。
fn t0() -> i64 {
    hour_of(now_us())
}

fn dataset() -> Dataset {
    Dataset {
        org: "default".into(),
        stream: "app".into(),
    }
}

struct World {
    dir: PathBuf,
    wal: Arc<FakeWal>,
    counters: Counters,
}

impl World {
    fn new() -> World {
        World::with_wal(Arc::new(FakeWal::new()))
    }

    fn with_wal(wal: Arc<FakeWal>) -> World {
        use std::sync::atomic::{AtomicU32, Ordering};
        static N: AtomicU32 = AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "amberlog-counters-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let store = Store::open(
            &Location::File { dir: dir.clone() },
            &S3Options::default(),
            "amberlog",
        )
        .unwrap();
        World {
            dir,
            counters: Counters::new(store, wal.clone()),
            wal,
        }
    }

    fn object_file(&self) -> PathBuf {
        self.dir
            .join("amberlog/default/app")
            .join(counters_path(t0()))
    }

    fn object(&self) -> Value {
        serde_json::from_slice(&std::fs::read(self.object_file()).unwrap()).unwrap()
    }

    fn window(&self, start: i64) -> Value {
        self.object()["windows"]
            .as_array()
            .unwrap()
            .iter()
            .find(|w| w["window_start"] == start)
            .cloned()
            .unwrap_or(Value::Null)
    }
}

impl Drop for World {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// 一个被确认批次的贡献：未被接受的记录都是逐条拒收。
fn ok(received: u64, durable: u64) -> Tally {
    Tally {
        received,
        durable,
        rejected: received - durable,
        received_503: 0,
    }
}

/// 只计 `received` 的贡献。
fn received_only(received: u64) -> Tally {
    Tally {
        received,
        ..Tally::default()
    }
}

fn element(source: &str, start: i64, sent: u64) -> SentElement {
    SentElement {
        source: source.into(),
        window_start_us: start,
        sent,
        basis: Basis::Delivery,
    }
}

#[test]
fn counters_path_is_hourly() {
    assert_eq!(
        counters_path(1_700_000_040_000_000),
        "counters/2023/11/14/22.json"
    );
    assert_eq!(window_of(1_700_000_099_999_999), 1_700_000_040_000_000);
}

#[tokio::test]
async fn object_shape_sparse_ascending() {
    let start = t0();
    let w = World::new();
    let d = dataset();
    w.counters
        .record_durable(&d, start + MINUTE_US, ok(3, 2), None, &[], WalPosition(5));
    w.counters
        .record_unconfirmed(&d, start, received_only(4), None);
    w.counters
        .record_unconfirmed(&d, start + 2 * MINUTE_US, received_only(0), None);
    w.counters.flush_all().await;
    let obj = w.object();
    assert_eq!(obj["format_version"], 1);
    assert_eq!(obj["dataset"], "default/app");
    let windows = obj["windows"].as_array().unwrap();
    assert_eq!(windows.len(), 2, "{obj}");
    assert_eq!(windows[0]["window_start"], start);
    assert_eq!(windows[0]["window_end"], start + MINUTE_US);
    assert_eq!(windows[0]["received"], 4);
    assert_eq!(windows[0]["durable"], 0);
    assert_eq!(windows[0]["sent"], json!({}));
    assert_eq!(windows[1]["received"], 3);
    assert_eq!(windows[1]["durable"], 2);
    // 写成之后按水位通知 WAL
    assert!(w.wal.releases().contains(&Release::Counts {
        dataset: d,
        hour_start_us: t0(),
        through: WalPosition(5),
    }));
}

#[tokio::test]
async fn sent_is_absolute_and_zero_kept() {
    let start = t0();
    let w = World::new();
    let d = dataset();
    w.counters
        .record_sent(&d, &[element("a", start, 100)], WalPosition(1));
    w.counters.flush_all().await;
    w.counters.record_sent(
        &d,
        &[
            element("a", start, 0),
            element("b", start, 9),
            element("b", start, 7),
        ],
        WalPosition(2),
    );
    w.counters.flush_all().await;
    let win = w.window(start);
    assert_eq!(win["sent"], json!({"a": 0, "b": 7}));
    assert_eq!(win["received"], 0);
}

#[tokio::test]
async fn existing_object_is_the_base_and_high_version_is_left_alone() {
    let start = t0();
    let w = World::new();
    let d = dataset();
    let file = w.object_file();
    std::fs::create_dir_all(file.parent().unwrap()).unwrap();
    std::fs::write(
        &file,
        json!({"format_version":1,"dataset":"default/app","extra":true,"windows":[
            {"window_start":start,"window_end":start+MINUTE_US,"received":10,"durable":9,"sent":{"a":1}}]})
        .to_string(),
    )
    .unwrap();
    w.counters
        .record_durable(&d, start, ok(2, 2), None, &[], WalPosition(1));
    w.counters.flush_all().await;
    assert_eq!(w.window(start)["received"], 12);
    assert_eq!(w.window(start)["sent"], json!({"a": 1}));

    // 此后被换成更高版本：不覆盖，并通知 WAL 不再为该小时保留段
    let high = br#"{"format_version":2,"whatever":[]}"#;
    std::fs::write(&file, high).unwrap();
    w.counters
        .record_durable(&d, start, ok(1, 1), None, &[], WalPosition(2));
    w.counters.flush_all().await;
    w.counters.flush_all().await;
    assert_eq!(std::fs::read(&file).unwrap(), high);
    assert!(w.wal.releases().contains(&Release::Counts {
        dataset: d,
        hour_start_us: t0(),
        through: WalPosition::MAX,
    }));
}

#[tokio::test]
async fn unparseable_objects_are_incompatible() {
    let start = t0();
    for bad in [
        &b"not json"[..],
        br#"{"format_version":0,"dataset":"default/app","windows":[]}"#,
        br#"{"format_version":1,"dataset":"default/app","windows":[{"window_start":1,"window_end":60000001,"received":1,"durable":1,"sent":{}}]}"#,
        br#"{"format_version":1,"dataset":"default/app","windows":[{"window_start":0,"window_end":60000000,"received":-1,"durable":1,"sent":{}}]}"#,
        br#"{"format_version":"1","dataset":"default/app","windows":[]}"#,
    ] {
        assert!(parse(bad).is_none(), "{}", String::from_utf8_lossy(bad));
        let w = World::new();
        let file = w.object_file();
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, bad).unwrap();
        w.counters
            .record_durable(&dataset(), start, ok(1, 1), None, &[], WalPosition(1));
        w.counters.flush_all().await;
        assert_eq!(std::fs::read(&file).unwrap(), bad);
    }
}

/// 重启后按对象摘要找到计数意图，水位之前的增量不再计入（ADR-0023 第 4 条）。
#[tokio::test]
async fn restore_counts_each_acknowledged_batch_once() {
    let start = t0();
    let d = dataset();
    let first = World::new();
    let counts = |pos: u64, r: u64, dur: u64| {
        (
            WalPosition(pos),
            WalEntry::Counts {
                dataset: d.clone(),
                window_start_us: start,
                received: r,
                durable: dur,
                rejected: r - dur,
                source: None,
                durable_ts: Some(Vec::new()),
            },
        )
    };
    first
        .counters
        .record_durable(&d, start, ok(3, 2), None, &[], WalPosition(1));
    first.counters.flush_all().await;
    let intents: Vec<(WalPosition, WalEntry)> = first
        .wal
        .entries()
        .into_iter()
        .filter(|(_, e)| matches!(e, WalEntry::CountIntent { .. }))
        .collect();
    assert_eq!(intents.len(), 1);

    // 「重启」：同一对象存储，重放交回位置 1 的增量、意图与写出之后的位置 5 的增量
    let mut replay = vec![counts(1, 3, 2)];
    replay.extend(intents.iter().map(|(_, e)| (WalPosition(3), e.clone())));
    replay.push(counts(5, 1, 1));
    let second = Counters::new(
        first.counters.store.clone(),
        Arc::new(FakeWal::new()) as Arc<dyn Wal>,
    );
    second.restore(&replay);
    second.flush_all().await;
    assert_eq!(first.window(start)["received"], 4);
    assert_eq!(first.window(start)["durable"], 3);

    // 对象与任何意图都不相符（例如意图记下后没写成）：水位为 0，全部重放增量计入
    std::fs::remove_file(first.object_file()).unwrap();
    let third = Counters::new(
        first.counters.store.clone(),
        Arc::new(FakeWal::new()) as Arc<dyn Wal>,
    );
    third.restore(&replay);
    third.flush_all().await;
    assert_eq!(first.window(start)["received"], 4);
    assert_eq!(first.window(start)["durable"], 3);
}

/// 写入失败后的下一次尝试先重记最近写成的意图，WAL 为每小时保留的最近两条意图总含它。
#[tokio::test]
async fn failed_attempt_re_records_the_confirmed_intent() {
    let start = t0();
    let w = World::new();
    let d = dataset();
    w.counters
        .record_durable(&d, start, ok(1, 1), None, &[], WalPosition(1));
    w.counters.flush_all().await;
    // 让对象所在目录不可写：写不成
    let file = w.object_file();
    let before = std::fs::read(&file).unwrap();
    let dir = file.parent().unwrap().to_owned();
    std::fs::remove_file(&file).unwrap();
    std::fs::remove_dir(&dir).unwrap();
    std::fs::write(&dir, b"not a directory").unwrap();
    w.counters
        .record_durable(&d, start, ok(1, 1), None, &[], WalPosition(2));
    w.counters.flush_all().await;
    std::fs::remove_file(&dir).unwrap();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(&file, &before).unwrap();
    w.counters.flush_all().await;

    let intents: Vec<(u64, Digest)> = w
        .wal
        .entries()
        .into_iter()
        .filter_map(|(_, e)| match e {
            WalEntry::CountIntent {
                through, digest, ..
            } => Some((through.0, digest)),
            _ => None,
        })
        .collect();
    let d1 = Digest::of(&before);
    assert_eq!(intents.len(), 4, "{intents:?}");
    assert_eq!(intents[0], (1, d1));
    assert_eq!(intents[1].0, 2);
    assert_eq!(intents[2], (1, d1), "失败之后先重记写成的意图");
    assert_eq!(intents[3].0, 2);
    assert_eq!(w.window(start)["received"], 2);
}

#[test]
fn ingest_counters_sent_validation() {
    let ok = |b: &str| parse_sent(b.as_bytes());
    let e =
        r#"{"source":"a","window_start":1700000040000000,"window_end":1700000100000000,"sent":1}"#;
    assert_eq!(
        ok(&format!("[{e}]")).unwrap(),
        vec![element("a", 1_700_000_040_000_000, 1)]
    );
    assert_eq!(ok("[]").unwrap(), vec![]);
    assert_eq!(ok(r#"[{"source":"a","#), Err(SentError::InvalidJson));
    let long = "a".repeat(128);
    assert!(
        ok(&format!(
            r#"[{{"source":"{long}","window_start":0,"window_end":60000000,"sent":0}}]"#
        ))
        .is_ok()
    );
    for bad in [
        e.to_owned(),
        format!("[{e}, 5]"),
        format!(
            r#"[{e}, {{"source":"b","window_start":1700000040000000,"window_end":1700000100000000}}]"#
        ),
        format!(
            r#"[{e}, {{"source":"b","window_start":"1700000040000000","window_end":1700000100000000,"sent":1}}]"#
        ),
        format!(
            r#"[{e}, {{"source":"b","window_start":1700000040000000,"window_end":1700000100000000,"sent":1.5}}]"#
        ),
        format!(
            r#"[{e}, {{"source":"b","window_start":1700000040000000,"window_end":1700000100000000,"sent":1e2}}]"#
        ),
        format!(
            r#"[{e}, {{"source":"_b","window_start":1700000040000000,"window_end":1700000100000000,"sent":1}}]"#
        ),
        format!(
            r#"[{e}, {{"source":"a b","window_start":1700000040000000,"window_end":1700000100000000,"sent":1}}]"#
        ),
        format!(
            r#"[{e}, {{"source":"a{long}","window_start":1700000040000000,"window_end":1700000100000000,"sent":1}}]"#
        ),
        format!(
            r#"[{e}, {{"source":"b","window_start":1700000040000000,"window_end":1700000160000000,"sent":1}}]"#
        ),
        format!(r#"[{e}, {{"source":"b","window_start":-60000000,"window_end":0,"sent":1}}]"#),
        format!(
            r#"[{e}, {{"source":"b","window_start":1700000030000000,"window_end":1700000090000000,"sent":1}}]"#
        ),
        format!(
            r#"[{e}, {{"source":"b","window_start":1700000040000000,"window_end":1700000100000000,"sent":-1}}]"#
        ),
        format!(
            r#"[{e}, {{"source":1,"window_start":1700000040000000,"window_end":1700000100000000,"sent":1}}]"#
        ),
    ] {
        assert!(
            matches!(ok(&bad), Err(SentError::InvalidRequest(_))),
            "{bad}"
        );
    }
}

/// `_sent` 的两条上界（REQ-AMB-OBS-INGEST-COUNTERS-V2）：`window_start` 至多为
/// 9999-12-31T23:59:00Z，`sent` 至多为 2^64−1，越界即整个请求 400 `invalid_request`，不饱和截断。
#[test]
fn ingest_counters_sent_upper_bounds() {
    let one = |start: i64, sent: &str| {
        parse_sent(
            format!(
                r#"[{{"source":"a","window_start":{start},"window_end":{},"sent":{sent}}}]"#,
                start + MINUTE_US
            )
            .as_bytes(),
        )
    };
    assert_eq!(
        one(MAX_SENT_WINDOW_START, "1").unwrap(),
        vec![element("a", MAX_SENT_WINDOW_START, 1)]
    );
    assert!(matches!(
        one(MAX_SENT_WINDOW_START + MINUTE_US, "1"),
        Err(SentError::InvalidRequest(_))
    ));
    assert_eq!(
        one(0, "18446744073709551615").unwrap(),
        vec![element("a", 0, u64::MAX)]
    );
    assert_eq!(
        one(0, "9223372036854775808").unwrap(),
        vec![element("a", 0, 1 << 63)]
    );
    for bad in [
        "18446744073709551616",
        "99999999999999999999999",
        "-1",
        "1.0",
    ] {
        assert!(
            matches!(one(0, bad), Err(SentError::InvalidRequest(_))),
            "{bad}"
        );
    }
}

/// 第 1 版计数对象中后加的字段：缺 `rejected`、`received_503` 读作 0，缺 `by_source` 读作没有来源；
/// 计数大于 2^64−1 或 `by_source` 多于 1000 个来源属内容无法解析（REQ-AMB-OBS-INGEST-COUNTERS-V2）。
#[test]
fn counters_object_new_fields_read() {
    let object = |w: Value| {
        json!({"format_version": 1, "dataset": "default/app", "windows": [w]})
            .to_string()
            .into_bytes()
    };
    let legacy =
        object(json!({"window_start":0,"window_end":MINUTE_US,"received":5,"durable":5,"sent":{}}));
    let w = &parse(&legacy).unwrap()[&0];
    assert_eq!(w.counts, ok(5, 5));
    assert!(w.by_source.is_empty());

    let full = object(
        json!({"window_start":0,"window_end":MINUTE_US,"received":5,"durable":3,
        "rejected":1,"received_503":1,"sent":{},
        "by_source":{"fw-1":{"received":2,"durable":1,"rejected":1,"received_503":0}}}),
    );
    let w = &parse(&full).unwrap()[&0];
    assert_eq!(
        (
            w.counts.rejected,
            w.counts.received_503,
            w.by_source["fw-1"]
        ),
        (1, 1, ok(2, 1))
    );

    let max = object(
        json!({"window_start":0,"window_end":MINUTE_US,"received":u64::MAX,"durable":0,"sent":{}}),
    );
    assert_eq!(parse(&max).unwrap()[&0].counts.received, u64::MAX);

    let too_many: Map<String, Value> = (0..=MAX_SOURCES)
        .map(|i| {
            (
                format!("s-{i}"),
                json!({"received":1,"durable":1,"rejected":0,"received_503":0}),
            )
        })
        .collect();
    for bad in [
        r#"{"format_version":1,"dataset":"default/app","windows":[{"window_start":0,"window_end":60000000,"received":18446744073709551616,"durable":0,"sent":{}}]}"#.as_bytes().to_vec(),
        r#"{"format_version":1,"dataset":"default/app","windows":[{"window_start":0,"window_end":60000000,"received":1,"durable":0,"rejected":18446744073709551616,"sent":{}}]}"#.as_bytes().to_vec(),
        r#"{"format_version":1,"dataset":"default/app","windows":[{"window_start":0,"window_end":60000000,"received":1,"durable":0,"sent":{"a":18446744073709551616}}]}"#.as_bytes().to_vec(),
        object(json!({"window_start":0,"window_end":MINUTE_US,"received":1,"durable":1,"sent":{},
            "by_source":{"a":{"received":1,"durable":"1","rejected":0,"received_503":0}}})),
        object(json!({"window_start":0,"window_end":MINUTE_US,"received":1,"durable":1,"sent":{},"by_source":[]})),
        object(json!({"window_start":0,"window_end":MINUTE_US,"received":1,"durable":1,"sent":{},"by_source":too_many})),
    ] {
        assert!(parse(&bad).is_none(), "{}", String::from_utf8_lossy(&bad));
    }
}

/// 写出的每个窗口都含 `rejected` 与 `received_503`；有来源的窗口另含 `by_source`，没有的不含；
/// 确认、全拒与 503 三种批次各按块计入；重放交回的带来源增量同样按来源计入。
#[tokio::test]
async fn counters_v2_fields_and_by_source() {
    let start = t0();
    let w = World::new();
    let d = dataset();
    w.counters
        .record_durable(&d, start, ok(3, 2), Some("fw-1"), &[], WalPosition(1));
    w.counters
        .record_durable(&d, start, ok(1, 1), None, &[], WalPosition(2));
    let all_rejected = Tally {
        received: 1,
        rejected: 1,
        ..Tally::default()
    };
    w.counters
        .record_unconfirmed(&d, start, all_rejected, Some("fw-1"));
    let unavailable = Tally {
        received: 2,
        received_503: 2,
        ..Tally::default()
    };
    w.counters
        .record_unconfirmed(&d, start, unavailable, Some("fw-2"));
    w.counters
        .record_durable(&d, start + MINUTE_US, ok(1, 1), None, &[], WalPosition(3));
    w.counters.flush_all().await;

    let first = w.window(start);
    assert_eq!(
        (
            &first["received"],
            &first["durable"],
            &first["rejected"],
            &first["received_503"]
        ),
        (&json!(7), &json!(3), &json!(2), &json!(2)),
        "{first}"
    );
    assert_eq!(
        first["by_source"],
        json!({
            "fw-1": {"received": 4, "durable": 2, "rejected": 2, "received_503": 0},
            "fw-2": {"received": 2, "durable": 0, "rejected": 0, "received_503": 2},
        })
    );
    let second = w.window(start + MINUTE_US);
    assert_eq!(
        (&second["rejected"], &second["received_503"]),
        (&json!(0), &json!(0))
    );
    assert!(second.get("by_source").is_none(), "{second}");

    // 重放交回的带来源增量（另一个进程、同一对象存储，对象与任何意图都不相符时全部计入）
    std::fs::remove_file(w.object_file()).unwrap();
    let again = Counters::new(
        w.counters.store.clone(),
        Arc::new(FakeWal::new()) as Arc<dyn Wal>,
    );
    again.restore(&[(
        WalPosition(1),
        WalEntry::Counts {
            dataset: d.clone(),
            window_start_us: start,
            received: 3,
            durable: 2,
            rejected: 1,
            source: Some("fw-1".into()),
            durable_ts: Some(Vec::new()),
        },
    )]);
    again.flush_all().await;
    assert_eq!(
        w.window(start)["by_source"],
        json!({"fw-1": {"received": 3, "durable": 2, "rejected": 1, "received_503": 0}})
    );
}

/// 一个窗口至多 1000 个来源：已满时新来源只计入总计；底稿已有 1000 个来源时，本进程的新来源同样不进 `by_source`。
#[tokio::test]
async fn counters_by_source_capped() {
    let start = t0();
    let w = World::new();
    let d = dataset();
    for i in 1..=MAX_SOURCES + 1 {
        let source = format!("s-{i}");
        w.counters.record_durable(
            &d,
            start,
            ok(1, 1),
            Some(&source),
            &[],
            WalPosition(i as u64),
        );
    }
    // 已在 `by_source` 中的来源照常累加
    w.counters
        .record_durable(&d, start, ok(1, 1), Some("s-1"), &[], WalPosition(5000));
    w.counters.flush_all().await;
    let window = w.window(start);
    assert_eq!(window["received"], MAX_SOURCES as u64 + 2);
    let by_source = window["by_source"].as_object().unwrap();
    assert_eq!(by_source.len(), MAX_SOURCES);
    assert!(!by_source.contains_key(&format!("s-{}", MAX_SOURCES + 1)));
    assert_eq!(by_source["s-1"]["received"], 2);

    // 另一个进程以该对象为底稿：已满的窗口不再收新来源，已有来源照常累加
    let again = Counters::new(
        w.counters.store.clone(),
        Arc::new(FakeWal::new()) as Arc<dyn Wal>,
    );
    again.record_durable(&d, start, ok(1, 1), Some("late"), &[], WalPosition(1));
    again.record_durable(&d, start, ok(1, 1), Some("s-2"), &[], WalPosition(2));
    again.flush_all().await;
    let window = w.window(start);
    assert_eq!(window["received"], MAX_SOURCES as u64 + 4);
    let by_source = window["by_source"].as_object().unwrap();
    assert_eq!(by_source.len(), MAX_SOURCES);
    assert!(!by_source.contains_key("late"));
    assert_eq!(by_source["s-2"]["received"], 2);
}

/// 读回只读已写出的计数对象：范围跨越的小时少时逐小时直读，多时先列举再读，两种读法结果相同；
/// 只取 `window_start` 落在范围内的窗口；不认得的键不读；内容无法解析与版本过高同样报 `Unparseable`。
#[tokio::test]
async fn read_persisted_direct_and_listed() {
    let w = World::new();
    let h = 1_787_392_800_000_000; // 2026-08-22T10:00:00Z
    let put = |hour: i64, body: &[u8]| {
        let p = w.dir.join("amberlog/default/app").join(counters_path(hour));
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, body).unwrap();
    };
    let object = |starts: &[i64]| {
        let windows: Vec<Value> = starts
            .iter()
            .map(|s| json!({"window_start": s, "window_end": s + MINUTE_US, "received": 2, "durable": 1, "sent": {"b": 1, "a": 0}}))
            .collect();
        json!({"format_version": 1, "dataset": "default/app", "windows": windows}).to_string()
    };
    put(h, object(&[h, h + MINUTE_US]).as_bytes());
    put(h + 100 * HOUR_US, object(&[h + 100 * HOUR_US]).as_bytes());
    // 路径写法不合的对象不认
    let stray = w
        .dir
        .join("amberlog/default/app/counters/2026/08/22/x.json");
    std::fs::write(stray, b"{").unwrap();
    // 进程内尚未写出的计数不出现
    w.counters
        .record_unconfirmed(&dataset(), h + 2 * MINUTE_US, received_only(5), None);

    let starts = |r: Result<Vec<PersistedWindow>, ReadError>| -> Vec<i64> {
        r.unwrap().iter().map(|w| w.window_start).collect()
    };
    let direct = w
        .counters
        .read_persisted(&dataset(), h + MINUTE_US, h + HOUR_US)
        .await;
    assert_eq!(starts(direct.clone()), [h + MINUTE_US]);
    let first = &direct.unwrap()[0];
    assert_eq!((first.received, first.durable), (2, 1));
    assert_eq!(first.sent.keys().collect::<Vec<_>>(), ["a", "b"]);
    let listed = w
        .counters
        .read_persisted(&dataset(), h - 1000 * HOUR_US, h + 1000 * HOUR_US)
        .await;
    assert_eq!(starts(listed), [h, h + MINUTE_US, h + 100 * HOUR_US]);
    assert_eq!(
        starts(w.counters.read_persisted(&dataset(), 0, h).await),
        Vec::<i64>::new()
    );

    put(h + 100 * HOUR_US, b"{not json");
    assert!(matches!(
        w.counters.read_persisted(&dataset(), 0, i64::MAX).await,
        Err(ReadError::Unparseable(_))
    ));
    put(
        h + 100 * HOUR_US,
        object(&[h])
            .replace("\"format_version\":1", "\"format_version\":2")
            .as_bytes(),
    );
    assert!(matches!(
        w.counters
            .read_persisted(&dataset(), h + 100 * HOUR_US, h + 101 * HOUR_US)
            .await,
        Err(ReadError::Unparseable(_))
    ));
}

impl World {
    fn window_in_hour(&self, hour: i64, start: i64) -> Value {
        let file = self
            .dir
            .join("amberlog/default/app")
            .join(counters_path(hour));
        let obj: Value = serde_json::from_slice(&std::fs::read(file).unwrap()).unwrap();
        obj["windows"]
            .as_array()
            .unwrap()
            .iter()
            .find(|w| w["window_start"] == start)
            .cloned()
            .unwrap_or(Value::Null)
    }
}

fn timestamped(source: &str, start: i64, sent: u64) -> SentElement {
    SentElement {
        basis: Basis::Timestamp,
        ..element(source, start, sent)
    }
}

/// 按记录时间戳的确认数写进记录分钟所在小时的对象（可以是早几个小时的），按来源另记；每个窗口都写
/// `durable_ts` 与 `sent_ts`，只有按时间戳计数的分钟也写；两种 `basis` 的上报互不覆盖（REQ-AMB-OBS-INGEST-COUNTERS-V3）。
#[tokio::test]
async fn durable_ts_by_record_minute_and_sent_ts() {
    let start = t0();
    let past = start - 3 * HOUR_US;
    let w = World::new();
    let d = dataset();
    w.counters.record_durable(
        &d,
        start + MINUTE_US,
        ok(4, 3),
        Some("fw-1"),
        &[(past + 2 * MINUTE_US, 2), (start + MINUTE_US, 1)],
        WalPosition(7),
    );
    w.counters.record_durable(
        &d,
        start + MINUTE_US,
        ok(1, 1),
        None,
        &[(past + 2 * MINUTE_US, 1)],
        WalPosition(9),
    );
    w.counters.record_sent(
        &d,
        &[
            element("a", start, 90),
            timestamped("a", start, 100),
            timestamped("a", start, 0),
        ],
        WalPosition(10),
    );
    w.counters.flush_all().await;

    let now = w.window(start + MINUTE_US);
    assert_eq!((&now["received"], &now["durable"]), (&json!(5), &json!(4)));
    assert_eq!(now["durable_ts"], 1, "{now}");
    assert_eq!(now["sent_ts"], json!({}), "{now}");
    assert_eq!(now["durable_ts_by_source"], json!({"fw-1": 1}), "{now}");

    let late = w.window_in_hour(past, past + 2 * MINUTE_US);
    assert_eq!(
        (&late["received"], &late["durable"], &late["durable_ts"]),
        (&json!(0), &json!(0), &json!(3)),
        "{late}"
    );
    assert_eq!(late["durable_ts_by_source"], json!({"fw-1": 2}), "{late}");
    assert_eq!(late["sent"], json!({}));
    assert_eq!(late["sent_ts"], json!({}));

    let reported = w.window(start);
    assert_eq!(reported["sent"], json!({"a": 90}), "{reported}");
    assert_eq!(reported["sent_ts"], json!({"a": 0}), "{reported}");
    assert_eq!(reported["durable_ts"], 0);
    assert!(reported.get("durable_ts_by_source").is_none(), "{reported}");

    // 每个涉及的小时各按水位通知 WAL
    for hour in [start, past] {
        assert!(
            w.wal.releases().iter().any(|r| matches!(r,
                Release::Counts { hour_start_us, through, .. }
                    if *hour_start_us == hour && through.0 >= 9)),
            "{hour}"
        );
    }
}

/// `durable_ts_by_source` 一个窗口至多 1000 个来源，已满时新来源只计入 `durable_ts`；以底稿合并时同样。
#[tokio::test]
async fn durable_ts_by_source_capped() {
    let start = t0();
    let w = World::new();
    let d = dataset();
    for i in 1..=MAX_SOURCES + 1 {
        w.counters.record_durable(
            &d,
            start,
            ok(1, 1),
            Some(&format!("s-{i}")),
            &[(start, 1)],
            WalPosition(i as u64),
        );
    }
    w.counters.flush_all().await;
    let again = Counters::new(
        w.counters.store.clone(),
        Arc::new(FakeWal::new()) as Arc<dyn Wal>,
    );
    again.record_durable(
        &d,
        start,
        ok(1, 1),
        Some("late"),
        &[(start, 1)],
        WalPosition(1),
    );
    again.record_durable(
        &d,
        start,
        ok(1, 1),
        Some("s-2"),
        &[(start, 1)],
        WalPosition(2),
    );
    again.flush_all().await;
    let window = w.window(start);
    assert_eq!(window["durable_ts"], MAX_SOURCES as u64 + 3);
    let by_source = window["durable_ts_by_source"].as_object().unwrap();
    assert_eq!(by_source.len(), MAX_SOURCES);
    assert!(!by_source.contains_key("late"));
    assert_eq!(by_source["s-2"], 2);
}

/// 重放：新写法的计数条目按各小时的水位各计一次；早期版本写下的计数条目按紧随其后的记录条目重算按时间戳的分组；
/// 旧 `_sent` 条目的元素按送达计入。
#[tokio::test]
async fn restore_durable_ts_once_per_hour_and_legacy_recomputed() {
    use amberlog_common::record::Record;
    let start = t0();
    let past = start - 2 * HOUR_US;
    let d = dataset();
    let first = World::new();
    first.counters.record_durable(
        &d,
        start,
        ok(2, 2),
        None,
        &[(past, 1), (start, 1)],
        WalPosition(1),
    );
    first.counters.flush_all().await;
    let intents: Vec<WalEntry> = first
        .wal
        .entries()
        .into_iter()
        .map(|(_, e)| e)
        .filter(|e| matches!(e, WalEntry::CountIntent { .. }))
        .collect();
    assert_eq!(intents.len(), 2, "两个小时各一条意图");

    let records = |ts: &[i64]| WalEntry::Records {
        dataset: d.clone(),
        records: ts
            .iter()
            .map(|t| Record {
                timestamp_us: *t,
                fields: Vec::new(),
            })
            .collect(),
    };
    let mut replay = vec![
        (
            WalPosition(1),
            WalEntry::Counts {
                dataset: d.clone(),
                window_start_us: start,
                received: 2,
                durable: 2,
                rejected: 0,
                source: None,
                durable_ts: Some(vec![(past, 1), (start, 1)]),
            },
        ),
        (WalPosition(2), records(&[past, start])),
    ];
    replay.extend(intents.iter().map(|e| (WalPosition(3), e.clone())));
    // 升级前写下的计数条目（不带分组）与紧随其后的记录条目
    replay.push((
        WalPosition(4),
        WalEntry::Counts {
            dataset: d.clone(),
            window_start_us: start,
            received: 3,
            durable: 2,
            rejected: 1,
            source: Some("fw-1".into()),
            durable_ts: None,
        },
    ));
    replay.push((
        WalPosition(5),
        records(&[past + MINUTE_US, past + MINUTE_US]),
    ));
    replay.push((
        WalPosition(6),
        WalEntry::Sent {
            dataset: d.clone(),
            elements: vec![element("a", past, 4)],
        },
    ));
    let second = Counters::new(
        first.counters.store.clone(),
        Arc::new(FakeWal::new()) as Arc<dyn Wal>,
    );
    second.restore(&replay);
    second.flush_all().await;

    let now = first.window(start);
    assert_eq!(
        (&now["received"], &now["durable_ts"]),
        (&json!(5), &json!(1))
    );
    assert_eq!(first.window_in_hour(past, past)["durable_ts"], 1);
    assert_eq!(first.window_in_hour(past, past)["sent"], json!({"a": 4}));
    let legacy = first.window_in_hour(past, past + MINUTE_US);
    assert_eq!(legacy["durable_ts"], 2, "{legacy}");
    assert_eq!(
        legacy["durable_ts_by_source"],
        json!({"fw-1": 2}),
        "{legacy}"
    );
}

/// 第 1 版计数对象中后来加的字段：缺 `durable_ts` 读作 0，缺 `sent_ts` 读作空对象，缺 `durable_ts_by_source`
/// 读作没有按来源的按时间戳确认数；这几个计数大于 2^64−1、类型不符或来源多于 1000 个属内容无法解析。
#[test]
fn counters_object_timestamp_fields_read() {
    let object = |w: Value| {
        json!({"format_version": 1, "dataset": "default/app", "windows": [w]})
            .to_string()
            .into_bytes()
    };
    let legacy =
        object(json!({"window_start":0,"window_end":MINUTE_US,"received":5,"durable":5,"sent":{}}));
    let w = &parse(&legacy).unwrap()[&0];
    assert_eq!(w.durable_ts, 0);
    assert!(w.sent_ts.is_empty() && w.durable_ts_by_source.is_empty());

    let full = object(
        json!({"window_start":0,"window_end":MINUTE_US,"received":0,"durable":0,
        "sent":{},"durable_ts":3,"durable_ts_by_source":{"fw-1":2},"sent_ts":{"fw-1":5}}),
    );
    let w = &parse(&full).unwrap()[&0];
    assert_eq!(
        (
            w.durable_ts,
            w.durable_ts_by_source["fw-1"],
            w.sent_ts["fw-1"]
        ),
        (3, 2, 5)
    );

    let too_many: Map<String, Value> = (0..=MAX_SOURCES)
        .map(|i| (format!("s-{i}"), json!(1)))
        .collect();
    let base = json!({"window_start":0,"window_end":MINUTE_US,"received":1,"durable":1,"sent":{}});
    for (key, value) in [
        ("durable_ts", json!(-1)),
        ("durable_ts", json!("1")),
        ("sent_ts", json!([])),
        ("sent_ts", json!({"a": -1})),
        ("durable_ts_by_source", json!({"a": 1.5})),
        ("durable_ts_by_source", Value::Object(too_many.clone())),
    ] {
        let mut w = base.clone();
        w[key] = value;
        let bytes = object(w);
        assert!(
            parse(&bytes).is_none(),
            "{}",
            String::from_utf8_lossy(&bytes)
        );
    }
    let huge = br#"{"format_version":1,"dataset":"default/app","windows":[{"window_start":0,"window_end":60000000,"received":1,"durable":1,"sent":{},"durable_ts":18446744073709551616}]}"#;
    assert!(parse(huge).is_none());
}

/// `basis` 缺省为按送达，`timestamp` 为按时间戳；其他取值、非字符串与 JSON null 都使整个请求不合规。
#[test]
fn ingest_counters_sent_basis() {
    let one = |basis: &str| {
        parse_sent(
            format!(r#"[{{"source":"a","window_start":0,"window_end":60000000,"sent":1{basis}}}]"#)
                .as_bytes(),
        )
    };
    assert_eq!(one("").unwrap()[0].basis, Basis::Delivery);
    assert_eq!(
        one(r#","basis":"delivery""#).unwrap()[0].basis,
        Basis::Delivery
    );
    assert_eq!(
        one(r#","basis":"timestamp""#).unwrap()[0].basis,
        Basis::Timestamp
    );
    for bad in [
        r#","basis":"ts""#,
        r#","basis":"Timestamp""#,
        r#","basis":1"#,
        r#","basis":null"#,
        r#","basis":["timestamp"]"#,
    ] {
        assert!(
            matches!(one(bad), Err(SentError::InvalidRequest(_))),
            "{bad}"
        );
    }
}

/// 后台判定的读法：已写出的加尚未写出的；尚未装载的小时按水位只并入水位之后的重放增量；对象读不出的小时单独报错，
/// 不沿用内存；读取不改动内存中的计数。
#[tokio::test]
async fn current_hours_adds_unwritten_counts() {
    let start = t0();
    let past = start - HOUR_US;
    let d = dataset();
    let w = World::new();
    w.counters.record_durable(
        &d,
        start,
        ok(2, 2),
        Some("fw-1"),
        &[(past, 2)],
        WalPosition(1),
    );
    w.counters.flush_all().await;
    // 写出之后又计入、尚未写出
    w.counters.record_durable(
        &d,
        start,
        ok(1, 1),
        Some("fw-1"),
        &[(start, 1)],
        WalPosition(2),
    );
    w.counters
        .record_sent(&d, &[timestamped("fw-1", past, 5)], WalPosition(3));

    let hours = w.counters.current_hours(&d, past, start + HOUR_US).await;
    assert_eq!(
        hours.iter().map(|(h, _)| *h).collect::<Vec<_>>(),
        [past, start]
    );
    let past_w = &hours[0].1.as_ref().unwrap()[0];
    assert_eq!((past_w.durable_ts, past_w.sent_ts["fw-1"]), (2, 5));
    let now_w = &hours[1].1.as_ref().unwrap()[0];
    assert_eq!(
        (
            now_w.received,
            now_w.durable_ts,
            now_w.durable_ts_by_source["fw-1"]
        ),
        (3, 1, 1)
    );
    // 内存未被改动：写出后对象与上面一致
    w.counters.flush_all().await;
    assert_eq!(w.window(start)["received"], 3);

    // 另一个进程重放同样的增量：水位之前的不再并入
    let intents: Vec<(WalPosition, WalEntry)> = w
        .wal
        .entries()
        .into_iter()
        .filter(|(_, e)| matches!(e, WalEntry::CountIntent { .. }))
        .collect();
    let counts = |pos: u64| {
        (
            WalPosition(pos),
            WalEntry::Counts {
                dataset: d.clone(),
                window_start_us: start,
                received: 1,
                durable: 1,
                rejected: 0,
                source: None,
                durable_ts: Some(vec![(start, 1)]),
            },
        )
    };
    let mut replay = vec![counts(1)];
    replay.extend(intents);
    replay.push(counts(100));
    let again = Counters::new(
        w.counters.store.clone(),
        Arc::new(FakeWal::new()) as Arc<dyn Wal>,
    );
    again.restore(&replay);
    let hours = again.current_hours(&d, start, start + HOUR_US).await;
    assert_eq!(hours[0].1.as_ref().unwrap()[0].received, 4);

    // 对象读不出：该小时报错，别的小时照常
    std::fs::write(w.object_file(), b"not json").unwrap();
    let hours = again.current_hours(&d, past, start + HOUR_US).await;
    assert!(hours[0].1.is_ok());
    assert!(matches!(hours[1].1, Err(ReadError::Unparseable(_))));
}

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use amberlog_query::system::{AlertHistory, HistoryError};
use amberlog_storage::store::{Location, S3Options, Store};

use super::object::{self, ObjectKey};
use super::{History, retention};
use crate::alerting::Recorder;
use crate::alerting::outcome::{DeliveryResult, EvaluationRecord, Outcome, SkipReason};

const HOUR: i64 = 3_600_000_000;
const MINUTE: i64 = 60_000_000;
/// 2026-10-19T03:00:00Z
const H: i64 = 1_792_378_800_000_000;
const END: i64 = 4_102_444_800_000_000;

struct Bed {
    dir: PathBuf,
    store: Store,
    history: Arc<History>,
}

fn bed() -> Bed {
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = std::env::temp_dir().join(format!(
        "amberlog-history-{}-{}",
        std::process::id(),
        N.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(
        &Location::File { dir: dir.clone() },
        &S3Options::default(),
        "amberlog",
    )
    .unwrap();
    let history = History::new(
        store.clone(),
        Duration::from_secs(30 * 86_400),
        Duration::from_secs(3600),
    );
    Bed {
        dir,
        store,
        history,
    }
}

impl Bed {
    fn file(&self, rel: &str) -> PathBuf {
        self.dir.join("amberlog/_system").join(rel)
    }

    fn put(&self, rel: &str, body: &str) {
        let f = self.file(rel);
        std::fs::create_dir_all(f.parent().unwrap()).unwrap();
        std::fs::write(f, body).unwrap();
    }

    fn get(&self, rel: &str) -> Option<String> {
        std::fs::read_to_string(self.file(rel)).ok()
    }

    async fn rows(
        &self,
        rules: Option<&[&str]>,
        start: i64,
        end: i64,
    ) -> Result<Vec<(String, i64)>, HistoryError> {
        let named: Option<BTreeSet<String>> =
            rules.map(|r| r.iter().map(|s| (*s).to_owned()).collect());
        let mut rows: Vec<(String, i64)> = self
            .history
            .evaluations("default", named.as_ref(), start, end)
            .await?
            .into_iter()
            .map(|r| (r.rule, r.scheduled_at))
            .collect();
        rows.sort();
        Ok(rows)
    }
}

impl Drop for Bed {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn matched(rule: &str, t: i64) -> EvaluationRecord {
    EvaluationRecord {
        org: "default".into(),
        rule: rule.into(),
        scheduled_at: t,
        started_at: Some(t + 10),
        finished_at: t + 20,
        window_start: t - 3 * MINUTE,
        window_end: t,
        outcome: Outcome::Matched,
        matched_rows: Some(2),
        truncated: Some(false),
        delivery: DeliveryResult::Delivered,
        attempts: 1,
        last_http_status: Some(200),
    }
}

fn skipped(rule: &str, t: i64) -> EvaluationRecord {
    EvaluationRecord {
        started_at: None,
        outcome: Outcome::Skipped(SkipReason::PreviousRunning),
        matched_rows: None,
        truncated: None,
        delivery: DeliveryResult::NotAttempted,
        attempts: 0,
        last_http_status: None,
        ..matched(rule, t)
    }
}

fn path(rule: &str, t: i64) -> String {
    ObjectKey::of(rule, t).path()
}

#[test]
fn object_path_follows_template() {
    assert_eq!(path("r1", H + 59 * MINUTE), "alerts/r1/2026/10/19/03.json");
    assert_eq!(
        ObjectKey::parse("alerts/r1/2026/10/19/03.json"),
        Some(ObjectKey::of("r1", H))
    );
    for bad in [
        "alerts/r1/notes.txt",
        "alerts/R1/2026/10/19/03.json",
        "alerts/r1/2026/10/19/3.json",
        "alerts/r1/2026/10/19/24.json",
        "alerts/r1/2026/02/30/03.json",
        "alerts/r1/2026/10/19/03.json.bak",
        "alerts/r1/x/2026/10/19/03.json",
        "other/r1/2026/10/19/03.json",
    ] {
        assert_eq!(ObjectKey::parse(bad), None, "{bad}");
    }
    assert!(ObjectKey::parse("alerts/missed_collection/2026/10/19/03.json").is_some());
}

#[test]
fn object_shape_round_trips_and_nulls() {
    let entries = [
        object::Entry::of(&skipped("r1", H + MINUTE)),
        object::Entry::of(&matched("r1", H)),
    ];
    let merged = object::merge(&[], &entries);
    let bytes = object::encode("default", "r1", &merged);
    let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let keys: BTreeSet<&str> = v.as_object().unwrap().keys().map(String::as_str).collect();
    assert_eq!(
        keys,
        BTreeSet::from(["evaluations", "format_version", "org", "rule"])
    );
    assert_eq!(v["format_version"], 1);
    let e = &v["evaluations"];
    assert_eq!(e[0]["scheduled_at"], H);
    assert_eq!(e[1]["scheduled_at"], H + MINUTE);
    assert_eq!(e[1].as_object().unwrap().len(), 12);
    for k in [
        "started_at",
        "matched_rows",
        "truncated",
        "last_http_status",
    ] {
        assert!(e[1][k].is_null(), "{k}");
    }
    assert_eq!(e[1]["reason"], "previous_running");
    assert!(e[0]["reason"].is_null());
    assert_eq!(object::parse(&bytes).unwrap().evaluations, merged);
}

#[test]
fn parse_rejects_wrong_shapes() {
    let good = r#"{"format_version":1,"org":"default","rule":"r1","evaluations":[]}"#;
    assert!(object::parse(good.as_bytes()).is_ok());
    for bad in [
        "not json",
        r#"{"format_version":2,"org":"default","rule":"r1","evaluations":[]}"#,
        r#"{"format_version":0,"org":"default","rule":"r1","evaluations":[]}"#,
        r#"{"format_version":1.0,"org":"default","rule":"r1","evaluations":[]}"#,
        r#"{"org":"default","rule":"r1","evaluations":[]}"#,
        r#"{"format_version":1,"org":"default","rule":"r1","evaluations":[],"x":1}"#,
        r#"{"format_version":1,"org":"default","rule":"r1","evaluations":[{"scheduled_at":1}]}"#,
        r#"{"format_version":1,"org":1,"rule":"r1","evaluations":[]}"#,
    ] {
        assert!(object::parse(bad.as_bytes()).is_err(), "{bad}");
    }
}

#[test]
fn merge_keeps_existing_on_same_scheduled_at() {
    let old = object::Entry::of(&matched("r1", H));
    let again = object::Entry::of(&skipped("r1", H));
    let later = object::Entry::of(&matched("r1", H + MINUTE));
    let merged = object::merge(std::slice::from_ref(&old), &[later.clone(), again]);
    assert_eq!(merged, vec![old, later]);
}

#[tokio::test]
async fn records_are_written_per_hour_and_appended() {
    let b = bed();
    b.history.record(matched("r1", H - MINUTE));
    b.history.record(matched("r1", H));
    assert_eq!(b.history.unwritten(), 2);
    b.history.flush_once().await;
    assert_eq!(b.history.unwritten(), 0);
    let before = object::parse(b.get(&path("r1", H - MINUTE)).unwrap().as_bytes()).unwrap();
    assert_eq!(before.evaluations.len(), 1);
    assert_eq!(before.evaluations[0].scheduled_at, H - MINUTE);

    b.history.record(matched("r1", H + MINUTE));
    b.history.flush_once().await;
    let now = object::parse(b.get(&path("r1", H)).unwrap().as_bytes()).unwrap();
    let times: Vec<i64> = now.evaluations.iter().map(|e| e.scheduled_at).collect();
    assert_eq!(times, [H, H + MINUTE]);
    assert_eq!((now.org.as_str(), now.rule.as_str()), ("default", "r1"));
}

#[tokio::test]
async fn first_write_resumes_existing_object() {
    let b = bed();
    let earlier = object::encode("default", "r1", &[object::Entry::of(&matched("r1", H))]);
    b.put(&path("r1", H), std::str::from_utf8(&earlier).unwrap());
    b.history.record(matched("r1", H + MINUTE));
    b.history.flush_once().await;
    let doc = object::parse(b.get(&path("r1", H)).unwrap().as_bytes()).unwrap();
    let times: Vec<i64> = doc.evaluations.iter().map(|e| e.scheduled_at).collect();
    assert_eq!(times, [H, H + MINUTE]);
}

#[tokio::test]
async fn unparseable_object_is_never_overwritten() {
    let b = bed();
    let v2 = r#"{"format_version":2,"org":"default","rule":"r1","evaluations":[]}"#;
    b.put(&path("r1", H), v2);
    b.history.record(matched("r1", H));
    b.history.flush_once().await;
    b.history.flush_once().await;
    assert_eq!(b.get(&path("r1", H)).as_deref(), Some(v2));
    assert_eq!(b.history.unwritten(), 1);
}

#[tokio::test]
async fn unparseable_object_seen_by_a_query_is_not_overwritten() {
    let b = bed();
    b.history.record(matched("r1", H));
    b.history.flush_once().await;
    // 本进程写出之后被外部改坏，再被作答读到
    b.put(&path("r1", H), "not json");
    let e = b.rows(Some(&["r1"]), 0, END).await.unwrap_err();
    assert!(matches!(e, HistoryError::Unparseable(_)), "{e:?}");
    b.history.record(matched("r1", H + MINUTE));
    b.history.flush_once().await;
    assert_eq!(b.get(&path("r1", H)).as_deref(), Some("not json"));
    assert_eq!(b.history.unwritten(), 1);
}

#[tokio::test]
async fn write_failure_keeps_records_until_store_returns() {
    let b = bed();
    let moved = b.dir.with_extension("away");
    std::fs::rename(&b.dir, &moved).unwrap();
    b.history.record(matched("r1", H));
    b.history.flush_once().await;
    assert_eq!(b.history.unwritten(), 1);
    std::fs::rename(&moved, &b.dir).unwrap();
    b.history.flush_once().await;
    assert_eq!(b.history.unwritten(), 0);
    assert!(b.get(&path("r1", H)).is_some());
}

#[tokio::test]
async fn rows_come_from_written_objects_only() {
    let b = bed();
    b.history.record(matched("a1", H));
    b.history.record(skipped("b1", H + MINUTE));
    assert_eq!(b.rows(None, 0, END).await.unwrap(), []);
    b.history.flush_once().await;
    assert_eq!(
        b.rows(None, 0, END).await.unwrap(),
        [("a1".to_owned(), H), ("b1".to_owned(), H + MINUTE)]
    );
    // 查询范围作用在计划时刻上
    assert_eq!(
        b.rows(None, H + 1, END).await.unwrap(),
        [("b1".to_owned(), H + MINUTE)]
    );
    // 其他 org 的对象不出现
    let other = object::encode("other", "c1", &[object::Entry::of(&matched("c1", H))]);
    b.put(&path("c1", H), std::str::from_utf8(&other).unwrap());
    assert_eq!(b.rows(None, 0, END).await.unwrap().len(), 2);
    let row = &b
        .history
        .evaluations("default", None, H + 1, END)
        .await
        .unwrap()[0];
    assert_eq!(row.outcome, "skipped");
    assert_eq!(row.reason.as_deref(), Some("previous_running"));
    assert_eq!(row.started_at, None);
}

#[tokio::test]
async fn named_rule_reads_only_its_prefix() {
    let b = bed();
    b.history.record(matched("a1", H));
    b.history.record(matched("b1", H));
    b.history.flush_once().await;
    b.put(&path("b1", H), "not json");
    assert_eq!(
        b.rows(Some(&["a1"]), 0, END).await.unwrap(),
        [("a1".to_owned(), H)]
    );
    assert!(matches!(
        b.rows(None, 0, END).await,
        Err(HistoryError::Unparseable(_))
    ));
    assert_eq!(b.rows(Some(&["no_such"]), 0, END).await.unwrap(), []);
    // 不合规则名的点名不列举任何前缀
    assert_eq!(b.rows(Some(&["../b1"]), 0, END).await.unwrap(), []);
}

#[tokio::test]
async fn objects_outside_the_range_are_not_read() {
    let b = bed();
    b.history.record(matched("r1", H));
    b.history.flush_once().await;
    b.put(&path("r1", H - 2 * HOUR), "not json");
    assert_eq!(
        b.rows(Some(&["r1"]), H, END).await.unwrap(),
        [("r1".to_owned(), H)]
    );
}

#[tokio::test]
async fn unavailable_store_fails_the_read() {
    let b = bed();
    b.history.record(matched("r1", H));
    b.history.flush_once().await;
    let moved = b.dir.with_extension("gone");
    std::fs::rename(&b.dir, &moved).unwrap();
    let r = b.rows(None, 0, END).await;
    std::fs::rename(&moved, &b.dir).unwrap();
    assert!(matches!(r, Err(HistoryError::Unavailable(_))), "{r:?}");
}

#[tokio::test]
async fn retention_deletes_only_expired_template_objects() {
    let b = bed();
    let now = amberlog_storage::lifecycle::now_us();
    let doc = |rule: &str, t: i64| {
        String::from_utf8(object::encode(
            "default",
            rule,
            &[object::Entry::of(&matched(rule, t))],
        ))
        .unwrap()
    };
    let old = now - 3 * HOUR;
    let recent = now - 30 * MINUTE;
    b.put(&path("old_rule", old), &doc("old_rule", old));
    b.put(&path("r1", old), &doc("r1", old));
    b.put(&path("r1", recent), &doc("r1", recent));
    b.put("alerts/r1/notes.txt", "keep");
    b.put("alerts/r1/2026/10/19/03.txt", "keep");
    retention::sweep(&b.store, Duration::from_secs(3600)).await;
    assert_eq!(b.get(&path("old_rule", old)), None);
    assert_eq!(b.get(&path("r1", old)), None);
    assert!(b.get(&path("r1", recent)).is_some());
    assert_eq!(b.get("alerts/r1/notes.txt").as_deref(), Some("keep"));
    assert_eq!(
        b.get("alerts/r1/2026/10/19/03.txt").as_deref(),
        Some("keep")
    );
}

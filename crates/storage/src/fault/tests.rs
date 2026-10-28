//! 可放行暂停点三种模式的单元测试（控制文件直接交给 `gate_at`，不经环境变量）。

use std::path::{Path, PathBuf};
use std::time::Duration;

use super::gate_at;

fn control(name: &str, content: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("amberlog-fault-gate-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("control");
    std::fs::write(&file, content).unwrap();
    let _ = std::fs::remove_file(dir.join("control.held"));
    file
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_default()
}

fn held_count(control: &Path) -> String {
    let mut held = control.as_os_str().to_owned();
    held.push(".held");
    read(Path::new(&held))
}

async fn until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !f() {
        assert!(tokio::time::Instant::now() < deadline, "等不到：{what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn spawn_gate(control: &Path) -> tokio::task::JoinHandle<()> {
    let control = control.to_owned();
    tokio::spawn(async move { gate_at(&control).await })
}

#[tokio::test]
async fn hold_stops_only_the_first_call() {
    let c = control("hold", "hold");
    let first = spawn_gate(&c);
    until("held", || read(&c) == "held").await;
    // 其后的调用不停
    tokio::time::timeout(Duration::from_secs(1), gate_at(&c))
        .await
        .unwrap();
    assert!(!first.is_finished());
    std::fs::write(&c, "go").unwrap();
    first.await.unwrap();
}

#[tokio::test]
async fn hold_all_counts_held_calls_and_cancelled_ones_leave() {
    let c = control("hold-all", "hold-all");
    let calls: Vec<_> = (0..3).map(|_| spawn_gate(&c)).collect();
    until("3 个停住", || held_count(&c) == "3").await;
    assert_eq!(read(&c), "hold-all");
    // 被取消（所在任务被丢弃）的调用减一
    calls[0].abort();
    until("取消后剩 2 个", || held_count(&c) == "2").await;
    std::fs::write(&c, "go").unwrap();
    for call in calls.into_iter().skip(1) {
        call.await.unwrap();
    }
    assert_eq!(held_count(&c), "0");
}

#[tokio::test]
async fn hold_each_stops_every_call_in_turn() {
    let c = control("hold-each", "hold-each");
    let calls: Vec<_> = (0..3).map(|_| spawn_gate(&c)).collect();
    for released in 0..3 {
        until("下一个停住", || read(&c) == "held").await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        // 一次只停一个：此前放行的都已返回，其余仍在等
        let done = calls.iter().filter(|h| h.is_finished()).count();
        assert_eq!(done, released);
        std::fs::write(&c, "go").unwrap();
        until("改回 hold-each 或下一个已停住", || {
            read(&c) != "go"
        })
        .await;
    }
    for call in calls {
        tokio::time::timeout(Duration::from_secs(5), call)
            .await
            .unwrap()
            .unwrap();
    }
    assert_eq!(read(&c), "hold-each");
    // 写入别的内容即结束逐次停住
    std::fs::write(&c, "off").unwrap();
    tokio::time::timeout(Duration::from_secs(1), gate_at(&c))
        .await
        .unwrap();
}

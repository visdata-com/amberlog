//! ING-ACK-RESPONSE「回了 503 的批次此后与重启后都不可见」「503 后整批重试只留下一份」的进程内覆盖：
//! 真实 WAL 经 `TESTBED_FAULT_WAL` 注入写入失败（本文件只有一个测试，环境变量不与其他测试相互干扰）。

mod common;

use amberlog_ingest::pipeline::Rejected;
use common::{NOW_US, Process, TempDir, ds};

#[tokio::test]
async fn a_503_batch_never_becomes_visible_and_a_retry_counts_once() {
    let dir = TempDir::new("ack503");
    let control = dir.0.join("fault");
    // SAFETY: 本测试二进制只有这一个测试，设置环境变量时没有其他线程读它。
    unsafe { std::env::set_var("TESTBED_FAULT_WAL", &control) };
    let wal_dir = dir.0.join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    {
        let p = Process::start(&wal_dir).await;
        p.pipeline
            .ingest(&ds("nv"), br#"[{"k":"base"}]"#, NOW_US)
            .await
            .unwrap();
        for op in ["write", "fsync"] {
            std::fs::write(&control, op).unwrap();
            assert!(matches!(
                p.pipeline
                    .ingest(&ds("nv"), br#"[{"k":"lost"}]"#, NOW_US)
                    .await
                    .unwrap_err(),
                Rejected::Unavailable(_)
            ));
            std::fs::remove_file(&control).unwrap();
            assert_eq!(p.strings(&ds("nv"), "k"), ["base"]);
        }
        // 撤除注入后原样重试
        std::fs::write(&control, "write").unwrap();
        let retry = br#"[{"k":"r1"},{"k":"r2"}]"#;
        assert!(p.pipeline.ingest(&ds("nv"), retry, NOW_US).await.is_err());
        std::fs::remove_file(&control).unwrap();
        p.pipeline.ingest(&ds("nv"), retry, NOW_US).await.unwrap();
        assert_eq!(p.strings(&ds("nv"), "k"), ["base", "r1", "r2"]);
    }
    let p = Process::start(&wal_dir).await;
    assert_eq!(p.strings(&ds("nv"), "k"), ["base", "r1", "r2"]);
}

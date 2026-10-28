//! ING-ACK-RESPONSE「200 之后强杀进程记录仍在」与 LIFE-DURABILITY「强杀后已确认记录零丢失」「就绪即意味着重放已完成」
//! 的进程内覆盖：真实 WAL 与可见集，「强杀」= 不做任何收尾就丢掉进程内状态。黑盒验收见 amberlog-server 的
//! tests/ing_ack_response.rs 与 tests/life_durability.rs。

mod common;

use common::{NOW_US, Process, TempDir, ds};

#[tokio::test]
async fn acknowledged_records_are_back_exactly_once_after_an_abrupt_stop() {
    let dir = TempDir::new("durability");
    {
        let p = Process::start(&dir.0).await;
        for chunk in 0..10 {
            let body: Vec<String> = (0..100)
                .map(|i| format!(r#"{{"seq":"{}"}}"#, chunk * 100 + i))
                .collect();
            let status = p
                .pipeline
                .ingest(
                    &ds("app"),
                    format!("[{}]", body.join(",")).as_bytes(),
                    NOW_US,
                )
                .await
                .unwrap();
            assert_eq!(status.successful, 100);
        }
        p.pipeline
            .ingest(&ds("app"), br#"[{"k":"survive"}]"#, NOW_US)
            .await
            .unwrap();
        assert_eq!(p.strings(&ds("app"), "seq").len(), 1000);
    }
    let p = Process::start(&dir.0).await;
    let mut seqs = p.strings(&ds("app"), "seq");
    assert_eq!(seqs.len(), 1000);
    seqs.sort();
    seqs.dedup();
    assert_eq!(seqs.len(), 1000, "seq 两两不同");
    assert_eq!(p.strings(&ds("app"), "k"), ["survive"]);
    // 再「强杀」一次：重放不产生重复
    drop(p);
    let p = Process::start(&dir.0).await;
    assert_eq!(p.strings(&ds("app"), "seq").len(), 1000);
}

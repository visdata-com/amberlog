//! REQ-AMB-STO-AUDIT-INVARIANTS 中直接调用离线查验的场景（`amberlog-storage` 集成测试），另有提交产物的摘要与单链的
//! 存储层核对（不对应块场景）。

mod common;

use amberlog_storage::audit::verify_data_files;
use amberlog_storage::chain::{Digest, SnapshotId};
use amberlog_storage::commit::Existence;

use common::*;

/// 经多次提交、含三个小时桶与多个数据文件的 `app`，返回提交者。
async fn populated(b: &Backend) -> Committer {
    let c = committer(&b.store, 8);
    assert_eq!(
        c.committer.ensure_dataset(&app()).await,
        Existence::Committed
    );
    for i in 0..4 {
        c.committer
            .commit(
                &app(),
                &request(data_files(
                    &[H10 + i, H10 + HOUR + i, H10 + 2 * HOUR + i],
                    i,
                )),
            )
            .await
            .unwrap();
    }
    c
}

#[tokio::test]
async fn audit_verifier_clean_when_untampered() {
    let b = file_backend();
    populated(&b).await;
    let s = snapshot(&b.store, &app(), current(&b.store, &app()).await.unwrap()).await;
    assert_eq!(s.buckets.len(), 3);
    assert_eq!(files(&b.store, &app(), &s).await.len(), 12);
    assert_eq!(verify_data_files(&b.store, &app()).await, Ok(Vec::new()));
}

#[tokio::test]
async fn audit_tamper_detected() {
    let b = file_backend();
    populated(&b).await;
    let s = snapshot(&b.store, &app(), current(&b.store, &app()).await.unwrap()).await;
    let f = manifests(&b.store, &app(), &s).await[1].files[2].clone();
    // 以内容不同的另一个合法数据文件替换 F
    let other = data_files(&[H10 + HOUR + 99], 99).remove(0);
    let on_disk = b
        .dir
        .as_ref()
        .unwrap()
        .0
        .join("amberlog/default/app")
        .join(&f.path);
    std::fs::write(&on_disk, &other.bytes).unwrap();

    let report = verify_data_files(&b.store, &app()).await.unwrap();
    assert_eq!(report.len(), 1, "{report:?}");
    assert_eq!(report[0].path, f.path);
    assert_eq!(report[0].recorded, f.content_digest);
    assert_eq!(report[0].actual, Some(Digest::of(&other.bytes)));
}

/// 同一文件的条目在该桶先后各版桶 manifest 中不变；快照单链，`_current` 只前进。
#[tokio::test]
async fn storage_file_entries_stable_and_chain_linear() {
    let b = file_backend();
    populated(&b).await;
    let top = current(&b.store, &app()).await.unwrap();
    assert_eq!(top, SnapshotId(5));
    let mut seen = std::collections::HashMap::new();
    let mut id = top;
    loop {
        let s = snapshot(&b.store, &app(), id).await;
        for m in manifests(&b.store, &app(), &s).await {
            assert!(m.deletions.is_empty());
            for f in m.files {
                let prev = seen.insert(f.path.clone(), (f.content_digest, f.added_in));
                if let Some(prev) = prev {
                    assert_eq!(prev, (f.content_digest, f.added_in), "{}", f.path);
                }
            }
        }
        match s.parent_snapshot_id {
            None => {
                assert_eq!(id, SnapshotId(1));
                break;
            }
            Some(p) => {
                assert_eq!(p, SnapshotId(id.0 - 1));
                id = p;
            }
        }
    }
    assert_eq!(seen.len(), 12);
}

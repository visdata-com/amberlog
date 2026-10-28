//! 在用快照登记随 [`StoreChain`] 的两种挂法：找链尾前登记下界、取缓存最新快照即登记确切编号。
//! 不对应块场景（REQ-AMB-STO-SNAPSHOT-GC-IN-USE 的场景由 amberlog-server 的系统测试端到端验证）；只用 `file://` 后端。

mod common;

use amberlog_storage::chain::{ChainReader, SnapshotId};

use common::*;

const H10: i64 = 1_700_002_800_000_000;

#[tokio::test]
async fn chain_registers_snapshots_in_use() {
    let b = file_backend();
    let d = app();
    let c = committer(&b.store, 8);
    c.committer.ensure_dataset(&d).await;
    for n in 0..3 {
        c.committer
            .commit(&d, &request(data_files(&[H10 + n], n)))
            .await
            .unwrap();
    }
    let chain = &c.chain;
    let table = chain.in_use();
    assert_eq!(table.oldest(&d), None);

    // 找链尾之前：下界取本进程已知的最大编号；定下后改为确切编号
    let tail = chain.current(&d).await.unwrap().unwrap();
    let mut pin = chain.reading(&d);
    assert_eq!(table.oldest(&d), Some(tail));
    pin.settle(SnapshotId(tail.0 - 1));
    assert_eq!(table.oldest(&d), Some(SnapshotId(tail.0 - 1)));

    // 缓存中的最新快照：登记其编号
    let (latest, held) = chain.pin_latest(&d).unwrap();
    assert_eq!(latest.snapshot_id, tail);
    assert_eq!(table.oldest(&d), Some(SnapshotId(tail.0 - 1)));
    drop(pin);
    assert_eq!(table.oldest(&d), Some(tail));
    drop(held);
    assert_eq!(table.oldest(&d), None);
}

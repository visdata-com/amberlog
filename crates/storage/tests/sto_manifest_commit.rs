//! REQ-AMB-STO-MANIFEST-COMMIT 中直接对两类对象存储后端验证的场景（`amberlog-storage` 集成测试）：条件写语义、
//! 引用不变量、并发提交的串行化。另有装载时找链尾等崩溃恢复的存储层核对（不对应块场景；端到端的崩溃场景在
//! amberlog-server 的 tests/sto_manifest_commit.rs）。
//!
//! 每个场景在 `file://` 临时目录与 MinIO/Silo 台架（`tools/testbed.sh <标签> up`）上各跑一遍。

mod common;

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use amberlog_storage::chain::{ChainReader, SnapshotId};
use amberlog_storage::commit::Existence;
use amberlog_storage::format;
use amberlog_storage::layout;
use amberlog_storage::objects::{Created, Swapped};
use bytes::Bytes;
use object_store::ObjectStoreExt;

use common::*;

// ---------- 条件写 ----------

#[tokio::test]
async fn commit_conditional_write_semantics() {
    for b in backends() {
        let s = &b.store;
        let d = app();
        let snap = s.object_path(&d, &layout::snapshot_path(SnapshotId(1)));
        assert_eq!(
            s.create(&snap, Bytes::from_static(b"first")).await.unwrap(),
            Created::Done,
            "{}",
            b.name
        );
        assert_eq!(
            s.create(&snap, Bytes::from_static(b"second"))
                .await
                .unwrap(),
            Created::AlreadyExists,
            "{}: 第二次创建应失败",
            b.name
        );
        assert_eq!(
            s.read(&snap).await.unwrap().unwrap().bytes,
            Bytes::from_static(b"first"),
            "{}: 内容仍是第一次写入的字节",
            b.name
        );

        let cur = s.object_path(&d, layout::CURRENT);
        assert_eq!(
            s.swap(&cur, None, Bytes::from_static(b"v1")).await.unwrap(),
            Swapped::Done
        );
        assert_eq!(
            s.swap(&cur, None, Bytes::from_static(b"v1b"))
                .await
                .unwrap(),
            Swapped::Conflict,
            "{}: 期望尚不存在而已存在",
            b.name
        );
        let v1 = s.read(&cur).await.unwrap().unwrap().version;
        assert_eq!(
            s.swap(&cur, Some(&v1), Bytes::from_static(b"v2"))
                .await
                .unwrap(),
            Swapped::Done,
            "{}: 以正确期望版本替换",
            b.name
        );
        assert_eq!(
            s.swap(&cur, Some(&v1), Bytes::from_static(b"v3"))
                .await
                .unwrap(),
            Swapped::Conflict,
            "{}: 以过期期望版本替换应失败",
            b.name
        );
        assert_eq!(
            s.read(&cur).await.unwrap().unwrap().bytes,
            Bytes::from_static(b"v2"),
            "{}: _current 不变",
            b.name
        );
    }
}

/// `file://` 所指目录不存在属对象存储不可访问：读、列举与写都如此，写入不把它建出来。
#[tokio::test]
async fn storage_missing_file_root_is_unavailable() {
    let b = file_backend();
    let dir = b.dir.as_ref().unwrap().0.clone();
    std::fs::remove_dir_all(&dir).unwrap();
    let s = &b.store;
    let p = s.object_path(&app(), layout::CURRENT);
    assert!(s.read(&p).await.is_err());
    assert!(s.list(&s.root).await.is_err());
    assert!(s.create(&p, Bytes::from_static(b"x")).await.is_err());
    assert!(s.swap(&p, None, Bytes::from_static(b"x")).await.is_err());
    assert!(!dir.exists());
}

// ---------- 写入次序 ----------

/// 提交者 20 秒内持续提交（每次含新的数据文件，轮流落在三个小时桶），观察者同时反复沿 `_current` 读到底。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_reference_invariant_under_load() {
    let runs = backends().into_iter().map(|b| async move {
        // 整个移入（只用到的字段才被捕获的话，临时目录会随闭包返回而删掉）
        let b = b;
        let d = app();
        let c = committer(&b.store, 8);
        assert_eq!(c.committer.ensure_dataset(&d).await, Existence::Committed);
        let deadline = Instant::now() + Duration::from_secs(20);
        let store = b.store.clone();
        let observer = tokio::spawn(async move {
            let mut rounds = 0;
            while Instant::now() < deadline {
                let n = current(&store, &app()).await.expect("_current 已存在");
                let s = snapshot(&store, &app(), n).await;
                for b in &s.buckets {
                    assert!(
                        exists(&store, &app(), &b.manifest).await,
                        "快照 {n} 引用的桶 manifest {} 不存在",
                        b.manifest
                    );
                }
                for path in files(&store, &app(), &s).await {
                    assert!(
                        exists(&store, &app(), &path).await,
                        "快照 {n} 可达的数据文件 {path} 不存在"
                    );
                }
                rounds += 1;
            }
            rounds
        });
        let mut commits = 0;
        while Instant::now() < deadline {
            let hour = H10 + (commits % 3) * HOUR;
            c.committer
                .commit(&d, &request(data_files(&[hour + commits], commits)))
                .await
                .unwrap();
            commits += 1;
        }
        let rounds = observer.await.unwrap();
        assert!(
            commits >= 3 && rounds >= 3,
            "{}: 提交 {commits} 次、观察 {rounds} 轮",
            b.name
        );
    });
    futures::future::join_all(runs).await;
}

// ---------- 冲突与重试 ----------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn commit_concurrent_cas() {
    for b in backends() {
        let d = app();
        let setup = committer(&b.store, 8);
        setup.committer.ensure_dataset(&d).await;
        let n = setup
            .committer
            .commit(&d, &request(data_files(&[H10], 0)))
            .await
            .unwrap();

        // 两个提交者都基于快照 N 装载，各自准备一个新数据文件，同时提交
        let x = committer(&b.store, 8);
        let y = committer(&b.store, 8);
        x.committer.load().await.unwrap();
        y.committer.load().await.unwrap();
        let fx = data_files(&[H10 + 1], 1);
        let fy = data_files(&[H10 + 2], 2);
        let (px, py) = (fx[0].path.clone(), fy[0].path.clone());
        let (qx, qy) = (request(fx), request(fy));
        let (rx, ry) = tokio::join!(x.committer.commit(&d, &qx), y.committer.commit(&d, &qy));
        rx.unwrap();
        ry.unwrap();

        let top = current(&b.store, &d).await.unwrap();
        assert_eq!(top, SnapshotId(n.0 + 2), "{}", b.name);
        let all = files(&b.store, &d, &snapshot(&b.store, &d, top).await).await;
        assert!(
            all.contains(&px) && all.contains(&py),
            "{}: {all:?}",
            b.name
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn commit_concurrent_many_linear() {
    for b in backends() {
        let d = app();
        let setup = committer(&b.store, 64);
        setup.committer.ensure_dataset(&d).await;
        let start = current(&b.store, &d).await.unwrap();

        let mut tasks = Vec::new();
        for w in 0..4i64 {
            let c = committer(&b.store, 64);
            c.committer.load().await.unwrap();
            tasks.push(tokio::spawn(async move {
                let mut mine = Vec::new();
                for i in 0..10i64 {
                    let f = data_files(&[H10 + w * HOUR + i], w * 100 + i);
                    mine.push(f[0].path.clone());
                    c.committer.commit(&app(), &request(f)).await.unwrap();
                }
                mine
            }));
        }
        let mut expected = BTreeSet::new();
        for t in tasks {
            expected.extend(t.await.unwrap());
        }

        let top = current(&b.store, &d).await.unwrap();
        assert_eq!(top, SnapshotId(start.0 + 40), "{}", b.name);
        let mut id = top;
        while id > start {
            let s = snapshot(&b.store, &d, id).await;
            assert_eq!(
                s.parent_snapshot_id,
                Some(SnapshotId(id.0 - 1)),
                "{}: 快照 {id} 的父",
                b.name
            );
            id = SnapshotId(id.0 - 1);
        }
        let reachable: BTreeSet<String> = files(&b.store, &d, &snapshot(&b.store, &d, top).await)
            .await
            .into_iter()
            .collect();
        assert_eq!(reachable, expected, "{}", b.name);
    }
}

// ---------- 崩溃后的恢复（存储层核对，不对应块场景） ----------

/// 写出快照 N+1 之后、更新提示之前崩溃（提示停在 N）：换一个进程（空的 WAL）装载时找链尾，以快照 N+1 作基线。
#[tokio::test]
async fn recovery_hint_lagging_tail_snapshot_is_baseline() {
    for b in backends() {
        let d = app();
        let x = committer(&b.store, 8);
        x.committer.ensure_dataset(&d).await;
        let n = x
            .committer
            .commit(&d, &request(data_files(&[H10], 1)))
            .await
            .unwrap();
        let tail = x
            .committer
            .commit(&d, &request(data_files(&[H10 + HOUR], 2)))
            .await
            .unwrap();
        // 把提示退回 N：等同于快照 N+1 已写出而提示还没更新
        put_current(&b.store, n).await;

        let y = committer(&b.store, 8);
        let baselines = y.committer.load().await.unwrap();
        assert_eq!(baselines.len(), 1, "{}", b.name);
        assert_eq!(
            baselines[0].snapshot_id,
            Some(tail),
            "{}: 链尾快照取作基线",
            b.name
        );
        assert!(baselines[0].columns.as_ref().unwrap().get("n").is_some());
        assert_eq!(
            current(&b.store, &d).await,
            Some(tail),
            "{}: 当前快照是链尾",
            b.name
        );

        // 此后的提交接在链尾之后
        let next = y
            .committer
            .commit(&d, &request(data_files(&[H10 + 2 * HOUR], 3)))
            .await
            .unwrap();
        assert_eq!(next, SnapshotId(tail.0 + 1), "{}", b.name);
        assert_eq!(
            snapshot(&b.store, &d, next).await.parent_snapshot_id,
            Some(tail)
        );
    }
}

/// 创世快照写出之后、写出提示之前崩溃：数据集按「在对象存储中存在」被找到，装载时找链尾得到快照 1。
#[tokio::test]
async fn recovery_genesis_without_hint_found_at_load() {
    for b in backends() {
        let d = app();
        let x = committer(&b.store, 8);
        x.committer.ensure_dataset(&d).await;
        b.store
            .inner
            .delete(&b.store.object_path(&d, layout::CURRENT))
            .await
            .unwrap();

        let y = committer(&b.store, 8);
        let baselines = y.committer.load().await.unwrap();
        assert_eq!(
            baselines.len(),
            1,
            "{}: 只有快照 1 的数据集也算存在",
            b.name
        );
        assert_eq!(baselines[0].snapshot_id, Some(SnapshotId(1)));
        assert_eq!(
            current(&b.store, &d).await,
            Some(SnapshotId(1)),
            "{}",
            b.name
        );
    }
}

/// 数据文件已上传、快照未写出时崩溃：当前快照不变，此后的提交不引用那个孤儿文件。
#[tokio::test]
async fn recovery_uploaded_data_without_snapshot_ignored() {
    for b in backends() {
        let d = app();
        let x = committer(&b.store, 8);
        x.committer.ensure_dataset(&d).await;
        let n = x
            .committer
            .commit(&d, &request(data_files(&[H10], 1)))
            .await
            .unwrap();
        let stray = data_files(&[H10 + 5], 9).remove(0);
        b.store
            .create(&b.store.object_path(&d, &stray.path), stray.bytes.clone())
            .await
            .unwrap();

        let y = committer(&b.store, 8);
        y.committer.load().await.unwrap();
        assert_eq!(current(&b.store, &d).await, Some(n), "{}", b.name);
        let next = y
            .committer
            .commit(&d, &request(data_files(&[H10 + 6], 2)))
            .await
            .unwrap();
        let reachable = files(&b.store, &d, &snapshot(&b.store, &d, next).await).await;
        assert!(!reachable.contains(&stray.path), "{}", b.name);
        assert_eq!(reachable.len(), 2, "{}", b.name);
    }
}

// ---------- 快照链缓存（存储层核对，不对应块场景） ----------

/// 同一批桶反复提交：缓存只留最新快照及其引用的桶 manifest，不随提交次数增长（每次提交都写出受影响桶的
/// 新版 manifest，旧版留在缓存里会使内存随提交次数平方增长）；旧快照与旧版 manifest 照样可读。
#[tokio::test]
async fn chain_cache_keeps_only_latest_snapshot() {
    for b in backends() {
        let d = app();
        let x = committer(&b.store, 8);
        x.committer.ensure_dataset(&d).await;
        let first = x
            .committer
            .commit(&d, &request(data_files(&[H10], 0)))
            .await
            .unwrap();
        for i in 1..5 {
            x.committer
                .commit(&d, &request(data_files(&[H10, H10 + HOUR], i)))
                .await
                .unwrap();
        }
        assert_eq!(x.chain.cached(&d), (1, 2), "{}", b.name);

        let old = x.chain.snapshot(&d, first).await.unwrap();
        let m = x
            .chain
            .bucket_manifest(&d, &old, &old.buckets[0])
            .await
            .unwrap();
        assert_eq!(m.files.len(), 1, "{}", b.name);
        assert_eq!(x.chain.cached(&d), (1, 2), "{}: 读旧快照不进缓存", b.name);
    }
}

async fn put_current(store: &amberlog_storage::store::Store, id: SnapshotId) {
    use object_store::PutPayload;
    store
        .inner
        .put(
            &store.object_path(&app(), layout::CURRENT),
            PutPayload::from(format::encode_current(id)),
        )
        .await
        .unwrap();
}

//! REQ-AMB-STO-HOT-LAYOUT-V4 在存储层的核对：两种停用状态的判定与粘滞、停用后不写出新对象、外部删改只影响出事的
//! 数据集、提交产物的布局。不对应块场景——块场景是经 `_json`、`_search` 的端到端场景，在 amberlog-server 的
//! tests/sto_hot_layout.rs。只用 `file://` 后端，测试直接改写目录下的对象。

mod common;

use amberlog_common::dataset::Dataset;
use amberlog_storage::chain::{ChainError, ChainReader, Digest, SnapshotId};
use amberlog_storage::commit::{CommitIntent, Existence, IntentOutcome};
use amberlog_storage::committer::{CommitError, CompactionRequest};
use amberlog_storage::gate::DatasetState;
use amberlog_storage::layout;
use amberlog_storage::registry::Persistence;
use amberlog_storage::wal::{Release, WalPosition};
use serde_json::Value;

use common::*;

fn path(b: &Backend, d: &Dataset, rel: &str) -> std::path::PathBuf {
    b.dir
        .as_ref()
        .unwrap()
        .0
        .join("amberlog")
        .join(&d.org)
        .join(&d.stream)
        .join(rel)
}

fn read_json(b: &Backend, d: &Dataset, rel: &str) -> Value {
    serde_json::from_slice(&std::fs::read(path(b, d, rel)).unwrap()).unwrap()
}

fn write(b: &Backend, d: &Dataset, rel: &str, bytes: &[u8]) {
    let p = path(b, d, rel);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(p, bytes).unwrap();
}

/// 建出 `app` 的两个快照（创世 + 一次含数据的提交），返回提交者与链头快照号。
async fn seeded(b: &Backend) -> (Committer, SnapshotId) {
    let c = committer(&b.store, 2);
    assert_eq!(
        c.committer.ensure_dataset(&app()).await,
        Existence::Committed
    );
    let n = c
        .committer
        .commit(&app(), &request(data_files(&[H10, H10 + HOUR], 1)))
        .await
        .unwrap();
    (c, n)
}

fn objects_under(b: &Backend, d: &Dataset, rel: &str) -> usize {
    fn walk(p: &std::path::Path) -> usize {
        match std::fs::read_dir(p) {
            Err(_) => 0,
            Ok(rd) => rd
                .map(|e| {
                    let e = e.unwrap();
                    if e.file_type().unwrap().is_dir() {
                        walk(&e.path())
                    } else {
                        1
                    }
                })
                .sum(),
        }
    }
    walk(&path(b, d, rel))
}

#[tokio::test]
async fn storage_commit_layout_and_summary() {
    let b = file_backend();
    let (_, n) = seeded(&b).await;
    assert_eq!(n, SnapshotId(2));
    let genesis = read_json(&b, &app(), "manifest/00000000000000000001.json");
    assert_eq!(genesis["parent_snapshot_id"], Value::Null);
    assert_eq!(genesis["buckets"], serde_json::json!([]));
    assert_eq!(
        genesis["schema"],
        serde_json::json!([{"name":"_timestamp","type":"Int64","field_id":1}])
    );
    assert_eq!(
        read_json(&b, &app(), "manifest/_current"),
        serde_json::json!({"snapshot_id": 2})
    );
    let s = read_json(&b, &app(), "manifest/00000000000000000002.json");
    assert_eq!(s["parent_snapshot_id"], 1);
    assert_eq!(s["summary"]["dataset"], "default/app");
    assert_eq!(s["summary"]["record_count"], 2);
    assert_eq!(s["summary"]["time_min"], H10);
    assert_eq!(s["summary"]["time_max"], H10 + HOUR);
    let buckets: Vec<&str> = s["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["bucket"].as_str().unwrap())
        .collect();
    assert_eq!(buckets, ["2026082210", "2026082211"]);
    assert_eq!(
        s["schema"][1],
        serde_json::json!({"name":"n","type":"Int64","field_id":2})
    );
    for x in s["buckets"].as_array().unwrap() {
        let rel = x["manifest"].as_str().unwrap();
        let bytes = std::fs::read(path(&b, &app(), rel)).unwrap();
        assert_eq!(Digest::of(&bytes).to_string(), x["content_digest"]);
        let m: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(m["deletions"], serde_json::json!([]));
        let f = &m["files"][0];
        assert_eq!(f["added_in"], 2);
        let data = std::fs::read(path(&b, &app(), f["path"].as_str().unwrap())).unwrap();
        assert_eq!(f["size_bytes"], data.len());
        assert_eq!(Digest::of(&data).to_string(), f["content_digest"]);
    }
    // 只写 data/ 与 manifest/
    let top: Vec<String> = std::fs::read_dir(path(&b, &app(), ""))
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    let mut top = top;
    top.sort();
    assert_eq!(top, ["data", "manifest"]);
}

/// 只重写受影响桶的 manifest，其余原样引用。
#[tokio::test]
async fn storage_commit_rewrites_affected_buckets_only() {
    let b = file_backend();
    let (c, n) = seeded(&b).await;
    let before = read_json(&b, &app(), &layout::snapshot_path(n));
    let m = c
        .committer
        .commit(&app(), &request(data_files(&[H10 + 2 * HOUR], 2)))
        .await
        .unwrap();
    let after = read_json(&b, &app(), &layout::snapshot_path(m));
    assert_eq!(after["buckets"][0], before["buckets"][0]);
    assert_eq!(after["buckets"][1], before["buckets"][1]);
    assert_eq!(after["buckets"][2]["bucket"], "2026082212");
}

#[tokio::test]
async fn storage_format_version_too_new_gates_dataset_only() {
    let b = file_backend();
    let (_, n) = seeded(&b).await;
    let web = Dataset::new("default", "web");
    let w = committer(&b.store, 2);
    w.committer.ensure_dataset(&web).await;
    // 链尾之后写出格式版本为 3 的快照 N+1（提示仍为 N，找链尾时探测到它）
    let mut v = read_json(&b, &app(), &layout::snapshot_path(n));
    v["format_version"] = 3.into();
    v["snapshot_id"] = (n.0 + 1).into();
    v["parent_snapshot_id"] = n.0.into();
    write(
        &b,
        &app(),
        &layout::snapshot_path(SnapshotId(n.0 + 1)),
        &serde_json::to_vec(&v).unwrap(),
    );

    let c = committer(&b.store, 2);
    c.committer.load().await.unwrap();
    assert_eq!(c.gates.state(&app()), DatasetState::IncompatibleFormat);
    assert_eq!(c.gates.state(&web), DatasetState::Normal);
    let data_before = objects_under(&b, &app(), "data");
    assert_eq!(
        c.committer
            .commit(&app(), &request(data_files(&[H10], 3)))
            .await,
        Err(CommitError::Disabled(DatasetState::IncompatibleFormat))
    );
    assert_eq!(
        objects_under(&b, &app(), "data"),
        data_before,
        "停用后不再上传数据文件"
    );
    assert!(!path(&b, &app(), &layout::snapshot_path(SnapshotId(n.0 + 2))).exists());
    // 其他数据集照常提交
    assert!(
        c.committer
            .commit(&web, &request(data_files(&[H10], 4)))
            .await
            .is_ok()
    );
    // 查询侧：不以此刻对象存储的状态为条件
    std::fs::remove_dir_all(b.dir.as_ref().unwrap().0.join("amberlog")).unwrap();
    assert_eq!(
        c.chain.current(&app()).await,
        Err(ChainError::Disabled(DatasetState::IncompatibleFormat))
    );
}

#[tokio::test]
async fn storage_manifest_unreadable_variants() {
    // (a) 提示内容无法解析
    let b = file_backend();
    seeded(&b).await;
    write(&b, &app(), layout::CURRENT, b"not json");
    let c = committer(&b.store, 2);
    c.committer.load().await.unwrap();
    assert_eq!(c.gates.state(&app()), DatasetState::ManifestUnreadable);

    // (a) 提示所指快照不存在；修复对象后仍停用，换进程（重启）后恢复
    let b = file_backend();
    let (_, n) = seeded(&b).await;
    let snap = path(&b, &app(), &layout::snapshot_path(n));
    let saved = std::fs::read(&snap).unwrap();
    std::fs::remove_file(&snap).unwrap();
    let c = committer(&b.store, 2);
    c.committer.load().await.unwrap();
    assert_eq!(c.gates.state(&app()), DatasetState::ManifestUnreadable);
    std::fs::write(&snap, saved).unwrap();
    assert!(matches!(
        c.chain.current(&app()).await,
        Err(ChainError::Disabled(DatasetState::ManifestUnreadable))
    ));
    let restarted = committer(&b.store, 2);
    restarted.committer.load().await.unwrap();
    assert_eq!(restarted.gates.state(&app()), DatasetState::Normal);

    // (a) 链尾快照内容无法解析
    let b = file_backend();
    let (_, n) = seeded(&b).await;
    write(
        &b,
        &app(),
        &layout::snapshot_path(SnapshotId(n.0 + 1)),
        b"{}",
    );
    let c = committer(&b.store, 2);
    c.committer.load().await.unwrap();
    assert_eq!(c.gates.state(&app()), DatasetState::ManifestUnreadable);

    // (b) 提示不存在而链尾大于 1
    let b = file_backend();
    seeded(&b).await;
    std::fs::remove_file(path(&b, &app(), layout::CURRENT)).unwrap();
    let c = committer(&b.store, 2);
    c.committer.load().await.unwrap();
    assert_eq!(c.gates.state(&app()), DatasetState::ManifestUnreadable);

    // (c) 找到的链尾小于本进程已知的最大编号：提示退回 N-1、快照 N 被删
    let b = file_backend();
    let (c, n) = seeded(&b).await;
    assert_eq!(c.chain.current(&app()).await, Ok(Some(n)));
    write(
        &b,
        &app(),
        layout::CURRENT,
        format!(r#"{{"snapshot_id":{}}}"#, n.0 - 1).as_bytes(),
    );
    std::fs::remove_file(path(&b, &app(), &layout::snapshot_path(n))).unwrap();
    assert_eq!(
        c.chain.current(&app()).await,
        Err(ChainError::Disabled(DatasetState::ManifestUnreadable))
    );
}

/// 提示落后于链尾：创建新快照之前先把提示补到链尾，提交写在链尾之后；成功后提示为新快照编号。
#[tokio::test]
async fn storage_lagging_hint_backfilled_before_create() {
    let b = file_backend();
    let (_, n) = seeded(&b).await;
    write(&b, &app(), layout::CURRENT, br#"{"snapshot_id":1}"#);
    let c = committer(&b.store, 2);
    c.committer.load().await.unwrap();
    assert_eq!(c.gates.state(&app()), DatasetState::Normal);
    let m = c
        .committer
        .commit(&app(), &request(data_files(&[H10], 3)))
        .await
        .unwrap();
    assert_eq!(m, SnapshotId(n.0 + 1));
    assert_eq!(hint(&b.store, &app()).await, Some(m));
    let s = snapshot(&b.store, &app(), m).await;
    assert_eq!(s.parent_snapshot_id, Some(n));
    let parent = std::fs::read(path(&b, &app(), &layout::snapshot_path(n))).unwrap();
    assert_eq!(s.parent_digest, Some(Digest::of(&parent)));
}

/// 父快照在本进程读过之后被改：再读核对不过，停用，不写出新快照。
#[tokio::test]
async fn storage_parent_changed_after_read_halts_commit() {
    let b = file_backend();
    let (c, n) = seeded(&b).await;
    let mut v = read_json(&b, &app(), &layout::snapshot_path(n));
    v["created_at"] = 1.into();
    write(
        &b,
        &app(),
        &layout::snapshot_path(n),
        &serde_json::to_vec(&v).unwrap(),
    );
    assert_eq!(
        c.committer
            .commit(&app(), &request(data_files(&[H10], 3)))
            .await,
        Err(CommitError::Disabled(DatasetState::ManifestUnreadable))
    );
    assert!(!path(&b, &app(), &layout::snapshot_path(SnapshotId(n.0 + 1))).exists());
}

/// 桶 manifest 被改（删去一个文件条目）：重写该桶时核对摘要不符，停用，不写出新快照。
#[tokio::test]
async fn storage_tampered_bucket_manifest_halts_commit() {
    let b = file_backend();
    let (_, n) = seeded(&b).await;
    let s = read_json(&b, &app(), &layout::snapshot_path(n));
    let rel = s["buckets"][0]["manifest"].as_str().unwrap().to_owned();
    let mut m = read_json(&b, &app(), &rel);
    m["files"] = serde_json::json!([]);
    write(&b, &app(), &rel, &serde_json::to_vec(&m).unwrap());

    let c = committer(&b.store, 2);
    c.committer.load().await.unwrap();
    assert_eq!(
        c.committer
            .commit(&app(), &request(data_files(&[H10 + 7], 3)))
            .await,
        Err(CommitError::Disabled(DatasetState::ManifestUnreadable))
    );
    assert_eq!(current(&b.store, &app()).await, Some(n));
}

/// 下一个快照编号被无法解析的外部对象占住：找链尾把它当链尾，读取不过即停用 (a)，不写出 N+2、不覆盖它；
/// 其他数据集照常。
#[tokio::test]
async fn storage_next_slot_occupied_suspends_dataset() {
    let b = file_backend();
    let (_, n) = seeded(&b).await;
    let web = Dataset::new("default", "web");
    let c = committer(&b.store, 1);
    c.committer.load().await.unwrap();
    c.committer.ensure_dataset(&web).await;
    write(
        &b,
        &app(),
        &layout::snapshot_path(SnapshotId(n.0 + 1)),
        b"{}",
    );
    assert_eq!(
        c.committer
            .commit(&app(), &request(data_files(&[H10], 3)))
            .await,
        Err(CommitError::Disabled(DatasetState::ManifestUnreadable))
    );
    assert_eq!(c.gates.state(&app()), DatasetState::ManifestUnreadable);
    assert_eq!(
        std::fs::read(path(
            &b,
            &app(),
            &layout::snapshot_path(SnapshotId(n.0 + 1))
        ))
        .unwrap(),
        b"{}",
        "不覆盖被占的快照编号"
    );
    assert!(!path(&b, &app(), &layout::snapshot_path(SnapshotId(n.0 + 2))).exists());
    assert_eq!(hint(&b.store, &app()).await, Some(n));
    let w = c
        .committer
        .commit(&web, &request(data_files(&[H10], 4)))
        .await
        .unwrap();
    assert_eq!(w, SnapshotId(2));
    assert_eq!(c.gates.state(&web), DatasetState::Normal);
}

/// 运行中 `_current` 消失 (c)；存在只在本地 WAL 的数据集不读对象存储。
#[tokio::test]
async fn storage_current_deleted_while_running_and_local_only_datasets() {
    let b = file_backend();
    let (c, n) = seeded(&b).await;
    assert_eq!(c.chain.current(&app()).await, Ok(Some(n)));
    std::fs::remove_file(path(&b, &app(), layout::CURRENT)).unwrap();
    assert_eq!(
        c.chain.current(&app()).await,
        Err(ChainError::Disabled(DatasetState::ManifestUnreadable))
    );
    assert!(matches!(
        c.committer
            .commit(&app(), &request(data_files(&[H10], 2)))
            .await,
        Err(CommitError::Disabled(_))
    ));
    assert!(
        !path(&b, &app(), layout::CURRENT).exists(),
        "不补建 _current"
    );

    let local = Dataset::new("default", "local");
    c.registry.record(&local, Persistence::LocalWal);
    std::fs::remove_dir_all(b.dir.as_ref().unwrap().0.join("amberlog")).unwrap();
    assert_eq!(c.chain.current(&local).await, Ok(None));
}

/// WAL 写不进去：不写出创世快照，数据集只在本进程内存在。
#[tokio::test]
async fn storage_no_genesis_without_wal() {
    let b = file_backend();
    let c = committer(&b.store, 2);
    c.wal.set_failing(true);
    assert_eq!(c.committer.ensure_dataset(&app()).await, Existence::Pending);
    assert!(!path(&b, &app(), "manifest").exists());
    assert_eq!(
        c.registry.persistence(&app()),
        Some(Persistence::Unpersisted)
    );
    c.wal.set_failing(false);
    assert_eq!(
        c.committer.ensure_dataset(&app()).await,
        Existence::Committed
    );
    assert_eq!(c.registry.persistence(&app()), Some(Persistence::Stored));
    assert_eq!(
        c.wal.releases(),
        [Release::Existence { dataset: app() }],
        "创世快照提交成功才发存在性的段回收通知"
    );
    c.committer
        .commit(&app(), &request(data_files(&[H10], 1)))
        .await
        .unwrap();
    assert_eq!(
        c.wal.releases().last(),
        Some(&Release::Records {
            dataset: app(),
            through: WalPosition(1)
        })
    );
}

#[tokio::test]
async fn storage_resolve_intent_outcomes() {
    let b = file_backend();
    let (x, n) = seeded(&b).await;
    let intent = |id: u64, digest: Digest| CommitIntent {
        dataset: app(),
        snapshot_id: SnapshotId(id),
        through: WalPosition(9),
        through_partial: None,
        digest,
        files: Vec::new(),
    };
    let bytes_of =
        |id: SnapshotId| std::fs::read(path(&b, &app(), &layout::snapshot_path(id))).unwrap();
    let d_n = Digest::of(&bytes_of(n));
    let c = committer(&b.store, 2);
    c.committer.load().await.unwrap();
    assert_eq!(
        c.committer.resolve_intent(intent(n.0, d_n)).await,
        Ok(IntentOutcome::Committed)
    );
    assert_eq!(
        c.committer
            .resolve_intent(intent(n.0, Digest::of(b"other")))
            .await,
        Ok(IntentOutcome::Foreign)
    );
    assert_eq!(
        c.committer.resolve_intent(intent(n.0 + 1, d_n)).await,
        Ok(IntentOutcome::Absent)
    );
    // 快照 P 已创建、提示仍为 N：创建即完成提交，按已提交判明，不改提示
    let p = x
        .committer
        .commit(&app(), &request(data_files(&[H10], 5)))
        .await
        .unwrap();
    let d_p = Digest::of(&bytes_of(p));
    write(
        &b,
        &app(),
        layout::CURRENT,
        format!(r#"{{"snapshot_id":{}}}"#, n.0).as_bytes(),
    );
    let y = committer(&b.store, 2);
    y.committer.load().await.unwrap();
    assert_eq!(
        y.committer.resolve_intent(intent(p.0, d_p)).await,
        Ok(IntentOutcome::Committed)
    );
    assert_eq!(hint(&b.store, &app()).await, Some(n));
    assert_eq!(current(&b.store, &app()).await, Some(p));
}

/// 合并提交在快照 `created_at` 不早于 `sealed_until`（该桶已越过入湖水位、不再处于 `sealed`）时放弃，不写出新快照；
/// 未越过时照常提交（REQ-AMB-STO-SEAL-COMPACTION-V2「提交一次合并时，若该桶已不处于 `sealed` 状态……放弃」）。
#[tokio::test]
async fn storage_compaction_abandoned_past_sealed_until() {
    let b = file_backend();
    let (c, n) = seeded(&b).await;
    let s = snapshot(&b.store, &app(), n).await;
    let input = manifests(&b.store, &app(), &s).await[0].files[0].clone();
    let hour = layout::HourBucket::parse(&s.buckets[0].bucket).unwrap();
    let compaction = |sealed_until| CompactionRequest {
        bucket: hour,
        inputs: vec![input.clone()],
        outputs: data_files(&[H10], 1),
        sealed_until,
    };
    assert_eq!(
        c.committer
            .commit_compaction(&app(), &compaction(0))
            .await
            .map(|_| ()),
        Err(CommitError::Stale)
    );
    assert_eq!(current(&b.store, &app()).await, Some(n));
    assert_eq!(c.gates.state(&app()), DatasetState::Normal);
    c.committer
        .commit_compaction(&app(), &compaction(i64::MAX))
        .await
        .unwrap();
    assert_eq!(current(&b.store, &app()).await, Some(SnapshotId(n.0 + 1)));
}

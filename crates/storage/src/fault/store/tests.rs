use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};

use super::*;

/// 根前缀为 `base/amberlog`、控制文件在临时目录的包装层，以及控制文件路径。
fn faulty(name: &str) -> (FaultyStore, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "amberlog-fault-store-{name}-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("control");
    let _ = std::fs::remove_file(&file);
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    (
        FaultyStore::new(
            inner,
            &Path::from("base/amberlog"),
            Some(file.clone()),
            None,
        ),
        file,
    )
}

fn key(rel: &str) -> Path {
    Path::from(format!("base/amberlog/{rel}"))
}

fn create() -> PutOptions {
    PutOptions {
        mode: PutMode::Create,
        ..Default::default()
    }
}

#[test]
fn rules_parse_and_bad_lines_are_ignored() {
    let rules = parse_rules(
        "# comment\n\
         write _iceberg/ 403\n\
         create * ignore-condition   # trailing comment\n\
         put default/app/manifest/_current after-503 skip=1 times=2\n\
         get,head * 404\n\
         bogus * 403\n\
         put * not-a-status\n\
         put * 403 extra\n",
    );
    assert_eq!(rules.len(), 4);
    assert_eq!(rules[0].ops, vec![Op::Put, Op::Create]);
    assert_eq!(rules[0].prefix.as_deref(), Some("_iceberg/"));
    assert_eq!(rules[0].effect, Effect::Fail(403));
    assert_eq!(rules[1].prefix, None);
    assert_eq!(rules[1].effect, Effect::IgnoreCondition);
    assert_eq!(rules[2].effect, Effect::After(503));
    assert_eq!((rules[2].skip, rules[2].times), (1, Some(2)));
    assert_eq!(rules[3].ops, vec![Op::Get, Op::Head]);
}

#[tokio::test]
async fn no_control_file_passes_everything_through() {
    let (store, _) = faulty("passthrough");
    store
        .put(
            &key("_iceberg/default/app/x"),
            PutPayload::from_static(b"a"),
        )
        .await
        .unwrap();
    assert_eq!(
        store
            .get(&key("_iceberg/default/app/x"))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"a"
    );
}

#[tokio::test]
async fn writes_under_a_prefix_fail_with_403_and_other_keys_do_not() {
    let (store, control) = faulty("iceberg-403");
    std::fs::write(&control, "write _iceberg/ 403\n").unwrap();
    let blocked = key("_iceberg/default/app/metadata/v1.metadata.json");
    let e = store
        .put(&blocked, PutPayload::from_static(b"a"))
        .await
        .unwrap_err();
    assert!(
        matches!(e, object_store::Error::PermissionDenied { .. }),
        "{e}"
    );
    let e = store
        .put_opts(&blocked, PutPayload::from_static(b"a"), create())
        .await
        .unwrap_err();
    assert!(matches!(e, object_store::Error::PermissionDenied { .. }));
    // 没有写进去
    assert!(matches!(
        store.inner.head(&blocked).await,
        Err(object_store::Error::NotFound { .. })
    ));
    // 别的前缀照常
    store
        .put(
            &key("default/app/manifest/_current"),
            PutPayload::from_static(b"1"),
        )
        .await
        .unwrap();
    // 撤掉控制文件即撤除注入
    std::fs::remove_file(&control).unwrap();
    store
        .put(&blocked, PutPayload::from_static(b"a"))
        .await
        .unwrap();
}

#[tokio::test]
async fn ignore_condition_turns_create_into_an_overwrite() {
    let (store, control) = faulty("ignore-condition");
    let probe = key("_system/conditional-put-probe");
    store
        .put_opts(&probe, PutPayload::from_static(b"1"), create())
        .await
        .unwrap();
    // 未注入时第二次「仅当不存在时创建」以已存在失败
    assert!(matches!(
        store
            .put_opts(&probe, PutPayload::from_static(b"2"), create())
            .await,
        Err(object_store::Error::AlreadyExists { .. })
    ));
    std::fs::write(&control, "create * ignore-condition\n").unwrap();
    store
        .put_opts(&probe, PutPayload::from_static(b"3"), create())
        .await
        .unwrap();
    assert_eq!(
        store
            .get(&probe)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"3"
    );
    // ignore-condition 只作用于 create：普通写入不受影响
    store
        .put(&probe, PutPayload::from_static(b"4"))
        .await
        .unwrap();
}

#[tokio::test]
async fn create_with_501_is_a_generic_error() {
    let (store, control) = faulty("create-501");
    std::fs::write(&control, "create * 501\n").unwrap();
    let e = store
        .put_opts(&key("_system/p"), PutPayload::from_static(b"1"), create())
        .await
        .unwrap_err();
    assert!(matches!(e, object_store::Error::Generic { .. }), "{e}");
    assert!(e.to_string().contains("501"), "{e}");
    // 普通写入不受 create 规则影响
    store
        .put(&key("_system/p"), PutPayload::from_static(b"1"))
        .await
        .unwrap();
}

#[tokio::test]
async fn after_status_performs_the_write_then_fails() {
    let (store, control) = faulty("after-503");
    std::fs::write(
        &control,
        "create default/app/manifest/00000000000000000002.json after-503 times=1\n",
    )
    .unwrap();
    let snap = key("default/app/manifest/00000000000000000002.json");
    let e = store
        .put_opts(&snap, PutPayload::from_static(b"s"), create())
        .await
        .unwrap_err();
    assert!(matches!(e, object_store::Error::Generic { .. }));
    // 写进去了
    assert_eq!(
        store
            .get(&snap)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"s"
    );
    // times=1：第二次照常转发，得到真实的「已存在」
    assert!(matches!(
        store
            .put_opts(&snap, PutPayload::from_static(b"s"), create())
            .await,
        Err(object_store::Error::AlreadyExists { .. })
    ));
}

#[tokio::test]
async fn skip_lets_the_first_requests_through() {
    let (store, control) = faulty("skip");
    std::fs::write(&control, "create _system/ 503 skip=1\n").unwrap();
    let probe = key("_system/conditional-put-probe");
    store
        .put_opts(&probe, PutPayload::from_static(b"1"), create())
        .await
        .unwrap();
    let e = store
        .put_opts(&probe, PutPayload::from_static(b"2"), create())
        .await
        .unwrap_err();
    assert!(matches!(e, object_store::Error::Generic { .. }));
}

#[tokio::test]
async fn reads_lists_and_deletes_can_be_failed() {
    let (store, control) = faulty("reads");
    let obj = key("default/app/data/2026/10/13/10/x.parquet");
    store
        .put(&obj, PutPayload::from_static(b"d"))
        .await
        .unwrap();
    std::fs::write(
        &control,
        "get default/app/data/ 403\nlist default/app/ 503\ndelete * 503\n",
    )
    .unwrap();
    assert!(matches!(
        store.get(&obj).await,
        Err(object_store::Error::PermissionDenied { .. })
    ));
    // head 不在 get 规则内
    store.head(&obj).await.unwrap();
    assert!(
        store
            .list_with_delimiter(Some(&key("default/app")))
            .await
            .is_err()
    );
    assert!(matches!(
        store.delete(&obj).await,
        Err(object_store::Error::Generic { .. })
    ));
    std::fs::write(&control, "head * 404\n").unwrap();
    assert!(matches!(
        store.head(&obj).await,
        Err(object_store::Error::NotFound { .. })
    ));
    std::fs::remove_file(&control).unwrap();
    store.delete(&obj).await.unwrap();
    assert!(matches!(
        store.head(&obj).await,
        Err(object_store::Error::NotFound { .. })
    ));
}

/// `TESTBED_FAULT_STORE_LOG`：每个请求（不论是否命中规则）记一行「操作 相对键」。
#[tokio::test]
async fn requests_are_logged_when_asked() {
    let dir = std::env::temp_dir().join(format!("amberlog-fault-store-log-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (control, log) = (dir.join("control"), dir.join("log"));
    let _ = std::fs::remove_file(&log);
    std::fs::write(&control, "get _iceberg/ 403\n").unwrap();
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = FaultyStore::new(
        inner,
        &Path::from("base/amberlog"),
        Some(control),
        Some(log.clone()),
    );
    store
        .put(&key("default/app/x"), PutPayload::from_static(b"x"))
        .await
        .unwrap();
    assert!(
        store
            .get(&key("_iceberg/default/app/metadata/v1.metadata.json"))
            .await
            .is_err()
    );
    store.get(&key("default/app/x")).await.unwrap();
    assert_eq!(
        std::fs::read_to_string(&log).unwrap(),
        "put default/app/x\nget _iceberg/default/app/metadata/v1.metadata.json\nget default/app/x\n"
    );
}

/// 只设请求记录、不设控制文件：照常转发，只记录（`get_ranges` 也记）。
#[tokio::test]
async fn log_only_records_without_injecting() {
    let dir = std::env::temp_dir().join(format!(
        "amberlog-fault-store-logonly-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let log = dir.join("log");
    let _ = std::fs::remove_file(&log);
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = FaultyStore::new(inner, &Path::from("base/amberlog"), None, Some(log.clone()));
    store
        .put(&key("_iceberg/x"), PutPayload::from_static(b"x"))
        .await
        .unwrap();
    store
        .get_ranges(&key("_iceberg/x"), std::slice::from_ref(&(0..1)))
        .await
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(&log).unwrap(),
        "put _iceberg/x\nget _iceberg/x\n"
    );
}

/// `stall`：命中的请求挂起，不挡其他请求；规则删去后照常转发（REQ-AMB-STO-MANIFEST-COMMIT-V5 的测试用法）。
#[tokio::test]
async fn stall_holds_matching_requests_until_the_rule_is_removed() {
    use std::time::Duration;

    let (store, control) = faulty("stall");
    std::fs::write(
        &control,
        "write default/app/ stall\nlist default/app/ stall\n",
    )
    .unwrap();
    assert_eq!(
        parse_rules("write default/app/ stall")[0].effect,
        Effect::Stall
    );
    let store = Arc::new(store);
    let app = key("default/app/manifest/00000000000000000002.json");
    let put = tokio::spawn({
        let store = Arc::clone(&store);
        let app = app.clone();
        async move {
            store
                .put_opts(&app, PutPayload::from_static(b"s"), create())
                .await
        }
    });
    let list = tokio::spawn({
        let store = Arc::clone(&store);
        async move {
            store
                .list_with_delimiter(Some(&key("default/app/manifest")))
                .await
        }
    });
    // 其他数据集与 `default/app/` 之下的读取照常
    store
        .put(&key("default/web/x"), PutPayload::from_static(b"w"))
        .await
        .unwrap();
    assert!(matches!(
        store.head(&app).await,
        Err(object_store::Error::NotFound { .. })
    ));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!put.is_finished() && !list.is_finished());
    // 改写控制文件但保留这两行：仍挂起
    std::fs::write(
        &control,
        "write default/app/ stall\nlist default/app/ stall\nget _iceberg/ 403\n",
    )
    .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!put.is_finished());
    std::fs::write(&control, "list default/app/ stall\n").unwrap();
    tokio::time::timeout(Duration::from_secs(5), put)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!list.is_finished());
    std::fs::remove_file(&control).unwrap();
    let listed = tokio::time::timeout(Duration::from_secs(5), list)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(listed.objects.len(), 1);
    assert_eq!(
        store
            .get(&app)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
            .as_ref(),
        b"s"
    );
}

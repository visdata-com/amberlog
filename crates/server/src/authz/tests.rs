//! 授权模块的进程内测试：凭据的判定顺序、范围匹配、授予的并集与两步判定。系统测试在
//! `crates/server/tests/auth_authz.rs`（启动可执行文件，经 HTTP 与运行日志断言）。

use axum::http::{HeaderMap, HeaderValue, header};

use super::*;

fn headers(auth: &[&str]) -> HeaderMap {
    let mut h = HeaderMap::new();
    for v in auth {
        h.append(header::AUTHORIZATION, HeaderValue::from_str(v).unwrap());
    }
    h
}

fn token(n: u8) -> String {
    token::from_random(&[n; 32])
}

fn table(entries: &str) -> Table {
    file::parse(&format!("format_version = 1\n{entries}")).unwrap()
}

/// 一个令牌条目：名为 `name`、哈希为令牌 `n` 的哈希行。
fn entry(name: &str, n: u8, rest: &str) -> String {
    let line = token::hash_line(&token::digest(token(n).as_bytes()));
    format!("[[tokens]]\nname = \"{name}\"\nhashes = [\"{line}\"]\n{rest}\n")
}

fn principal(grants: &str) -> Principal {
    let t = table(&entry("p", 1, &format!("grants = {grants}")));
    match t.lookup(token(1).as_bytes(), jiff::Timestamp::now()) {
        Lookup::Valid(p) => p,
        Lookup::Invalid => panic!("令牌应有效"),
    }
}

#[test]
fn bearer_forms() {
    let authz = Authz::with_token("s3cret");
    let check = |values: &[&str]| authz.authenticate(&headers(values)).is_some();
    for ok in [
        "Bearer s3cret",
        "bearer s3cret",
        "BEARER s3cret",
        "Bearer   s3cret",
        "Bearer s3cret  ",
    ] {
        assert!(check(&[ok]), "{ok:?}");
    }
    for bad in [
        "Bearer S3CRET",
        "Bearer s3cre",
        "Bearer s3cretX",
        "Bearer",
        "Bearer ",
        "Bearers3cret",
        "s3cret",
        "Basic czNjcmV0",
        "Bearer\ts3cret",
    ] {
        assert!(!check(&[bad]), "{bad:?}");
    }
    assert!(!check(&[]));
    assert!(!check(&["Bearer s3cret", "Bearer s3cret"]));
    assert!(!check(&["Bearer s3cret", "Bearer wrong"]));
}

#[test]
fn digest_comparison() {
    assert!(constant_time_eq(b"", b""));
    assert!(constant_time_eq(b"s3cret", b"s3cret"));
    assert!(!constant_time_eq(b"s3cret", b""));
    assert!(!constant_time_eq(b"s3cret", b"s3cret\0"));
    assert!(!constant_time_eq(b"a", b"b"));
}

/// 先与 `auth.token` 比较，再按服务令牌查授权文件；不符合格式的凭据不与任何条目比对（REQ-AMB-AUTH-CREDENTIAL-V3）。
#[test]
fn credential_order() {
    let src = FileSource {
        text: "authz.toml".into(),
        path: "authz.toml".into(),
    };
    // 不符合服务令牌格式的字符串 X，其哈希行也登记在条目里
    let x = "not-a-service-token";
    let x_line = token::hash_line(&token::digest(x.as_bytes()));
    let t = table(&format!(
        "{}[[tokens]]\nname = \"x\"\nhashes = [\"{x_line}\"]\ngrants = [{{ role = \"admin\" }}]\n",
        entry(
            "q",
            1,
            r#"grants = [{ role = "querier", streams = ["*"] }]"#
        )
    ));
    let authz = Authz::enabled(Some("s3cret".into()), Some((src, t)), LogAccess::Reads);
    let who = |cred: &str| {
        authz
            .authenticate(&headers(&[&format!("Bearer {cred}")]))
            .map(|p| p.id().to_owned())
    };
    assert_eq!(who("s3cret").as_deref(), Some("token:auth.token"));
    assert_eq!(who(&token(1)).as_deref(), Some("token:q"));
    assert_eq!(who(x), None);
    assert_eq!(who(&token(1)[..46]), None);
    assert_eq!(who(&token(2)), None);

    // 只配授权文件、不配 `auth.token`
    let src = FileSource {
        text: "a".into(),
        path: "a".into(),
    };
    let only_file = Authz::enabled(
        None,
        Some((
            src,
            table(&entry(
                "q",
                1,
                r#"grants = [{ role = "writer", streams = ["app"] }]"#,
            )),
        )),
        LogAccess::Reads,
    );
    assert!(
        only_file
            .authenticate(&headers(&[&format!("Bearer {}", token(1))]))
            .is_some()
    );
    assert!(
        only_file
            .authenticate(&headers(&["Bearer s3cret"]))
            .is_none()
    );

    // 认证关闭：不要求凭据，按管理员作答
    let p = Authz::disabled().authenticate(&HeaderMap::new()).unwrap();
    assert_eq!(p.kind(), PrincipalKind::Unauthenticated);
    assert!(p.is_admin() && p.in_scope(Role::Writer, "anything"));
}

/// 条目的 `expires_at` 不晚于认证时刻即无效（REQ-AMB-AUTH-AUTHZ-TOKEN-EXPIRY）。
#[test]
fn token_expiry() {
    let t = table(&entry(
        "e",
        1,
        "expires_at = \"2030-01-01T00:00:00Z\"\ngrants = [{ role = \"admin\" }]",
    ));
    let at = |s: &str| s.parse::<jiff::Timestamp>().unwrap();
    assert!(matches!(
        t.lookup(token(1).as_bytes(), at("2029-12-31T23:59:59Z")),
        Lookup::Valid(_)
    ));
    for now in ["2030-01-01T00:00:00Z", "2031-01-01T00:00:00Z"] {
        assert!(
            matches!(t.lookup(token(1).as_bytes(), at(now)), Lookup::Invalid),
            "{now}"
        );
    }
}

/// 精确名、前缀模式与 `*`（REQ-AMB-AUTH-AUTHZ-TERM-SCOPE、-TERM-IN-SCOPE）。
#[test]
fn scope_patterns() {
    assert_eq!(Pattern::parse("*"), Some(Pattern::All));
    assert_eq!(
        Pattern::parse("app_*"),
        Some(Pattern::Prefix("app_".into()))
    );
    assert_eq!(Pattern::parse("app"), Some(Pattern::Exact("app".into())));
    assert_eq!(
        Pattern::parse(&"a".repeat(64)),
        Some(Pattern::Exact("a".repeat(64)))
    );
    assert_eq!(
        Pattern::parse(&format!("{}*", "a".repeat(63))),
        Some(Pattern::Prefix("a".repeat(63)))
    );
    for bad in [
        "",
        "**",
        "App",
        "a*b",
        "_app",
        "app-x",
        "*app",
        &"a".repeat(65),
        &format!("{}*", "a".repeat(64)),
    ] {
        assert_eq!(Pattern::parse(bad), None, "{bad:?}");
    }
    let q1 = principal(r#"[{ role = "querier", streams = ["app_*"] }]"#);
    let q2 = principal(r#"[{ role = "querier", streams = ["app"] }]"#);
    let q3 = principal(r#"[{ role = "querier", streams = ["*"] }]"#);
    for (p, app, app_web, apple) in [
        (&q1, false, true, false),
        (&q2, true, false, false),
        (&q3, true, true, true),
    ] {
        assert_eq!(
            (
                p.in_scope(Role::Querier, "app"),
                p.in_scope(Role::Querier, "app_web"),
                p.in_scope(Role::Querier, "apple")
            ),
            (app, app_web, apple)
        );
        assert!(!p.in_scope(Role::Writer, "app_web"));
    }
    assert!(q3.scope_all(&[Role::Querier]) && !q1.scope_all(&[Role::Querier]));
    assert!(!q3.scope_all(&[Role::GovernanceViewer]));
}

/// 授予取并集；管理员拥有其余五种角色的全部权限（REQ-AMB-AUTH-AUTHZ-UNION、-ADMIN-ALL）。
#[test]
fn grants_union_and_admin() {
    let m = principal(
        r#"[{ role = "writer", streams = ["app"] }, { role = "querier", streams = ["web"] }, { role = "writer", streams = ["x_*"] }]"#,
    );
    assert!(m.in_scope(Role::Writer, "app") && m.in_scope(Role::Writer, "x_1"));
    assert!(m.in_scope(Role::Querier, "web") && !m.in_scope(Role::Querier, "app"));
    assert!(!m.in_scope(Role::Writer, "web"));
    assert_eq!(m.roles(), vec!["querier", "writer"]);
    assert!(!m.is_admin() && !m.has_role(Role::MetricsReader));

    let a = principal(r#"[{ role = "admin" }]"#);
    for r in Role::ALL {
        assert!(a.has_role(r) && a.in_scope(r, "anything"), "{r:?}");
    }
    assert!(a.scope_all(&[Role::GovernanceViewer]));
    assert_eq!(a.roles(), vec!["admin"]);
    assert_eq!(Principal::auth_token().roles(), vec!["admin"]);

    let c = principal(
        r#"[{ role = "catalog_reader", streams = ["lake_*"] }, { role = "querier", streams = ["app"] }]"#,
    );
    assert!(
        c.in_catalog_scope("lake_1") && c.in_catalog_scope("app") && !c.in_catalog_scope("web")
    );
}

fn access(grants: &str, endpoint: EndpointKind) -> Access {
    let a = Access::new(principal(grants));
    a.set_endpoint(endpoint);
    a
}

fn ds(stream: &str) -> amberlog_common::dataset::Dataset {
    amberlog_common::dataset::Dataset::new("default", stream)
}

/// 路径一步：端点权限、写入范围、读的范围与来源限定（REQ-AMB-AUTH-AUTHZ B 组）。
#[test]
fn path_step_decisions() {
    let none = HeaderMap::new();
    let w = r#"[{ role = "writer", streams = ["app_*"] }]"#;
    let q = r#"[{ role = "querier", streams = ["app"] }]"#;
    let g = r#"[{ role = "governance_viewer", streams = ["*"] }]"#;
    let mr = r#"[{ role = "metrics_reader" }]"#;
    type Case<'a> = (&'a str, EndpointKind, Option<&'a str>, Result<(), Denied>);
    let cases: &[Case] = &[
        (w, EndpointKind::Json, Some("app_x"), Ok(())),
        (w, EndpointKind::Json, Some("hr"), Err(Denied::Forbidden)),
        (w, EndpointKind::Sent, Some("hr"), Err(Denied::Forbidden)),
        (w, EndpointKind::Search, None, Err(Denied::Forbidden)),
        (w, EndpointKind::Metrics, None, Err(Denied::Forbidden)),
        (q, EndpointKind::Json, Some("app"), Err(Denied::Forbidden)),
        (q, EndpointKind::Search, None, Ok(())),
        (q, EndpointKind::Fields, Some("app"), Ok(())),
        (q, EndpointKind::Fields, Some("hr"), Err(Denied::Absent)),
        (q, EndpointKind::Catalog, None, Ok(())),
        (q, EndpointKind::Catalog, Some("hr"), Err(Denied::Absent)),
        (g, EndpointKind::Search, None, Ok(())),
        (g, EndpointKind::Fields, Some("app"), Err(Denied::Forbidden)),
        (g, EndpointKind::Catalog, None, Err(Denied::Forbidden)),
        (mr, EndpointKind::Metrics, None, Ok(())),
        (mr, EndpointKind::Search, None, Err(Denied::Forbidden)),
    ];
    for (grants, endpoint, stream, want) in cases {
        let d = stream.map(ds);
        assert_eq!(
            access(grants, *endpoint).path_step(d.as_ref(), &none),
            *want,
            "{grants} {endpoint:?} {stream:?}"
        );
    }
    let a = access(w, EndpointKind::Json);
    let _ = a.path_step(Some(&ds("hr")), &none);
    assert_eq!(a.dataset().as_deref(), Some("default/hr"));
}

/// `_json` 的来源限定（REQ-AMB-AUTH-AUTHZ-SOURCE-RESTRICT-JSON、-SOURCE-UNRESTRICTED）与 `_sent` 的请求体一步。
#[test]
fn source_restriction() {
    let t = table(&format!(
        "{}{}",
        entry(
            "s",
            1,
            "sources = [\"vector-a\"]\ngrants = [{ role = \"writer\", streams = [\"app\"] }]"
        ),
        entry(
            "w",
            2,
            "grants = [{ role = \"writer\", streams = [\"app\"] }]"
        ),
    ));
    let now = jiff::Timestamp::now();
    let Lookup::Valid(s) = t.lookup(token(1).as_bytes(), now) else {
        panic!()
    };
    let Lookup::Valid(w) = t.lookup(token(2).as_bytes(), now) else {
        panic!()
    };
    let with_sources = |values: &[&str]| {
        let mut h = HeaderMap::new();
        for v in values {
            h.append("x-amberlog-source", HeaderValue::from_str(v).unwrap());
        }
        h
    };
    let json = |p: &Principal, h: &HeaderMap| {
        let a = Access::new(p.clone());
        a.set_endpoint(EndpointKind::Json);
        a.path_step(Some(&ds("app")), h)
    };
    for bad in [
        &[][..],
        &["vector-b"],
        &["vector-a", "vector-a"],
        &["VECTOR-A"],
    ] {
        assert_eq!(
            json(&s, &with_sources(bad)),
            Err(Denied::Forbidden),
            "{bad:?}"
        );
    }
    assert_eq!(json(&s, &with_sources(&["vector-a"])), Ok(()));
    for any in [&[][..], &["any.src"]] {
        assert_eq!(json(&w, &with_sources(any)), Ok(()), "{any:?}");
    }
    // `_sent` 只在请求体一步看元素的 `source`，路径一步不看请求头
    let sent = Access::new(s.clone());
    sent.set_endpoint(EndpointKind::Sent);
    assert_eq!(sent.path_step(Some(&ds("app")), &HeaderMap::new()), Ok(()));
    assert_eq!(sent.sent_sources(["vector-a", "vector-a"]), Ok(()));
    assert_eq!(
        sent.sent_sources(["vector-a", "vector-b"]),
        Err(Denied::Forbidden)
    );
    assert_eq!(Access::new(w).sent_sources(["x"]), Ok(()));
    assert_eq!(
        Access::new(Principal::auth_token()).sent_sources(["x"]),
        Ok(())
    );
}

/// `_search` 的请求体一步：系统表、普通数据集、只有治理查看者时查普通数据集 403（REQ-AMB-AUTH-AUTHZ-DENY-ENDPOINT 括注）。
#[test]
fn search_target_decisions() {
    let q = access(
        r#"[{ role = "querier", streams = ["app"] }]"#,
        EndpointKind::Search,
    );
    let g = access(
        r#"[{ role = "governance_viewer", streams = ["app"] }]"#,
        EndpointKind::Search,
    );
    let app = ds("app");
    let hr = ds("hr");
    assert_eq!(q.search_target(SearchTarget::Stream(&app)), Ok(()));
    assert_eq!(
        q.search_target(SearchTarget::Stream(&hr)),
        Err(Denied::Absent)
    );
    assert_eq!(q.search_target(SearchTarget::System("_ledger")), Ok(()));
    assert_eq!(
        g.search_target(SearchTarget::Stream(&app)),
        Err(Denied::Forbidden)
    );
    assert_eq!(
        g.search_target(SearchTarget::Stream(&hr)),
        Err(Denied::Forbidden)
    );
    assert_eq!(
        g.search_target(SearchTarget::System("_reconciliation")),
        Ok(())
    );
    assert_eq!(g.dataset().as_deref(), Some("_reconciliation"));
}

#[test]
fn role_and_log_access_names() {
    for r in Role::ALL {
        assert_eq!(Role::parse(r.as_str()), Some(r));
    }
    assert_eq!(Role::parse("superuser"), None);
    assert_eq!(Role::parse("Admin"), None);
    assert_eq!(LogAccess::parse("reads"), Some(LogAccess::Reads));
    assert_eq!(LogAccess::parse("READS"), None);
}

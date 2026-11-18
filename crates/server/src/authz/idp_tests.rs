//! 身份平台的映射（REQ-AMB-AUTH-AUTHZ-FILE-IDP-GRANTS、-IDP-MAPPING、-PRINCIPAL-IDP）的进程内测试。
//! 系统测试在 `crates/server/tests/auth_oidc.rs`。

use super::*;
use crate::oidc::Claims;

fn table(text: &str) -> Result<Table, file::Invalid> {
    file::parse(&format!("format_version = 1\n{text}"))
}

fn claims(subject: &str, groups: &[&str], client_id: Option<&str>) -> Claims {
    Claims {
        subject: subject.to_owned(),
        groups: groups.iter().map(|g| (*g).to_owned()).collect(),
        client_id: client_id.map(str::to_owned),
        preferred_username: None,
    }
}

const Q_APP: &str = r#"grants = [{ role = "querier", streams = ["app"] }]"#;

#[test]
fn idp_grants_format() {
    let t = table(&format!(
        "[[idp_grants]]\ngroup = \"sec\"\n{Q_APP}\n[[idp_grants]]\nsubject = \"u-1\"\ngrants = [{{ role = \"admin\" }}]\n[[idp_grants]]\nclient_id = \"bot\"\n{Q_APP}\n"
    ))
    .unwrap();
    assert_eq!(t.idp_grants.len(), 3);
    assert_eq!(t.idp_grants[1].matcher, IdpMatch::Subject("u-1".to_owned()));
    for bad in [
        format!("[[idp_grants]]\n{Q_APP}\n"),
        format!("[[idp_grants]]\ngroup = \"a\"\nsubject = \"b\"\n{Q_APP}\n"),
        format!("[[idp_grants]]\ngroup = \"\"\n{Q_APP}\n"),
        format!("[[idp_grants]]\ngroup = 1\n{Q_APP}\n"),
        "[[idp_grants]]\ngroup = \"a\"\n".to_owned(),
        format!("[[idp_grants]]\ngroup = \"a\"\nrole = \"x\"\n{Q_APP}\n"),
        "[[idp_grants]]\ngroup = \"a\"\ngrants = [{ role = \"superuser\", streams = [\"app\"] }]\n"
            .to_owned(),
        "idp_grants = 1\n".to_owned(),
    ] {
        assert!(table(&bad).is_err(), "{bad}");
    }
}

#[test]
fn idp_principal_mapping() {
    let t = table(
        "[[idp_grants]]\ngroup = \"sec\"\ngrants = [{ role = \"querier\", streams = [\"audit_*\"] }]\n\
         [[idp_grants]]\nsubject = \"u-1\"\ngrants = [{ role = \"writer\", streams = [\"app\"] }]\n\
         [[idp_grants]]\nclient_id = \"bot\"\ngrants = [{ role = \"metrics_reader\" }]\n",
    )
    .unwrap();
    let p = Principal::idp(claims("u-1", &["sec", "other"], Some("bot")), Some(&t));
    assert_eq!(p.id(), "oidc:u-1");
    assert_eq!(p.kind(), PrincipalKind::Idp);
    assert_eq!(p.roles(), vec!["metrics_reader", "querier", "writer"]);
    assert!(p.in_scope(Role::Querier, "audit_ssh"));
    assert!(!p.in_scope(Role::Querier, "app"));
    // 按字节相等比较；没有任何授予时仍是认证成功的主体
    let none = Principal::idp(claims("U-1", &["Sec"], Some("Bot")), Some(&t));
    assert!(none.roles().is_empty());
    assert!(!none.is_admin());
    let mut c = claims("u-2", &[], None);
    c.preferred_username = Some("alice".to_owned());
    assert_eq!(Principal::idp(c, None).name(), Some("alice"));
    assert_eq!(Principal::auth_token().name(), None);
}

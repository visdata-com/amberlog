//! 目录前缀下的路由与方法匹配（REQ-AMB-STO-COLD-CATALOG-TERM-ENDPOINTS、-UNROUTED-405、-METHOD-405）。
//!
//! 按请求目标中未经百分号解码的路径以 `/` 分段匹配七个模板：固定段按原样比较，`{prefix}`、`{namespace}`、`{table}`
//! 各匹配一个非空段；查询串不参与匹配。带尾斜杠、含空段或固定段经百分号编码的路径因此不匹配任何模板。

use axum::http::Method;

/// 目录前缀（REQ-AMB-STO-COLD-CATALOG-TERM-PREFIX）。
pub const PREFIX: &str = "/iceberg/";

/// 路径匹配到的模板与其中的变量段（原样，未解码）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route<'a> {
    /// `/v1/config`
    Config,
    /// `/v1/{prefix}/namespaces`
    Namespaces { prefix: &'a str },
    /// `/v1/{prefix}/namespaces/{namespace}`
    Namespace { prefix: &'a str, namespace: &'a str },
    /// `/v1/{prefix}/namespaces/{namespace}/tables`
    Tables { prefix: &'a str, namespace: &'a str },
    /// `/v1/{prefix}/namespaces/{namespace}/tables/{table}`
    Table {
        prefix: &'a str,
        namespace: &'a str,
        table: &'a str,
    },
}

/// `/v1/config` 响应中 `endpoints` 的七个模板原文（REQ-AMB-STO-COLD-CATALOG-CONFIG-ENDPOINTS）。
pub const ENDPOINTS: [&str; 7] = [
    "GET /v1/config",
    "GET /v1/{prefix}/namespaces",
    "GET /v1/{prefix}/namespaces/{namespace}",
    "HEAD /v1/{prefix}/namespaces/{namespace}",
    "GET /v1/{prefix}/namespaces/{namespace}/tables",
    "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
];

impl<'a> Route<'a> {
    /// 匹配原始路径 `path`（以 [`PREFIX`] 开头）；不匹配任何模板为 `None`。
    pub fn of(path: &'a str) -> Option<Route<'a>> {
        let rest = path.strip_prefix(PREFIX)?;
        let segs: Vec<&str> = rest.split('/').collect();
        let var = |s: &&str| !s.is_empty();
        match segs.as_slice() {
            ["v1", "config"] => Some(Route::Config),
            ["v1", prefix, "namespaces"] if var(prefix) => Some(Route::Namespaces { prefix }),
            ["v1", prefix, "namespaces", namespace] if var(prefix) && var(namespace) => {
                Some(Route::Namespace { prefix, namespace })
            }
            ["v1", prefix, "namespaces", namespace, "tables"] if var(prefix) && var(namespace) => {
                Some(Route::Tables { prefix, namespace })
            }
            ["v1", prefix, "namespaces", namespace, "tables", table]
                if var(prefix) && var(namespace) && var(table) =>
            {
                Some(Route::Table {
                    prefix,
                    namespace,
                    table,
                })
            }
            _ => None,
        }
    }

    /// 该路径支持 `HEAD`（命名空间与表）。
    fn has_head(self) -> bool {
        matches!(self, Route::Namespace { .. } | Route::Table { .. })
    }

    /// 405 时 `Allow` 的取值。
    pub fn allow(self) -> &'static str {
        if self.has_head() { "GET, HEAD" } else { "GET" }
    }

    /// 方法与路径都匹配时的端点名（指标标签 `endpoint`，REQ-AMB-STO-COLD-CATALOG-METRIC）；方法不被该路径支持为 `None`。
    pub fn endpoint(self, method: &Method) -> Option<&'static str> {
        let head = if *method == Method::GET {
            false
        } else if *method == Method::HEAD && self.has_head() {
            true
        } else {
            return None;
        };
        Some(match (self, head) {
            (Route::Config, _) => "config",
            (Route::Namespaces { .. }, _) => "list_namespaces",
            (Route::Namespace { .. }, false) => "get_namespace",
            (Route::Namespace { .. }, true) => "head_namespace",
            (Route::Tables { .. }, _) => "list_tables",
            (Route::Table { .. }, false) => "get_table",
            (Route::Table { .. }, true) => "head_table",
        })
    }
}

/// 查询串中第一个名为 `name` 的参数的原始取值（未解码；没有 `=` 的取空串）。
pub fn query_param<'q>(query: Option<&'q str>, name: &str) -> Option<&'q str> {
    query?.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        (k == name).then_some(v)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn templates() {
        assert_eq!(Route::of("/iceberg/v1/config"), Some(Route::Config));
        assert_eq!(
            Route::of("/iceberg/v1/amberlog/namespaces"),
            Some(Route::Namespaces { prefix: "amberlog" })
        );
        assert_eq!(
            Route::of("/iceberg/v1/amberlog/namespaces/default%1Fapp/tables/snapshots"),
            Some(Route::Table {
                prefix: "amberlog",
                namespace: "default%1Fapp",
                table: "snapshots"
            })
        );
        for unrouted in [
            "/iceberg/",
            "/iceberg/foo",
            "/iceberg/v1/config/",
            "/iceberg/v1//namespaces",
            "/iceberg/v1/%63onfig",
            "/iceberg/v1/amberlog/namespaces/default/tables/app/",
            "/iceberg/v1/amberlog/namespaces/default/tables/app/credentials",
            "/iceberg/v1/amberlog/tables/rename",
            "/iceberg/v1/oauth/tokens",
            "/iceberg/v1/namespaces/default",
            "/iceberg/v1/namespaces/default/tables/app",
            "/iceberg/v1/amberlog/no-such-endpoint",
        ] {
            assert_eq!(Route::of(unrouted), None, "{unrouted}");
        }
    }

    #[test]
    fn methods() {
        let t = Route::of("/iceberg/v1/p/namespaces/n/tables/t").unwrap();
        assert_eq!(t.endpoint(&Method::HEAD), Some("head_table"));
        assert_eq!(t.endpoint(&Method::DELETE), None);
        assert_eq!(t.allow(), "GET, HEAD");
        assert_eq!(Route::Config.endpoint(&Method::HEAD), None);
        assert_eq!(Route::Config.endpoint(&Method::OPTIONS), None);
        assert_eq!(Route::Config.allow(), "GET");
    }

    #[test]
    fn query() {
        assert_eq!(
            query_param(Some("a=1&parent=x&parent=y"), "parent"),
            Some("x")
        );
        assert_eq!(query_param(Some("parent"), "parent"), Some(""));
        assert_eq!(query_param(Some("parents=1"), "parent"), None);
        assert_eq!(query_param(None, "parent"), None);
    }
}

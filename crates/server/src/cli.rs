//! 命令行（REQ-AMB-CFG-STARTUP-V7「可执行文件与命令行」）：`serve [--config <path>]`、`catalog [--config <path>]`
//! （只读目录模式，REQ-AMB-STO-COLD-CATALOG）、`retire [--config <path>]`（优雅退役，REQ-AMB-LIFE-RETIREMENT）、`--version`，
//! 以及离线子命令 `evidence export` 与 `evidence verify`（REQ-AMB-STO-EVIDENCE-EXPORT-ARGS、REQ-AMB-STO-EVIDENCE-VERIFY-ARGS）。
//!
//! 这里只判参数的写法（未知参数、缺必填、同一参数出现多次）；参数取值的校验属启动流程的「配置」一步，在各子命令的模块里。

use std::ffi::OsString;
use std::path::PathBuf;

pub const USAGE: &str = "usage: amberlog serve [--config <path>]\n       \
amberlog catalog [--config <path>]\n       \
amberlog retire [--config <path>]\n       \
amberlog evidence export [--config <path>] --dataset <org>/<stream> [--from <time>] [--to <time>] --output <path>\n       \
amberlog evidence verify [--config <path>] --evidence <path> [--previous <path>] [--retention <duration>]\n       \
amberlog --version";

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Version,
    Serve {
        config: Option<PathBuf>,
    },
    Catalog {
        config: Option<PathBuf>,
    },
    /// 优雅退役（REQ-AMB-LIFE-RETIREMENT）。
    Retire {
        config: Option<PathBuf>,
    },
    EvidenceExport(ExportArgs),
    EvidenceVerify(VerifyArgs),
}

/// `amberlog evidence export` 的参数（取值未校验）。
#[derive(Debug, PartialEq, Eq)]
pub struct ExportArgs {
    pub config: Option<PathBuf>,
    pub dataset: OsString,
    pub from: Option<OsString>,
    pub to: Option<OsString>,
    pub output: PathBuf,
}

/// `amberlog evidence verify` 的参数（取值未校验）。
#[derive(Debug, PartialEq, Eq)]
pub struct VerifyArgs {
    pub config: Option<PathBuf>,
    pub evidence: PathBuf,
    pub previous: Option<PathBuf>,
    pub retention: Option<OsString>,
}

/// 用法错误的说明（向 stderr 输出，随后以退出码 2 退出）。
#[derive(Debug, PartialEq, Eq)]
pub struct UsageError(pub String);

/// 解析不含程序名的参数。
pub fn parse(args: impl IntoIterator<Item = OsString>) -> Result<Command, UsageError> {
    let mut args = args.into_iter();
    let first = args
        .next()
        .ok_or_else(|| UsageError("missing command".into()))?;
    match first.to_str() {
        Some("--version") => match args.next() {
            None => Ok(Command::Version),
            Some(extra) => Err(unexpected(&extra)),
        },
        Some("serve") => parse_config(args).map(|config| Command::Serve { config }),
        Some("catalog") => parse_config(args).map(|config| Command::Catalog { config }),
        Some("retire") => parse_config(args).map(|config| Command::Retire { config }),
        Some("evidence") => match args.next().as_ref().and_then(|a| a.to_str()) {
            Some("export") => parse_export(args),
            Some("verify") => parse_verify(args),
            Some(other) => Err(UsageError(format!("unknown evidence command {other:?}"))),
            None => Err(UsageError("evidence requires export or verify".into())),
        },
        _ => Err(UsageError(format!(
            "unknown command {:?}",
            first.to_string_lossy()
        ))),
    }
}

/// 子命令之后只认一个可选的 `--config <path>`（或 `--config=<path>`）。
fn parse_config(mut args: impl Iterator<Item = OsString>) -> Result<Option<PathBuf>, UsageError> {
    let mut config: Option<PathBuf> = None;
    while let Some(arg) = args.next() {
        let path = match arg.to_str() {
            Some("--config") => args
                .next()
                .ok_or_else(|| UsageError("--config requires a path".into()))?,
            Some(s) if s.starts_with("--config=") => OsString::from(&s["--config=".len()..]),
            _ => return Err(unexpected(&arg)),
        };
        if path.is_empty() {
            return Err(UsageError("--config requires a path".into()));
        }
        if config.replace(PathBuf::from(path)).is_some() {
            return Err(UsageError("--config given more than once".into()));
        }
    }
    Ok(config)
}

/// 按名字收集 `--name value` 与 `--name=value`（二者等价）；`names` 之外的参数、缺值与重复都是用法错误。
fn options(
    mut args: impl Iterator<Item = OsString>,
    names: &[&str],
) -> Result<std::collections::BTreeMap<String, OsString>, UsageError> {
    let mut got = std::collections::BTreeMap::new();
    while let Some(arg) = args.next() {
        let text = arg.to_string_lossy();
        let (name, value) = match text.split_once('=') {
            Some((n, v)) if names.contains(&n) => (n.to_owned(), OsString::from(v)),
            _ if names.contains(&text.as_ref()) => {
                let v = args
                    .next()
                    .ok_or_else(|| UsageError(format!("{text} requires a value")))?;
                (text.into_owned(), v)
            }
            _ => return Err(unexpected(&arg)),
        };
        if value.is_empty() {
            return Err(UsageError(format!("{name} requires a value")));
        }
        if got.insert(name.clone(), value).is_some() {
            return Err(UsageError(format!("{name} given more than once")));
        }
    }
    Ok(got)
}

fn required(
    got: &mut std::collections::BTreeMap<String, OsString>,
    name: &str,
) -> Result<OsString, UsageError> {
    got.remove(name)
        .ok_or_else(|| UsageError(format!("{name} is required")))
}

fn parse_export(args: impl Iterator<Item = OsString>) -> Result<Command, UsageError> {
    let mut got = options(
        args,
        &["--config", "--dataset", "--from", "--to", "--output"],
    )?;
    Ok(Command::EvidenceExport(ExportArgs {
        dataset: required(&mut got, "--dataset")?,
        output: required(&mut got, "--output")?.into(),
        config: got.remove("--config").map(PathBuf::from),
        from: got.remove("--from"),
        to: got.remove("--to"),
    }))
}

fn parse_verify(args: impl Iterator<Item = OsString>) -> Result<Command, UsageError> {
    let mut got = options(
        args,
        &["--config", "--evidence", "--previous", "--retention"],
    )?;
    Ok(Command::EvidenceVerify(VerifyArgs {
        evidence: required(&mut got, "--evidence")?.into(),
        config: got.remove("--config").map(PathBuf::from),
        previous: got.remove("--previous").map(PathBuf::from),
        retention: got.remove("--retention"),
    }))
}

fn unexpected(arg: &OsString) -> UsageError {
    UsageError(format!("unexpected argument {:?}", arg.to_string_lossy()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(args: &[&str]) -> Result<Command, UsageError> {
        parse(args.iter().map(OsString::from))
    }

    #[test]
    fn accepted_forms() {
        assert_eq!(p(&["--version"]), Ok(Command::Version));
        assert_eq!(p(&["serve"]), Ok(Command::Serve { config: None }));
        let with = Ok(Command::Serve {
            config: Some("a.toml".into()),
        });
        assert_eq!(p(&["serve", "--config", "a.toml"]), with);
        assert_eq!(p(&["serve", "--config=a.toml"]), with);
        assert_eq!(p(&["catalog"]), Ok(Command::Catalog { config: None }));
        assert_eq!(
            p(&["catalog", "--config", "c.toml"]),
            Ok(Command::Catalog {
                config: Some("c.toml".into())
            })
        );
        assert_eq!(p(&["retire"]), Ok(Command::Retire { config: None }));
        assert_eq!(
            p(&["retire", "--config", "a.toml"]),
            Ok(Command::Retire {
                config: Some("a.toml".into())
            })
        );
        assert_eq!(
            p(&[
                "evidence",
                "export",
                "--dataset=default/app",
                "--output",
                "e.tar",
                "--from",
                "f"
            ]),
            Ok(Command::EvidenceExport(ExportArgs {
                config: None,
                dataset: "default/app".into(),
                from: Some("f".into()),
                to: None,
                output: "e.tar".into(),
            }))
        );
        assert_eq!(
            p(&["evidence", "verify", "--evidence", "p", "--retention=3h"]),
            Ok(Command::EvidenceVerify(VerifyArgs {
                config: None,
                evidence: "p".into(),
                previous: None,
                retention: Some("3h".into()),
            }))
        );
    }

    #[test]
    fn usage_errors() {
        for args in [
            &[][..],
            &["frobnicate"],
            &["serve", "--bogus"],
            &["serve", "--config", "a.toml", "--config", "a.toml"],
            &["serve", "--config=a.toml", "--config", "b.toml"],
            &["serve", "--config"],
            &["serve", "--config="],
            &["--version", "x"],
            &["--help"],
            &["serve", "--help"],
            &["catalogue"],
            &["catalog", "--bogus"],
            &["catalog", "--config"],
            &["retire", "--bogus"],
            &["retire", "--config"],
            &["evidence"],
            &["evidence", "check"],
            &["evidence", "export", "--output", "a.tar"],
            &["evidence", "export", "--dataset", "default/app"],
            &[
                "evidence",
                "export",
                "--dataset",
                "default/app",
                "--output",
                "b",
                "--verbose",
            ],
            &[
                "evidence",
                "export",
                "--dataset=a",
                "--dataset",
                "b",
                "--output",
                "c",
            ],
            &["evidence", "verify"],
            &["evidence", "verify", "--evidence", "p", "--strict"],
        ] {
            assert!(p(args).is_err(), "{args:?}");
        }
    }
}

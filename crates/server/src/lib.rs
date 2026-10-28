//! 接入面（Gateway）、生命周期编排（Lifecycle）与健康与指标（Telemetry）、告警（[`alerting`]）、检索界面的页面（[`ui`]），以及可执行文件 `amberlog` 的入口。

pub mod alerting;
pub mod api;
pub mod catalog;
pub mod cli;
pub mod config;
pub mod form;
pub mod gateway;
pub mod logging;
pub mod shutdown;
pub mod startup;
pub mod telemetry;
pub mod ui;
pub mod verdicts;
pub mod version;

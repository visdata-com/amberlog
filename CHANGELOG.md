# Changelog / 变更记录

This file records the user-visible changes of each release. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

Versions follow [Semantic Versioning](https://semver.org/). Before 1.0:

- A minor version bump (for example 0.1.x → 0.2.0) can contain incompatible changes, including changes to APIs, configuration keys, the command line and data formats.
- A patch version bump (for example 0.1.0 → 0.1.1) contains only compatible fixes.

The software version and the version of the storage format are independent of each other.

本文件记录每个版本对使用者可见的变化，只用英文写。版本号遵循[语义化版本](https://semver.org/lang/zh-CN/)：1.0 之前，次版本号加一可能有不兼容的改动，修订号加一只有兼容的修复。软件版本号和存储格式的版本号各自独立。

## [Unreleased]

## [0.1.0] - 2026-10-28

### Added

- First public release.
- Log ingestion (JSON). Within the throughput limit, acknowledged records can be searched within 10 seconds; when the backlog exceeds its memory limit, overflow records are kept only in the WAL and can be searched only after they are read back into memory (see section 4.1 of Known limitations). Hot-tier storage (object storage plus a local write-ahead log).
- Search: an SQL search API, and a search UI in the browser at `/ui` (search, ledger and reconciliation screens). One search covers both the hot tier and the cold tier.
- Cold tier: an hour bucket is converted into an Apache Iceberg table after it passes the lake-entry watermark.
- External engines read the cold tier directly: through the in-bucket convention, or through the read-only Iceberg REST catalog endpoints.
- Deletion when the retention period expires, with deletion records.
- Export and independent verification of audit evidence (`amberlog evidence export`, `amberlog evidence verify`).
- Graceful retirement (`amberlog retire`): converts all data into Iceberg tables before handover.
- Dataset ledger and ingestion reconciliation (system tables `_ledger` and `_reconciliation`): per minute, compares the sent count that the collector reports with the confirmed count, and gives a verdict. Reconciliation compares only counts; it cannot prove that the collector missed nothing.
- Scheduled alerting with webhook notifications; a built-in alert rule `missed_collection` (off by default).
- Inbound TLS and server-side encryption for object storage; health checks and Prometheus metrics; Bearer token authentication.

---

Apache and Apache Iceberg are registered trademarks or trademarks of the Apache Software Foundation in the United States and other countries. Amberlog is not affiliated with or endorsed by the Apache Software Foundation. / Apache、Apache Iceberg 是 Apache 软件基金会在美国和其他国家的注册商标或商标。Amberlog 与 Apache 软件基金会没有隶属关系，也未获其认可。

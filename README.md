# Amberlog

[中文](README.zh-CN.md)

**Amberlog — a data-sovereignty-first platform for long-term data preservation.** Your data is kept for the long term in open formats on your own object storage, at low cost and with provable audit integrity: if the vendor disappears or the product is retired, the retained data stays complete, readable and auditable. The current first workload is enterprise logs (stored in the open table format Apache Iceberg, which any standard engine can read directly).

Written in Rust and released under the Apache License 2.0.

## Status

This is the first public release of Amberlog, at an early stage. Before you deploy, read [Known limitations and roadmap](docs/known-limitations.md) and decide how far to use it in your environment.

## Data sovereignty

- **Format**: retained data is stored as standard Apache Iceberg tables, which any engine that supports Iceberg can read directly. Engines such as DuckDB and Apache Spark find the tables directly by the in-bucket convention and do not need Amberlog to be running; Trino finds them through Amberlog's read-only catalog process, which only reads object storage and can run separately from the write instance (see the [External engine guide](docs/external-engines.md)).
- **Storage**: data lives in your own object-storage bucket (tested on MinIO; AWS S3 and Ceph RGW are supported through the S3-compatible API but not yet tested). Object storage is the only long-term location; the local machine keeps only a short-lived buffer (the write-ahead log) between acknowledgement and commit.
- **Code**: Apache-2.0, so you can audit, modify and maintain it yourself.

Open direct reads apply to retained data, that is, data that has been converted into Iceberg tables. Recently written data first sits in Amberlog's own hot tier (by default it moves to the cold tier 72 hours after the end of its hour), which external engines cannot read; when you stop using Amberlog, graceful retirement converts that data into Iceberg tables too.

## Design: hot and cold tiers

Log ingestion is frequent, comes in small batches, and must be searchable soon after it is written. Iceberg commits are designed for batch processing: frequent small commits bloat the metadata and conflict with each other. Amberlog resolves this with two tiers:

- The **hot tier** takes the frequent writes with its own lightweight manifests. Within the throughput limit, acknowledged records can be searched within 10 seconds; when the backlog exceeds its memory limit, overflow records are kept only in the WAL and can be searched only after they are read back into memory (see section 4.1 of [Known limitations](docs/known-limitations.md)).
- The **cold tier** receives the data in hourly batches; a single rewrite both compacts the files and commits them to Iceberg.

The Iceberg tables of the cold tier are the primary copy of the data, not an export next to private storage: there is no second copy and no inconsistency window caused by export delay, so the audit evidence chain is complete.

## Getting started

### Quickstart

You need Docker with the Compose plugin:

```sh
git clone https://github.com/visdata-com/amberlog.git
cd amberlog/examples/quickstart
docker compose up -d --wait
./demo.sh
```

The script writes a few records, searches them with Amberlog, and then reads the Iceberg table in object storage directly with DuckDB. See [Quickstart](examples/quickstart/README.md).

### Single binary

Download the package for your platform from [Releases](https://github.com/visdata-com/amberlog/releases) (Linux x86_64 or aarch64, glibc 2.28 or later), unpack it, and try it with local disk storage. The commands use the x86_64 package name and also need `openssl` (to generate the token) and `curl`:

```sh
tar -xzf amberlog-0.1.0-x86_64-unknown-linux-gnu.tar.gz
cd amberlog-0.1.0-x86_64-unknown-linux-gnu
TOKEN=$(openssl rand -hex 32)
echo "$TOKEN"    # must not be empty; you need it again in the second terminal
mkdir -p /tmp/amberlog/store
AMBERLOG_OBJECT_STORE__URL=file:///tmp/amberlog/store \
AMBERLOG_WAL__DIR=/tmp/amberlog/wal \
AMBERLOG_AUTH__TOKEN="$TOKEN" \
AMBERLOG_SERVER__HTTP_ADDR=127.0.0.1:8800 \
  ./amberlog serve
```

Write and search in another terminal. Set `TOKEN` there to the same value first:

```sh
TOKEN=<the token printed in the first terminal>
curl -H "Authorization: Bearer $TOKEN" \
  -d '[{"level":"info","message":"hello"}]' \
  http://127.0.0.1:8800/api/default/app/_json

curl -H "Authorization: Bearer $TOKEN" \
  -d '{"query":{"sql":"SELECT level, message FROM app","start_time":0,"end_time":4102444800000000}}' \
  http://127.0.0.1:8800/api/default/_search
```

```text
{"code":200,"status":[{"name":"app","successful":1,"failed":0,"errors":[]}]}
{"took":12,"hits":[{"level":"info","message":"hello"}],"total":1,"from":0,"size":100,"scan_records":1,"scan_files":0,"scan_bytes":0}
```

`start_time` and `end_time` are UTC microseconds; the range includes the start and excludes the end. You can also open `http://127.0.0.1:8800/ui` in a browser, enter the token and search there.

For production deployment (object storage, TLS, systemd, containers), see the [Installation and deployment guide](docs/install.md).

## Documentation

Every document below, except the changelog, has a Chinese version next to it (`*.zh-CN.md`).

| Document | Contents |
|---|---|
| [Installation and deployment guide](docs/install.md) | Binary and container installation, object storage, read-only credentials for external engines, TLS, capacity planning |
| [Configuration reference](docs/configuration.md) | All configuration keys, environment variables, startup checks and exit codes |
| [API reference](docs/api.md) | Ingestion, search, system tables, errors, operational endpoints, the Iceberg read-only catalog |
| [Search UI and alerting](docs/ui-and-alerting.md) | The search, ledger and reconciliation screens; scheduled alerting |
| [Operations guide](docs/operations.md) | Health checks, metrics and alerts, shutdown and upgrade, audit evidence, graceful retirement |
| [External engine guide](docs/external-engines.md) | Reading the cold-tier tables with DuckDB, Spark, Trino, Apache Flink, Apache Doris, ClickHouse and StarRocks |
| [Known limitations and roadmap](docs/known-limitations.md) | What this release cannot do yet, and where it is heading |
| [Collector examples](examples/collectors/README.md) | Configuration for Vector and Fluent Bit |
| [Changelog](CHANGELOG.md) | Changes in each release |

## Contributing

- Questions and suggestions: see [Support](SUPPORT.md).
- Security issues: report them privately as described in the [Security policy](SECURITY.md); do not open a public issue.
- Code contributions: see the [Contributing guide](CONTRIBUTING.md) and the [Code of conduct](CODE_OF_CONDUCT.md).

## License and acknowledgements

Amberlog is released under the [Apache License 2.0](LICENSE). It is built on many open-source projects; see [Acknowledgements and trademarks](ACKNOWLEDGEMENTS.md).

Apache, Apache Iceberg, Apache Spark, Apache Flink and Apache Doris are registered trademarks or trademarks of the Apache Software Foundation in the United States and other countries. Other names are trademarks of their respective owners. Amberlog is not affiliated with or endorsed by the Apache Software Foundation or these owners.

# Quickstart

[中文](README.zh-CN.md)

> Applies to Amberlog 0.1.0.

This directory uses Docker Compose to start a trial environment on your computer. It demonstrates three things:

1. Ingestion: write JSON logs into Amberlog.
2. Search: search the logs you just wrote with SQL.
3. External engines reading the cold tier: once older logs enter the cold tier, they form a standard Apache Iceberg table in object storage. DuckDB reads this table directly with a read-only credential, without going through Amberlog.

**This environment is for trial use only. Do not use it in production.** The passwords and the token are example values written into the files, and Amberlog listens with plain HTTP. The ports are open only on `127.0.0.1` of your machine.

## 1. What is in the environment

| Service | Image | Role |
|---|---|---|
| `minio` | `pgsty/silo` (RELEASE.2026-10-18T00-00-00Z, pinned by digest) | Object storage: Silo, a community fork of MinIO maintained by PGSTY. All Amberlog data is in the storage bucket `amberlog-data` |
| `setup` | The same `pgsty/silo` image, which includes `mc` | One-off job: creates the storage bucket and two users, then exits |
| `amberlog` | `ghcr.io/visdata-com/amberlog:latest` | The Amberlog write instance |
| `duckdb` | Built from `duckdb/` in this directory: `python:3.12-slim` plus DuckDB 1.5.6 | External query engine. Keeps running; run queries with the `lake-query` command |

Accounts, passwords and the token:

| Purpose | User name | Password or token | Permissions |
|---|---|---|---|
| Silo administrator, for the console | `quickstart-admin` | `quickstart-admin-secret` | All |
| Amberlog accessing Silo | `amberlog-app` | `amberlog-app-secret` | Only the storage bucket `amberlog-data`, read and write |
| DuckDB accessing Silo | `lake-reader` | `lake-reader-secret` | Only reading and listing the cold-tier tables under `amberlog-data/amberlog/_iceberg` |
| Clients accessing Amberlog | — | Token `quickstart-token` | Request header `Authorization: Bearer quickstart-token` |

Local ports:

| Address | What it is |
|---|---|
| `http://127.0.0.1:8800` | Amberlog: ingestion, search, metrics, the search UI `/ui`, the read-only catalog `/iceberg/` |
| `http://127.0.0.1:9000` | The Silo S3 API |
| `http://127.0.0.1:9001` | The Silo console |

## 2. Prerequisites

- Docker Engine with the Compose plugin (the `docker compose` command). Tested with Docker Compose v5.6.0.
- `curl`.
- Ports 8800, 9000 and 9001 on your machine are free.
- Network access on the first start: the images are pulled, and building the DuckDB image installs `duckdb` from PyPI and downloads three DuckDB extensions.

The Amberlog image is available for `linux/amd64` and `linux/arm64`. This guide was tested on `linux/amd64`.

## 3. Start

```sh
cd examples/quickstart
docker compose up -d --wait
```

`--wait` returns after Amberlog is ready. The first start pulls the images and builds the DuckDB image, which downloads DuckDB from PyPI. In our tests this took about 2 to 6 minutes longer, depending on the network.

To use a specific Amberlog image version, set the environment variable `QUICKSTART_IMAGE`:

```sh
QUICKSTART_IMAGE=ghcr.io/visdata-com/amberlog:0.1.0 docker compose up -d --wait
```

## 4. Demo

```sh
./demo.sh
```

The script expects an empty environment. It runs these five steps:

1. Waits until Amberlog is ready.
2. Writes 5 records to the dataset `default/app`: 3 with a time 4 days ago, 2 with the current time.
3. Searches the last 5 days with Amberlog: all 5 records, old and new, are found.
4. Waits until the 3 records from 4 days ago enter the cold tier.
5. Reads the cold-tier table directly with DuckDB: only these 3 old records are visible.

The script prints its step headings in Chinese. Example output (`took`, the timestamps and the wait time differ on each run):

```text
== 1. 等 Amberlog 就绪
{"status":"ready"}

== 2. 写入 5 条记录到数据集 default/app：3 条是 4 天前的，2 条是现在的
{"code":200,"status":[{"name":"app","successful":5,"failed":0,"errors":[]}]}

== 3. 用 Amberlog 检索最近 5 天：新旧记录都能查到
{"took":20,"hits":[{"level":"error","n":2},{"level":"info","n":2},{"level":"warn","n":1}],"total":3,"from":0,"size":100,"scan_records":5,"scan_files":0,"scan_bytes":0}
{"took":9,"hits":[{"_timestamp":1792224601000000,"level":"error","message":"db timeout"},{"_timestamp":1792572176000000,"level":"error","message":"disk full"}],"total":2,"from":0,"size":100,"scan_records":5,"scan_files":0,"scan_bytes":0}

== 4. 等 4 天前的记录进入冷层（最多 3 分钟）
冷层表 default.app 已出现（等了约 9 秒）

== 5. 用 DuckDB 和只读凭据直接读对象存储里的冷层表，不经过 Amberlog：只看得到已入湖的旧记录
┌─────────┬───────┐
│  level  │   n   │
│ varchar │ int64 │
├─────────┼───────┤
│ error   │     1 │
│ info    │     1 │
│ warn    │     1 │
└─────────┴───────┘

┌──────────────────────────┬─────────┬──────────────┬───────────┬────────────┐
│        _timestamp        │  level  │   message    │ user.name │ latency_ms │
│ timestamp with time zone │ varchar │   varchar    │  varchar  │   double   │
├──────────────────────────┼─────────┼──────────────┼───────────┼────────────┤
│ 2026-10-17 08:10:00+00   │ info    │ user login   │ alice     │       NULL │
│ 2026-10-17 08:10:01+00   │ error   │ db timeout   │ NULL      │     3000.0 │
│ 2026-10-17 08:10:02+00   │ warn    │ slow request │ NULL      │      870.0 │
└──────────────────────────┴─────────┴──────────────┴───────────┴────────────┘
```

In step 3, the search finds 2 records with `level` `error`; in step 5, DuckDB reads only the one from 4 days ago.

Before you run the demo again, delete the data (see section 7) and start again.

## 5. Why only the old records enter the cold tier

Amberlog groups data into hour buckets. An hour bucket enters the cold tier 72 hours after the hour ends; this moment is the lake-entry watermark. 72 hours is the default of the configuration key `lifecycle.lake_watermark`.

- The records from 4 days ago are already past the lake-entry watermark, so Amberlog writes them into the cold-tier table soon after it starts.
- The current records are still in the hot tier. Amberlog's own search reads both the hot and the cold tier, so step 3 finds all 5 records. External engines read only the cold-tier table and will see these 2 records after 72 hours.

The configuration keys are described in the [Configuration reference](../../docs/configuration.md).

## 6. Try it yourself

The examples below first put the token into a variable:

```sh
TOKEN=quickstart-token
```

### 6.1 Ingest

```sh
curl -sS -X POST http://127.0.0.1:8800/api/default/app/_json \
  -H "Authorization: Bearer $TOKEN" \
  -d '[{"level": "info", "message": "hello from curl"}]'
```

A record without `_timestamp` takes the time it was received. The request format is in section 2 of the [API reference](../../docs/api.md).

### 6.2 Search

`start_time` and `end_time` are UTC microseconds:

```sh
now=$(($(date +%s) * 1000000))
curl -sS -X POST http://127.0.0.1:8800/api/default/_search \
  -H "Authorization: Bearer $TOKEN" \
  -d "{\"query\": {\"sql\": \"SELECT _timestamp, level, message FROM app ORDER BY _timestamp DESC\",
       \"start_time\": $((now - 3600000000)), \"end_time\": $((now + 60000000))}}"
```

You can also open `http://127.0.0.1:8800/ui` in a browser, enter the token `quickstart-token` on the page and search there.

### 6.3 Read the cold-tier table with DuckDB

```sh
docker compose exec duckdb lake-query \
  "SELECT count(*) FROM iceberg_scan('s3://amberlog-data/amberlog/_iceberg/default/app')"
```

`lake-query` runs a piece of SQL, which may contain several statements. It first creates a DuckDB S3 secret with the read-only credential `lake-reader`, then runs your SQL.

The statement above finds the table by the in-bucket convention: it names the table directory directly. A cold-tier table lives at `s3://<bucket>/<storage.prefix>/_iceberg/<org>/<stream>` in the storage bucket. This way does not need any Amberlog process to be running.

You can also find the table through Amberlog's read-only Iceberg catalog. Then Amberlog must be running and you pass the token; DuckDB still reads the data files directly from Silo with the read-only credential:

```sh
docker compose exec duckdb lake-query "
  ATTACH 'amberlog' AS amb (TYPE ICEBERG, ENDPOINT 'http://amberlog:8800/iceberg',
      TOKEN 'quickstart-token', ACCESS_DELEGATION_MODE 'none');
  SELECT count(*) FROM amb.default.app"
```

The catalog endpoints are in section 9 of the [API reference](../../docs/api.md).

Try the limits of the read-only credential: `lake-reader` cannot read the hot tier and cannot write under the prefix of the cold-tier tables. Both statements below fail; Silo denies access:

```sh
docker compose exec duckdb lake-query \
  "SELECT count(*) FROM read_parquet('s3://amberlog-data/amberlog/default/app/**/*.parquet')"
docker compose exec duckdb lake-query \
  "COPY (SELECT 1 AS x) TO 's3://amberlog-data/amberlog/_iceberg/default/app/data/x.parquet'"
```

For reading the cold-tier tables with other engines, such as Apache Spark, Trino, Apache Flink and Apache Doris, see the [External engine guide](../../docs/external-engines.md).

### 6.4 Look at the objects in the Silo console

Open `http://127.0.0.1:9001` in a browser and sign in as `quickstart-admin`. In the storage bucket `amberlog-data`:

- `amberlog/_iceberg/default/app/` is the cold-tier table: `metadata/` holds the Iceberg metadata and `data/` the Apache Parquet data files.
- `amberlog/default/app/` is the hot tier.

### 6.5 Look at the logs

```sh
docker compose logs amberlog
```

Two `warn` lines at startup are expected in this environment: `plaintext_listen` (listening with plain HTTP) and `object_store_plaintext` (connecting to Silo with plain HTTP). Section 8 explains why.

## 7. Stop and clean up

| Command | Effect |
|---|---|
| `docker compose stop` | Stops the containers and keeps the data |
| `docker compose down` | Removes the containers and the network and keeps the data volumes. The data is still there after the next `up` |
| `docker compose down -v` | Removes the containers, the network and both data volumes; all data is deleted |
| `docker image rm amberlog-quickstart-duckdb` | Removes the locally built DuckDB image |

## 8. Differences from a production deployment

To start with one command, this environment takes some shortcuts that a production deployment must not take:

- Amberlog listens with plain HTTP and declares `server.behind_tls_proxy = true`, although there is no reverse proxy in front of it. In production, configure a certificate, or really put Amberlog behind a reverse proxy that terminates TLS.
- Silo uses plain HTTP, a single node and a single disk.
- The passwords and the token are example values written into `compose.yaml`. In production, use randomly generated values, put them into configuration files that only the service user can read, and keep them out of compose files and environment variables.
- All Amberlog configuration is given as environment variables. The configuration keys and their environment variables are in the [Configuration reference](../../docs/configuration.md).

External engines must use a read-only credential, never Amberlog's own read-write credential. The scope of the read-only credential is exactly the policy that the `setup` service in `compose.yaml` gives to `lake-reader`.

For the production deployment steps, see the [Installation and deployment guide](../../docs/install.md).

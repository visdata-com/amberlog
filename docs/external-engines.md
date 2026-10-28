# External engine guide

[中文](external-engines.zh-CN.md)

> Applies to Amberlog 0.1.0.

This guide is for people who want to read Amberlog data with other query engines: where the cold-tier tables are, how to find them, how to configure each engine and write the SQL, and which differences are known. This guide covers DuckDB, Apache Spark, Trino, Apache Flink, Apache Doris, ClickHouse and StarRocks. The configuration and the results for ClickHouse and StarRocks come from tests on an earlier version; they were not tested again on this version (section 10).

How to create the read-only credential and start the read-only catalog mode is described in section 7 of the [Installation and deployment guide](install.md). The catalog endpoints are in section 9 of the [API reference](api.md).

## 1. Read this first

- **External engines can read only the cold tier.** An hour bucket enters the lake, and becomes part of a cold-tier table, only `lifecycle.lake_watermark` (default 72 hours) after the hour bucket ends. Until then the data is in the hot tier and can be searched only through Amberlog (section 2.1).
- **Engines that find tables by the in-bucket convention do not need Amberlog to be running.** They read the data files directly from object storage. Trino is the exception: it finds tables through Amberlog's catalog endpoint. A separate process in read-only catalog mode can provide the catalog endpoint; this process only reads object storage and can run separately from the write instance (section 3).
- **External engines must use a read-only credential.** The read-only credential grants only the cold-tier prefix: the key `<storage.prefix>/_iceberg` itself and everything under `<storage.prefix>/_iceberg/`. Do not give Amberlog's own read-write credential to an external engine (section 7.1 of the [Installation and deployment guide](install.md)).
- **Column names are kept as they are**, including dots, hyphens and capital letters; quote them in SQL (section 4).
- **Do not use external engines to change cold-tier tables.** Writes, dropping tables, expiring snapshots, rewriting files and removing orphans are all done by Amberlog itself (section 9).

## 2. Cold-tier tables

### 2.1 When data enters a cold-tier table

- Lake-entry time = end of the hour bucket + `lifecycle.lake_watermark` (default 72 hours). The hour buckets of one dataset enter the lake in order, oldest first.
- When late records arrive for an hour bucket that has already entered the lake, the late records go to the hot tier first and enter the lake again later.
- A dataset that has never entered the lake has no cold-tier table.
- The metric `amberlog_lake_lag_seconds{org,stream}` is the lake-entry backlog: how many seconds the oldest hour bucket that is due but has not entered the lake is overdue. It is 0 when there is no backlog (section 4.2 of the [Operations guide](operations.md)).

Amberlog's own search reads both the hot and the cold tier. External engines read only the cold tier, so for the same time range an external engine may see fewer records than Amberlog finds.

When you stop using Amberlog, `amberlog retire` moves all hot-tier data into the cold-tier tables; after that, external engines can read all the data (section 7 of the [Operations guide](operations.md)).

To see a cold-tier table soon in a trial, use one of these two ways:

- Write records whose `_timestamp` is more than 72 hours in the past. Their hour bucket is already past the lake-entry watermark, so they enter the lake a few seconds after they are written. The [Quickstart](../examples/quickstart/README.md) does this (section 5 there). If their hour bucket ended more than `lifecycle.lake_watermark` plus `lifecycle.lake_alert_after` ago (96 hours with the defaults), the log also has one `lake_backlog_high` warning.
- In a trial environment only, shorten the two keys, for example `lifecycle.seal_grace = "1m"` and `lifecycle.lake_watermark = "2m"` (the watermark must be greater than the seal grace; see the [Configuration reference](configuration.md)). Records of the current hour then enter the lake a few minutes after the hour ends, so you may have to wait up to about one hour. Do not use these values in production.

### 2.2 Location and layout

There is one cold-tier table per dataset, in the Apache Iceberg table format. The Iceberg namespace is the organization name (only `default`), and the table name is the dataset name.

For example, with `object_store.url = "s3://logs"`, `storage.prefix = "amberlog"` and the dataset `default/app`:

```text
s3://logs/amberlog/_iceberg/default/app/                 ← table directory
  metadata/v1.metadata.json                               ← metadata version 1
  metadata/v2.metadata.json
  metadata/version-hint.text                              ← hint file; contains the current version number, e.g. "2"
  metadata/<manifest lists and manifests>.avro
  data/_timestamp_day=2026-10-15/<file id>.parquet         ← data files
```

- Only cold-tier tables are under `_iceberg/`. The hot tier and Amberlog's other objects are elsewhere.
- Each commit writes a new `v<N>.metadata.json`, numbered consecutively from 1, and then updates the hint file. The hint file can lag behind for a while; it then points to an older version that is still complete.
- The table location in the metadata is `s3://<bucket>/<storage.prefix>/_iceberg/<org>/<stream>` and does not contain the object-storage endpoint. External engines configure the endpoint themselves.
- Iceberg table format version 2. The partition spec has one field, `_timestamp_day`, the day of `_timestamp`. When you filter on `_timestamp`, engines can skip partitions by day.
- There are no position-delete files and no equality-delete files. Amberlog deletes data only by removing whole data files.
- The manifest entry of every data file has the lower and upper bounds of `_timestamp` and the null count of every column; engines use them to skip files they do not need.

### 2.3 Columns and types

| Amberlog type | Iceberg type |
|---|---|
| `_timestamp` | `timestamptz` (required column) |
| `Int64` | `long` |
| `Float64` | `double` |
| `Boolean` | `boolean` |
| `Utf8` | `string` |

- Once a column is in a cold-tier table, its name and type do not change. Columns are only added, never removed; new columns are appended.
- A data file can contain only some of the columns; missing columns read as NULL.
- When one field receives values of different types over time, the values of the other type go into a **shadow column** `<field name>__<type>`, where the type is `int64`, `float64`, `boolean` or `utf8`. For example, if `status` was an integer and a string value arrives later, the string goes into `status__utf8`.

### 2.4 Snapshots

| What Amberlog does | Snapshot summary `operation` | Snapshot summary `amberlog.operation` |
|---|---|---|
| Lake entry | `append` | `lake` |
| Deletion at expiry (when `retention.period` is set) | `delete` | `ttl` |
| Cold compaction (merging the small files of a day partition into larger files) | `replace` | `compaction` |

- Cold compaction does not change records. Programs that read increments only can skip snapshots whose `operation` is `replace`.
- Amberlog expires old snapshots according to `cold_retention.snapshots` (default 1) and `cold_retention.age` (default 168 hours). How far back you can read is in section 8.
- After all data has expired, the table still exists and queries return 0 rows.

## 3. Two ways to find a table

| Way | What it needs | Engines |
|---|---|---|
| **In-bucket convention**: the engine points at the table directory and determines the current version itself | The read-only credential | DuckDB, Spark, Flink, Doris, ClickHouse, StarRocks |
| **Catalog endpoint**: the engine finds tables through Amberlog's read-only Iceberg REST catalog `/iceberg` | The read-only credential; the catalog token; a running write instance or a running process in read-only catalog mode | The engines above, and Trino. Trino can use only this way |

Both ways read the same files. The catalog endpoint only reports the state in the bucket and stores nothing else.

### 3.1 In-bucket convention

Each engine determines the current version in its own way:

| Engine | How it determines the current version |
|---|---|
| DuckDB | Reads only the hint file. Without a hint file it reports an error and cannot read (section 7) |
| Spark, Flink, Doris, StarRocks (hadoop catalog) | Reads the hint file first, then looks for newer versions |
| ClickHouse (`icebergS3`) | Lists `metadata/` and takes the version with the highest number; does not need the hint file |
| Trino | Not supported |

Every engine can also read one specific version by giving the full path of a `v<N>.metadata.json`.

### 3.2 Catalog endpoint

- Address: `http(s)://<host>:<port>/iceberg`. The write instance (`amberlog serve`) always provides it, on the same listening address as `/api/`.
- Authentication: `Authorization: Bearer <token>`. The configuration keys of each engine are in section 5.
- `{prefix}` needs no configuration: `GET /iceberg/v1/config` returns `storage.prefix`, and clients use it automatically.
- `warehouse`: Amberlog ignores this parameter. Some engines require it; any value works.
- The catalog endpoint **does not hand out object-storage credentials**. Engines configure the read-only credential themselves.
- The catalog endpoint is read-only and does not provide creating, dropping or altering tables.

A token given to an external engine can also ingest and search on the write instance. To narrow the permissions, start a separate process in read-only catalog mode (`amberlog catalog`) with another token and the read-only credential. It provides only the catalog endpoint, can run at the same time as the write instance, and can also run alone after the write instance stops or is retired (section 7.2 of the [Installation and deployment guide](install.md)).

## 4. Column names, quoting and reserved words

The column names in the cold-tier tables are the same as in Amberlog, for example `kubernetes.pod.name`, `X-Request-Id`, `userAgent`. Quote such names in SQL:

| Engine | Quoting column names | Case of column names |
|---|---|---|
| DuckDB | Double quotes: `"X-Request-Id"` | Not case-sensitive |
| Spark | Backquotes: `` `X-Request-Id` `` | Not case-sensitive |
| Trino | Double quotes: `"X-Request-Id"` | Not case-sensitive; shown in lower case |
| Flink | Backquotes: `` `X-Request-Id` `` | **Case-sensitive**; write `userAgent` exactly |
| Doris | Backquotes: `` `X-Request-Id` `` | Not case-sensitive |
| ClickHouse | Backquotes or double quotes | **Case-sensitive**; write `userAgent` exactly |
| StarRocks | Backquotes: `` `X-Request-Id` `` | Not case-sensitive |

The namespace `default` and the dataset names appear as they are.

- `default` is a reserved word in Flink, Doris and StarRocks and must be in backquotes: `` <catalog>.`default`.`app` ``.
- When ClickHouse reads a table through the catalog endpoint, the namespace and the table name form one identifier: `` <database>.`default.app` ``.
- A dataset name that starts with a digit (for example `2fa_logs`) must be quoted in Trino: `"2fa_logs"`; in Flink it must be in backquotes. Doris 4.1.4.1 and StarRocks read it without quotes. Quote it in DuckDB and Spark as well.

### 4.1 Two columns that differ only in case

When two column names differ only in case (for example `Foo` and `foo`), the column that appeared first keeps its name in the cold-tier table and the later one is renamed `<original name>__2` (`__3` if that is taken, and so on). For example, `Foo` and `foo` are called `Foo` and `foo__2` in the cold-tier table.

- The renaming happens only in the cold-tier table. Amberlog's own search still uses the original name.
- The comment of a renamed column is its original name. How to see it:

| Engine | Statement | Tested result |
|---|---|---|
| DuckDB 1.5.6 (catalog endpoint) | Query the table once, then `SELECT column_name, comment FROM duckdb_columns() WHERE table_name = 'case_logs'` | The comment of `foo__2` is `foo` |
| DuckDB 1.4.5 | — | Not visible |
| Spark | `` DESCRIBE <catalog>.`default`.`case_logs` `` | The comment of `foo__2` is `foo` |
| Trino | `SHOW COLUMNS FROM <catalog>."default"."case_logs"` | The Comment of `foo__2` is `foo`; `Foo` is shown as `foo` |
| Doris 4.1.4.1 | `` SHOW FULL COLUMNS FROM <catalog>.`default`.`case_logs` `` | The Comment of `foo__2` is `foo`. `DESCRIBE` does not show it |
| Flink | — | Not visible; `DESCRIBE` does not show comments |
| StarRocks | `` SHOW FULL COLUMNS FROM <catalog>.`default`.`case_logs` `` | The Comment of `foo__2` is `foo` |
| ClickHouse | — | Not visible |

## 5. Engine by engine

The examples use these values; replace them with your own:

- Object-storage endpoint `https://minio.example.com:9000`, bucket `logs`, `storage.prefix` `amberlog`.
- Read-only credential `lake-reader` / `<read-only secret key>`.
- Catalog address `https://amberlog.example.com:8801/iceberg`, token `<catalog token>`.
- Dataset `default/app`.

The tests used plain-http endpoints (section 10). With https, the engine must trust the CA of the object-storage and Amberlog certificates.

### 5.1 DuckDB

Tested versions: 1.5.6 and 1.4.5 (Python package), with the `httpfs`, `avro` and `iceberg` extensions.

Install the Python package in a virtual environment. Some systems, such as Debian 13, refuse `pip install` into the system Python:

```sh
python3 -m venv ~/duckdb
~/duckdb/bin/pip install duckdb==1.5.6
```

Run the SQL below with `duckdb.connect().sql(...)` in `~/duckdb/bin/python`. The `INSTALL` statements download the three extensions from the internet the first time, so the machine needs internet access then.

First create the object-storage credential:

```sql
INSTALL httpfs; LOAD httpfs;
INSTALL avro; LOAD avro;
INSTALL iceberg; LOAD iceberg;
CREATE SECRET lake (TYPE S3, KEY_ID 'lake-reader', SECRET '<read-only secret key>',
    ENDPOINT 'minio.example.com:9000', URL_STYLE 'path', USE_SSL true, REGION 'us-east-1');
```

Way 1, in-bucket convention. Point `iceberg_scan` at the table directory:

```sql
SELECT count(*) FROM iceberg_scan('s3://logs/amberlog/_iceberg/default/app');
SELECT "X-Request-Id", "kubernetes.pod.name"
FROM iceberg_scan('s3://logs/amberlog/_iceberg/default/app')
WHERE _timestamp >= TIMESTAMPTZ '2026-10-16 00:00:00+00' LIMIT 10;
```

Way 2, catalog endpoint. `ACCESS_DELEGATION_MODE 'none'` tells DuckDB not to ask the catalog for object-storage credentials; the data files are read with the `lake` secret above:

```sql
CREATE SECRET amb_catalog (TYPE ICEBERG, TOKEN '<catalog token>');
ATTACH 'amberlog' AS amb (TYPE ICEBERG,
    ENDPOINT 'https://amberlog.example.com:8801/iceberg',
    SECRET amb_catalog, ACCESS_DELEGATION_MODE 'none');
SELECT count(*) FROM amb."default"."app";
```

The first argument of `ATTACH` is the warehouse; any value works.

Section 6.3 of the [Quickstart](../examples/quickstart/README.md) has a DuckDB environment you can run right away.

### 5.2 Spark

Tested versions: Spark 3.5.9 and 4.0.4 (official images), Iceberg 1.10.2. Jars to load:

| Spark | Jars |
|---|---|
| 3.5 | `iceberg-spark-runtime-3.5_2.12-1.10.2.jar`, `iceberg-aws-bundle-1.10.2.jar`, `hadoop-aws-3.3.4.jar`, `aws-java-sdk-bundle-1.12.262.jar` |
| 4.0 | `iceberg-spark-runtime-4.0_2.13-1.10.2.jar`, `iceberg-aws-bundle-1.10.2.jar`, `hadoop-aws-3.4.1.jar`, and `bundle-2.24.6.jar` from AWS SDK 2.24.6 |

Common setting:

```text
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

Way 1, in-bucket convention: a hadoop catalog that reads object storage through S3A.

```text
spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.endpoint=https://minio.example.com:9000
spark.hadoop.fs.s3a.endpoint.region=us-east-1
spark.hadoop.fs.s3a.path.style.access=true
spark.hadoop.fs.s3a.access.key=lake-reader
spark.hadoop.fs.s3a.secret.key=<read-only secret key>
spark.sql.catalog.amb_lake=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.amb_lake.type=hadoop
spark.sql.catalog.amb_lake.warehouse=s3://logs/amberlog/_iceberg
```

Query: ``SELECT `X-Request-Id` FROM amb_lake.`default`.`app` LIMIT 10``.

Way 2, catalog endpoint. Object storage is read by Iceberg's own S3FileIO, with the credential in the catalog settings:

```text
spark.sql.catalog.amb=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.amb.type=rest
spark.sql.catalog.amb.uri=https://amberlog.example.com:8801/iceberg
spark.sql.catalog.amb.warehouse=amberlog
spark.sql.catalog.amb.token=<catalog token>
spark.sql.catalog.amb.token-refresh-enabled=false
spark.sql.catalog.amb.io-impl=org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.amb.s3.endpoint=https://minio.example.com:9000
spark.sql.catalog.amb.s3.path-style-access=true
spark.sql.catalog.amb.s3.access-key-id=lake-reader
spark.sql.catalog.amb.s3.secret-access-key=<read-only secret key>
spark.sql.catalog.amb.client.region=us-east-1
```

Query: ``SELECT `X-Request-Id`, `kubernetes.pod.name` FROM amb.`default`.`app` LIMIT 10``.

- `token-refresh-enabled=false`: Amberlog tokens are static and there is no refresh endpoint.
- A `count(*)` without a filter is answered from the counts in the manifests, without reading data files.
- Metadata tables work as usual, for example `` SELECT operation, summary['amberlog.operation'] FROM amb.`default`.`app`.snapshots ``; read an older snapshot by its snapshot id with `VERSION AS OF <snapshot id>`.

### 5.3 Trino

Tested version: 483. Trino can find tables only through the catalog endpoint.

Catalog file `etc/catalog/amb.properties`:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=https://amberlog.example.com:8801/iceberg
iceberg.rest-catalog.warehouse=amberlog
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.token=<catalog token>
iceberg.rest-catalog.oauth2.token-refresh-enabled=false
fs.s3.enabled=true
s3.endpoint=https://minio.example.com:9000
s3.region=us-east-1
s3.path-style-access=true
s3.aws-access-key=lake-reader
s3.aws-secret-key=<read-only secret key>
```

Query: `SELECT "X-Request-Id", "kubernetes.pod.name" FROM amb."default"."app" LIMIT 10`.

- Trino 483 enables S3 with `fs.s3.enabled`. The old key `fs.native-s3.enabled` produces a warning at startup.
- With an https catalog address, Trino's JVM must trust the CA of the Amberlog certificate.
- Metadata tables work as usual, for example `SELECT operation FROM amb."default"."app$snapshots"`.

### 5.4 Flink

Tested version: Flink 1.20.5 (official image `flink:1.20-java17`), batch mode. Jars to put into `lib/`: `iceberg-flink-runtime-1.20-1.10.2.jar`, `iceberg-aws-bundle-1.10.2.jar`, `hadoop-client-api-3.3.4.jar`, `hadoop-client-runtime-3.3.4.jar`, `hadoop-aws-3.3.4.jar`, `aws-java-sdk-bundle-1.12.262.jar`.

Way 1, in-bucket convention: a hadoop catalog.

1. Configure S3A in the Apache Hadoop configuration file `core-site.xml`:

   ```xml
   <configuration>
     <property><name>fs.s3.impl</name><value>org.apache.hadoop.fs.s3a.S3AFileSystem</value></property>
     <property><name>fs.s3a.endpoint</name><value>https://minio.example.com:9000</value></property>
     <property><name>fs.s3a.endpoint.region</name><value>us-east-1</value></property>
     <property><name>fs.s3a.path.style.access</name><value>true</value></property>
     <property><name>fs.s3a.access.key</name><value>lake-reader</value></property>
     <property><name>fs.s3a.secret.key</name><value>&lt;read-only secret key&gt;</value></property>
   </configuration>
   ```

2. Set the environment variable `HADOOP_CONF_DIR` to the directory that contains `core-site.xml`. Alternatively, give this directory with `'hadoop-conf-dir'` when you create the catalog.
3. Create the catalog and query:

   ```sql
   CREATE CATALOG amb_lake WITH (
     'type' = 'iceberg', 'catalog-type' = 'hadoop',
     'warehouse' = 's3://logs/amberlog/_iceberg');
   SELECT `X-Request-Id`, `userAgent` FROM amb_lake.`default`.`app` LIMIT 10;
   ```

The read-only credential needs a grant on the prefix key `amberlog/_iceberg` itself (section 7.1 of the [Installation and deployment guide](install.md)). Without it, `SHOW DATABASES` fails with 403; querying tables works as usual.

Way 2, catalog endpoint:

```sql
CREATE CATALOG amb WITH (
  'type' = 'iceberg', 'catalog-type' = 'rest',
  'uri' = 'https://amberlog.example.com:8801/iceberg', 'warehouse' = 'amberlog',
  'token' = '<catalog token>', 'token-refresh-enabled' = 'false',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'https://minio.example.com:9000', 's3.path-style-access' = 'true',
  's3.access-key-id' = 'lake-reader', 's3.secret-access-key' = '<read-only secret key>',
  'client.region' = 'us-east-1');
SELECT count(*) FROM amb.`default`.`app`;
```

- Flink is case-sensitive for column names, and `default` must be in backquotes (section 4).
- Only batch mode was tested. Streaming reads were not tested.

### 5.5 Doris

Tested versions: 4.1.4.1 and 3.1.4 (official noavx2 binary packages). 3.1.4 has a `count(*)` defect; see section 7.

Way 1, in-bucket convention: a hadoop catalog.

```sql
CREATE CATALOG amb_lake PROPERTIES (
  "type" = "iceberg", "iceberg.catalog.type" = "hadoop",
  "warehouse" = "s3://logs/amberlog/_iceberg",
  "s3.endpoint" = "https://minio.example.com:9000",
  "s3.access_key" = "lake-reader", "s3.secret_key" = "<read-only secret key>",
  "s3.region" = "us-east-1", "use_path_style" = "true");
SELECT `X-Request-Id` FROM amb_lake.`default`.`app` LIMIT 10;
```

The read-only credential must include a grant on the prefix key `amberlog/_iceberg` itself. Without it, Doris cannot list the namespaces and the whole catalog is unusable: `Database [default] does not exist`.

Way 2, catalog endpoint. Doris requires `warehouse`; any value works:

```sql
CREATE CATALOG amb PROPERTIES (
  "type" = "iceberg", "iceberg.catalog.type" = "rest",
  "uri" = "https://amberlog.example.com:8801/iceberg", "warehouse" = "amberlog",
  "iceberg.rest.security.type" = "oauth2", "iceberg.rest.oauth2.token" = "<catalog token>",
  "s3.endpoint" = "https://minio.example.com:9000",
  "s3.access_key" = "lake-reader", "s3.secret_key" = "<read-only secret key>",
  "s3.region" = "us-east-1", "use_path_style" = "true");
SELECT count(`level`) FROM amb.`default`.`app`;
```

Doris shows and compares `timestamptz` columns in the session time zone. To see them in UTC, first run `SET time_zone = '+00:00'`.

### 5.6 ClickHouse

Tested versions: 26.9.14.10 and 25.8.33.6. They were tested on an earlier version of Amberlog (section 10) and were not tested again on this version. 25.8.33.6 has a defect when it reads through the in-bucket convention; see section 7. The official builds require a CPU with AVX2.

Way 1, in-bucket convention. Point `icebergS3` at the table directory and give the object-storage credential as positional arguments:

```sql
SELECT count() FROM icebergS3('https://minio.example.com:9000/logs/amberlog/_iceberg/default/app',
    'lake-reader', '<read-only secret key>');
```

Way 2, catalog endpoint. Pass the token with `auth_header`. For 26.9:

```sql
SET allow_database_iceberg = 1;
CREATE DATABASE amb ENGINE = DataLakeCatalog('https://amberlog.example.com:8801/iceberg')
SETTINGS catalog_type = 'rest', warehouse = 'amberlog',
    storage_endpoint = 'https://minio.example.com:9000/logs',
    aws_access_key_id = 'lake-reader', aws_secret_access_key = '<read-only secret key>',
    vended_credentials = 0,
    auth_header = 'Authorization: Bearer <catalog token>';
SELECT count() FROM amb.`default.app`;
```

- 26.x needs `vended_credentials = 0`. Without it, ClickHouse reports an error.
- In 25.8, give the object-storage credential as positional arguments: `DataLakeCatalog('<catalog address>', 'lake-reader', '<read-only secret key>')`, and leave these two out of `SETTINGS`. If you put them in the settings, reading data files returns 403.
- ClickHouse is case-sensitive for column names: you cannot write `userAgent` as `useragent`.

### 5.7 StarRocks

Tested version: 4.1.6 (official image `starrocks/allin1-ubuntu:4.1.6`). It was tested on an earlier version of Amberlog (section 10) and was not tested again on this version. The official builds require a CPU with AVX2.

Way 1, in-bucket convention: a hadoop catalog.

```sql
CREATE EXTERNAL CATALOG amb_lake PROPERTIES (
  "type" = "iceberg", "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "s3://logs/amberlog/_iceberg",
  "aws.s3.endpoint" = "https://minio.example.com:9000",
  "aws.s3.access_key" = "lake-reader", "aws.s3.secret_key" = "<read-only secret key>",
  "aws.s3.enable_path_style_access" = "true", "aws.s3.region" = "us-east-1");
SELECT `X-Request-Id` FROM amb_lake.`default`.`app` LIMIT 10;
```

The read-only credential needs a grant on the prefix key `amberlog/_iceberg` itself. Without it, `SHOW DATABASES` fails with 403; querying tables works as usual.

Way 2, catalog endpoint:

```sql
CREATE EXTERNAL CATALOG amb PROPERTIES (
  "type" = "iceberg", "iceberg.catalog.type" = "rest",
  "iceberg.catalog.uri" = "https://amberlog.example.com:8801/iceberg",
  "iceberg.catalog.warehouse" = "amberlog",
  "iceberg.catalog.security" = "oauth2", "iceberg.catalog.oauth2.token" = "<catalog token>",
  "iceberg.catalog.vended-credentials-enabled" = "false",
  "aws.s3.endpoint" = "https://minio.example.com:9000",
  "aws.s3.access_key" = "lake-reader", "aws.s3.secret_key" = "<read-only secret key>",
  "aws.s3.enable_path_style_access" = "true", "aws.s3.region" = "us-east-1");
SELECT count(*) FROM amb.`default`.`app`;
```

The tests read the table both with `vended-credentials-enabled` on and with it off. We recommend turning it off: the catalog endpoint does not hand out credentials anyway, and with it off the engine uses only the read-only credential that you configured.

## 6. What the read-only credential blocks

In the tests, every write attempt by every engine was blocked:

| Operation | Result |
|---|---|
| Spark, Trino, Flink, Doris or StarRocks creating or dropping a table through the catalog endpoint | The client sees that the catalog does not offer these endpoints and refuses locally: `Server does not support endpoint: …` |
| DuckDB or ClickHouse dropping a table through the catalog endpoint | The catalog endpoint answers HTTP 405 |
| DuckDB, Spark or Flink `INSERT`, or writing a file into the table directory | Object storage refuses to write the data file (403 Access Denied) |
| Doris `INSERT` | Writing the data file fails: `failed to put object` |
| StarRocks `INSERT` | Writing the data file fails: `Access Denied` |
| ClickHouse `INSERT` | By default ClickHouse does not allow writes to Iceberg tables: `Insert into iceberg is in beta`. It tries to write only when you turn on the setting `allow_insert_into_iceberg`. What happens after you turn it on was not tested |
| Trino `INSERT` | Writing the data file fails: `HIVE_WRITER_CLOSE_ERROR: Error committing write to Parquet file` |

After these attempts, no new file appeared in the table directory.

The first two rows only block the commit; they cannot stop an engine from writing data files into the bucket first. Only the read-only credential stops that, so external engines must use a read-only credential.

## 7. Known differences and version requirements

| Engine | Difference | What to do |
|---|---|---|
| DuckDB | With the in-bucket convention it reads only the hint file. Without a hint file, `iceberg_scan` fails with `No version was provided and no version-hint could be found`; with a lagging hint file it reads an older version | Read through the catalog endpoint, or give the full path of a `v<N>.metadata.json` |
| DuckDB 1.4.5 | Column comments are not visible through the catalog endpoint (section 4.1) | Use 1.5.6 |
| Trino | No in-bucket convention | Read through the catalog endpoint |
| Trino | An unquoted table name that starts with a digit is a syntax error | Use double quotes |
| Flink, Doris | `default` without backquotes is a syntax error | Write `` `default` `` |
| Flink, Doris (hadoop catalog) | When the read-only credential does not include the prefix key `<storage.prefix>/_iceberg` itself: Flink's `SHOW DATABASES` fails with 403 and querying tables works as usual; for Doris the whole catalog is unusable | Grant it as in section 7.1 of the [Installation and deployment guide](install.md), or read through the catalog endpoint |
| Doris 3.1.4 | A `count(*)` without a column (for example `SELECT count(*) FROM <table>`) fails with a null-pointer error: `Cannot invoke "String.equals(Object)" …`. Both ways of finding tables give this error. Aggregates on a column and `count(*)` with a filter are both correct. The cause is in Doris: it answers such a query directly from the total fields in the snapshot summary, and Amberlog does not write these optional fields into its snapshot summaries | Write `count(<column>)`, or use 4.1.4.1 |
| Doris 4.1.4.1 | Does not have the defect in the row above | — |
| StarRocks (hadoop catalog) | When the read-only credential does not include the prefix key itself, `SHOW DATABASES` fails with 403; querying tables works as usual | Grant it as in section 7.1 of the [Installation and deployment guide](install.md) |
| StarRocks 4.1.6 | Does not skip files by column statistics during planning; filtering by day and skipping by `_timestamp` work as usual. The values read are correct | — |
| ClickHouse 25.8.33.6 | `IS NULL` reads wrong through `icebergS3` (in-bucket convention): 0 rows instead of the expected 5. Reading through the catalog endpoint is correct | Read through the catalog endpoint, or use 26.9.14.10 |
| ClickHouse 26.9.14.10 | Both ways of finding tables read correctly. On columns with null values it does not skip files by column statistics; it filters inside row groups instead, and the results are correct | — |
| ClickHouse | Versions 25.11 to 26.1 have a defect that reads columns with a dot in the top-level column name as null values; 26.2.1.102 fixes it (public information from the engine side; these versions were not tested). The tested 25.8.33.6 and 26.9.14.10 read columns with dots correctly | Avoid 25.11 to 26.1 |

Apart from the table above, every tested version read correct values, and skipping files by day partition, by `_timestamp` and by column statistics all worked.

## 8. How far back you can read

Iceberg engines can read older snapshots by snapshot id or by time (time travel). Amberlog promises only this:

- The current snapshot is always readable.
- After a snapshot stops being the current one, it stays readable for at least `storage.delete_delay` (default 1 hour): the manifests it references are not deleted, and neither are the data files it references that later commits removed.

Beyond that, nothing is promised. An older snapshot still listed in the metadata may reference files that have already been deleted; reading it then fails with a missing-file error, so read the current snapshot instead. Old `v<N>.metadata.json` files are kept for good, but the manifests and data files they reference may already be deleted.

## 9. What not to do

While a write instance manages the storage, Amberlog is the only writer of the cold-tier tables. Do not, through any channel:

- write data, create or drop tables, or change a table schema;
- expire snapshots (for example `expire_snapshots`);
- rewrite data files or manifests (for example `rewrite_data_files`, `rewrite_manifests`);
- remove orphan files (for example `remove_orphan_files`);
- delete or rewrite any object in a table directory, including `v<N>.metadata.json` and the hint file.

Amberlog does this maintenance itself: deletion at expiry, cold compaction, snapshot expiry and orphan removal. If someone deletes or changes objects in a table directory, see section 4.4 of the [Operations guide](operations.md) for how Amberlog detects and handles it.

After retirement, the maintenance is your responsibility. After you run such operations, verification of the evidence exported at retirement reports inconsistencies (section 7.4 of the [Operations guide](operations.md)).

## 10. Test scope

Everything in this guide was tested in this environment:

- Amberlog: a release package of 0.1.0 built before the release (Linux x86_64).
- Object storage: MinIO RELEASE.2025-09-07T16-13-09Z, single node, on the same machine. The catalog endpoint and object storage both used plain http, listening on the loopback address only.
- Engines: DuckDB 1.5.6 and 1.4.5; Spark 3.5.9 and 4.0.4 with Iceberg 1.10.2; Trino 483; Flink 1.20.5 with Iceberg 1.10.2; Doris 4.1.4.1 and 3.1.4.
- Data: 5 datasets, with column names containing dots, hyphens and capital letters, a shadow column, two columns that differ only in case, a table name starting with a digit, and a dataset with 110 hour buckets.
- Three table states: after lake entry (read with DuckDB 1.5.6 and Trino only); after cold compaction; after deletion at expiry and snapshot expiry (read with every engine). The catalog endpoint was provided by the write instance first; for the last state it was provided by the read-only catalog mode, configured with the read-only credential only.
- For Flink and Doris, the in-bucket convention was also read once more with a second read-only credential that includes the grant on the prefix key (section 7).
- ClickHouse 26.9.14.10 and 25.8.33.6, StarRocks 4.1.6: unlike the engines above, these two engines were not tested on this version. They require a CPU with AVX2. They were tested on 2026-10-18, on cold-tier tables written by an earlier pre-release version of Amberlog, with the same data and the same checks, and were not tested again on this version. The cold-tier tables written by that version and by this version have the same layout; for the same data, the data files written by the two versions also have the same content digests.
- For every engine, way of finding tables and table state, these were checked: row counts, values of each kind of column name, `IS NULL`, string and numeric ranges, the shadow column, the two columns that differ only in case, the table name starting with a digit, and skipping files by day partition, by `_timestamp` and by column statistics. Except for the differences in section 7, all results matched the written data.

Not tested:

- engine versions other than the above;
- other object stores such as AWS S3 and Ceph RGW;
- https catalog and object-storage endpoints;
- objects encrypted with SSE-KMS (the read-only credential then also needs decrypt permission for the key);
- read performance at large data volumes.

# 外部引擎直读指南

[English](external-engines.md)

> 适用于 Amberlog 0.1.0。

本文写给要用其他查询引擎读 Amberlog 数据的人：冷层表在哪里，怎样找到它，各引擎怎样配置、SQL 怎样写，有哪些已知的差异。本文覆盖 DuckDB、Apache Spark、Trino、Apache Flink、Apache Doris、ClickHouse 与 StarRocks。其中 ClickHouse 与 StarRocks 的写法与结论来自较早一个版本上的实测，这个版本没有重测（第 10 节）。

只读凭据怎样建、只读目录模式怎样起，见 [安装与部署指南](install.zh-CN.md) 第 7 节。目录端点的接口见 [API 参考](api.zh-CN.md) 第 9 节。

## 1. 先看这几条

- **外部引擎只能读冷层。** 一个小时桶结束 `lifecycle.lake_watermark`（默认 72 小时）以后才入湖，成为冷层表的一部分。在这之前，数据在热层，只能经 Amberlog 查（第 2.1 节）。
- **按桶内约定找表的引擎，不需要 Amberlog 在运行。** 数据文件由引擎直接从对象存储读取。Trino 例外：它要经 Amberlog 的目录端点找表。目录端点可以由单独的只读目录模式进程提供，这个进程只读对象存储，可以和写入实例分开运行（第 3 节）。
- **外部引擎必须用只读凭据。** 只读凭据只授权冷层前缀：前缀键 `<storage.prefix>/_iceberg` 本身，以及 `<storage.prefix>/_iceberg/` 之下的全部对象。不要把 Amberlog 自己的读写凭据交给外部引擎（[安装与部署指南](install.zh-CN.md) 第 7.1 节）。
- **列名保留原样**，含点号、连字符与大写字母，SQL 里要加引号（第 4 节）。
- **不要用外部引擎改冷层表。** 写入、删表、过期快照、重写文件、清理孤儿，都由 Amberlog 自己做（第 9 节）。

## 2. 冷层表

### 2.1 数据什么时候进入冷层表

- 入湖时刻 = 小时桶的结束时刻 + `lifecycle.lake_watermark`（默认 72 小时）。同一数据集的小时桶按从早到晚的顺序入湖。
- 一个小时桶入湖以后又来了迟到的记录时，迟到的记录先进热层，以后再入湖一次。
- 还没入湖过的数据集没有冷层表。
- 指标 `amberlog_lake_lag_seconds{org,stream}` 是入湖积压：最早一个已到入湖时间、还没入湖的小时桶，已超过入湖时间多少秒。没有积压时为 0（[运维指南](operations.zh-CN.md) 第 4.2 节）。

Amberlog 自己的检索同时读热层与冷层。外部引擎只读冷层，所以同一个时间范围，外部引擎读到的可能比 Amberlog 查到的少。

不再使用 Amberlog 时，用 `amberlog retire` 把热层的全部数据转进冷层表，之后外部引擎能读到全部数据（[运维指南](operations.zh-CN.md) 第 7 节）。

试用时想较快看到冷层表，用下面两种办法之一：

- 写入 `_timestamp` 早于 72 小时以前的记录。它们的小时桶已经越过入湖水位，写入后几秒内就入湖。[一键试用](../examples/quickstart/README.zh-CN.md) 就是这样做的（见该文第 5 节）。记录所在的小时桶结束已超过 `lifecycle.lake_watermark` 加 `lifecycle.lake_alert_after`（默认共 96 小时）时，日志里还会有一条 `lake_backlog_high` 警告。
- 只在试用环境里，把两个键调短，例如 `lifecycle.seal_grace = "1m"`、`lifecycle.lake_watermark = "2m"`（水位要大于封口宽限，见[配置项参考](configuration.zh-CN.md)）。当前小时的记录在小时结束后几分钟内入湖，所以最多可能要等约一小时。生产环境不要用这样的值。

### 2.2 位置与布局

每个数据集一张冷层表，格式是 Apache Iceberg。Iceberg 命名空间是组织名（只有 `default`），表名是数据集名。

以 `object_store.url = "s3://logs"`、`storage.prefix = "amberlog"`、数据集 `default/app` 为例：

```text
s3://logs/amberlog/_iceberg/default/app/                 ← 表目录
  metadata/v1.metadata.json                               ← 第 1 版元数据
  metadata/v2.metadata.json
  metadata/version-hint.text                              ← 提示文件，内容是当前版本号，例如 "2"
  metadata/<清单列表与清单>.avro
  data/_timestamp_day=2026-10-15/<文件编号>.parquet        ← 数据文件
```

- `_iceberg/` 之下只有冷层表。热层与 Amberlog 的其他对象都不在这里。
- 每提交一次，写一个新的 `v<N>.metadata.json`，N 从 1 起连续编号，然后更新提示文件。提示文件可能暂时落后，这时它所指的版本是一个较旧、完整的版本。
- 元数据里的表位置是 `s3://<桶>/<storage.prefix>/_iceberg/<org>/<stream>`，不含对象存储的端点地址。外部引擎要自己配置端点。
- Iceberg 表格式第 2 版。分区只有一个字段 `_timestamp_day`，按 `_timestamp` 取日。按 `_timestamp` 过滤时，引擎可以按天跳过分区。
- 没有位置删除文件，也没有等值删除文件。Amberlog 删除数据时只删整个数据文件。
- 每个数据文件的清单条目都有 `_timestamp` 的上下界与各列的空值个数，引擎据此跳过不需要的文件。

### 2.3 列与类型

| Amberlog 的类型 | Iceberg 类型 |
|---|---|
| `_timestamp` | `timestamptz`（必需列） |
| `Int64` | `long` |
| `Float64` | `double` |
| `Boolean` | `boolean` |
| `Utf8` | `string` |

- 一列进入冷层表以后，名字与类型不再变。列只增不减，新列追加在后面。
- 一个数据文件可以只含部分列，缺少的列读作 NULL。
- 同一个字段先后出现不同类型的取值时，另一种类型的取值落进**影子列** `<字段名>__<类型>`，类型是 `int64`、`float64`、`boolean` 或 `utf8`。例如 `status` 本来是整数，后来出现了字符串，字符串落进 `status__utf8`。

### 2.4 快照

| Amberlog 做的事 | 快照摘要 `operation` | 快照摘要 `amberlog.operation` |
|---|---|---|
| 入湖 | `append` | `lake` |
| 到期删除（配置了 `retention.period`） | `delete` | `ttl` |
| 冷层合并（把一天分区里的小文件合成较大的文件） | `replace` | `compaction` |

- 冷层合并不改变记录。只读增量的程序可以跳过 `operation` 为 `replace` 的快照。
- Amberlog 按 `cold_retention.snapshots`（默认 1 个）与 `cold_retention.age`（默认 168 小时）过期旧快照。能读到多早的快照，见第 8 节。
- 数据全部到期以后，表仍在，查询得 0 行。

## 3. 找表的两种办法

| 办法 | 需要什么 | 引擎 |
|---|---|---|
| **桶内约定**：引擎直接指向表目录，自己确定当前版本 | 只读凭据 | DuckDB、Spark、Flink、Doris、ClickHouse、StarRocks |
| **目录端点**：引擎经 Amberlog 的 Iceberg REST 只读目录 `/iceberg` 找表 | 只读凭据；目录的令牌；一个在运行的写入实例或只读目录模式进程 | 上面各引擎与 Trino。Trino 只能用这种 |

两种办法读的是同一份文件。目录端点只把桶里的状态转述出来，不另存任何东西。

### 3.1 桶内约定

引擎各自确定当前版本：

| 引擎 | 怎样确定当前版本 |
|---|---|
| DuckDB | 只读提示文件。提示文件不存在时报错，读不了（第 7 节） |
| Spark、Flink、Doris、StarRocks（hadoop 型目录） | 先读提示文件，再向后找更新的版本 |
| ClickHouse（`icebergS3`） | 列出 `metadata/`，取编号最大的版本，不需要提示文件 |
| Trino | 没有这种办法 |

各引擎也都可以直接给出某个 `v<N>.metadata.json` 的完整路径，读那一版。

### 3.2 目录端点

- 地址：`http(s)://<主机>:<端口>/iceberg`。写入实例（`amberlog serve`）总是提供它，与 `/api/` 在同一个监听地址上。
- 认证：`Authorization: Bearer <令牌>`。各引擎的配置键见第 5 节。
- `{prefix}` 不用手配：`GET /iceberg/v1/config` 下发 `storage.prefix`，客户端自动使用。
- `warehouse`：Amberlog 不看这个参数。有的引擎要求填写，填什么都可以。
- 目录端点**不下发对象存储凭据**。引擎要自己配置只读凭据。
- 目录端点只读，不提供建表、删表、改表。

交给外部引擎的令牌，在写入实例上同样能写入、能检索。要缩小权限，另起一个只读目录模式的进程（`amberlog catalog`），给它另一个令牌与只读凭据。它只提供目录端点，可以与写入实例同时运行，写入实例停机或退役后也能单独运行（[安装与部署指南](install.zh-CN.md) 第 7.2 节）。

## 4. 列名、引号与保留字

冷层表的列名与 Amberlog 里的相同，例如 `kubernetes.pod.name`、`X-Request-Id`、`userAgent`。这些列名在 SQL 里要加引号：

| 引擎 | 列名的引号 | 列名的大小写 |
|---|---|---|
| DuckDB | 双引号：`"X-Request-Id"` | 不区分 |
| Spark | 反引号：`` `X-Request-Id` `` | 不区分 |
| Trino | 双引号：`"X-Request-Id"` | 不区分，显示为小写 |
| Flink | 反引号：`` `X-Request-Id` `` | **区分**，`userAgent` 要写准 |
| Doris | 反引号：`` `X-Request-Id` `` | 不区分 |
| ClickHouse | 反引号或双引号 | **区分**，`userAgent` 要写准 |
| StarRocks | 反引号：`` `X-Request-Id` `` | 不区分 |

命名空间 `default` 与数据集名按原样出现。

- `default` 在 Flink、Doris 与 StarRocks 里是保留字，必须加反引号：`` <目录>.`default`.`app` ``。
- ClickHouse 经目录端点读表时，命名空间与表名合成一个标识符：`` <库>.`default.app` ``。
- 数字开头的数据集名（例如 `2fa_logs`）在 Trino 里必须加引号：`"2fa_logs"`；在 Flink 里必须加反引号。Doris 4.1.4.1 与 StarRocks 不加引号也能读。在 DuckDB 与 Spark 里也请加引号。

### 4.1 只差大小写的两列

两列的名字只差大小写时（例如 `Foo` 与 `foo`），冷层表里先出现的一列保留原名，后出现的一列改名为 `<原名>__2`（已被占用时 `__3`，依此类推）。例如 `Foo` 与 `foo` 在冷层表里叫 `Foo` 与 `foo__2`。

- 改名只在冷层表里。Amberlog 自己的检索仍用原名。
- 改名的列，列注释是它的原名。查看的办法：

| 引擎 | 写法 | 实测结果 |
|---|---|---|
| DuckDB 1.5.6（经目录端点） | 先查一次这张表，再 `SELECT column_name, comment FROM duckdb_columns() WHERE table_name = 'case_logs'` | `foo__2` 的注释为 `foo` |
| DuckDB 1.4.5 | — | 看不到 |
| Spark | `` DESCRIBE <目录>.`default`.`case_logs` `` | `foo__2` 的注释为 `foo` |
| Trino | `SHOW COLUMNS FROM <目录>."default"."case_logs"` | `foo__2` 的 Comment 为 `foo`；`Foo` 显示为 `foo` |
| Doris 4.1.4.1 | `` SHOW FULL COLUMNS FROM <目录>.`default`.`case_logs` `` | `foo__2` 的 Comment 为 `foo`。`DESCRIBE` 看不到 |
| Flink | — | 看不到，`DESCRIBE` 不显示注释 |
| StarRocks | `` SHOW FULL COLUMNS FROM <目录>.`default`.`case_logs` `` | `foo__2` 的 Comment 为 `foo` |
| ClickHouse | — | 看不到 |

## 5. 各引擎的写法

下面的例子用这些值，换成你自己的：

- 对象存储端点 `https://minio.example.com:9000`，桶 `logs`，`storage.prefix` 为 `amberlog`。
- 只读凭据 `lake-reader` / `<只读凭据的访问密钥>`。
- 目录地址 `https://amberlog.example.com:8801/iceberg`，令牌 `<目录的令牌>`。
- 数据集 `default/app`。

实测在明文 http 的端点上做（第 10 节）。用 https 时，引擎要信任对象存储与 Amberlog 的证书所属的 CA。

### 5.1 DuckDB

实测版本：1.5.6、1.4.5（Python 包），用 `httpfs`、`avro`、`iceberg` 三个扩展。

在虚拟环境里装 Python 包。有的系统（如 Debian 13）不允许往系统 Python 里 `pip install`：

```sh
python3 -m venv ~/duckdb
~/duckdb/bin/pip install duckdb==1.5.6
```

下面的 SQL 在 `~/duckdb/bin/python` 里用 `duckdb.connect().sql(...)` 执行。`INSTALL` 语句第一次执行时要从网上下载三个扩展，那时机器要能上网。

先建对象存储凭据：

```sql
INSTALL httpfs; LOAD httpfs;
INSTALL avro; LOAD avro;
INSTALL iceberg; LOAD iceberg;
CREATE SECRET lake (TYPE S3, KEY_ID 'lake-reader', SECRET '<只读凭据的访问密钥>',
    ENDPOINT 'minio.example.com:9000', URL_STYLE 'path', USE_SSL true, REGION 'us-east-1');
```

办法一，桶内约定。`iceberg_scan` 指向表目录：

```sql
SELECT count(*) FROM iceberg_scan('s3://logs/amberlog/_iceberg/default/app');
SELECT "X-Request-Id", "kubernetes.pod.name"
FROM iceberg_scan('s3://logs/amberlog/_iceberg/default/app')
WHERE _timestamp >= TIMESTAMPTZ '2026-10-16 00:00:00+00' LIMIT 10;
```

办法二，目录端点。`ACCESS_DELEGATION_MODE 'none'` 表示不向目录要对象存储凭据，读数据文件用上面的 `lake`：

```sql
CREATE SECRET amb_catalog (TYPE ICEBERG, TOKEN '<目录的令牌>');
ATTACH 'amberlog' AS amb (TYPE ICEBERG,
    ENDPOINT 'https://amberlog.example.com:8801/iceberg',
    SECRET amb_catalog, ACCESS_DELEGATION_MODE 'none');
SELECT count(*) FROM amb."default"."app";
```

`ATTACH` 的第一个参数是 warehouse，填什么都可以。

[一键试用](../examples/quickstart/README.zh-CN.md) 第 6.3 节有一个可以直接运行的 DuckDB 环境。

### 5.2 Spark

实测版本：Spark 3.5.9 与 4.0.4（官方镜像），Iceberg 1.10.2。要加载的 jar：

| Spark | jar |
|---|---|
| 3.5 | `iceberg-spark-runtime-3.5_2.12-1.10.2.jar`、`iceberg-aws-bundle-1.10.2.jar`、`hadoop-aws-3.3.4.jar`、`aws-java-sdk-bundle-1.12.262.jar` |
| 4.0 | `iceberg-spark-runtime-4.0_2.13-1.10.2.jar`、`iceberg-aws-bundle-1.10.2.jar`、`hadoop-aws-3.4.1.jar`、AWS SDK 2.24.6 的 `bundle-2.24.6.jar` |

公共设置：

```text
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
```

办法一，桶内约定：hadoop 型目录，经 S3A 读对象存储。

```text
spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.endpoint=https://minio.example.com:9000
spark.hadoop.fs.s3a.endpoint.region=us-east-1
spark.hadoop.fs.s3a.path.style.access=true
spark.hadoop.fs.s3a.access.key=lake-reader
spark.hadoop.fs.s3a.secret.key=<只读凭据的访问密钥>
spark.sql.catalog.amb_lake=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.amb_lake.type=hadoop
spark.sql.catalog.amb_lake.warehouse=s3://logs/amberlog/_iceberg
```

查询：``SELECT `X-Request-Id` FROM amb_lake.`default`.`app` LIMIT 10``。

办法二，目录端点。对象存储由 Iceberg 自己的 S3FileIO 读，凭据写在目录的设置里：

```text
spark.sql.catalog.amb=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.amb.type=rest
spark.sql.catalog.amb.uri=https://amberlog.example.com:8801/iceberg
spark.sql.catalog.amb.warehouse=amberlog
spark.sql.catalog.amb.token=<目录的令牌>
spark.sql.catalog.amb.token-refresh-enabled=false
spark.sql.catalog.amb.io-impl=org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.amb.s3.endpoint=https://minio.example.com:9000
spark.sql.catalog.amb.s3.path-style-access=true
spark.sql.catalog.amb.s3.access-key-id=lake-reader
spark.sql.catalog.amb.s3.secret-access-key=<只读凭据的访问密钥>
spark.sql.catalog.amb.client.region=us-east-1
```

查询：``SELECT `X-Request-Id`, `kubernetes.pod.name` FROM amb.`default`.`app` LIMIT 10``。

- `token-refresh-enabled=false`：Amberlog 的令牌是静态的，没有刷新端点。
- 不带过滤条件的 `count(*)` 由清单里的计数直接作答，不读数据文件。
- 元数据表照常可用，例如 `` SELECT operation, summary['amberlog.operation'] FROM amb.`default`.`app`.snapshots ``；按快照号读较早的快照用 `VERSION AS OF <快照号>`。

### 5.3 Trino

实测版本：483。Trino 只能经目录端点找表。

目录配置文件 `etc/catalog/amb.properties`：

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=https://amberlog.example.com:8801/iceberg
iceberg.rest-catalog.warehouse=amberlog
iceberg.rest-catalog.security=OAUTH2
iceberg.rest-catalog.oauth2.token=<目录的令牌>
iceberg.rest-catalog.oauth2.token-refresh-enabled=false
fs.s3.enabled=true
s3.endpoint=https://minio.example.com:9000
s3.region=us-east-1
s3.path-style-access=true
s3.aws-access-key=lake-reader
s3.aws-secret-key=<只读凭据的访问密钥>
```

查询：`SELECT "X-Request-Id", "kubernetes.pod.name" FROM amb."default"."app" LIMIT 10`。

- Trino 483 用 `fs.s3.enabled` 打开 S3。旧键 `fs.native-s3.enabled` 启动时报警告。
- 目录地址用 https 时，Trino 的 JVM 要信任 Amberlog 证书所属的 CA。
- 元数据表照常可用，例如 `SELECT operation FROM amb."default"."app$snapshots"`。

### 5.4 Flink

实测版本：Flink 1.20.5（官方镜像 `flink:1.20-java17`），批模式。放进 `lib/` 的 jar：`iceberg-flink-runtime-1.20-1.10.2.jar`、`iceberg-aws-bundle-1.10.2.jar`、`hadoop-client-api-3.3.4.jar`、`hadoop-client-runtime-3.3.4.jar`、`hadoop-aws-3.3.4.jar`、`aws-java-sdk-bundle-1.12.262.jar`。

办法一，桶内约定：hadoop 型目录。

1. 在 Apache Hadoop 的配置文件 `core-site.xml` 里配 S3A：

   ```xml
   <configuration>
     <property><name>fs.s3.impl</name><value>org.apache.hadoop.fs.s3a.S3AFileSystem</value></property>
     <property><name>fs.s3a.endpoint</name><value>https://minio.example.com:9000</value></property>
     <property><name>fs.s3a.endpoint.region</name><value>us-east-1</value></property>
     <property><name>fs.s3a.path.style.access</name><value>true</value></property>
     <property><name>fs.s3a.access.key</name><value>lake-reader</value></property>
     <property><name>fs.s3a.secret.key</name><value>&lt;只读凭据的访问密钥&gt;</value></property>
   </configuration>
   ```

2. 让环境变量 `HADOOP_CONF_DIR` 指向 `core-site.xml` 所在的目录；或者建目录时用 `'hadoop-conf-dir'` 指定这个目录。
3. 建目录并查询：

   ```sql
   CREATE CATALOG amb_lake WITH (
     'type' = 'iceberg', 'catalog-type' = 'hadoop',
     'warehouse' = 's3://logs/amberlog/_iceberg');
   SELECT `X-Request-Id`, `userAgent` FROM amb_lake.`default`.`app` LIMIT 10;
   ```

只读凭据要含前缀键 `amberlog/_iceberg` 本身的授权（[安装与部署指南](install.zh-CN.md) 第 7.1 节）。没有它时 `SHOW DATABASES` 报 403，查表照常。

办法二，目录端点：

```sql
CREATE CATALOG amb WITH (
  'type' = 'iceberg', 'catalog-type' = 'rest',
  'uri' = 'https://amberlog.example.com:8801/iceberg', 'warehouse' = 'amberlog',
  'token' = '<目录的令牌>', 'token-refresh-enabled' = 'false',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'https://minio.example.com:9000', 's3.path-style-access' = 'true',
  's3.access-key-id' = 'lake-reader', 's3.secret-access-key' = '<只读凭据的访问密钥>',
  'client.region' = 'us-east-1');
SELECT count(*) FROM amb.`default`.`app`;
```

- Flink 区分列名的大小写，`default` 必须加反引号（第 4 节）。
- 只测了批模式，没有测流式读取。

### 5.5 Doris

实测版本：4.1.4.1 与 3.1.4（官方 noavx2 二进制包）。3.1.4 有一个 `count(*)` 的缺陷，见第 7 节。

办法一，桶内约定：hadoop 型目录。

```sql
CREATE CATALOG amb_lake PROPERTIES (
  "type" = "iceberg", "iceberg.catalog.type" = "hadoop",
  "warehouse" = "s3://logs/amberlog/_iceberg",
  "s3.endpoint" = "https://minio.example.com:9000",
  "s3.access_key" = "lake-reader", "s3.secret_key" = "<只读凭据的访问密钥>",
  "s3.region" = "us-east-1", "use_path_style" = "true");
SELECT `X-Request-Id` FROM amb_lake.`default`.`app` LIMIT 10;
```

只读凭据必须含前缀键 `amberlog/_iceberg` 本身的授权。没有它时 Doris 列不出命名空间，整个目录不可用：`Database [default] does not exist`。

办法二，目录端点。Doris 要求填 `warehouse`，填什么都可以：

```sql
CREATE CATALOG amb PROPERTIES (
  "type" = "iceberg", "iceberg.catalog.type" = "rest",
  "uri" = "https://amberlog.example.com:8801/iceberg", "warehouse" = "amberlog",
  "iceberg.rest.security.type" = "oauth2", "iceberg.rest.oauth2.token" = "<目录的令牌>",
  "s3.endpoint" = "https://minio.example.com:9000",
  "s3.access_key" = "lake-reader", "s3.secret_key" = "<只读凭据的访问密钥>",
  "s3.region" = "us-east-1", "use_path_style" = "true");
SELECT count(`level`) FROM amb.`default`.`app`;
```

`timestamptz` 列按会话时区显示与比较。要按 UTC 看，先 `SET time_zone = '+00:00'`。

### 5.6 ClickHouse

实测版本：26.9.14.10 与 25.8.33.6，在 Amberlog 的较早版本上实测（第 10 节），这个版本没有重测。25.8.33.6 经桶内约定读有缺陷，见第 7 节。官方构建要求 CPU 支持 AVX2。

办法一，桶内约定。`icebergS3` 指向表目录，对象存储凭据按位置参数写：

```sql
SELECT count() FROM icebergS3('https://minio.example.com:9000/logs/amberlog/_iceberg/default/app',
    'lake-reader', '<只读凭据的访问密钥>');
```

办法二，目录端点。令牌用 `auth_header` 传。26.9 的写法：

```sql
SET allow_database_iceberg = 1;
CREATE DATABASE amb ENGINE = DataLakeCatalog('https://amberlog.example.com:8801/iceberg')
SETTINGS catalog_type = 'rest', warehouse = 'amberlog',
    storage_endpoint = 'https://minio.example.com:9000/logs',
    aws_access_key_id = 'lake-reader', aws_secret_access_key = '<只读凭据的访问密钥>',
    vended_credentials = 0,
    auth_header = 'Authorization: Bearer <目录的令牌>';
SELECT count() FROM amb.`default.app`;
```

- 26.x 要设 `vended_credentials = 0`，否则报错。
- 25.8 的对象存储凭据要按位置参数写：`DataLakeCatalog('<目录地址>', 'lake-reader', '<只读凭据的访问密钥>')`，`SETTINGS` 里不写这两项；写在设置里时，读数据文件回 403。
- ClickHouse 区分列名的大小写，`userAgent` 不能写成 `useragent`。

### 5.7 StarRocks

实测版本：4.1.6（官方镜像 `starrocks/allin1-ubuntu:4.1.6`），在 Amberlog 的较早版本上实测（第 10 节），这个版本没有重测。官方构建要求 CPU 支持 AVX2。

办法一，桶内约定：hadoop 型目录。

```sql
CREATE EXTERNAL CATALOG amb_lake PROPERTIES (
  "type" = "iceberg", "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "s3://logs/amberlog/_iceberg",
  "aws.s3.endpoint" = "https://minio.example.com:9000",
  "aws.s3.access_key" = "lake-reader", "aws.s3.secret_key" = "<只读凭据的访问密钥>",
  "aws.s3.enable_path_style_access" = "true", "aws.s3.region" = "us-east-1");
SELECT `X-Request-Id` FROM amb_lake.`default`.`app` LIMIT 10;
```

只读凭据要含前缀键 `amberlog/_iceberg` 本身的授权。没有它时 `SHOW DATABASES` 报 403，查表照常。

办法二，目录端点：

```sql
CREATE EXTERNAL CATALOG amb PROPERTIES (
  "type" = "iceberg", "iceberg.catalog.type" = "rest",
  "iceberg.catalog.uri" = "https://amberlog.example.com:8801/iceberg",
  "iceberg.catalog.warehouse" = "amberlog",
  "iceberg.catalog.security" = "oauth2", "iceberg.catalog.oauth2.token" = "<目录的令牌>",
  "iceberg.catalog.vended-credentials-enabled" = "false",
  "aws.s3.endpoint" = "https://minio.example.com:9000",
  "aws.s3.access_key" = "lake-reader", "aws.s3.secret_key" = "<只读凭据的访问密钥>",
  "aws.s3.enable_path_style_access" = "true", "aws.s3.region" = "us-east-1");
SELECT count(*) FROM amb.`default`.`app`;
```

`vended-credentials-enabled` 开与关都实测能读。建议关掉：目录端点本来不下发凭据，关掉以后引擎只用你配的只读凭据。

## 6. 只读凭据挡住了什么

实测中，各引擎的写操作都被挡住：

| 操作 | 结果 |
|---|---|
| Spark、Trino、Flink、Doris、StarRocks 经目录端点建表、删表 | 客户端读到目录不提供这两个端点，在本地拒绝：`Server does not support endpoint: …` |
| DuckDB、ClickHouse 经目录端点删表 | 目录端点回 HTTP 405 |
| DuckDB、Spark、Flink `INSERT`，或往表目录写文件 | 对象存储拒绝写数据文件（403 Access Denied） |
| Doris `INSERT` | 写数据文件失败：`failed to put object` |
| StarRocks `INSERT` | 写数据文件失败：`Access Denied` |
| ClickHouse `INSERT` | 默认不允许写 Iceberg 表：`Insert into iceberg is in beta`。要打开设置 `allow_insert_into_iceberg` 才会尝试写入，打开以后的情形没有实测 |
| Trino `INSERT` | 写数据文件失败：`HIVE_WRITER_CLOSE_ERROR: Error committing write to Parquet file` |

试过这些写操作以后，表目录里没有多出任何文件。

前两条只挡住提交，挡不住引擎先把数据文件写进桶。只有只读凭据挡得住，所以外部引擎必须用只读凭据。

## 7. 已知差异与版本要求

| 引擎 | 差异 | 怎么办 |
|---|---|---|
| DuckDB | 桶内约定只读提示文件。提示文件不存在时 `iceberg_scan` 报错 `No version was provided and no version-hint could be found`；提示文件落后时读到较旧的版本 | 经目录端点读；或给出 `v<N>.metadata.json` 的完整路径 |
| DuckDB 1.4.5 | 经目录端点看不到列注释（第 4.1 节） | 用 1.5.6 |
| Trino | 没有桶内约定的读法 | 经目录端点读 |
| Trino | 数字开头的表名不加引号时报语法错误 | 加双引号 |
| Flink、Doris | `default` 不加反引号时报语法错误 | 写成 `` `default` `` |
| Flink、Doris（hadoop 型目录） | 只读凭据不含前缀键 `<storage.prefix>/_iceberg` 本身时：Flink 的 `SHOW DATABASES` 报 403，查表照常；Doris 整个目录不可用 | 按 [安装与部署指南](install.zh-CN.md) 第 7.1 节授权；或经目录端点读 |
| Doris 3.1.4 | 不带列的 `count(*)`（例如 `SELECT count(*) FROM <表>`）报空指针：`Cannot invoke "String.equals(Object)" …`。两种找表办法都这样。带列的聚合、带过滤的 `count(*)` 都正确。原因在 Doris：它用快照摘要里的合计字段直接回答这种查询，Amberlog 的快照摘要不写这些可选字段 | 写成 `count(<列>)`；或用 4.1.4.1 |
| Doris 4.1.4.1 | 没有上一行的缺陷 | — |
| StarRocks（hadoop 型目录） | 只读凭据不含前缀键本身时，`SHOW DATABASES` 报 403，查表照常 | 按 [安装与部署指南](install.zh-CN.md) 第 7.1 节授权 |
| StarRocks 4.1.6 | 规划阶段不按列统计跳过文件；按日过滤与按 `_timestamp` 跳过照常。读值正确 | — |
| ClickHouse 25.8.33.6 | 经 `icebergS3`（桶内约定）查 `IS NULL` 读错：应得 5 行，得 0 行。经目录端点读正确 | 经目录端点读；或用 26.9.14.10 |
| ClickHouse 26.9.14.10 | 两种找表办法都读对。在有空值的列上不按列统计跳过文件，改在行组里过滤，结果正确 | — |
| ClickHouse | 顶层列名带点号的列，25.11～26.1 有读成空值的缺陷，26.2.1.102 修复（引擎一侧的公开信息，这几个版本没有实测）。实测的 25.8.33.6 与 26.9.14.10 读带点号的列都正确 | 避开 25.11～26.1 |

上表以外，实测的各版本读值都正确，按日分区、按 `_timestamp` 与按列统计跳过文件都生效。

## 8. 能读到多早的快照

Iceberg 引擎可以按快照号或时间读较早的快照（时间旅行）。Amberlog 只承诺：

- 当前快照始终可读。
- 一个快照不再是当前快照以后，至少在 `storage.delete_delay`（默认 1 小时）之内仍可读：它引用的清单不删，它引用的、已被后来的提交移除的数据文件也不删。

超过这段时间，不承诺。元数据里仍列出的较早快照，可能引用已经删除的文件，读它会报文件不存在，这时改读当前快照。旧的 `v<N>.metadata.json` 一直保留，但它引用的清单与数据文件可能已经删除。

## 9. 不要做的事

写入实例管理这份存储期间，冷层表只有 Amberlog 一个写方。不要经任何渠道：

- 写入数据、建表、删表、改表结构；
- 过期快照（例如 `expire_snapshots`）；
- 重写数据文件或清单（例如 `rewrite_data_files`、`rewrite_manifests`）；
- 清理孤儿文件（例如 `remove_orphan_files`）；
- 删除或改写表目录下的任何对象，含 `v<N>.metadata.json` 与提示文件。

这些维护由 Amberlog 自己做：到期删除、冷层合并、快照过期、孤儿清理。有人删改了表目录下的对象时，Amberlog 怎样发现与处理见 [运维指南](operations.zh-CN.md) 第 4.4 节。

退役以后，维护由你自己负责。做过这些动作以后，用退役时导出的证据查验会报不一致（[运维指南](operations.zh-CN.md) 第 7.4 节）。

## 10. 实测范围

本文的写法都在下面的环境里实测过：

- Amberlog：0.1.0 发布前构建的发版包（Linux x86_64）。
- 对象存储：MinIO RELEASE.2025-09-07T16-13-09Z，单机，同一台机器上。目录端点与对象存储都用明文 http，只在回环地址上监听。
- 引擎：DuckDB 1.5.6 与 1.4.5；Spark 3.5.9 与 4.0.4，配 Iceberg 1.10.2；Trino 483；Flink 1.20.5，配 Iceberg 1.10.2；Doris 4.1.4.1 与 3.1.4。
- 数据：5 个数据集，含带点号、连字符、大写字母的列，影子列，只差大小写的两列，数字开头的表名，以及一个有 110 个小时桶的数据集。
- 三种表状态：入湖以后（只用 DuckDB 1.5.6 与 Trino 读）；冷层合并以后；到期删除与快照过期以后（各引擎都读）。目录端点先由写入实例提供，最后一种状态改由只读目录模式提供，只读目录模式只配只读凭据。
- Flink、Doris 的桶内约定另用一份含前缀键授权的只读凭据各读一遍（第 7 节）。
- ClickHouse 26.9.14.10 与 25.8.33.6、StarRocks 4.1.6：这两种引擎没有在这个版本上实测（上面的引擎都在这个版本上实测）。它们要求 CPU 支持 AVX2，是在 2026-10-18 用 Amberlog 较早的一个预发布版本写出的冷层表上，按同一组数据与核对项实测的，这个版本没有重测。那个版本与这个版本写出的冷层表布局相同；同一组数据，两个版本写出的数据文件内容摘要也相同。
- 每个引擎、每种找表办法、每种表状态都核对：行数，各类列名的读值，`IS NULL`，字符串与数值区间，影子列，只差大小写的两列，数字开头的表名，以及按日分区、按 `_timestamp`、按列统计跳过文件。除第 7 节列出的差异以外，结果都与写入的数据一致。

没有实测的：

- 上面以外的引擎版本；
- AWS S3、Ceph RGW 等其他对象存储；
- https 的目录端点与对象存储端点；
- SSE-KMS 加密的对象（只读凭据还要有该密钥的解密权限）；
- 大数据量下的读性能。

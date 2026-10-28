# 一键试用

[English](README.md)

> 适用于 Amberlog 0.1.0。

本目录用 Docker Compose 在你的电脑上起一套试用环境，演示三件事：

1. 写入：把 JSON 日志写进 Amberlog。
2. 检索：用 SQL 检索刚写入的日志。
3. 外部引擎直读冷层：较早的日志进入冷层后，是对象存储里的标准 Apache Iceberg 表。DuckDB 用只读凭据直接读这张表，不经过 Amberlog。

**这套环境只供试用，不要用于生产。** 口令与令牌都是写在文件里的示例值，Amberlog 以明文 HTTP 监听。端口只开在本机的 `127.0.0.1` 上。

## 1. 环境里有什么

| 服务 | 镜像 | 作用 |
|---|---|---|
| `minio` | `pgsty/silo`（RELEASE.2026-10-18T00-00-00Z，按摘要固定） | 对象存储：Silo，PGSTY 维护的 MinIO 社区分支。Amberlog 的全部数据都在桶 `amberlog-data` 里 |
| `setup` | 同一个 `pgsty/silo` 镜像，里面带 `mc` | 一次性作业：建桶，建两个用户，做完就退出 |
| `amberlog` | `ghcr.io/visdata-com/amberlog:latest` | Amberlog 写入实例 |
| `duckdb` | 本目录 `duckdb/` 下构建：`python:3.12-slim` 加 DuckDB 1.5.6 | 外部查询引擎。常驻，用 `lake-query` 命令执行查询 |

账号、口令与令牌：

| 用途 | 用户名 | 口令或令牌 | 权限 |
|---|---|---|---|
| Silo 管理员，登录控制台用 | `quickstart-admin` | `quickstart-admin-secret` | 全部 |
| Amberlog 访问 Silo | `amberlog-app` | `amberlog-app-secret` | 只能访问桶 `amberlog-data`，可读可写 |
| DuckDB 访问 Silo | `lake-reader` | `lake-reader-secret` | 只能读取与列举 `amberlog-data/amberlog/_iceberg` 之下的冷层表 |
| 客户端访问 Amberlog | — | 令牌 `quickstart-token` | 请求头 `Authorization: Bearer quickstart-token` |

本机端口：

| 地址 | 是什么 |
|---|---|
| `http://127.0.0.1:8800` | Amberlog：写入、检索、指标、检索界面 `/ui`、只读目录 `/iceberg/` |
| `http://127.0.0.1:9000` | Silo 的 S3 接口 |
| `http://127.0.0.1:9001` | Silo 控制台 |

## 2. 准备

- Docker Engine 与 Compose 插件（`docker compose` 命令）。实测用 Docker Compose v5.6.0。
- `curl`。
- 本机的 8800、9000、9001 端口没有被占用。
- 第一次启动时能连网：要拉取镜像，构建 DuckDB 镜像时要从 PyPI 安装 `duckdb`、下载 DuckDB 的三个扩展。

Amberlog 镜像有 `linux/amd64` 与 `linux/arm64` 两种。本说明在 `linux/amd64` 上实测。

## 3. 启动

```sh
cd examples/quickstart
docker compose up -d --wait
```

`--wait` 等 Amberlog 就绪后才返回。第一次启动要拉镜像，还要构建 DuckDB 镜像（从 PyPI 下载 DuckDB）。实测多等约 2～6 分钟，取决于网络。

要用指定版本的 Amberlog 镜像时，设环境变量 `QUICKSTART_IMAGE`：

```sh
QUICKSTART_IMAGE=ghcr.io/visdata-com/amberlog:0.1.0 docker compose up -d --wait
```

## 4. 演示

```sh
./demo.sh
```

脚本按空环境编写。它依次做下面五步：

1. 等 Amberlog 就绪。
2. 向数据集 `default/app` 写入 5 条记录：3 条的时间是 4 天前，2 条是现在。
3. 用 Amberlog 检索最近 5 天的记录：新旧 5 条都能查到。
4. 等 4 天前的 3 条记录进入冷层。
5. 用 DuckDB 直接读冷层表：只读到这 3 条旧记录。

输出示例（`took`、时间戳与等待秒数每次不同）：

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

第 3 步的检索结果里，`level` 为 `error` 的有 2 条；第 5 步 DuckDB 只读到其中 4 天前的那 1 条。

再演示一遍前，先清空数据（见第 7 节），再重新启动。

## 5. 为什么只有旧记录进入冷层

Amberlog 把数据按小时分桶。一个小时桶在这个小时结束 72 小时之后进入冷层，这个时刻叫入湖水位时刻。72 小时是配置项 `lifecycle.lake_watermark` 的默认值。

- 4 天前的记录已经越过入湖水位时刻，Amberlog 启动后很快就把它们写进冷层表。
- 现在的记录还在热层。Amberlog 自己的检索同时读热层与冷层，所以第 3 步查得到全部 5 条。外部引擎只读冷层表，72 小时后才读得到这 2 条。

配置项的说明见[配置项参考](../../docs/configuration.zh-CN.md)。

## 6. 自己动手

下面的例子先把令牌放进变量：

```sh
TOKEN=quickstart-token
```

### 6.1 写入

```sh
curl -sS -X POST http://127.0.0.1:8800/api/default/app/_json \
  -H "Authorization: Bearer $TOKEN" \
  -d '[{"level": "info", "message": "hello from curl"}]'
```

没有 `_timestamp` 的记录取接收时刻。请求格式见 [API 参考](../../docs/api.zh-CN.md)第 2 节。

### 6.2 检索

`start_time` 与 `end_time` 是 UTC 微秒：

```sh
now=$(($(date +%s) * 1000000))
curl -sS -X POST http://127.0.0.1:8800/api/default/_search \
  -H "Authorization: Bearer $TOKEN" \
  -d "{\"query\": {\"sql\": \"SELECT _timestamp, level, message FROM app ORDER BY _timestamp DESC\",
       \"start_time\": $((now - 3600000000)), \"end_time\": $((now + 60000000))}}"
```

也可以在浏览器里打开 `http://127.0.0.1:8800/ui`，在页面里输入令牌 `quickstart-token` 后检索。

### 6.3 用 DuckDB 读冷层表

```sh
docker compose exec duckdb lake-query \
  "SELECT count(*) FROM iceberg_scan('s3://amberlog-data/amberlog/_iceberg/default/app')"
```

`lake-query` 执行一段 SQL，可以含多条语句。它先用只读凭据 `lake-reader` 建好 DuckDB 的 S3 secret，再执行你给的 SQL。

上面的写法按桶内约定找表：直接给出表目录，冷层表在桶内的位置是 `s3://<桶>/<storage.prefix>/_iceberg/<org>/<stream>`。这种写法不需要任何 Amberlog 进程在运行。

也可以经 Amberlog 的 Iceberg 只读目录找表。这时要 Amberlog 在运行，并带上令牌；数据文件仍由 DuckDB 用只读凭据直接从 Silo 读：

```sh
docker compose exec duckdb lake-query "
  ATTACH 'amberlog' AS amb (TYPE ICEBERG, ENDPOINT 'http://amberlog:8800/iceberg',
      TOKEN 'quickstart-token', ACCESS_DELEGATION_MODE 'none');
  SELECT count(*) FROM amb.default.app"
```

目录端点见 [API 参考](../../docs/api.zh-CN.md)第 9 节。

试一下只读凭据的边界：`lake-reader` 读不到热层，也写不进冷层表所在的前缀。下面两条都失败，Silo 拒绝访问：

```sh
docker compose exec duckdb lake-query \
  "SELECT count(*) FROM read_parquet('s3://amberlog-data/amberlog/default/app/**/*.parquet')"
docker compose exec duckdb lake-query \
  "COPY (SELECT 1 AS x) TO 's3://amberlog-data/amberlog/_iceberg/default/app/data/x.parquet'"
```

用 Apache Spark、Trino、Apache Flink、Apache Doris 等其他引擎读冷层表的写法，见 [外部引擎直读指南](../../docs/external-engines.zh-CN.md)。

### 6.4 在 Silo 控制台里看对象

浏览器打开 `http://127.0.0.1:9001`，用 `quickstart-admin` 登录。桶 `amberlog-data` 下：

- `amberlog/_iceberg/default/app/` 是冷层表：`metadata/` 下是 Iceberg 元数据，`data/` 下是 Apache Parquet 数据文件。
- `amberlog/default/app/` 是热层。

### 6.5 看日志

```sh
docker compose logs amberlog
```

启动时有两条 `warn` 是这套环境预期内的：`plaintext_listen`（以明文监听）与 `object_store_plaintext`（以明文连 Silo）。原因见第 8 节。

## 7. 停止与清理

| 命令 | 作用 |
|---|---|
| `docker compose stop` | 停止容器，保留数据 |
| `docker compose down` | 删除容器与网络，保留数据卷。再次 `up` 时数据还在 |
| `docker compose down -v` | 删除容器、网络与两个数据卷，数据全部删除 |
| `docker image rm amberlog-quickstart-duckdb` | 删除本地构建的 DuckDB 镜像 |

## 8. 与正式部署的差别

这套环境为了一条命令起得来，做了几处正式部署不应做的简化：

- Amberlog 以明文 HTTP 监听，并声明了 `server.behind_tls_proxy = true`，其实前面并没有反向代理。正式部署要配置证书，或者真的放在终止 TLS 的反向代理后面。
- Silo 用明文 HTTP，单节点、单块盘。
- 口令与令牌是写在 `compose.yaml` 里的示例值。正式部署用随机生成的值，写进只有运行用户读得到的配置文件，不放在编排文件与环境变量里。
- Amberlog 的配置全部用环境变量给出。配置项与对应的环境变量见[配置项参考](../../docs/configuration.zh-CN.md)。

外部引擎必须用只读凭据，不能用 Amberlog 自己的读写凭据。只读凭据的授权范围就是 `compose.yaml` 里 `setup` 服务给 `lake-reader` 的策略。

正式部署的步骤见[安装与部署指南](../../docs/install.zh-CN.md)。

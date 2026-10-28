# 运维指南

[English](operations.md)

> 适用于 Amberlog 0.1.0。

本文写给运维人员：怎样看 Amberlog 是否正常、哪些指标和日志要设告警、积压了怎么办、怎样停机与重启、怎样导出与查验审计证据、怎样做优雅退役。除各节另有说明外，文中的命令与输出都在测试环境里实际运行过（S3 兼容存储 MinIO，Linux）。

配置项见 [配置项参考](configuration.zh-CN.md)，HTTP 接口见 [API 参考](api.zh-CN.md)。例子里的 `$AMBERLOG` 是 Amberlog 的地址，`$TOKEN` 是 `auth.token` 的值。

## 1. 先认识几个词

- **写入实例**：以 `amberlog serve` 运行的进程。它受理写入与查询，并在后台做提交、封口合并、入湖、巡检等工作。同一份存储（同一 `object_store.url` 与 `storage.prefix`）上同一时刻只能运行一个写入实例。
- **WAL**（预写日志）：`wal.dir` 下的本地文件。写入请求先写进 WAL 并 fsync，才回 200。之后后台把记录**提交**到对象存储。
- **热层**与**冷层**：记录先以 Amberlog 自己的格式存放在热层；按小时分桶，桶结束 `lifecycle.lake_watermark`（默认 72 小时）以后**入湖**，转为冷层的标准 Apache Iceberg 表，外部引擎可以直接读。
- **离线子命令**：`amberlog retire`、`amberlog evidence export`、`amberlog evidence verify`。它们不监听端口，做完一件事就退出。

## 2. 健康检查

| 端点 | 要令牌 | 回 200 的条件 | 用作 |
|---|---|---|---|
| `GET /healthz` | 否 | 进程能处理 HTTP 请求 | 存活探针。失败时重启进程 |
| `GET /readyz` | 否 | `wal.dir` 可写；最近一次对象存储探测成功；不在停机过程中 | 就绪探针。失败时不再把流量导给它 |

对象存储探测每 30 秒内做一次：写入并读回 `<storage.prefix>/_system/readyz-probe`。

未就绪时，`/readyz` 回 503，`details.checks` 说明是哪一项：

```sh
curl -sS "$AMBERLOG/readyz"
```

```json
{"code":503,"reason":"not_ready","message":"not ready","details":{"checks":{"wal":false,"object_store":true,"shutting_down":false}}}
```

（这个例子是在运行中把 `wal.dir` 改成不可写得到的。）

- `wal` 为 false：`wal.dir` 不可写，例如磁盘满或权限被改。写入请求在 WAL 写入或 fsync 真正失败时回 503；已经打开的 WAL 文件仍能写时照常回 200。
- `object_store` 为 false：对象存储探测失败，例如网络断开、凭据失效、存储服务宕机。
- `shutting_down` 为 true：进程收到了 SIGTERM 或 SIGINT，正在停机。

**`/readyz` 回 503 时，`/api/` 请求并不会因此被拒**，各按自己的规则作答。例如对象存储不可用时，写入照常写进 WAL 并回 200，记录在对象存储恢复以后再提交；需要读对象存储的查询回 503 `unavailable`。

Kubernetes 的探针可以这样配：

```yaml
livenessProbe:
  httpGet: { path: /healthz, port: 8800 }
  periodSeconds: 10
readinessProbe:
  httpGet: { path: /readyz, port: 8800 }
  periodSeconds: 10
  failureThreshold: 3
```

启用了进程内 TLS 时，加 `scheme: HTTPS`。

## 3. 指标与告警

### 3.1 抓取

`GET /metrics` 返回 Prometheus 文本格式，要求令牌。Prometheus 的抓取配置：

```yaml
scrape_configs:
  - job_name: amberlog
    metrics_path: /metrics
    authorization:
      credentials_file: /etc/prometheus/amberlog-token   # 文件内容为 auth.token 的值
    static_configs:
      - targets: ["amberlog.example.com:8800"]
```

启用了 TLS 时加 `scheme: https`，必要时配 `tls_config.ca_file`。

```sh
curl -sS "$AMBERLOG/metrics" -H "Authorization: Bearer $TOKEN" | grep -v '^#'
```

```text
amberlog_ingest_records_total{org="default",stream="app",outcome="accepted"} 301
amberlog_ingest_records_total{org="default",stream="app",outcome="rejected"} 0
amberlog_manifest_commits_total{org="default",stream="app",result="ok"} 1
amberlog_manifest_commits_total{org="default",stream="app",result="failed"} 0
amberlog_ready 1
amberlog_wal_bytes 23105
amberlog_backlog_memory_bytes 21986
amberlog_lake_lag_seconds{org="default",stream="app"} 0
...
```

稳定指标的完整列表见 [API 参考 8.3 节](api.zh-CN.md#83-get-metrics)。只有稳定指标承诺不改名、不删除，告警规则只用它们。

### 3.2 建议的告警

下表的表达式按 Prometheus 的写法给出，阈值按自己的环境调整。

| 告警 | 表达式 | 说明与处理 |
|---|---|---|
| 未就绪 | `amberlog_ready == 0`，持续 2 分钟 | 查 `/readyz` 的 `details.checks`，见第 2 节 |
| 提交失败 | `increase(amberlog_manifest_commits_total{result="failed"}[10m]) > 0` | 记录写进了 WAL，但提交不到对象存储。对象存储不可用期间，有待提交记录的数据集每个 `flush.interval` 计一次失败。见 4.1 节 |
| WAL 积压 | `amberlog_wal_bytes > <wal.alert_bytes 的值>` | 同时有 `wal_backlog_high` 日志事件。见 4.1 节 |
| 内存积压 | `amberlog_backlog_memory_bytes` 接近 `ingest.max_backlog_memory_bytes` | 待提交的记录多到内存装不下，超出部分只留在 WAL 里。说明提交跟不上或停了 |
| 入湖积压 | `amberlog_lake_lag_seconds > 86400` | 同时有 `lake_backlog_high` 日志事件。见 4.2 节 |
| 数据集停用 | `amberlog_dataset_disabled == 1` | 同时有 `dataset_disabled` 日志事件。见 4.3 节 |
| 查询被拒 | `increase(amberlog_search_rejected_total[5m]) > 0` | `reason="too_many_queries"`：并发查询超过 `query.max_concurrency`；`reason="query_memory_exceeded"`：单个查询超过 `query.max_memory_bytes` |
| 写入被逐条拒绝 | `increase(amberlog_ingest_records_total{outcome="rejected"}[10m]) > 0` | 采集端发来的记录不合规则。拒绝原因在写入应答的 `errors` 里，见 [API 参考 2.4 节](api.zh-CN.md#24-逐条拒绝) |
| 可能漏采 | `amberlog_reconciliation_windows{verdict="missing"} > 0` | 过去 24 小时内有分钟窗口的确认数少于采集端上报的发送数。同时有 `reconciliation_missing` 日志事件。查系统表 `_reconciliation`，见 [API 参考 6.2 节](api.zh-CN.md#62-_reconciliation对账) |
| 上报中断 | `amberlog_reconciliation_windows{verdict="silent"} > 0` | 有采集来源停止上报发送数。同时有 `source_silent` 日志事件 |
| 告警通知失败 | `increase(amberlog_alert_deliveries_total{result="abandoned"}[1h]) > 0` | Amberlog 的告警规则命中了，但 webhook 送不出去。查 `_alert_evaluations` 的 `delivery` 与 `last_http_status` |

漏采判定与内置告警规则 `missed_collection` 只对以 `basis` 为 `timestamp` 上报发送数、并在写入时带 `X-Amberlog-Source` 的来源起作用，见 [API 参考第 3 节](api.zh-CN.md#3-发送数上报-_sent)。

### 3.3 按日志告警

运行日志写到 stderr。`log.format` 为 `json`（默认）时每行一个 JSON 对象。带 `event` 字段的 14 个**稳定事件**字段名不变、不受 `log.level` 过滤，可以用日志系统按 `event` 建告警。完整列表见 [配置项参考第 8 节](configuration.zh-CN.md#8-稳定的日志事件)。

建议至少对下列事件告警：

| 事件 | `level` | 含义 |
|---|---|---|
| `wal_backlog_high` | `warn` | WAL 积压超过 `wal.alert_bytes` |
| `lake_backlog_high` | `warn` | 入湖积压超过 `lifecycle.lake_alert_after` |
| `dataset_disabled` | `error` | 有数据集停用 |
| `digest_mismatch` | `error` | 数据文件的内容与记录的摘要不符，可能被外部改动 |
| `tls_reload_failed` | `error` | 证书文件换成了无效内容，进程仍在用旧证书 |
| `reconciliation_missing` | `warn` | 漏采判定发现有分钟窗口的两个计数不等：确认数少于采集端上报的发送数 |
| `source_silent` | `warn` | 某个来源上报中断 |
| `auth_disabled`、`plaintext_listen`、`object_store_plaintext`、`webhook_plaintext` | `warn` | 启动时的安全提示：认证关闭、明文监听、对象存储或 webhook 走明文 |

Amberlog 以 5xx 回应请求时，会写一行 `level` 为 `error` 的日志，含该响应的 `x-request-id` 和出错原因。用户报告错误时，请他提供响应头里的 `x-request-id`，用它在日志里查找。

## 4. 积压与异常的处理

### 4.1 WAL 积压与提交失败

写入的记录先进 WAL，再由后台提交到对象存储。提交跟不上或停了，WAL 就会增长。实测输出的事件：

```json
{"ts":"2026-10-21T06:44:07.914513Z","level":"warn","msg":"wal.dir has grown past wal.alert_bytes: confirmed records are piling up locally","event":"wal_backlog_high","wal_bytes":23499,"alert_bytes":2000}
```

（例子把 `wal.alert_bytes` 设成了 2000 字节，默认是 8 GiB。）

按顺序检查：

1. `amberlog_ready` 与 `/readyz` 的 `object_store` 是否为 false。是，则对象存储不可用：检查网络、存储服务、凭据与证书。恢复以后，积压的记录自动提交，不需要人工操作。
2. `amberlog_manifest_commits_total{result="failed"}` 是否在增长。
3. 有没有 `dataset_disabled` 事件。停用的数据集的记录留在 WAL 里，不会提交，见 4.3 节。
4. 写入量是否超过了提交的能力。持续高于提交能力时，WAL 会一直增长。

处理期间注意：

- **不要删除 `wal.dir` 里的文件。** 里面是已经回过 200、还没提交的记录。删掉就丢了。
- `wal.dir` 所在磁盘要留出余量。磁盘写满后 `/readyz` 回 503，写入请求回 503。
- 内存中保留的待提交记录不超过 `ingest.max_backlog_memory_bytes`，超出部分只留在 WAL 里，稍后照常提交。所以对象存储长时间不可用不会耗尽内存，但会占用磁盘。

### 4.2 入湖积压

一个小时桶结束 `lifecycle.lake_watermark` 以后，应当入湖。`amberlog_lake_lag_seconds` 是最早一个该入湖而还没入湖的桶，已经超出入湖时间多少秒。超过 `lifecycle.lake_alert_after`（默认 24 小时）时输出事件：

```json
{"ts":"2026-10-21T06:44:09.914658Z","level":"warn","msg":"hour buckets past the lake watermark have not entered the cold tier for longer than lifecycle.lake_alert_after","event":"lake_backlog_high","dataset":"default/app","bucket":"2026102008","lag_seconds":74649,"alert_after_seconds":3600}
```

（例子把 `lifecycle.lake_watermark` 设成 `1h`、`lifecycle.lake_alert_after` 设成 `1h`，然后写入了前一天的数据，所以一启动就积压。随后这个桶入湖，积压消失。）

`bucket` 是桶的 UTC 小时，`YYYYMMDDHH`。用台账查看各数据集的状态：

```sh
curl -sS -X POST "$AMBERLOG/api/default/_search" -H "Authorization: Bearer $TOKEN" \
  -d '{"query":{"sql":"SELECT dataset, tier, pending_lake_record_count FROM _ledger","start_time":0,"end_time":1}}'
```

```json
{"took":27,"hits":[{"dataset":"default/app","tier":"hot_cold","pending_lake_record_count":0}],"total":1,"from":0,"size":100,"scan_records":0,"scan_files":0,"scan_bytes":0}
```

`tier` 为 `hot_pending_lake`、`pending_lake_record_count` 大于 0 时，说明有数据该入湖还没入湖。常见原因：

- 对象存储不可用。恢复后自动继续。
- 入湖前核对数据文件的摘要，发现不符（有 `digest_mismatch` 事件）。这个桶停止入湖，数据留在热层，照常可查。数据文件可能被外部改动过，见 4.4 节。
- 摘要相符，但把这个桶重写成冷层数据文件时失败。Amberlog 输出一行 `level` 为 `warn` 的日志，含 `dataset`、`bucket` 与失败原因 `reason`，并给非稳定指标 `amberlog_lake_rewrite_failures_total{org,stream}` 加 1。本进程不再为这个桶入湖，数据留在热层，照常可查；同一数据集更晚的桶照常入湖。重启进程后会再试一次。这行日志不是稳定事件，文字可能随版本变化，告警请用 `amberlog_lake_lag_seconds`（这一项没有实测：发版构建里造不出重写失败）。

### 4.3 数据集停用

Amberlog 读到某个数据集的存储格式版本不受当前程序支持，或它的 manifest 读不懂时，停用这个数据集：

- 输出 `dataset_disabled` 事件，`state` 为 `format_version` 或 `manifest_unreadable`；`amberlog_dataset_disabled` 为 1；`_ledger` 的 `status` 为 `disabled_format_version` 或 `disabled_manifest_unreadable`。
- 对它的写入与查询，格式版本不兼容时回 503 `unsupported_format_version`，manifest 不可读时回 500 `internal`。其他数据集不受影响。
- 已确认还没提交的记录留在 WAL 里，不丢。

处理：

- `format_version`：数据由更新版本的 Amberlog 写出过，或是早期版本写出的旧格式。换用能读这个格式的版本。
- `manifest_unreadable`：manifest 被外部改动或损坏。不要自己改对象存储里的文件，保留现场，报告问题。

### 4.4 数据被外部改动

Amberlog 假定只有它自己写 `<storage.prefix>/` 下的对象。有人绕过 Amberlog 改动或删除这些对象（例如用存储的管理工具，或用外部引擎改冷层表）时：

- 合并、入湖或冷层合并时发现数据文件与记录的摘要不符，输出 `digest_mismatch` 事件，这个文件不再参与合并或入湖。
- 审计证据的导出与查验会报告不一致，见第 6 节。

建议：

- 给 Amberlog 的凭据之外，不要给任何人或程序对 `<storage.prefix>/` 的写权限。外部引擎只给冷层前缀 `<storage.prefix>/_iceberg/` 的只读权限。
- 对存储桶开启版本控制，误删误改时可以找回原对象。
- 写入实例管理这份存储期间，不要用外部引擎对冷层表做过期快照、重写数据文件、清理孤儿等维护。这些是 Amberlog 自己的工作。

## 5. 停机、重启与升级

### 5.1 停机

向进程发 SIGTERM（或 SIGINT）。进程会：

1. 不再受理新的 `/api/` 请求，新请求回 503 `unavailable`；`/readyz` 回 503。
2. 等在途请求完成，最多等 `server.shutdown_timeout`（默认 10 秒）。
3. 把内存中的记录提交到对象存储，并把计数写出。
4. 以退出码 0 退出。

提交失败（例如对象存储此刻不可用）时，记录留在 WAL 里，下次启动时重放，不会丢。停机过程中再发一次信号，进程立即以非 0 退出码退出，WAL 同样保留。

`systemd` 的单元里用默认的 `KillSignal=SIGTERM`，并把 `TimeoutStopSec` 设得比 `server.shutdown_timeout` 长，给提交留出时间，例如 60 秒。

### 5.2 重启

用**同一个 `wal.dir`** 重启。启动时先重放 WAL，`/readyz` 首次回 200 时重放已经完成，全部已确认的记录都可以查到。

- 进程被强杀（SIGKILL）或主机崩溃后重启，已确认的记录不会丢，也不会重复。
- 换了一个 `wal.dir` 启动时，原 WAL 里还没提交的记录不会进来。不要这样做，除非原 WAL 已经确认为空。
- 启动失败时看退出码：2 配置错误，3 `wal.dir` 或对象存储不可用（含 WAL 损坏），4 端口无法监听。详见 [配置项参考第 9 节](configuration.zh-CN.md#9-退出码)。

### 5.3 升级

补丁版本（例如 0.1.0 → 0.1.1）不改数据格式，可以随意升级和降级。改数据格式的次版本，变更记录里有升级须知：要逐个次版本升级，不能跳过，也不能降级。冷层表永远保持可读。见 [已知限制与路线图](known-limitations.zh-CN.md) 第 6.2 节。

1. 读新版本的变更记录，确认配置项、接口与数据格式有没有不兼容的变化，有没有升级须知。
2. 按 5.1 节停掉旧进程，等它以退出码 0 退出。
3. 用同一份配置和同一个 `wal.dir` 启动新版本。
4. 等 `/readyz` 回 200，查看有没有 `dataset_disabled` 事件。

## 6. 审计证据：导出与查验

Amberlog 的每次提交都记下了文件摘要与前后快照的链接，删除也留下记录（留痕）。证据导出把某个数据集在某一时刻的这条链「钉住」，打成一个证据包；证据查验在以后任何时候核对：现在对象存储里的数据，是不是那条链的延长，有没有被改动或被提前删除。

两件事都用同一个可执行文件，不需要连接正在运行的 Amberlog，也不写对象存储，所以可以在写入实例运行时进行，也可以由审计方在自己的机器上用只读凭据进行。

### 6.1 凭据与配置

- 必填配置只有 `object_store.url`。`object_store.url` 与 `storage.prefix` 要与写入实例相同；S3 的端点、区域、证书等配置项照常使用。不需要 `wal.dir` 与 `auth.token`。
- 凭据要能读取与列举整个 `<storage.prefix>/`，含 `_system/` 与 `_iceberg/`。只给部分前缀的凭据不够用。开了 SSE-KMS 的桶，凭据还要有解密权限。
- 审计用的凭据与外部引擎用的冷层只读凭据分开签发。

### 6.2 导出证据

```text
amberlog evidence export [--config <path>] --dataset <org>/<stream> [--from <时刻>] [--to <时刻>] --output <证据包路径>
```

| 参数 | 说明 |
|---|---|
| `--dataset` | 数据集，如 `default/app` |
| `--from`、`--to` | 时间段，UTC 整点，写作 `YYYY-MM-DDTHH:00:00Z`，左闭右开。两个都给或都不给；都不给是全时段 |
| `--output` | 证据包的路径。不能已经存在；所在目录要存在且可写。用本地磁盘存储时，不能写在存储目录里 |

```sh
amberlog evidence export --config /etc/amberlog/amberlog.toml \
  --dataset default/app --output app-full.evidence.tar > app-full.receipt.json
echo $?
```

```text
0
```

stdout 上是**回执**：一行 JSON，记下导出时钉住的链头。

```json
{"evidence_format_version":1,"dataset":"default/app","exported_at":1792565051708062,"amberlog_version":"0.1.0",
 "snapshot":{"snapshot_id":3,"path":"manifest/00000000000000000003.json","content_digest":"sha256:847a33e5e386e49ff318f96a39692d63e4da03af0f0f2cdfec79cf229746d714"},
 "retired_segment":null,"cold_deletion_segment":null,
 "cold":{"metadata_version":1,"path":"metadata/v1.metadata.json","content_digest":"sha256:2712c64af9ad5cc519cd00f67457cc7a33b7a6e5aefba5bbd077607350708483"}}
```

（为便于阅读折了行，实际是一行。）

只导出一段时间：

```sh
amberlog evidence export --config /etc/amberlog/amberlog.toml --dataset default/app \
  --from 2026-10-20T08:00:00Z --to 2026-10-20T09:00:00Z \
  --output app-2026102008.evidence.tar > app-2026102008.receipt.json
```

**证据包**是一个不压缩的 tar 文件，可以用 `tar` 解开查看：

| 成员 | 内容 |
|---|---|
| `evidence.json` | 数据集、时间段、导出时刻、程序版本、回执、Iceberg 快照对应表、退役记录 |
| `receipt.json` | 回执，与 stdout 上的那一行逐字节相同 |
| `files.jsonl` | 时间段内每个数据文件一行：层（`hot`、`cold`）、路径、状态（`live` 或 `deleted`）、摘要、记录数、字节数、时间范围；已删除的文件另有删除原因、截止时刻与删除时刻 |
| `check.json` | 导出时的核对结果：`result` 为 `ok` 或 `problems_found` |
| `objects/` | 所涉元数据的原样副本：快照、组成各桶 manifest 的全部清单片、留痕段、冷层元数据、退役记录 |
| `SHA256SUMS` | 其余各成员的 SHA-256，在解开的目录里执行 `sha256sum -c SHA256SUMS` 可以核对 |

```sh
mkdir x && tar -xf app-full.evidence.tar -C x && cd x && sha256sum -c SHA256SUMS | tail -2 && cat check.json
```

```text
objects/default/app/manifest/buckets/2026102106-01m5ek7qqk8z5hcz7awgpy50k0.json: OK
receipt.json: OK
{"result":"ok","problems":[],"transitions":[],"notes":[]}
```

证据包不含数据文件本身，但含清单片，里面有各列取值的统计片段。**证据包要按敏感材料保管。**

导出的退出码：

| 退出码 | 含义 |
|---|---|
| 0 | 证据包已写完，导出时的核对没有发现问题 |
| 5 | 证据包已写完，`check.json` 里记有问题 |
| 2 | 参数或配置有误；`--output` 已存在；数据集不存在 |
| 3 | 启动时读不了对象存储 |
| 6 | 没做完：导出期间对象存储不可用、链在导出期间前进导致所钉住的对象已被回收（重跑即可）、收到中止信号，或数据是早期版本的旧格式。不留下证据包 |

退出码不是 0 或 5 时，`--output` 上不会出现文件，可以用同一路径重跑。

### 6.3 查验证据

```text
amberlog evidence verify [--config <path>] --evidence <证据包> [--previous <旧回执或旧证据包>] [--retention <时长>]
```

| 参数 | 说明 |
|---|---|
| `--evidence` | 要查验的证据包 |
| `--previous` | 以前保存的回执或证据包。给了它，查验另证明现在的链也是那一次的延长 |
| `--retention` | 约定的保留期，如 `1095d`。给了它，另核对到期删除没有早于约定 |

```sh
amberlog evidence verify --config /etc/amberlog/amberlog.toml \
  --evidence app-full.evidence.tar --retention 1095d > app-full.verify.json
echo $?
```

```text
0
```

stdout 上是一行 JSON 报告（下面折了行，`...` 处省略了部分字段）：

```json
{"result":"pass","dataset":"default/app","verified_at":1792565052012298,"evidence_exported_at":1792565051708062,
 "anchors":[{"source":"evidence","kind":"package","snapshot_id":3,"exported_at":1792565051708062}],
 "current":{"snapshot":{"snapshot_id":3,...},"retired_segment":null,"cold_deletion_segment":null,"cold":{"metadata_version":1,...}},
 "retention_checked":true,"checked":{"data_files":3,"digests_recomputed":2},
 "problems":[],"unprovable":[],
 "transitions":[{"kind":"traced_still_present","object":"default/app/data/2026/10/20/08/01m5ek7qq6jg62cfz4ptmxaawm.parquet","detail":"the deleted file is still in the object store (delayed physical deletion)"}],
 "notes":[]}
```

| 成员 | 说明 |
|---|---|
| `result` | `pass`（通过）、`inconsistent`（发现不一致）或 `unprovable`（证明不了） |
| `anchors` | 用作起点的回执：证据包自带的，以及 `--previous` 给的 |
| `current` | 查验时的链头 |
| `checked` | 核对的数据文件数，与重新读取并计算了摘要的数据文件数 |
| `problems` | 不一致。有它时 `result` 为 `inconsistent` |
| `unprovable` | 证明不了的项，例如读不到某个文件、旧回执所记的快照已过保留期被回收 |
| `transitions` | 正常的中间状态，不影响结果。上例的 `traced_still_present` 是：文件已经入湖并留痕，原文件还在等延后的物理删除（`storage.delete_delay`） |
| `notes` | 提示，不影响结果 |

查验会读出范围内每个存活数据文件的全部字节，重新计算摘要，不抽样。全时段查验的读取量与数据总量同级，请预留时间与流量。

查验的退出码：

| 退出码 | 含义 |
|---|---|
| 0 | 通过 |
| 5 | 发现不一致，看 `problems` |
| 6 | 证明不了，看 `unprovable`；或查验期间收到中止信号（这时没有报告） |
| 2 | 参数、文件或配置有误；`--evidence` 不是证据包；`--previous` 与 `--evidence` 不是同一数据集 |
| 3 | 启动时读不了对象存储 |

`unprovable` 里只有 `snapshot_anchor_recycled`、`problems` 为空，是正常情形：旧回执所记的快照已超过快照保留期被回收（`hot_retention.snapshots` 与 `hot_retention.age`），留痕段与冷层版本照常核对过了。有 `unavailable` 或 `data_unreadable` 时，检查凭据、网络与存储后重跑。有 `format_unsupported` 时，这个版本的程序不认识证据包、旧回执或此刻数据集的格式版本，查验就此停止，其余项都没有核对：用与这个格式相符的版本查验。

### 6.4 推荐的审计流程

**运营方：**

1. 按约定的周期（例如每月）对每个数据集导出证据包。
2. 把证据包与回执交给审计方。

**审计方：**

1. 把回执存到运营方改不到的地方。
2. 保存整个证据包，不只是回执。
3. 查验：`amberlog evidence verify --evidence <新证据包> --previous <上一次保存的回执或证据包> --retention <约定保留期>`。
4. 按 `result` 与各数组读报告。

用 `--previous` 查验时，此前那次导出之后发生的整链重写也能被发现。实测：导出第一个证据包之后又写入了一批数据，再导出第二个证据包，用第一个回执查验（报告省略了部分字段）：

```sh
amberlog evidence verify --config /etc/amberlog/amberlog.toml \
  --evidence app-full-2.evidence.tar --previous app-full.receipt.json
```

```json
{"result":"pass",...,"anchors":[{"source":"evidence","kind":"package","snapshot_id":4,...},{"source":"previous","kind":"receipt","snapshot_id":3,...}],...}
```

**证据能证明和不能证明的：**

- 能证明：证据包所描述的数据与导出时对象存储里的链一致；查验时的链是旧回执所记链头的延长；存活数据文件的内容没变；按到期删除的每个数据文件，其中的记录都早于它的截止时刻（给了 `--retention` 时，截止时刻也不晚于删除时刻减约定保留期）。
- 不能证明：对存储有写权限的人从头重写整条链，在审计方保存第一份回执之前发生的，查不出来。所以审计方要尽早、定期保存回执。
- 只交回执时不能证明：有写权限的人按格式写出一个新的热层快照，去掉一个存活数据文件而不留删除留痕。拿列着该文件的旧证据包查验时能查出（报告记 `vanished`）。所以审计方要保存整个证据包。
- 查验不是第二份独立实现：查验工具与写入数据的程序是同一份实现。「独立」只指查验不依赖正在运行的 Amberlog。不用 Amberlog 也能手工核对一部分：在解开的目录里执行 `sha256sum -c SHA256SUMS`，并把数据文件的摘要与字节数同 `files.jsonl` 比对。手工核对只覆盖查验的一部分核对项。
- 覆盖不到：已确认但还没提交到对象存储的记录（在 WAL 里）；已被快照回收删掉的旧快照；入湖与热层封口合并前后的记录条数是否守恒；快照回收是否删多了；计数记录；退役记录（它不在摘要链上，查验只把它与证据包里的副本比较，不符时只记提示）。

## 7. 优雅退役

不再使用 Amberlog 时，用 `amberlog retire` 把热层的全部数据转进冷层的 Iceberg 表，做完收尾，最后写下**退役记录** `<storage.prefix>/_system/retirement.json`。之后，数据完全由标准 Iceberg 表承载。DuckDB、Apache Spark 等引擎按桶内约定直接找到表，不需要 Amberlog 在运行；Trino 要经 Amberlog 的只读目录进程找表，这个进程只读对象存储（见[外部引擎直读指南](external-engines.zh-CN.md)第 3 节）。

### 7.1 退役前

按顺序做：

1. 向写入实例发 SIGTERM，等它以退出码 0 退出。
2. 确认这份存储上没有别的写入实例在运行。只读目录（`amberlog catalog`）与证据子命令不算。
3. 准备 `serve` 所用的**同一份配置**（配置文件与全部 `AMBERLOG_` 环境变量），含同一个 `wal.dir`。`retire` 会重放这个 WAL，把其中还没提交的记录也转进冷层。
4. 确认对象存储凭据能读、写、列举与删除 `<storage.prefix>/` 下的对象。

`retire` 必填的配置只有 `object_store.url`、`wal.dir`，以及 S3 下的 `object_store.s3.conditional_put`；不需要 `auth.token`。

### 7.2 执行

```sh
AMBERLOG_STORAGE__DELETE_DELAY=1s amberlog retire --config /etc/amberlog/amberlog.toml > retire-report.json
echo $?
```

```text
0
```

`retire` 依次：重放 WAL 并提交；把每个还有数据的小时桶入湖（不管是否到了入湖时间）；物理删除已入湖的热层文件；全部完成后写退役记录。物理删除前要等 `storage.delete_delay`（默认 1 小时）。退役时没有在途查询，可以像上面这样只为这一次把它设小，取值必须大于 0。

stdout 上是一行报告：

```json
{"status":"retired","interrupted":false,"record":"amberlog/_system/retirement.json",
 "datasets":[{"dataset":"default/app","settled":true,"snapshot_id":5,"cold_metadata_version":2,"buckets_laked":1,"files_deleted":3}],
 "exceptions":[]}
```

| 字段 | 说明 |
|---|---|
| `status` | `retired`（本次写出了退役记录）、`already_retired`（此前已退役）、`exceptions`（有要人处理的例外）、`incomplete`（没做完） |
| `interrupted` | 是否收到过 SIGTERM 或 SIGINT |
| `record` | 退役记录的位置 |
| `datasets` | 每个数据集：是否已收尾（`settled`）、最后的热层快照与冷层版本、本次入湖的桶数、本次删除的文件数 |
| `exceptions` | 每个例外：数据集、种类（`kind`）、桶、相关对象 |

| 退出码 | 含义 | 下一步 |
|---|---|---|
| 0 | `retired` 或 `already_retired` | 做 7.4 节 |
| 5 | `exceptions` | 按下表处理例外，然后重跑 |
| 6 | `incomplete`：收到中止信号，或运行中对象存储不可用 | 排除问题后重跑 |
| 2 | 配置或用法错误（没有报告） | 改正后重跑 |
| 3 | 启动时 `wal.dir` 或对象存储不可用，或 WAL 损坏（没有报告） | 人工修复后重跑 |

| 例外 `kind` | 含义 | 处理 |
|---|---|---|
| `digest_mismatch` | 某个桶的数据文件内容与记录的摘要不符，这个桶没有入湖 | 数据文件被改动过。从备份或存储的版本控制恢复原文件，然后重跑 |
| `input_missing` | 某个桶的数据文件不见了，这个桶没有入湖 | 把 `paths` 所列文件恢复到原位置，然后重跑 |
| `dataset_disabled` | 数据集处于停用状态（见 4.3 节） | 换用能读该格式的版本，然后重跑 |
| `unsettled` | 各步都做过了，仍未收尾 | 按 `paths` 检查，然后重跑 |
| `store_unavailable`、`delete_failed`、`record_write_failed` | 对象存储不可用，或没有删除权限 | 恢复访问、补上权限，然后重跑 |

有任何例外时都不写退役记录，没有「带例外也算完成」的选项。

**中途停止或崩溃都可以重跑。** `retire` 不写进度文件，每一步按对象存储与 WAL 的现状判定，做过的自然跳过；重跑完成后，每条已确认的记录在冷层里恰好出现一次。退役记录写出之前，`serve` 仍能照常启动，所以中途放弃退役也可以直接恢复服务。

### 7.3 退役之后

- 再运行 `retire`，回 `already_retired`，退出码 0。
- `serve` 拒绝在已退役的存储上启动，以退出码 2 退出：

```text
{"ts":"...","level":"error","msg":"the storage prefix has been retired: amberlog/_system/retirement.json exists (delete it only to revive the prefix on purpose)","exit_code":2}
```

- 读数据有两种方式：
  - DuckDB、Spark 等支持桶内约定的引擎直接读冷层表，不需要任何 Amberlog 进程。表在 `<storage.prefix>/_iceberg/<org>/<stream>/`，`metadata/version-hint.text` 已经写成当前版本号。
  - 运行只读目录 `amberlog catalog`，外部引擎经它的 Iceberg REST 端点 `/iceberg/` 找表；Trino 只能用这种办法。它只需要冷层前缀的读权限，退役后照常运行。

```sh
curl -sS "$AMBERLOG/iceberg/v1/amberlog/namespaces/default/tables" -H "Authorization: Bearer $TOKEN"
```

```json
{"identifiers":[{"namespace":["default"],"name":"app"}]}
```

- 退役后桶里会留下热层的快照、manifest、留痕、计数记录等元数据，以及可能有的孤儿文件。Amberlog 不再管理它们，由你自行处置。
- 退役之后 Amberlog 不再做到期删除。到期数据的删除改由你负责，也不再留痕。

### 7.4 退役后立即导出证据

退役记录本身不是证据。`retire` 以退出码 0 退出以后，立即对每个数据集做一次全时段导出与查验，把回执、证据包与查验报告一起存档，作为退役时刻的证据：

```sh
amberlog evidence export --config /etc/amberlog/amberlog.toml --dataset default/app \
  --output app-retired.evidence.tar > app-retired.receipt.json
amberlog evidence verify --config /etc/amberlog/amberlog.toml \
  --evidence app-retired.evidence.tar --previous app-full.receipt.json > app-retired.verify.json
```

实测两条命令都以退出码 0 结束，查验结果为 `pass`。证据包的 `evidence.json` 里带有退役记录。

退役以后，对冷层表的任何改动（包括用外部引擎删除过期分区、过期快照、重写文件）都会使此后用这份证据做的查验报不一致，查验分不出这是你的维护还是未经授权的改动。所以要在任何维护之前先完成这一步。

## 8. 已知现象

本节只列运维时会碰到的现象。这个版本的全部已知限制见 [已知限制与路线图](known-limitations.zh-CN.md)。

- **本地磁盘存储的目录在运行中消失时，`/readyz` 不报未就绪。** 用 `file://` 存储时，存储目录在运行中被移走或所在的卷被卸载，Amberlog 会把这个目录重新建出来，`/readyz` 仍回 200；之后提交失败、查询回 500。请用 `amberlog_manifest_commits_total{result="failed"}` 告警（3.2 节）发现这种情形，并用挂载检查保护存储卷。下一阶段修正（[已知限制与路线图](known-limitations.zh-CN.md) 第 7.1 节）。S3 存储不受影响。

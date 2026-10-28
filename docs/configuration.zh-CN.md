# 配置项参考

[English](configuration.md)

> 适用于 Amberlog 0.1.0。

本文列出 Amberlog 的全部配置项：每一项的含义、默认值、取值范围和对应的环境变量名。文中的环境变量名、取值范围、必填项与退出码都在 Linux 上实测过。

## 1. 配置从哪里来

Amberlog 从三处取配置，优先级从高到低：

1. **环境变量**：名称以 `AMBERLOG_` 开头。
2. **配置文件**：TOML 格式，用 `--config <路径>` 指定。不给 `--config` 时不读任何配置文件。
3. **内置默认值**。

配置只在启动时读取。改了配置要重启进程才生效。例外：入站 TLS 的证书与私钥文件的内容会自动重读（见 [`server.tls.cert_file`](#server)）。

### 1.1 配置键与环境变量名

配置键用点号分层，例如 `object_store.url`。在配置文件里写成 TOML 的表：

```toml
[object_store]
url = "s3://my-bucket/amberlog"
```

对应的环境变量名按下面的规则得出：

1. 加前缀 `AMBERLOG_`。
2. 把配置键改成大写。
3. 把每个点号 `.` 换成两个下划线 `__`。键名里原有的单个下划线不变。

例如：

| 配置键 | 环境变量 |
|---|---|
| `object_store.url` | `AMBERLOG_OBJECT_STORE__URL` |
| `server.tls.cert_file` | `AMBERLOG_SERVER__TLS__CERT_FILE` |
| `alerts.missed_collection.enabled` | `AMBERLOG_ALERTS__MISSED_COLLECTION__ENABLED` |

环境变量名必须与配置键的大写形式完全一致。小写或拼错的名字（如 `AMBERLOG_server__http_addr`）按不认识的配置键处理。

取值为空字符串的环境变量算作「已设置，值为空字符串」，不算未设置。对 `wal.dir`、`auth.token` 和几个文件路径项，空字符串又视同没有配置（见各项说明）。

### 1.2 不认识的配置键

配置文件或环境变量里出现 Amberlog 不认识的配置键时，进程拒绝启动，以退出码 2 退出，并在 stderr 指出这个键名。这条规则防止拼错的键被静默忽略。

**所以，同一环境里的其他程序不要使用 `AMBERLOG_` 开头的环境变量。** 例如采集器的令牌变量，不要叫 `AMBERLOG_TOKEN`。

有三个配置项只能写在配置文件里：`index_hints`、`alerts.rules` 和 `alerts.missed_collection.webhook_headers`。以环境变量设置它们（名称以 `AMBERLOG_INDEX_HINTS`、`AMBERLOG_ALERTS__RULES` 或 `AMBERLOG_ALERTS__MISSED_COLLECTION__WEBHOOK_HEADERS` 开头）时，同样按不认识的配置键处理。

## 2. 取值的写法

| 类型 | 配置文件里 | 环境变量里 |
|---|---|---|
| 布尔 | TOML 布尔：`true`、`false` | 只认小写的 `true`、`false`。`True`、`TRUE`、`1`、`0`、`yes`、`on` 都不合法 |
| 整数 | TOML 整数（不加引号） | 十进制整数，可带前导 `-`，在 64 位有符号整数范围内。带 `_`、`+`、小数点或指数（如 `1e3`）都不合法 |
| 时长 | TOML 字符串，如 `"5m"` | 同样的写法，如 `5m` |
| 字符串、路径 | TOML 字符串 | 原样 |

**时长**写成 `<整数><单位>`：

- 整数是一个或多个十进制数字，不带正负号、小数点、空格或下划线。
- 单位是 `ms`、`s`、`m`、`h`、`d` 之一，只能小写。`d` 等于 86400 秒。
- 换算成微秒后不能超过 2^63−1（约 106751991 天）。

合法的例子：`500ms`、`10s`、`5m`、`72h`、`30d`。不合法的例子：`5`、`1.5h`、`5 s`、`5S`、`+5s`、`5us`。

在配置文件里，值的 TOML 类型必须对。例如 `auth.enabled = "false"`（字符串）和 `flush.max_records = "10"`（字符串）都按类型错误以退出码 2 退出。

**相对路径**按进程的工作目录解析。

取值不合法（类型、格式或范围不对）时，进程以退出码 2 退出，并在 stderr 指出键名。

## 3. 各子命令的必填项

| 子命令 | 必填的配置项 |
|---|---|
| `amberlog serve` | `object_store.url`、`wal.dir`；`auth.enabled` 为 true（默认）时还有 `auth.token`；`object_store.url` 为 `s3://` 时还有 `object_store.s3.conditional_put` |
| `amberlog catalog` | `object_store.url`；`auth.enabled` 为 true 时还有 `auth.token` |
| `amberlog retire` | `object_store.url`、`wal.dir`；`object_store.url` 为 `s3://` 时还有 `object_store.s3.conditional_put` |
| `amberlog evidence export`、`amberlog evidence verify` | `object_store.url` |

缺少必填项时，进程向 stderr 输出一行含缺失键名的错误，以退出码 2 退出。

不论以哪个子命令运行，Amberlog 都识别并校验全部配置键。某个子命令用不到的配置项照常校验，但不起作用。各子命令用到哪些配置项，见[第 6 节](#6-各运行方式下起作用的配置项)。

## 4. 配置项一览

「范围」一列写允许的取值。超出范围按取值不合法处理。字节数的默认值后面括注了换算。

### server

| 配置键 | 环境变量 | 默认值 | 范围 | 说明 |
|---|---|---|---|---|
| `server.http_addr` | `AMBERLOG_SERVER__HTTP_ADDR` | `0.0.0.0:8800` | `<IPv4 地址>:<端口>` 或 `[<IPv6 地址>]:<端口>`，端口 0–65535。不接受主机名（如 `localhost`）和 IPv6 区域标识 | 监听地址。全部端点（`/api/`、`/metrics`、`/healthz`、`/readyz`、`/ui`、`/iceberg/`）都在这个地址上。地址合法但无法监听（如端口被占用）时，以退出码 4 退出 |
| `server.shutdown_timeout` | `AMBERLOG_SERVER__SHUTDOWN_TIMEOUT` | `10s` | 时长 | 收到 SIGTERM 或 SIGINT 后，等待在途请求完成的最长时间。届满后不再等待 |
| `server.tls.cert_file` | `AMBERLOG_SERVER__TLS__CERT_FILE` | 无 | 文件路径。空字符串视同没有配置 | 入站 TLS 的证书链文件（PEM）。第一张是服务端证书，其后是中间证书。必须与 `server.tls.key_file` 成对配置 |
| `server.tls.key_file` | `AMBERLOG_SERVER__TLS__KEY_FILE` | 无 | 文件路径。空字符串视同没有配置 | 入站 TLS 的私钥文件（PEM）。见下文「入站 TLS」 |
| `server.behind_tls_proxy` | `AMBERLOG_SERVER__BEHIND_TLS_PROXY` | `false` | 布尔 | 明文声明：表示 `server.http_addr` 前面有反向代理终止 TLS。见下文「明文监听的规则」 |

**明文监听的规则**（`serve` 与 `catalog`）：

| 是否配置了证书 | 监听地址 | `server.behind_tls_proxy` | 结果 |
|---|---|---|---|
| 是 | 任意 | `false` | 只以 TLS 监听 |
| 是 | 任意 | `true` | 以退出码 2 退出：证书与明文声明不能同时配置 |
| 否 | 回环地址（`127.0.0.0/8` 或 `::1`） | 任意 | 以明文监听 |
| 否 | 非回环地址 | `false` | 以退出码 2 退出，stderr 指出 `server.http_addr` 与 `server.behind_tls_proxy` |
| 否 | 非回环地址 | `true` | 以明文监听，启动时输出一条 `plaintext_listen` 警告日志 |

`0.0.0.0`、`[::]` 不是回环地址。所以用默认的 `server.http_addr` 启动时，必须配置证书或设置 `server.behind_tls_proxy = true`。离线子命令（`retire`、`evidence export`、`evidence verify`）不监听，不受这条规则限制。

**入站 TLS：**

- 配置证书以后，这个地址只接受 TLS 1.2 与 TLS 1.3 连接。明文 HTTP 请求得不到任何 HTTP 响应。
- 不要求客户端证书。
- 接受的私钥：未加口令的 RSA（2048、3072、4096 位，PKCS#8 或 PKCS#1 编码）、ECDSA（P-256、P-384，PKCS#8 或 SEC1 编码）、Ed25519（PKCS#8 编码）。私钥必须与证书文件里的第一张证书匹配。
- 文件不存在、不可读、内容无效，或私钥与证书不匹配时，以退出码 2 退出。
- 运行中替换两个文件的内容，不需要重启：进程每隔不超过 60 秒读一次两个文件；内容变成新的有效证书与私钥以后，最迟 120 秒，新建立的连接就使用新证书。已建立的连接不受影响。新内容无效时，进程继续用原来的证书，并输出一条 `tls_reload_failed` 错误日志。

### storage

| 配置键 | 环境变量 | 默认值 | 范围 | 说明 |
|---|---|---|---|---|
| `storage.prefix` | `AMBERLOG_STORAGE__PREFIX` | `amberlog` | 匹配 `^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$` | Amberlog 写入的全部对象都在 `object_store.url` 所指位置下的这个前缀之下 |
| `storage.commit_max_retries` | `AMBERLOG_STORAGE__COMMIT_MAX_RETRIES` | `8` | 整数，不小于 0 | 提交（热层快照与冷层提交）遇到并发冲突时的最多重试次数，按指数退避 |
| `storage.delete_delay` | `AMBERLOG_STORAGE__DELETE_DELAY` | `1h` | 时长，大于 0 | 数据文件因入湖、到期删除或合并而不再使用以后，至少保留这么久才物理删除。冷层不再使用的快照元数据也按这个时长保留。它给正在进行的查询和外部引擎的读取留出时间 |

### object_store

| 配置键 | 环境变量 | 默认值 | 范围 | 说明 |
|---|---|---|---|---|
| `object_store.url` | `AMBERLOG_OBJECT_STORE__URL` | 无（必填） | `file:///<绝对路径>` 或 `s3://<桶名>[/<路径>]`，见下文 | 数据存放的位置 |
| `object_store.s3.conditional_put` | `AMBERLOG_OBJECT_STORE__S3__CONDITIONAL_PUT` | 无 | `create_only`，或同义的 `etag` | 运维确认这个存储支持以请求头 `If-None-Match: *` 实现「仅当不存在时创建」。`s3://` 下以 `serve` 或 `retire` 运行时必填。见下文「条件写探测」 |
| `object_store.s3.endpoint` | `AMBERLOG_OBJECT_STORE__S3__ENDPOINT` | 无 | 以 `http://` 或 `https://` 开头的绝对 URL | S3 兼容存储的端点地址，如 `https://minio.example.com:9000`。端点以 `http://` 开头时照常以明文连接，启动时输出一条 `object_store_plaintext` 警告日志 |
| `object_store.s3.region` | `AMBERLOG_OBJECT_STORE__S3__REGION` | `us-east-1` | 非空字符串 | 区域 |
| `object_store.s3.access_key_id` | `AMBERLOG_OBJECT_STORE__S3__ACCESS_KEY_ID` | 无 | 字符串 | 访问密钥 ID。与 `secret_access_key` 成对配置，只配其一时以退出码 2 退出并指出缺的那个。两个都不配时，从 [AWS 凭据环境变量](#7-s3-凭据环境变量) 与凭据链取得凭据 |
| `object_store.s3.secret_access_key` | `AMBERLOG_OBJECT_STORE__S3__SECRET_ACCESS_KEY` | 无 | 字符串 | 访问密钥 |
| `object_store.s3.force_path_style` | `AMBERLOG_OBJECT_STORE__S3__FORCE_PATH_STYLE` | `false` | 布尔 | 为 true 时，请求地址用路径式（`<端点>/<桶名>/<对象键>`），不用虚拟主机式（`<桶名>.<端点>/<对象键>`） |
| `object_store.s3.ca_file` | `AMBERLOG_OBJECT_STORE__S3__CA_FILE` | 无 | 文件路径。空字符串视同没有配置 | 在系统信任的根证书之外，追加信任这个文件（PEM）里的全部证书，用来验证对象存储端点的证书。不替换系统根证书。`s3://` 下文件不存在、不可读或内容无效时，以退出码 2 退出。Amberlog 总是验证对象存储端点的证书，不提供跳过验证的配置 |
| `object_store.s3.server_side_encryption` | `AMBERLOG_OBJECT_STORE__S3__SERVER_SIDE_ENCRYPTION` | 无 | `sse_s3` 或 `sse_kms` | 服务端加密。`sse_s3`：每个写入请求带 `x-amz-server-side-encryption: AES256`。`sse_kms`：带 `x-amz-server-side-encryption: aws:kms`。不配时不带加密请求头，对象按存储桶自己的默认加密设置落盘 |
| `object_store.s3.sse_kms_key_id` | `AMBERLOG_OBJECT_STORE__S3__SSE_KMS_KEY_ID` | 无 | 非空字符串。只能在 `server_side_encryption` 为 `sse_kms` 时配置，否则以退出码 2 退出 | KMS 密钥 ID，写入请求另带 `x-amz-server-side-encryption-aws-kms-key-id`。不配时使用存储方的默认 KMS 密钥 |

**`object_store.url` 的写法：**

- 方案只能是小写的 `file` 或 `s3`。`S3://`、`File://`、`http://` 都不合法。
- `file://` 后面必须是绝对路径，即写成三个斜杠：`file:///var/lib/amberlog/data`。
- `s3://` 后面是桶名，可以再跟一段路径作为对象键前缀：`s3://my-bucket/amberlog`。桶名由字母、数字、`-`、`.`、`_`、`~` 组成，大小写原样使用。
- 不能含用户名口令（`user:pass@`）、查询串（`?`）和片段（`#`）。
- 路径部分按 RFC 3986 逐段做百分号解码。不能有两个相邻的 `/`，解码后的段不能是 `.` 或 `..`。末尾的 `/` 不改变含义。

**条件写探测：** 以 `serve` 或 `retire` 启动时，Amberlog 在 `<storage.prefix>/_system/` 下对同一个新键先后两次做「仅当不存在时创建」写入。第一次应成功，第二次应以「已存在」失败。如果第二次也成功，或存储对这种写入回 HTTP 400 或 501，说明这个存储不支持 Amberlog 的提交协议，进程以退出码 2 退出。

**`file://` 与服务端加密：** `object_store.url` 为 `file://` 时，`object_store.s3.*` 各项照常校验取值，但不起作用。例如不读取 `ca_file` 所指的文件。

### wal

| 配置键 | 环境变量 | 默认值 | 范围 | 说明 |
|---|---|---|---|---|
| `wal.dir` | `AMBERLOG_WAL__DIR` | 无（`serve`、`retire` 必填） | 目录路径。空字符串视同没有配置 | 预写日志（WAL）目录。已确认、尚未提交到对象存储的记录先写在这里。目录不存在时自动创建；无法创建或不可写时以退出码 3 退出 |
| `wal.alert_bytes` | `AMBERLOG_WAL__ALERT_BYTES` | `8589934592`（8 GiB） | 整数，不小于 1 | 全局。WAL 中积压的字节数从不超过它变为超过它时，输出一条 `wal_backlog_high` 警告日志。回落到它以下之前不再重复输出 |

### auth

| 配置键 | 环境变量 | 默认值 | 范围 | 说明 |
|---|---|---|---|---|
| `auth.enabled` | `AMBERLOG_AUTH__ENABLED` | `true` | 布尔 | 为 false 时关闭认证：全部端点都不要求令牌，`auth.token` 不再必填，启动时输出一条 `auth_disabled` 警告日志 |
| `auth.token` | `AMBERLOG_AUTH__TOKEN` | 无（`serve`、`catalog` 在认证开启时必填） | 只含 `A`–`Z`、`a`–`z`、`0`–`9` 与 `.` `_` `~` `+` `/` `-`，末尾可以跟若干个 `=`（RFC 6750 的 `b64token`）。空字符串视同没有配置 | 访问令牌。客户端在请求头里发送 `Authorization: Bearer <令牌>`。`/api/`、`/metrics` 与 `/iceberg/` 下的请求要求令牌；`/healthz`、`/readyz` 不要求 |

令牌可以这样生成：

```sh
openssl rand -hex 32
```

### ingest 与 flush

| 配置键 | 环境变量 | 默认值 | 范围 | 说明 |
|---|---|---|---|---|
| `ingest.max_body_bytes` | `AMBERLOG_INGEST__MAX_BODY_BYTES` | `33554432`（32 MiB） | 整数，不小于 1 | 请求体的字节上限。压缩的请求体按收到的字节数和解压后的字节数分别比较。超过时回 HTTP 413、`reason` 为 `payload_too_large`。适用于 `/api/` 下的全部 POST 端点 |
| `ingest.max_future` | `AMBERLOG_INGEST__MAX_FUTURE` | `24h` | 时长 | 记录的 `_timestamp` 最多可以比服务端接收时刻晚多少。更晚的记录逐条拒绝，`reason` 为 `timestamp_out_of_range` |
| `ingest.max_past` | `AMBERLOG_INGEST__MAX_PAST` | 无（不限） | 时长 | 配置时，`_timestamp` 早于「接收时刻减这个值」的记录逐条拒绝，`reason` 为 `timestamp_out_of_range` |
| `ingest.max_columns` | `AMBERLOG_INGEST__MAX_COLUMNS` | `1000` | 整数，不小于 1 | 每个数据集的列数上限，含 `_timestamp`。一条记录要新增的列会使列数超过上限时，这条记录被拒绝，`reason` 为 `too_many_columns`。字段都落在已有列上的记录不受影响 |
| `ingest.max_backlog_memory_bytes` | `AMBERLOG_INGEST__MAX_BACKLOG_MEMORY_BYTES` | `268435456`（256 MiB） | 整数，不小于 1 | 全局。已确认、尚未提交的记录在内存中最多占多少字节（按它们在 WAL 中所占的字节计）。放不进内存的记录只留在 WAL 里，稍后照常提交。对象存储长时间不可用时，这一项防止内存随积压无限增长 |
| `flush.interval` | `AMBERLOG_FLUSH__INTERVAL` | `5s` | 时长，大于 0 | 写入提交的周期：把已确认的记录写成数据文件并提交到对象存储 |
| `flush.max_records` | `AMBERLOG_FLUSH__MAX_RECORDS` | `100000` | 整数，不小于 1 | 按数据集计。待提交的记录达到这个条数时，不等 `flush.interval` 就触发提交。它也是一次提交的记录数上限 |

### query

| 配置键 | 环境变量 | 默认值 | 范围 | 说明 |
|---|---|---|---|---|
| `query.default_size` | `AMBERLOG_QUERY__DEFAULT_SIZE` | `100` | 整数，不小于 0，不大于 `query.max_size` | `_search` 请求不带 `size` 时，最多返回的行数 |
| `query.max_size` | `AMBERLOG_QUERY__MAX_SIZE` | `10000` | 整数，不小于 1 | `_search` 请求的 `size` 上限。超过时回 HTTP 400、`reason` 为 `invalid_request`。告警规则的评估也按这个行数取结果 |
| `query.max_concurrency` | `AMBERLOG_QUERY__MAX_CONCURRENCY` | `4` | 整数，不小于 1 | 同时执行的查询数上限，`_search` 与告警规则的评估合计（内置规则 `missed_collection` 不计）。已满时，新的 `_search` 立即回 HTTP 429、`reason` 为 `too_many_queries`，不排队 |
| `query.max_memory_bytes` | `AMBERLOG_QUERY__MAX_MEMORY_BYTES` | `536870912`（512 MiB） | 整数，不小于 1 | 单个查询由系统计量的内存上限。超过时中止这个查询，`_search` 回 HTTP 400、`reason` 为 `query_memory_exceeded` |

### log

| 配置键 | 环境变量 | 默认值 | 范围 | 说明 |
|---|---|---|---|---|
| `log.level` | `AMBERLOG_LOG__LEVEL` | `info` | `error`、`warn`、`info`、`debug`、`trace`（只能小写） | 运行日志的级别。[稳定事件](#8-稳定的日志事件)不受这个级别过滤 |
| `log.format` | `AMBERLOG_LOG__FORMAT` | `json` | `json` 或 `text` | 运行日志写到 stderr。`json` 时每行是一个 JSON 对象，含 `ts`（RFC 3339 UTC）、`level` 与 `msg`。`text` 的行格式可能随版本变化，要按日志做告警或解析时用 `json` |

### lifecycle 与 compaction

Amberlog 把每个数据集的记录按 `_timestamp` 分进一小时一个的时间桶。桶结束以后先封口、再入湖（从热层转入冷层的 Apache Iceberg 表）。

| 配置键 | 环境变量 | 默认值 | 范围 | 说明 |
|---|---|---|---|---|
| `lifecycle.seal_grace` | `AMBERLOG_LIFECYCLE__SEAL_GRACE` | `10m` | 时长，大于 0 | 时间桶结束后再过多久封口。封口以后，桶里大小相近的小文件会被合并 |
| `lifecycle.lake_watermark` | `AMBERLOG_LIFECYCLE__LAKE_WATERMARK` | `72h` | 时长，大于 0，且大于 `lifecycle.seal_grace` | 时间桶结束后再过多久入湖。入湖以后，这些数据由外部引擎可直接读取的 Iceberg 表承载 |
| `lifecycle.lake_alert_after` | `AMBERLOG_LIFECYCLE__LAKE_ALERT_AFTER` | `24h` | 时长，大于 0 | 某个数据集的入湖积压时长从不超过它变为超过它时，输出一条 `lake_backlog_high` 警告日志 |
| `compaction.target_file_bytes` | `AMBERLOG_COMPACTION__TARGET_FILE_BYTES` | `134217728`（128 MiB） | 整数，不小于 1 | 目标文件大小。封口合并只合并小于它一半的文件；冷层合并合并小于它的文件，其中早先冷层合并写出的文件要小于它的一半；入湖与合并写出的每个文件不超过它（只含一条记录的文件除外） |
| `cold_compaction.check_interval` | `AMBERLOG_COLD_COMPACTION__CHECK_INTERVAL` | `24h` | 时长，大于 0 | 冷层合并的检查间隔。冷层合并把一天分区里的小文件合并成大文件 |

### 巡检与快照保留

| 配置键 | 环境变量 | 默认值 | 范围 | 说明 |
|---|---|---|---|---|
| `hot_fsck.interval` | `AMBERLOG_HOT_FSCK__INTERVAL` | `1h` | 时长，大于 0 | 热层巡检的周期。巡检清理孤儿文件，并物理删除已过 `storage.delete_delay` 的不再使用的数据文件。冷层孤儿清理、热层快照回收和告警评估记录的清理也按这个周期执行 |
| `hot_fsck.min_orphan_age` | `AMBERLOG_HOT_FSCK__MIN_ORPHAN_AGE` | `1h` | 时长，大于 0 | 孤儿文件（写出了但没有被任何提交引用的文件）上传完成后至少这么久才会被清理 |
| `hot_retention.snapshots` | `AMBERLOG_HOT_RETENTION__SNAPSHOTS` | `1000` | 整数，不小于 2 | 热层快照回收保留的最近快照个数 |
| `hot_retention.age` | `AMBERLOG_HOT_RETENTION__AGE` | `168h` | 时长，大于 0 | 热层快照回收保留的时长。最近 `hot_retention.snapshots` 个快照与创建时刻在 `hot_retention.age` 以内的快照都保留 |
| `cold_retention.snapshots` | `AMBERLOG_COLD_RETENTION__SNAPSHOTS` | `1` | 整数，不小于 1 | 冷层 Iceberg 表保留的最近快照个数 |
| `cold_retention.age` | `AMBERLOG_COLD_RETENTION__AGE` | `168h` | 时长，大于 0 | 冷层保留的快照时长。最近 `cold_retention.snapshots` 个快照与时间戳在 `cold_retention.age` 以内的快照都保留。外部引擎做时间旅行查询时，只能查到保留下来的快照 |
| `cold_retention.check_interval` | `AMBERLOG_COLD_RETENTION__CHECK_INTERVAL` | `24h` | 时长，大于 0 | 冷层快照保留的执行间隔 |

### retention（数据保留期）

| 配置键 | 环境变量 | 默认值 | 范围 | 说明 |
|---|---|---|---|---|
| `retention.period` | `AMBERLOG_RETENTION__PERIOD` | 无（不删除） | 时长，大于 0、不大于 `36500d`，且大于 `lifecycle.lake_watermark` | 数据保留期。配置时，数据文件里最晚的 `_timestamp` 加上保留期已经过去的文件被删除，热层与冷层都如此。不配置时不因时间早晚删除任何数据 |
| `retention.check_interval` | `AMBERLOG_RETENTION__CHECK_INTERVAL` | `1h` | 时长，大于 0 | 到期删除的执行间隔。只在配置了 `retention.period` 时起作用 |

删除的数据在审计记录里留痕。数据集台账里的 `retention` 与 `expires_at` 两列按 `retention.period` 显示。

### reconciliation（漏采判定）

| 配置键 | 环境变量 | 默认值 | 范围 | 说明 |
|---|---|---|---|---|
| `reconciliation.grace` | `AMBERLOG_RECONCILIATION__GRACE` | `5m` | 时长，换算后是整分钟，不小于 `1m`、不大于 `24h` | 宽限期。一个统计窗口结束后，过了宽限期才比较这个窗口上报的发送数与确认数、给出漏采判定；之前判定为 `pending`。在宽限期之后才到达的记录与发送数上报算迟到 |

### alerts（告警）

| 配置键 | 环境变量 | 默认值 | 范围 | 说明 |
|---|---|---|---|---|
| `alerts.rules` | 不能用环境变量设置 | 无（零条规则） | TOML 表数组，最多 100 条，见下文 | 告警规则 |
| `alerts.max_concurrency` | `AMBERLOG_ALERTS__MAX_CONCURRENCY` | `1` | 整数，不小于 1 | 同时执行查询的告警评估个数（告警名额） |
| `alerts.history_retention` | `AMBERLOG_ALERTS__HISTORY_RETENTION` | `30d` | 时长，不小于 `1h`、不大于 `36500d` | 评估记录的保留期 |
| `alerts.ca_file` | `AMBERLOG_ALERTS__CA_FILE` | 无 | 文件路径。空字符串视同没有配置 | 验证 `https://` webhook 接收端的证书时，在系统根证书之外追加信任的 CA 证书（PEM）。以 `serve` 运行时文件不存在、不可读或内容无效，以退出码 2 退出。它与 `object_store.s3.ca_file` 互不通用 |
| `alerts.missed_collection.enabled` | `AMBERLOG_ALERTS__MISSED_COLLECTION__ENABLED` | `false` | 布尔 | 是否启用内置告警规则 `missed_collection`：某个窗口的漏采判定转为缺失时发通知。为 true 而没有配置 `webhook_url` 时，以退出码 2 退出 |
| `alerts.missed_collection.interval` | `AMBERLOG_ALERTS__MISSED_COLLECTION__INTERVAL` | `5m` | 时长，换算后是整分钟，不小于 `1m`、不大于 `24h` | 内置规则的评估间隔 |
| `alerts.missed_collection.webhook_url` | `AMBERLOG_ALERTS__MISSED_COLLECTION__WEBHOOK_URL` | 无 | 同告警规则的 `webhook_url` | 内置规则的通知地址 |
| `alerts.missed_collection.webhook_headers` | 不能用环境变量设置 | 无 | 同告警规则的 `webhook_headers` | 内置规则的通知请求附带的请求头 |

告警只在以 `amberlog serve` 运行时评估。`catalog` 与离线子命令照常校验规则，但不评估、不发通知。

**告警规则**写成 `[[alerts.rules]]`，每条规则只能有下面这些字段：

| 字段 | 必填 | 类型 | 说明 |
|---|---|---|---|
| `name` | 是 | 字符串 | 匹配 `^[a-z0-9][a-z0-9_]{0,63}$`，不以 `amberlog_` 开头，不是 `missed_collection`。全部规则的名字互不相同（含 `enabled = false` 的） |
| `interval` | 是 | 字符串（时长） | 评估间隔，整分钟，`1m`–`24h`。评估在 UTC 时间上对齐到 `interval` 的整数倍 |
| `window` | 否 | 字符串（时长） | 每次评估查询的时间范围是评估时刻之前的这段时长，整分钟，`1m`–`7d`。不配时等于 `interval` |
| `org` | 否 | 字符串 | 只能是 `default`（当前版本只有这一个组织） |
| `sql` | 是 | 字符串 | 查询语句，写法同 `_search` 的 `sql`。启动时检查语法与表名，不检查数据集与列是否存在 |
| `condition` | 是 | 表 | 行数条件 `{ rows = "> 0" }`，或列条件 `{ column = "n", op = ">", threshold = 100 }`。比较符为 `>`、`>=`、`<`、`<=`、`=`、`!=` |
| `webhook_url` | 是 | 字符串 | 通知地址，`https://` 或 `http://` 开头，带主机名，不含用户名口令与片段。`http://` 地址照常以明文发送，启动时输出一条 `webhook_plaintext` 警告日志（日志里的地址去掉查询串） |
| `webhook_headers` | 否 | 表 | 通知请求附带的请求头，如鉴权头。不能是 `Host`、`Content-Type`、`Content-Length`、`Transfer-Encoding`、`Connection`、`User-Agent`。值不会写进日志、评估记录或指标 |
| `enabled` | 否 | 布尔 | 默认 true。为 false 的规则照常校验，但不评估 |

规则不合以上要求时，以退出码 2 退出，stderr 指出 `alerts.rules` 与出错规则的 `name`（`name` 本身有问题时指出它在表数组中的序号，从 1 起）。

### index_hints（索引提示）

| 配置键 | 环境变量 | 默认值 | 范围 | 说明 |
|---|---|---|---|---|
| `index_hints` | 不能用环境变量设置 | 无 | TOML 表，见下文 | 为指定数据集的指定列建等值 bloom 过滤器，加速 `列 = 值` 一类查询 |

每个键是 `<org>/<stream>` 形式的数据集名，`<org>` 只能是 `default`，`<stream>` 匹配 `^[a-z0-9][a-z0-9_]{0,63}$`。值是列名的字符串数组，元素非空、互不相同，可以是空数组。数据集或列还不存在时不报错。

```toml
[index_hints]
"default/app" = ["host", "trace_id"]
"default/nginx" = ["request_id"]
```

索引提示不改变查询结果，只影响查询要读多少数据。它只在配置里，不写入对象存储。以 `amberlog retire` 退役时，入湖写出的文件也按它建过滤器，所以退役时用与 `serve` 相同的配置。

## 5. 配置项之间的约束

下列组合以退出码 2 退出，stderr 指出所涉的键名：

| 约束 | 违反时 |
|---|---|
| `query.default_size` 不大于 `query.max_size` | 指出两个键名 |
| `lifecycle.lake_watermark` 大于 `lifecycle.seal_grace` | 指出两个键名 |
| 配置了 `retention.period` 时，它大于 `lifecycle.lake_watermark` | 指出两个键名 |
| `server.tls.cert_file` 与 `server.tls.key_file` 成对配置 | 指出未配置的那个 |
| 配置了证书时，`server.behind_tls_proxy` 不为 true | 指出两个键名 |
| `object_store.s3.access_key_id` 与 `object_store.s3.secret_access_key` 成对配置 | 指出未配置的那个 |
| 配置了 `object_store.s3.sse_kms_key_id` 时，`object_store.s3.server_side_encryption` 为 `sse_kms` | 指出两个键名 |
| `alerts.missed_collection.enabled` 为 true 时，配置了 `alerts.missed_collection.webhook_url` | 指出 `alerts.missed_collection` |

## 6. 各运行方式下起作用的配置项

| 子命令 | 起作用的配置项 |
|---|---|
| `amberlog serve` | 全部 |
| `amberlog catalog`（只读目录模式） | `server.http_addr`、`server.shutdown_timeout`、`server.tls.cert_file`、`server.tls.key_file`、`server.behind_tls_proxy`、`storage.prefix`、`object_store.url`、`object_store.s3.endpoint`、`object_store.s3.region`、`object_store.s3.access_key_id`、`object_store.s3.secret_access_key`、`object_store.s3.force_path_style`、`object_store.s3.ca_file`、`auth.enabled`、`auth.token`、`log.level`、`log.format` |
| `amberlog retire` | `storage.prefix`、`storage.commit_max_retries`、`storage.delete_delay`、`object_store.url`、`object_store.s3.*` 各项、`wal.dir`、`ingest.max_columns`、`flush.max_records`、`ingest.max_backlog_memory_bytes`、`compaction.target_file_bytes`、`index_hints`、`retention.period`（只记入退役记录，不执行到期删除）、`log.level`、`log.format` |
| `amberlog evidence export`、`amberlog evidence verify` | `storage.prefix`、`object_store.url`、`object_store.s3.*` 中除服务端加密两项以外的各项、`log.level`、`log.format`。不创建、不读写 `wal.dir` |

[S3 凭据环境变量](#7-s3-凭据环境变量)在每种运行方式下都起作用。

其余配置项照常校验，但不起作用。例如以 `retire` 运行时，`server.http_addr` 是非回环地址也不会使它失败。

## 7. S3 凭据环境变量

没有配置 `object_store.s3.access_key_id` 与 `secret_access_key` 时，Amberlog 只从下面 13 个环境变量（及其所指的凭据来源）取得 S3 凭据。变量名必须全大写。

| 用途 | 环境变量 |
|---|---|
| 静态密钥与会话令牌 | `AWS_ACCESS_KEY_ID`、`AWS_SECRET_ACCESS_KEY`、`AWS_SESSION_TOKEN`、`AWS_TOKEN` |
| Web Identity | `AWS_WEB_IDENTITY_TOKEN_FILE`、`AWS_ROLE_ARN`、`AWS_ROLE_SESSION_NAME`、`AWS_ENDPOINT_URL_STS` |
| 容器凭据 | `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`、`AWS_CONTAINER_CREDENTIALS_FULL_URI`、`AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE` |
| 实例元数据 | `AWS_METADATA_ENDPOINT`、`AWS_IMDSV1_FALLBACK` |

这 13 个以外、以 `AWS_` 开头的环境变量不影响 Amberlog 的行为，如 `AWS_ENDPOINT_URL`、`AWS_REGION`、`AWS_SERVER_SIDE_ENCRYPTION`。端点、区域、条件写与服务端加密只由 `object_store.s3.*` 配置项决定。

这 13 个变量中某个的取值不合法时，进程以退出码 3 退出。

## 8. 稳定的日志事件

下列 14 个事件的日志行带字段 `event`，取值为事件名。它们的字段名只增不删、不改名，也不受 `log.level` 过滤，可以用来建告警。其他日志行的文字可能随版本变化。

| 事件 | `level` | 何时输出 | 其余字段 |
|---|---|---|---|
| `starting` | `info` | 配置校验通过后的第一行日志。配置有错（退出码 2）时不输出 | `version` |
| `auth_disabled` | `warn` | `auth.enabled` 为 false，在 `starting` 之后、`ready` 之前 | |
| `plaintext_listen` | `warn` | 非回环地址以明文监听（见明文监听的规则） | `http_addr` |
| `object_store_plaintext` | `warn` | `object_store.s3.endpoint` 以 `http://` 开头 | `endpoint`（去掉用户名口令与查询串） |
| `webhook_plaintext` | `warn` | 每条 `webhook_url` 为 `http://` 的启用的规则各一次 | `rule`、`url`（去掉查询串） |
| `wal_stale` | `warn` | 启动重放 WAL 时，某个数据集的 WAL 已过时、有滞留记录 | `dataset`、`recovered`、`discarded` |
| `ready` | `info` | `GET /readyz` 首次能回 200 时，每个进程一次 | `version`、`http_addr`（实际监听的地址）、`object_store`（去掉用户名口令与查询串）、`mode`（`serve` 或 `catalog`）、`tls`（布尔） |
| `dataset_disabled` | `error` | 某个数据集因格式版本不兼容或 manifest 不可读而停用，每个数据集一次 | `dataset`、`state`（`format_version` 或 `manifest_unreadable`） |
| `digest_mismatch` | `error` | 合并、入湖或冷层合并时，数据文件的摘要与记录的不符 | `dataset`、`tier`（`hot` 或 `cold`）、`path` |
| `wal_backlog_high` | `warn` | 见 `wal.alert_bytes` | `wal_bytes`、`alert_bytes` |
| `lake_backlog_high` | `warn` | 见 `lifecycle.lake_alert_after` | `dataset`、`bucket`、`lag_seconds`、`alert_after_seconds` |
| `tls_reload_failed` | `error` | 证书重读读到无效内容，同一份无效内容只输出一次 | `cert_file`、`key_file` |
| `reconciliation_missing` | `warn` | 漏采判定把一个窗口判为缺失 | `dataset`、`window_start`、`sources`、`missing` |
| `source_silent` | `warn` | 漏采判定发现某个来源上报中断 | `dataset`、`source`、`last_window_start` |

`log.format` 为 `text` 时这些事件同样输出，但行格式可能随版本变化。

## 9. 退出码

| 退出码 | 含义 |
|---|---|
| 0 | 正常退出（含 `--version`） |
| 1 | 其他启动失败 |
| 2 | 命令行用法错误；配置文件不存在、不可读或不是合法 TOML；配置错误（缺少必填项、不认识的键、取值不合法、配置项之间的约束、TLS 证书或 CA 文件无效、告警规则或索引提示无效）；对象存储不支持条件写；存储前缀已经退役（`serve`） |
| 3 | `wal.dir` 无法创建或不可写；对象存储不可访问；读取 WAL 或已有数据失败；S3 凭据环境变量取值不合法 |
| 4 | `server.http_addr` 无法监听 |
| 5、6 | 只用于离线子命令：5 表示做完了但发现了问题，6 表示没做完。各子命令的含义见 [运维指南](operations.zh-CN.md) 第 6.2、6.3、7.2 节 |

启动时依次校验：配置、`wal.dir`、对象存储、WAL 重放、监听。第一个失败的步骤决定退出码。

## 10. 命令行

```text
amberlog serve [--config <path>]
amberlog catalog [--config <path>]
amberlog retire [--config <path>]
amberlog evidence export [--config <path>] --dataset <org>/<stream> [--from <t>] [--to <t>] --output <path>
amberlog evidence verify [--config <path>] --evidence <path> [--previous <path>] [--retention <时长>]
amberlog --version
```

- `--config=<path>` 与 `--config <path>` 等价。`--config` 出现多次或缺路径时按用法错误处理。
- `amberlog --version` 输出一行 `amberlog <版本号> (<提交号>)`。
- 没有子命令、子命令不认识或参数不认识时，向 stderr 输出用法，以退出码 2 退出。
- 只能用配置文件和环境变量配置，没有对应每个配置项的命令行参数。

## 11. 例子

### 11.1 MinIO，前面有反向代理终止 TLS

```toml
# /etc/amberlog/amberlog.toml
[server]
http_addr = "0.0.0.0:8800"
behind_tls_proxy = true

[object_store]
url = "s3://amberlog-data/prod"

[object_store.s3]
endpoint = "https://minio.example.com:9000"
force_path_style = true
conditional_put = "create_only"

[wal]
dir = "/var/lib/amberlog/wal"

[retention]
period = "1095d"
```

密钥与令牌也可以用环境变量传入。这样做时要知道：同一用户的其他进程能看到环境变量，容器里 `docker inspect` 也能看到。正式部署建议写进只让运行用户读得到的配置文件，见 [安装与部署指南](install.zh-CN.md) 第 4.3、4.4 节。用环境变量的写法：

```sh
export AMBERLOG_AUTH__TOKEN='change-me-to-a-random-token'
export AMBERLOG_OBJECT_STORE__S3__ACCESS_KEY_ID='amberlog-app'
export AMBERLOG_OBJECT_STORE__S3__SECRET_ACCESS_KEY='example-secret-change-me'
amberlog serve --config /etc/amberlog/amberlog.toml
```

### 11.2 进程内 TLS 与告警规则

```toml
[server]
http_addr = "0.0.0.0:8443"

[server.tls]
cert_file = "/etc/amberlog/tls/server.crt"
key_file = "/etc/amberlog/tls/server.key"

[object_store]
url = "s3://amberlog-data"

[object_store.s3]
region = "us-west-2"
conditional_put = "create_only"
server_side_encryption = "sse_kms"

[wal]
dir = "/var/lib/amberlog/wal"

[alerts]
max_concurrency = 2

[[alerts.rules]]
name = "error_burst"
interval = "1m"
window = "5m"
sql = "SELECT count(*) AS n FROM app WHERE level = 'error'"
condition = { column = "n", op = ">", threshold = 100 }
webhook_url = "https://hooks.example.com/amberlog"
webhook_headers = { Authorization = "Bearer example-webhook-token" }

[alerts.missed_collection]
enabled = true
webhook_url = "https://hooks.example.com/amberlog-missed"
```

### 11.3 本机试用，使用本地目录

```sh
mkdir -p /tmp/amberlog-data
AMBERLOG_SERVER__HTTP_ADDR=127.0.0.1:8800 \
AMBERLOG_OBJECT_STORE__URL=file:///tmp/amberlog-data \
AMBERLOG_WAL__DIR=/tmp/amberlog-wal \
AMBERLOG_AUTH__TOKEN=local-trial-token \
amberlog serve
```

回环地址上可以不配置证书。`file://` 所指的目录必须已经存在，否则以退出码 3 退出。

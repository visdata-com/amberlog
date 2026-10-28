# API 参考

[English](api.md)

> 适用于 Amberlog 0.1.0。

本文说明 Amberlog 的 HTTP 接口：写入、检索、字段列表、系统表、错误结构、认证和运维端点。每个例子都在测试环境里实际运行过，文中的响应是实际得到的响应（`took`、`scan_*`、`x-request-id` 这类每次不同的值除外）。

配置项见 [配置项参考](configuration.zh-CN.md)。

## 1. 概览

### 1.1 端点一览

全部端点都在 `server.http_addr` 所指的同一个地址上。

| 方法 | 路径 | 要求令牌 | 用途 |
|---|---|---|---|
| `POST` | `/api/{org}/{stream}/_json` | 是 | 写入记录（[第 2 节](#2-写入-_json)） |
| `POST` | `/api/{org}/{stream}/_sent` | 是 | 上报采集端的发送数（[第 3 节](#3-发送数上报-_sent)） |
| `POST` | `/api/{org}/_search` | 是 | SQL 检索，也用于查询系统表（[第 4 节](#4-检索-_search)、[第 6 节](#6-系统表)） |
| `GET` | `/api/{org}/{stream}/_fields` | 是 | 字段列表（[第 5 节](#5-字段列表-_fields)） |
| `GET` | `/healthz` | 否 | 存活探针（[第 8 节](#8-运维端点)） |
| `GET` | `/readyz` | 否 | 就绪探针 |
| `GET` | `/metrics` | 是 | Prometheus 指标 |
| `GET` | `/ui` | 否 | 检索界面（浏览器打开） |
| `GET`、`HEAD` | `/iceberg/v1/...` | 是 | Apache Iceberg REST 只读目录，供外部查询引擎使用（[第 9 节](#9-iceberg-只读目录)） |

路径参数：

- `{org}`：组织。当前版本只有 `default`。其他取值回 HTTP 404、`reason` 为 `org_not_found`。
- `{stream}`：数据集名。匹配 `^[a-z0-9][a-z0-9_]{0,63}$`，即小写字母、数字和下划线，不以下划线开头，长 1–64。不合规则时回 HTTP 400、`reason` 为 `invalid_stream_name`。第一次写入时自动创建数据集。

本文把 `<org>/<stream>` 称为**数据集**，例如 `default/app`。

### 1.2 例子里的变量

例子用两个环境变量：

```text
export AMBERLOG=http://127.0.0.1:8800       # Amberlog 的地址；启用 TLS 时写 https://
export TOKEN='change-me-to-a-random-token'  # 配置项 auth.token 的值
```

启用了入站 TLS、证书由内部 CA 签发时，给 `curl` 加 `--cacert <CA 证书>`。

本文的例子在一个新实例上按出现顺序执行，后面的例子查询前面写入的数据。这个实例除必填项外，另配了 `reconciliation.grace = "1m"` 与 [6.3 节](#63-_alert_evaluations告警评估记录)的告警规则。

例子写入的记录用固定时间戳，在 2026-10-20 08:00 UTC 前后；文中的输出来自这个时间之后 72 小时以内的一次运行。离这些时间戳超过 72 小时再跑时，它们的小时桶已经越过入湖水位（默认 72 小时），写入后很快入湖，有三处输出会不同：6.1 节 `_ledger` 例子的 `tier` 是 `hot_cold`，不是 `hot`；第 9 节取表的例子返回表的元数据，不是 404；超过 96 小时（入湖水位加 `lifecycle.lake_alert_after`，都取默认值）时，日志里还多一条 `lake_backlog_high` 警告。

### 1.3 认证

`/api/`、`/metrics` 与 `/iceberg/` 下的请求要带请求头：

```text
Authorization: Bearer <令牌>
```

- 令牌就是配置项 `auth.token` 的值，逐字节比较。方案名 `Bearer` 不区分大小写。
- 不接受放在查询参数或 Cookie 里的令牌。
- 令牌缺失、方案不是 `Bearer`、令牌不对，或请求带了多个 `Authorization` 头时，回 HTTP 401、`reason` 为 `unauthorized`，并带响应头 `WWW-Authenticate: Bearer`。
- 在 `/api/` 下，认证先于路由：没有有效令牌时，不存在的路径也回 401，不回 404。
- `/healthz`、`/readyz` 与 `/ui` 不要求令牌。
- 配置项 `auth.enabled` 为 false 时，全部端点都不要求令牌。

当前版本只有这一个令牌，没有授权模型：持有令牌的请求可以访问全部 `/api/` 端点与目录端点。

```sh
curl -sS -i -X POST "$AMBERLOG/api/default/app/_json" -d '[]'
```

```text
HTTP/1.1 401 Unauthorized
content-type: application/json
www-authenticate: Bearer
x-request-id: 06cd844ed432f7470000000d

{"code":401,"reason":"unauthorized","message":"missing or invalid credentials"}
```

### 1.4 请求编号

每个响应都带响应头 `x-request-id`。请求带了这个头时原样回传，否则由 Amberlog 生成。Amberlog 以 5xx 作答时，会在运行日志里写一行 `level` 为 `error` 的日志，含这个编号与出错原因。排查问题时，把编号告诉运维人员即可在日志里找到对应的行。

```sh
curl -sS -i "$AMBERLOG/healthz" -H "x-request-id: my-trace-123"
```

```text
HTTP/1.1 200 OK
content-type: application/json
x-request-id: my-trace-123

{"status":"ok"}
```

### 1.5 请求体

下列规则适用于 `/api/` 下的全部 POST 端点（`_json`、`_sent`、`_search`）：

- **格式**：JSON。不按 `Content-Type` 请求头接受或拒绝请求，可以不带。
- **压缩**：`Content-Encoding` 可以不带，或取 `identity`、`gzip`、`x-gzip`、`zstd`（不区分大小写）。其他取值（如 `br`、`deflate`）或同时列出多个编码时，回 HTTP 415、`reason` 为 `unsupported_encoding`。解压失败时回 HTTP 400、`reason` 为 `invalid_encoding`。
- **大小**：收到的字节数与解压后的字节数都不能超过配置项 `ingest.max_body_bytes`（默认 32 MiB），超过时回 HTTP 413、`reason` 为 `payload_too_large`。不对单条记录另设上限。
- **合法 JSON**：以 UTF-8 字节序标记开头、含非法 UTF-8、含未成对的 UTF-16 代理项转义（如 `"\ud800"`），或含超出双精度范围的数字（如 `1e400`）的请求体，不是合法 JSON。`_search` 与 `_sent` 的请求体嵌套超过 128 层也不合法。`_json` 的这类问题只拒绝出问题的那条记录，见 [2.4 节](#24-逐条拒绝)。
- **重复键**：JSON 对象里同一个键出现多次时，取最后一次的值。
- 响应体不压缩。

## 2. 写入 `_json`

```text
POST /api/{org}/{stream}/_json
```

请求体是记录的 JSON 数组，每个元素是一条记录。请求体是单个 JSON 对象时，按只含一条记录的批次处理。

### 2.1 例子

```sh
curl -sS -X POST "$AMBERLOG/api/default/app/_json" \
  -H "Authorization: Bearer $TOKEN" \
  -H "X-Amberlog-Source: web-1" \
  -d '[
    {"_timestamp": "2026-10-20T08:00:00Z", "level": "info", "message": "user login", "user": {"id": 42, "name": "alice"}, "latency_ms": 12.5},
    {"_timestamp": 1792483260000, "level": "error", "message": "db timeout", "latency_ms": 3000.0, "tags": ["db", "timeout"]},
    {"_timestamp": 1792483320000000, "level": "warn", "message": "slow request", "latency_ms": 870.0},
    {"_secret": "x"},
    "not an object",
    {"_timestamp": "yesterday", "message": "bad time"}
  ]'
```

```json
{"code":200,"status":[{"name":"app","successful":3,"failed":3,"errors":[
  {"index":3,"reason":"reserved_field","message":"top-level keys starting with '_' are reserved"},
  {"index":4,"reason":"not_an_object","message":"the record is not a JSON object"},
  {"index":5,"reason":"timestamp_invalid","message":"_timestamp is not a valid timestamp"}]}]}
```

（为便于阅读，这里和下文把较长的响应折了行。）

用 gzip 压缩请求体：

```sh
printf '[{"level":"info","message":"compressed batch"}]' | gzip | \
curl -sS -X POST "$AMBERLOG/api/default/app/_json" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Encoding: gzip" --data-binary @-
```

```json
{"code":200,"status":[{"name":"app","successful":1,"failed":0,"errors":[]}]}
```

### 2.2 应答

| 情形 | 状态码 | 响应体 |
|---|---|---|
| 批次被受理，且不是「全部被拒」 | 200 | `{"code":200,"status":[{"name":"<stream>","successful":n,"failed":m,"errors":[...]}]}` |
| 空数组 `[]` | 200 | `successful` 与 `failed` 都是 0 |
| 至少一条记录，且全部被逐条拒绝 | 400 | 错误信封，`reason` 为 `all_records_rejected`，`details.status` 与 200 的 `status` 形状相同 |
| 请求体整体不是合法 JSON | 400 | `reason` 为 `invalid_json`，不落库任何记录 |
| 写本地 WAL 失败 | 503 | `reason` 为 `unavailable` |

- `status` 恰有一个元素。`successful` 加 `failed` 等于批次中的记录数。
- `errors` 对每条失败记录有一个元素，按 `index` 升序。`index` 是该记录在批次中的序号，从 0 起。`reason` 见 [2.4 节](#24-逐条拒绝)。
- **只有在全部成功的记录写入本地 WAL 并 fsync 之后，才回 HTTP 200。** 收到 200 的记录已经持久化。在吞吐上限以内，写入确认后 10 秒内可以检索到；积压超过内存上限时，只留在 WAL 里的溢出记录要等读回内存才能查到（见[已知限制](known-limitations.zh-CN.md)第 4.1 节）。
- 收到 503 时，这一批没有被确认，其中的记录以后也不会出现。客户端应重试整批。
- 写入是追加语义：内容完全相同的两条记录都会保留，不去重。重试一批已经收到 200 的数据会产生重复记录。
- 一条记录被拒，不影响同批其他记录。

全部被拒的例子：

```sh
curl -sS -X POST "$AMBERLOG/api/default/app/_json" \
  -H "Authorization: Bearer $TOKEN" -d '[{"_x": 1}, {"bad name": 1}]'
```

```json
{"code":400,"reason":"all_records_rejected","message":"every record in the batch was rejected; retrying the same batch will not succeed",
 "details":{"status":[{"name":"app","successful":0,"failed":2,"errors":[
  {"index":0,"reason":"reserved_field","message":"top-level keys starting with '_' are reserved"},
  {"index":1,"reason":"invalid_field_name","message":"a field name is invalid or two fields flatten to the same name"}]}]}}
```

### 2.3 记录怎么落库

**字段与类型：**

| JSON 取值 | 列的类型 |
|---|---|
| 字符串 | `Utf8` |
| 布尔 | `Boolean` |
| 不带小数点和指数、在 int64 范围内的数字 | `Int64` |
| 带小数点或指数的数字 | `Float64`（按 IEEE 754 就近舍入为双精度） |
| 超出 int64 范围的整数 | `Utf8`，值为原样的十进制字符串，不舍入 |
| 嵌套对象 | 展平：各层键名用 `.` 连接，如 `{"user":{"id":42}}` 落为列 `user.id` |
| 数组 | 整个数组序列化为 JSON 文本，落为 `Utf8` 列 |
| `null`、字段缺失 | 该列为 NULL。值为 `null` 的新字段不建列 |
| 空对象 `{}` | 不建列 |
| 空字符串 `""` | 空字符串，不是 NULL |

- 展平最多 8 层：第 8 层的键的值仍是非空对象时，这个对象不再展平，整体序列化为 JSON 文本，落为以该键展平后的名字为名的 `Utf8` 列。
- **字段名**（展平后）只能含 `A`–`Z`、`a`–`z`、`0`–`9`、`-`、`_`、`.`，以字母或数字开头和结尾，不超过 128 字节，`_` 与 `.` 不能相邻（不能有 `__`、`..`、`._`、`_.`）。
- 以 `_` 开头的顶层字段名是系统保留的，只有 `_timestamp` 可以写。
- 每个数据集的列数上限是配置项 `ingest.max_columns`（默认 1000），含 `_timestamp`。

**类型不一致的字段**：同一个字段名在不同记录里取不同类型时，记录照常接受。第一次出现的类型占用原名，其他类型的值落进**影子列**，列名为 `<字段名>__<类型后缀>`，后缀是 `int64`、`float64`、`boolean`、`utf8` 之一。一列的类型登记以后不再改变。查询时，字段名只指同名的那一列，不含影子列里的值。

```sh
curl -sS -X POST "$AMBERLOG/api/default/app/_json" \
  -H "Authorization: Bearer $TOKEN" -d '[{"_timestamp": "2026-10-20T08:03:00Z", "latency_ms": "n/a", "message": "type drift"}]'
curl -sS "$AMBERLOG/api/default/app/_fields" -H "Authorization: Bearer $TOKEN"
```

```json
{"code":200,"status":[{"name":"app","successful":1,"failed":0,"errors":[]}]}
{"fields":[{"name":"_timestamp","type":"Int64"},{"name":"latency_ms","type":"Float64"},
 {"name":"latency_ms__utf8","type":"Utf8"},{"name":"level","type":"Utf8"},{"name":"message","type":"Utf8"},
 {"name":"tags","type":"Utf8"},{"name":"user.id","type":"Int64"},{"name":"user.name","type":"Utf8"}]}
```

**`_timestamp`**：记录的时间，以自 Unix epoch 起的 UTC 微秒数（Int64）存储和返回。

| 写法 | 含义 |
|---|---|
| 不带、`null` 或 `0` | 用服务端接收请求的时刻 |
| 数字，绝对值 ≥ 1e17 | 纳秒 |
| 数字，绝对值在 [1e14, 1e17) | 微秒 |
| 数字，绝对值在 [1e11, 1e14) | 毫秒 |
| 数字，绝对值 < 1e11 | 秒，可带小数 |
| 字符串 | RFC 3339，必须带 `Z` 或数字偏移，如 `2026-10-20T16:00:00+08:00` |

不足一微秒的部分向零截断。不做任何时区换算。

### 2.4 逐条拒绝

下列 `reason` 只出现在 `errors` 里。一条记录同时有多个问题时，按表中从上到下的顺序只报第一个。

| `reason` | 含义 |
|---|---|
| `malformed_record` | 这条记录的字符串含非法 UTF-8 或未成对的代理项转义，或含超出双精度范围的小数，或嵌套超过 128 层 |
| `not_an_object` | 这个元素不是 JSON 对象 |
| `reserved_field` | 含 `_timestamp` 以外以 `_` 开头的顶层字段 |
| `invalid_field_name` | 字段名不合规则，或两个字段展平后同名（如 `{"a.b":1,"a":{"b":2}}`） |
| `timestamp_invalid` | `_timestamp` 是无法解析的字符串（含不带时区的时间）、布尔、对象或数组 |
| `timestamp_out_of_range` | `_timestamp` 早于 1970-01-01、晚于接收时刻加 `ingest.max_future`（默认 24 小时）、晚于 9999-12-31T23:59:59.999999Z，或在配置了 `ingest.max_past` 时早于接收时刻减该值 |
| `too_many_columns` | 这条记录要新增的列会使列数超过 `ingest.max_columns` |

### 2.5 来源标识 `X-Amberlog-Source`

可以给 `_json` 请求带请求头 `X-Amberlog-Source: <来源>`，例如一台机器的名字。

- 取值匹配 `^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`。不匹配、为空，或同一请求带了多个这个头时，回 HTTP 400、`reason` 为 `invalid_request`，不落库任何记录。
- 它不写进记录，不改变记录内容和应答，只用于按来源分开计数，供[对账](#62-_reconciliation对账)与漏采判定使用。

```sh
curl -sS -X POST "$AMBERLOG/api/default/app/_json" \
  -H "Authorization: Bearer $TOKEN" -H "X-Amberlog-Source: bad source" -d '[]'
```

```json
{"code":400,"reason":"invalid_request","message":"X-Amberlog-Source must appear at most once and match ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$"}
```

## 3. 发送数上报 `_sent`

```text
POST /api/{org}/{stream}/_sent
```

采集端用这个接口上报「某个来源在某一分钟发出了多少条」。Amberlog 把它与实际收到、确认的条数对比，结果在系统表 `_reconciliation` 与 `_ledger` 里，也用于漏采判定与内置告警规则 `missed_collection`。

对账只比较采集端上报的发送数与 Amberlog 确认的条数。判定为一致，不能证明采集端没有漏采；采集端自己没有计入的记录，这个办法也查不出来。

请求体是 JSON 数组，每个元素：

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `source` | 字符串 | 是 | 来源标识，规则同 `X-Amberlog-Source` |
| `window_start` | 整数 | 是 | 这一分钟的起点，UTC 微秒，必须是整分钟 |
| `window_end` | 整数 | 是 | 必须等于 `window_start + 60000000` |
| `sent` | 整数 | 是 | 发送条数，非负 |
| `basis` | 字符串 | 否 | `delivery`（默认）或 `timestamp`，见下文 |

- `basis` 为 `timestamp` 时，`sent` 是按记录的 `_timestamp` 所在分钟统计的条数。Amberlog 用它与按 `_timestamp` 统计的确认数逐来源对比，给出这一分钟的判定（[第 6.2 节](#62-_reconciliation对账)）。要做漏采判定，就用这种上报，并且写入请求要带同一个 `X-Amberlog-Source`。
- `basis` 为 `delivery` 时，`sent` 的统计口径由采集端自己约定（例如按发出的时刻归分钟），只在对账表里与 `received` 并列显示。
- 同一 `(source, window_start, basis)` 多次上报时，以最后一次为准，不累加。
- 未定义的字段被忽略。
- 任一元素不合规则时，整个请求回 HTTP 400、`reason` 为 `invalid_request`，不记录任何元素。
- 只有在全部元素写入本地 WAL 并 fsync 之后才回 200：`{"code":200,"accepted":<元素数>}`。写 WAL 失败时回 503。

下面的例子按 `_timestamp` 所在分钟，上报 [2.1 节](#21-例子) 那三条成功记录：

```sh
curl -sS -X POST "$AMBERLOG/api/default/app/_sent" \
  -H "Authorization: Bearer $TOKEN" \
  -d '[
    {"source": "web-1", "basis": "timestamp", "window_start": 1792483200000000, "window_end": 1792483260000000, "sent": 1},
    {"source": "web-1", "basis": "timestamp", "window_start": 1792483260000000, "window_end": 1792483320000000, "sent": 1},
    {"source": "web-1", "basis": "timestamp", "window_start": 1792483320000000, "window_end": 1792483380000000, "sent": 1}
  ]'
```

```json
{"code":200,"accepted":3}
```

## 4. 检索 `_search`

```text
POST /api/{org}/_search
```

### 4.1 请求体

```json
{"query": {"sql": "SELECT ...", "start_time": 1792454400000000, "end_time": 1792540800000000, "size": 100, "from": 0, "timezone": "Asia/Shanghai"}}
```

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `query.sql` | 字符串 | 是 | SQL，见 [4.3 节](#43-sql) |
| `query.start_time` | 整数 | 是 | 时间范围起点，UTC 微秒，含 |
| `query.end_time` | 整数 | 是 | 时间范围终点，UTC 微秒，不含。必须大于 `start_time`，两者都不能为负 |
| `query.size` | 整数 | 否 | 最多返回的行数。默认 `query.default_size`（100），不能为负，不能大于 `query.max_size`（10000） |
| `query.from` | 整数 | 否 | 按最终排序跳过的行数，默认 0，不能为负 |
| `query.timezone` | 字符串 | 否 | IANA 时区名（如 `Asia/Shanghai`，不区分大小写）或 `UTC`，默认 `UTC`。只影响 `histogram` 的分桶。`+08:00` 这类固定偏移不是时区名 |

整数字段必须是不带小数点和指数的 JSON 整数；`1.0`、`"100"`、`null` 都按类型不符处理。未定义的字段被忽略。

查询数据集时，Amberlog 自动加上条件 `_timestamp >= start_time AND _timestamp < end_time`。

### 4.2 成功响应

```sh
curl -sS -X POST "$AMBERLOG/api/default/_search" -H "Authorization: Bearer $TOKEN" -d '{"query": {
  "sql": "SELECT _timestamp, level, message, \"user.name\", latency_ms FROM app",
  "start_time": 1792454400000000, "end_time": 1792540800000000}}'
```

```json
{"took":33,"hits":[
  {"_timestamp":1792483380000000,"message":"type drift"},
  {"_timestamp":1792483320000000,"level":"warn","message":"slow request","latency_ms":870.0},
  {"_timestamp":1792483260000000,"level":"error","message":"db timeout","latency_ms":3000.0},
  {"_timestamp":1792483200000000,"level":"info","message":"user login","user.name":"alice","latency_ms":12.5}],
 "total":4,"from":0,"size":100,"scan_records":4,"scan_files":1,"scan_bytes":3483}
```

| 字段 | 说明 |
|---|---|
| `took` | 耗时，毫秒 |
| `hits` | 结果行。每行是「输出列名 → 值」的对象，键的先后与 `SELECT` 列表一致。**值为 NULL 的列不出现在该行里** |
| `total` | 本次响应中 `hits` 的行数（不是满足条件的总行数） |
| `from`、`size` | 本次实际采用的取值 |
| `scan_files`、`scan_bytes`、`scan_records` | 读取了数据的数据文件个数、字节数合计、记录数合计（`scan_records` 另含内存中尚未写成文件的行）。查询系统表时都是 0 |

值的写法：`Int64` 为 JSON 整数，`Float64` 为带小数点或指数的 JSON 数字（如 `870.0`），`Boolean` 为 JSON 布尔，`Utf8` 为 JSON 字符串，`_timestamp` 为 UTC 微秒整数。

**行的先后与分页：**

- 没有 `ORDER BY`、也不是聚合查询时，按 `_timestamp` 降序。
- 先执行 SQL（含 `LIMIT`）得到排好序的行，再跳过 `from` 行，再取至多 `size` 行。`LIMIT` 不改变响应里的 `size`。
- 排序键完全相同的行，两次查询之间先后可能不同。要稳定分页，在 `ORDER BY` 里加上能区分各行的列。

```sh
curl -sS -X POST "$AMBERLOG/api/default/_search" -H "Authorization: Bearer $TOKEN" -d '{"query": {
  "sql": "SELECT message FROM app ORDER BY _timestamp",
  "start_time": 1792454400000000, "end_time": 1792540800000000, "size": 2, "from": 2}}'
```

```json
{"took":26,"hits":[{"message":"slow request"},{"message":"type drift"}],"total":2,"from":2,"size":2,"scan_records":4,"scan_files":1,"scan_bytes":3483}
```

**全有或全无**：作答需要读的任何对象读不到时，整个请求回错误，不返回据部分数据得出的结果。

### 4.3 SQL

Amberlog 接受 SQL 的一个子集。一条请求只能有一条 `SELECT`，末尾可以有一个 `;`，可以有 `--` 与 `/* */` 注释。是否能解析，按 PostgreSQL 17 的文法判定。

```text
SELECT <* 或输出项, ...>
FROM <数据集名或系统表名>
[WHERE <条件>]
[GROUP BY <项>, ...]
[ORDER BY <项> [ASC|DESC], ...]
[LIMIT <n>]
```

| 部分 | 支持的写法 |
|---|---|
| 输出项 | `*`；列名；聚合调用；`histogram` 调用。每项可带 `AS <别名>`，别名不能以 `_` 开头，`AS` 不能省略 |
| `FROM` | 一个数据集名（同一 `{org}` 下的 `<stream>`）或系统表名。不支持表别名、`JOIN`、子查询 |
| `WHERE` | 比较 `=` `!=` `<>` `<` `<=` `>` `>=`；`AND`、`OR`、`NOT`；`IS [NOT] NULL`；`[NOT] IN (...)`；`[NOT] LIKE`；`str_match`、`match_all`；圆括号分组。比较的两侧是列名或字面量 |
| `GROUP BY` | 列名、`histogram` 调用或输出列名 |
| `ORDER BY` | 列名、输出列名、聚合调用或 `histogram` 调用，默认 `ASC`。NULL 排在非 NULL 值之后（`DESC` 时之前） |
| `LIMIT` | 0 至 9223372036854775807 的整数字面量 |
| 字面量 | 单引号字符串（`''` 表示一个单引号，`\` 是普通字符）、十进制数（可带正负号、小数、指数）、`TRUE`、`FALSE`、`NULL` |

不支持：`JOIN`、子查询、`UNION`、`HAVING`、`DISTINCT`、窗口函数、类型转换（`::`、`CAST`）、上表以外的函数。用到它们时回 HTTP 400、`reason` 为 `sql_unsupported`。

**列名的引号**：不加引号的标识符按小写解析。含大写字母、`.` 或 `-` 的列名要用双引号，如 `"user.name"`、`"HTTP-Status"`。

**比较的规则：**

- `Int64` 列、`Float64` 列都可以与数值字面量比较。`Int64` 列按精确值比较；`Float64` 列先把字面量舍入为双精度再比较。
- `Int64` 列与 `Float64` 列之间、数值与字符串之间、布尔与非布尔之间不能比较，回 HTTP 400、`reason` 为 `invalid_sql`。
- 不能和 `NULL` 字面量比较（`= NULL`、`IN (NULL)`），用 `IS NULL`。
- 字符串按 Unicode 码点逐字符比较，区分大小写。布尔值 `FALSE` 小于 `TRUE`。
- `LIKE`：`%` 匹配任意个字符，`_` 匹配一个字符，`\` 转义下一个字符，区分大小写。左侧必须是 `Utf8` 列，模式必须是字符串字面量。

```sh
curl -sS -X POST "$AMBERLOG/api/default/_search" -H "Authorization: Bearer $TOKEN" -d '{"query": {
  "sql": "SELECT message, latency_ms FROM app WHERE level IN ('"'error'"', '"'warn'"') AND latency_ms > 500 ORDER BY latency_ms DESC",
  "start_time": 1792454400000000, "end_time": 1792540800000000}}'
```

```json
{"took":32,"hits":[{"message":"db timeout","latency_ms":3000.0},{"message":"slow request","latency_ms":870.0}],"total":2,"from":0,"size":100,"scan_records":4,"scan_files":1,"scan_bytes":3483}
```

（例子里的 `'"'error'"'` 是 shell 在单引号字符串里写出单引号的办法，SQL 实际收到的是 `'error'`。）

### 4.4 全文匹配：`str_match` 与 `match_all`

两者只能作为 `WHERE` 里的一个完整条件出现，可以用 `AND`、`OR`、`NOT` 组合。

| 写法 | 为真的条件 |
|---|---|
| `str_match(<列>, '<文本>')` | 该列不为 NULL，且包含这段文本。区分大小写。列必须是 `Utf8` 列 |
| `match_all('<文本>')` | 这一行至少有一个 `Utf8` 列（含影子列，与 `SELECT` 列表无关）包含这段文本。只把 ASCII 字母当作不区分大小写 |

文本里的 `%`、`_`、`\` 按字面匹配，不是通配符。

```sh
curl -sS -X POST "$AMBERLOG/api/default/_search" -H "Authorization: Bearer $TOKEN" -d '{"query": {
  "sql": "SELECT message FROM app WHERE match_all('"'TIMEOUT'"') OR str_match(\"user.name\", '"'ali'"')",
  "start_time": 1792454400000000, "end_time": 1792540800000000}}'
```

```json
{"took":29,"hits":[{"message":"db timeout"},{"message":"user login"}],"total":2,"from":0,"size":100,"scan_records":4,"scan_files":1,"scan_bytes":3483}
```

### 4.5 聚合

支持 `count(*)`，以及以一个列名为参数的 `count`、`sum`、`avg`、`min`、`max`。不支持 `DISTINCT`、`FILTER` 等修饰。

| 函数 | 结果 |
|---|---|
| `count(*)` | 行数，Int64 |
| `count(<列>)` | 该列不为 NULL 的行数，Int64 |
| `sum(<列>)` | 和，类型同该列。只用于 `Int64`、`Float64` 列 |
| `avg(<列>)` | 平均值，Float64。只用于 `Int64`、`Float64` 列 |
| `min(<列>)`、`max(<列>)` | 最小值、最大值，类型同该列 |

- 含 `GROUP BY`，或 `SELECT`、`ORDER BY` 里有聚合调用的查询是**聚合查询**。
- 有 `GROUP BY` 时，每组一行；没有输入行时不产生行。没有 `GROUP BY` 时恰好一行。NULL 自成一组。
- 聚合查询的 `SELECT` 不能是 `*`，也不能有既不是聚合调用、又不是分组项的列。
- 没有 `ORDER BY` 的聚合查询，结果行的先后不固定。
- 一组里没有非 NULL 值时，`sum`、`avg`、`min`、`max` 为 NULL（该键不出现）。
- `Int64` 列的 `sum` 超出 Int64 范围，或 `Float64` 的结果不是有限值时，回 HTTP 400、`reason` 为 `numeric_overflow`。
- 不带别名的聚合调用，输出列名是函数名的小写，如 `count`。

```sh
curl -sS -X POST "$AMBERLOG/api/default/_search" -H "Authorization: Bearer $TOKEN" -d '{"query": {
  "sql": "SELECT level, count(*) AS n, max(latency_ms) AS max_ms FROM app GROUP BY level ORDER BY n DESC, level",
  "start_time": 1792454400000000, "end_time": 1792540800000000}}'
```

```json
{"took":39,"hits":[{"level":"error","n":1,"max_ms":3000.0},{"level":"info","n":1,"max_ms":12.5},{"level":"warn","n":1,"max_ms":870.0},{"n":1}],"total":4,"from":0,"size":100,"scan_records":4,"scan_files":1,"scan_bytes":3483}
```

最后一行是 `level` 为 NULL 的那一组（`type drift` 那条记录），所以没有 `level` 键；它的 `latency_ms` 落在影子列里，`max_ms` 为 NULL。

### 4.6 时间分桶：`histogram`

```text
histogram(_timestamp, '<间隔>')
```

- 间隔写法同配置项的时长：`<整数><ms|s|m|h|d>`，如 `'5m'`、`'1h'`、`'1d'`。`'60s'` 与 `'1m'` 视为同一个调用。
- 值是这一行所在桶的起点，UTC 微秒，Int64。
- 按请求的 `query.timezone` 在当地时间上分桶：例如 `'1d'` 在 `Asia/Shanghai` 下按北京时间的零点切分。时区数据编译在程序里，不读取运行环境的时区文件。
- 只能出现在 `SELECT`、`GROUP BY`、`ORDER BY` 中，并且作为一项本身出现。只对有记录的桶产生行，不补空桶。

```sh
curl -sS -X POST "$AMBERLOG/api/default/_search" -H "Authorization: Bearer $TOKEN" -d '{"query": {
  "sql": "SELECT histogram(_timestamp, '"'1h'"') AS hour, count(*) AS n FROM app GROUP BY hour ORDER BY hour",
  "start_time": 1792454400000000, "end_time": 1792540800000000, "timezone": "Asia/Shanghai"}}'
```

```json
{"took":36,"hits":[{"hour":1792483200000000,"n":4}],"total":1,"from":0,"size":100,"scan_records":4,"scan_files":1,"scan_bytes":3483}
```

`1792483200000000` 是 2026-10-20T08:00:00Z，即北京时间 16:00。

### 4.7 检索的错误

`_search` 同时有多个问题时，大致按下面的顺序只报第一个：

1. `invalid_request`（400）：缺字段、类型不符、`size` 或 `from` 越界、时区名不认识。
2. `invalid_time_range`（400）：`end_time` 不大于 `start_time`，或有负值。
3. `invalid_sql`（400）：SQL 无法解析，或除空白、注释和 `;` 外什么都没有。
4. `sql_unsupported`（400）：用到了子集以外的写法。
5. `stream_not_found`（400）：`FROM` 的数据集不存在，或名字不合规则。
6. 数据集停用：`unsupported_format_version`（503）或 `internal`（500）。
7. `too_many_queries`（429）：同时执行的查询数已达 `query.max_concurrency`。立即拒绝，不排队，可以稍后重试。
8. 读对象存储失败：`unavailable`（503）。
9. `unknown_column`（400）：列不存在。
10. `invalid_sql`（400）：比较两侧类型不可比、和 `NULL` 比较、`LIKE` 左侧不是 `Utf8` 列、输出列名重复、聚合或 `histogram` 写法不对。
11. 执行中：读数据失败 `unavailable`（503）或 `internal`（500）；`query_memory_exceeded`（400，单个查询的内存超过 `query.max_memory_bytes`）；`numeric_overflow`（400）。

```sh
for sql in "SELEC 1" "SELECT * FROM app a JOIN app b ON true" "SELECT * FROM nosuch" "SELECT nosuch FROM app" "SELECT * FROM app WHERE level = 1"; do
  curl -sS -X POST "$AMBERLOG/api/default/_search" -H "Authorization: Bearer $TOKEN" \
    -d "{\"query\":{\"sql\":\"$sql\",\"start_time\":1792454400000000,\"end_time\":1792540800000000}}"; echo
done
```

```text
{"code":400,"reason":"invalid_sql","message":"Invalid statement: syntax error at or near \"SELEC\""}
{"code":400,"reason":"sql_unsupported","message":"a FROM item other than a stream name (subquery, JOIN, function…) is not supported"}
{"code":400,"reason":"stream_not_found","message":"stream `nosuch` does not exist"}
{"code":400,"reason":"unknown_column","message":"column `nosuch` does not exist in `app`"}
{"code":400,"reason":"invalid_sql","message":"cannot compare column `level` (Utf8) with this literal"}
```

## 5. 字段列表 `_fields`

```text
GET /api/{org}/{stream}/_fields
```

返回数据集当前的全部列，含 `_timestamp` 与影子列，按列名的 Unicode 码点升序。`type` 为 `Int64`、`Float64`、`Boolean`、`Utf8` 之一。

- 不读对象存储，对象存储不可用时照常作答。
- 不占用查询的并发名额。
- 数据集不存在时回 HTTP 400、`reason` 为 `stream_not_found`。

```sh
curl -sS "$AMBERLOG/api/default/app/_fields" -H "Authorization: Bearer $TOKEN"
```

```json
{"fields":[{"name":"_timestamp","type":"Int64"},{"name":"latency_ms","type":"Float64"},
 {"name":"latency_ms__utf8","type":"Utf8"},{"name":"level","type":"Utf8"},{"name":"message","type":"Utf8"},
 {"name":"tags","type":"Utf8"},{"name":"user.id","type":"Int64"},{"name":"user.name","type":"Utf8"}]}
```

## 6. 系统表

系统表用 `_search` 查询，写法与查询数据集相同：`FROM` 后面写系统表名。系统表只读，没有写入接口。

| 系统表 | 每一行是 | 时间范围的作用 | 不带 `ORDER BY` 时的先后 |
|---|---|---|---|
| `_ledger` | 一个数据集（数据资产台账） | 不影响有哪些行，只影响来源与对账汇总几列的统计范围 | `dataset` 升序 |
| `_reconciliation` | 一个数据集的一分钟窗口（对账） | 只含 `window_start` 在范围内的窗口 | `window_start` 降序，再按 `dataset` 升序 |
| `_alert_evaluations` | 一次告警评估 | 只含 `scheduled_at` 在范围内的评估 | `scheduled_at` 降序，再按 `rule` 升序 |

- 查询系统表时 `scan_*` 都是 0。
- 系统表的数据由计数对象和评估记录生成，写出有延迟：写入或评估之后，约一分钟内出现在系统表里。
- `_reconciliation` 与 `_ledger` 的 `WHERE` 里写了 `dataset = '<数据集>'`（以 `AND` 连接）时，只读这些数据集的数据，查询更快。`_alert_evaluations` 写 `rule = '<规则名>'` 同理。

### 6.1 `_ledger`：数据资产台账

| 列 | 类型 | 说明 |
|---|---|---|
| `dataset` | Utf8 | `<org>/<stream>` |
| `status` | Utf8 | `active`；数据集因格式版本不兼容停用时为 `disabled_format_version`；因 manifest 不可读停用时为 `disabled_manifest_unreadable` |
| `time_min`、`time_max` | Int64 | 数据集里最早、最晚的 `_timestamp`（热层与冷层合计） |
| `record_count`、`size_bytes` | Int64 | 记录数、数据文件字节数（热层与冷层合计） |
| `tier` | Utf8 | 数据在哪一层：`hot`、`hot_pending_lake`（热层里有已到入湖时间、还没入湖的数据）、`cold`、`hot_cold`。没有数据时为 NULL |
| `pending_lake_record_count` | Int64 | 已到入湖时间、还没入湖的记录数 |
| `retention` | Utf8 | 配置项 `retention.period` 的原文，未配置时为 NULL |
| `expires_at` | Int64 | `time_min` 加保留期，未配置保留期时为 NULL。只是显示，删除按到期删除的规则执行 |
| `sources` | Utf8 | 时间范围内以 `basis` 为 `delivery` 上报过发送数的来源，按码点升序以 `,` 连接 |
| `ts_sources` | Utf8 | 时间范围内以 `basis` 为 `timestamp` 上报过发送数的来源 |
| `reconciliation` | Utf8 | `three_way`（有 `sources`）、`two_way`（没有），计数对象无法读懂时为 `unknown` |
| `windows_pending`、`windows_consistent`、`windows_missing`、`windows_duplicate`、`windows_silent`、`windows_undeterminable` | Int64 | 时间范围内各判定结果的窗口个数，判定见 [6.2 节](#62-_reconciliation对账) |
| `silent_sources` | Int64 | 时间范围内上报中断过的来源个数 |

`status` 不为 `active` 的行，数据量各列为 NULL。`reconciliation` 为 `unknown` 的行，判定汇总各列为 NULL。

```sh
curl -sS -X POST "$AMBERLOG/api/default/_search" -H "Authorization: Bearer $TOKEN" -d '{"query": {
  "sql": "SELECT * FROM _ledger",
  "start_time": 1792483200000000, "end_time": 1792483380000000}}'
```

```json
{"took":30,"hits":[{"dataset":"default/app","status":"active","time_min":1792483200000000,"time_max":1792560462673048,
  "record_count":5,"size_bytes":4894,"tier":"hot","pending_lake_record_count":0,"reconciliation":"two_way","ts_sources":"web-1",
  "windows_pending":0,"windows_consistent":3,"windows_missing":0,"windows_duplicate":0,"windows_silent":0,"windows_undeterminable":0,
  "silent_sources":0}],"total":1,"from":0,"size":100,"scan_records":0,"scan_files":0,"scan_bytes":0}
```

`time_max` 是 2.1 节 gzip 例子那条记录的接收时刻（它没带 `_timestamp`）。查询的时间范围不影响 `time_min`、`time_max`、`record_count` 这些列。

### 6.2 `_reconciliation`：对账

每一行是一个数据集的一分钟窗口。

| 列 | 类型 | 说明 |
|---|---|---|
| `dataset` | Utf8 | `<org>/<stream>` |
| `window_start`、`window_end` | Int64 | 窗口的起点与终点，UTC 微秒，`[window_start, window_end)` |
| `sent` | Int64 | 这一分钟各来源以 `basis` 为 `delivery` 上报的发送数之和，没有上报时为 NULL |
| `sent_sources` | Utf8 | 这些来源，按码点升序以 `,` 连接 |
| `received` | Int64 | 按**接收时刻**归到这一分钟的、从请求体解析出的记录数（含被逐条拒绝的） |
| `durable` | Int64 | 按接收时刻归到这一分钟的、已确认（收到 HTTP 200）的记录数 |
| `gap_sent_received` | Int64 | `sent` 减 `received` |
| `gap_received_durable` | Int64 | `received` 减 `durable` |
| `ts_sent` | Int64 | 按 `_timestamp` 所在分钟统计、以 `basis` 为 `timestamp` 上报的发送数之和（只计受判来源），没有时为 NULL |
| `ts_sent_sources` | Utf8 | 受判来源，即这一分钟以 `basis` 为 `timestamp` 上报过的来源 |
| `ts_durable` | Int64 | 受判来源按 `_timestamp` 所在分钟统计的确认数之和（只计带了 `X-Amberlog-Source` 的写入） |
| `verdict` | Utf8 | 判定结果，见下表 |

`verdict` 的取值（宽限期是配置项 `reconciliation.grace`，默认 5 分钟）：

| 取值 | 含义 |
|---|---|
| `pending` | 窗口结束后还没过宽限期，或有来源正在补发，暂不判定 |
| `consistent` | 每个受判来源的确认数都等于其发送数 |
| `missing` | 有受判来源的确认数少于其发送数（可能漏采） |
| `duplicate` | 没有缺失，但有受判来源的确认数多于其发送数（可能重复写入） |
| `silent` | 有来源在前 24 小时内上报过，这一分钟却没有上报（上报中断） |
| `undeterminable` | 没有受判来源，也没有上报中断，无从判定 |

判定在查询时按当时的计数计算，迟到的记录补齐以后，`missing` 会变成 `consistent`。

「有来源正在补发」指下列情形之一，此时相关窗口按 `pending` 计：

- 过去一个宽限期内，Amberlog 确认过这个来源的迟到记录（`_timestamp` 所在分钟结束后超过宽限期才写入的记录），或受理过它迟到的 `basis` 为 `timestamp` 的上报；
- 过去一个宽限期内，Amberlog 对这个数据集的 `_json` 或 `_sent` 回过 503；
- 进程启动后还不到一个宽限期。

所以补写历史数据以后，要等一个宽限期，判定才会稳定。

```sh
curl -sS -X POST "$AMBERLOG/api/default/_search" -H "Authorization: Bearer $TOKEN" -d '{"query": {
  "sql": "SELECT window_start, ts_sent, ts_durable, verdict FROM _reconciliation WHERE dataset = '"'default/app'"'",
  "start_time": 1792483200000000, "end_time": 1792483380000000}}'
```

```json
{"took":16,"hits":[
  {"window_start":1792483320000000,"ts_sent":1,"ts_durable":1,"verdict":"consistent"},
  {"window_start":1792483260000000,"ts_sent":1,"ts_durable":1,"verdict":"consistent"},
  {"window_start":1792483200000000,"ts_sent":1,"ts_durable":1,"verdict":"consistent"}],
 "total":3,"from":0,"size":100,"scan_records":0,"scan_files":0,"scan_bytes":0}
```

### 6.3 `_alert_evaluations`：告警评估记录

告警规则配置在配置文件里（见配置项参考的 `alerts.rules`）。本文的例子用的是这条规则：

```toml
[[alerts.rules]]
name = "error_burst"
interval = "1m"
window = "5m"
sql = "SELECT count(*) AS n FROM app WHERE level = 'error'"
condition = { column = "n", op = ">", threshold = 1000 }
webhook_url = "https://hooks.example.com/amberlog"
```

每次评估结束后写一条记录，保留期为配置项 `alerts.history_retention`（默认 30 天）。已从配置里删去的规则，其评估记录在保留期内照样出现。

| 列 | 类型 | 说明 |
|---|---|---|
| `rule` | Utf8 | 规则名 |
| `scheduled_at` | Int64 | 计划评估的时刻，UTC 微秒 |
| `started_at` | Int64 | 开始执行查询的时刻；没有执行查询时为 NULL |
| `finished_at` | Int64 | 评估结束的时刻 |
| `window_start`、`window_end` | Int64 | 这次评估查询的时间范围 |
| `outcome` | Utf8 | `matched`（命中）、`not_matched`（未命中）、`skipped`（跳过）、`error`（出错） |
| `reason` | Utf8 | `skipped` 与 `error` 的原因，如 `previous_running`、`alert_slots_full`、`query_slots_full`、`shutting_down`，或查询的错误 `reason` |
| `matched_rows` | Int64 | 满足条件的行数；`skipped`、`error` 时为 NULL |
| `truncated` | Boolean | 查询结果是否超过 `query.max_size` 行而被截断 |
| `delivery` | Utf8 | 通知结果：`delivered`、`delivered_after_retry`、`abandoned`、`not_attempted` |
| `attempts` | Int64 | 发出的通知请求次数 |
| `last_http_status` | Int64 | 最后一次通知请求收到的状态码 |

```sh
curl -sS -X POST "$AMBERLOG/api/default/_search" -H "Authorization: Bearer $TOKEN" -d "{\"query\": {
  \"sql\": \"SELECT rule, scheduled_at, outcome, matched_rows, delivery FROM _alert_evaluations\",
  \"start_time\": $(( ($(date +%s) - 3600) * 1000000 )), \"end_time\": $(( ($(date +%s) + 60) * 1000000 ))}}"
```

```json
{"took":19,"hits":[
  {"rule":"error_burst","scheduled_at":1792560540000000,"outcome":"not_matched","matched_rows":0,"delivery":"not_attempted"},
  {"rule":"error_burst","scheduled_at":1792560480000000,"outcome":"not_matched","matched_rows":0,"delivery":"not_attempted"}],
 "total":2,"from":0,"size":100,"scan_records":0,"scan_files":0,"scan_bytes":0}
```

规则每分钟评估一次，返回几行取决于实例已经运行了几分钟。

## 7. 错误结构

### 7.1 错误信封

除 `/iceberg/` 以外，所有 4xx、5xx 响应的响应体都是这样一个 JSON 对象（`Content-Type: application/json`）：

```json
{"code": 400, "reason": "invalid_json", "message": "the request body is not valid JSON"}
```

| 字段 | 说明 |
|---|---|
| `code` | 等于 HTTP 状态码 |
| `reason` | 机器可读的原因，取值见下表。**按 `reason` 编写程序逻辑** |
| `message` | 给人看的说明。文字可能随版本变化，不要按它判断 |
| `details` | 可选。`all_records_rejected` 时含 `status`；`/readyz` 回 503 时含 `checks` |

- 对 `HEAD` 请求的错误响应没有响应体。
- 5xx 的 `message` 不含内部路径、对象键、存储端点、桶名或底层库的原始错误。详细原因在运行日志里，用 `x-request-id` 查找。
- 已发布的 `reason` 不会删除，也不会改变含义。新版本可能增加新的 `reason`，客户端要能处理不认识的值。
- 状态码只有 400、401、404、405、413、415、429、500、503。
- 405 响应带 `Allow` 头，列出该路径支持的方法。401 响应带 `WWW-Authenticate: Bearer`。

### 7.2 `reason` 一览

| `reason` | 状态码 | 含义 |
|---|---|---|
| `invalid_json` | 400 | 请求体不是合法 JSON |
| `invalid_encoding` | 400 | 按 `Content-Encoding` 解压失败 |
| `invalid_request` | 400 | 字段缺失、类型不符或取值不合规则（`_search`、`_sent`、`X-Amberlog-Source`） |
| `all_records_rejected` | 400 | `_json` 批次里的记录全部被逐条拒绝 |
| `invalid_stream_name` | 400 | 路径里的数据集名不合规则 |
| `stream_not_found` | 400 | 查询的数据集不存在 |
| `invalid_sql` | 400 | SQL 无法解析，或语义不合规则 |
| `sql_unsupported` | 400 | SQL 用到了子集以外的写法 |
| `invalid_time_range` | 400 | `_search` 的时间范围不合法 |
| `unknown_column` | 400 | 查询的列不存在 |
| `numeric_overflow` | 400 | 聚合结果溢出 |
| `query_memory_exceeded` | 400 | 单个查询的内存超过 `query.max_memory_bytes` |
| `unauthorized` | 401 | 令牌缺失或无效 |
| `org_not_found` | 404 | 路径里的组织不存在 |
| `not_found` | 404 | 路径不存在 |
| `namespace_not_found`、`table_not_found` | 404 | 只用于 `/iceberg/` |
| `method_not_allowed` | 405 | 这个路径不支持该方法 |
| `payload_too_large` | 413 | 请求体超过 `ingest.max_body_bytes` |
| `unsupported_encoding` | 415 | 不支持的 `Content-Encoding` |
| `too_many_queries` | 429 | 同时执行的查询数已满，稍后重试 |
| `internal` | 500 | 服务端内部错误，或数据集的 manifest 不可读 |
| `unsupported_format_version` | 503 | 数据集的存储格式版本不受这个版本的程序支持 |
| `unavailable` | 503 | 对象存储或本地 WAL 暂时不可用，或进程正在停机。可以重试 |
| `not_ready` | 503 | 只用于 `/readyz` |

`errors[]` 里的逐条原因见 [2.4 节](#24-逐条拒绝)。`type_conflict` 保留不用，当前版本不再产生。

### 7.3 校验的先后

`/api/` 下的请求按下面的顺序校验，第一个失败的步骤决定响应：

1. 停机：进程正在停机时，新请求回 503 `unavailable`。
2. 认证：401 `unauthorized`。
3. 路由与方法：404 `not_found`、405 `method_not_allowed`。
4. `{org}`：404 `org_not_found`。
5. `{stream}`：400 `invalid_stream_name`。
6. 数据集状态（只对路径含 `{stream}` 的端点）：503 `unsupported_format_version`，或 500 `internal`。
7. 来源标识（只对 `_json`）：400 `invalid_request`。
8. `Content-Encoding`：415 `unsupported_encoding`。
9. 请求体大小与解压：413 `payload_too_large`、400 `invalid_encoding`。
10. 请求体解析：400 `invalid_json` 等。

停机期间，在途的请求在 `server.shutdown_timeout`（默认 10 秒）内照常完成。

## 8. 运维端点

### 8.1 `GET /healthz`

进程能处理 HTTP 请求时回 200，不检查对象存储与 WAL。用作存活探针。

```sh
curl -sS "$AMBERLOG/healthz"
```

```json
{"status":"ok"}
```

### 8.2 `GET /readyz`

以下条件都成立时回 200 `{"status":"ready"}`：

- `wal.dir` 可写；
- 最近一次对象存储探测成功（每 30 秒内探测一次：写入并读回 `<storage.prefix>/_system/readyz-probe`）；
- 进程不在停机过程中。

否则回 503，`reason` 为 `not_ready`，`details.checks` 含 `wal`、`object_store`、`shutting_down` 三个布尔值。用作就绪探针。

`/readyz` 回 503 时，`/api/` 请求并不会因此被拒，各自按自己的规则作答。

以 `amberlog catalog` 运行的只读目录进程不写探测对象，只做只读的列举探测，`wal` 恒为 true。

```sh
curl -sS "$AMBERLOG/readyz"
```

```json
{"status":"ready"}
```

### 8.3 `GET /metrics`

Prometheus 文本格式（`Content-Type: text/plain; version=0.0.4; charset=utf-8`），要求令牌。Prometheus 抓取配置里用 `authorization.credentials_file` 读令牌文件，不要把令牌直接写进配置（[运维指南](operations.zh-CN.md) 第 3.1 节）。

```sh
curl -sS "$AMBERLOG/metrics" -H "Authorization: Bearer $TOKEN" | grep -v '^#' | grep amberlog_ingest_records_total
```

```text
amberlog_ingest_records_total{org="default",stream="app",outcome="accepted"} 5
amberlog_ingest_records_total{org="default",stream="app",outcome="rejected"} 5
```

**稳定指标**：下列指标不会改名或删除，已有的标签不会删除，可以放心用于告警和看板。其他 `amberlog_` 指标可能随版本变化。

| 指标 | 类型 | 说明 |
|---|---|---|
| `amberlog_build_info{version,commit}` | gauge | 恒为 1，标签为版本号与提交号 |
| `amberlog_ready` | gauge | `/readyz` 此刻会回 200 时为 1，否则为 0 |
| `amberlog_ingest_records_total{org,stream,outcome}` | counter | `_json` 接受（`accepted`）与逐条拒绝（`rejected`）的记录数 |
| `amberlog_manifest_commits_total{org,stream,result}` | counter | 提交次数，`result` 为 `ok` 或 `failed`。对象存储不可用期间，每个 `flush.interval` 计一次 `failed` |
| `amberlog_dataset_disabled{org,stream,state}` | gauge | 停用的数据集，`state` 为 `format_version` 或 `manifest_unreadable`，值为 1。没有停用时没有这个序列 |
| `amberlog_wal_bytes` | gauge | `wal.dir` 下文件的总字节数 |
| `amberlog_backlog_memory_bytes` | gauge | 内存中待提交记录占用的字节数 |
| `amberlog_lake_lag_seconds{org,stream}` | gauge | 入湖积压：最早一个已到入湖时间、还没入湖的小时桶，已超过入湖时间多少秒。没有积压时为 0 |
| `amberlog_search_rejected_total{reason}` | counter | 因 `too_many_queries` 或 `query_memory_exceeded` 被拒的 `_search` 次数 |
| `amberlog_alert_evaluations_total{rule,outcome}` | counter | 告警评估次数，按结局 |
| `amberlog_alert_deliveries_total{rule,result}` | counter | 告警通知结果，`result` 为 `delivered` 或 `abandoned` |
| `amberlog_reconciliation_windows{org,stream,verdict}` | gauge | 最近一次后台漏采判定中（过去 24 小时），各判定结果的窗口个数 |

## 9. Iceberg 只读目录

Amberlog 在 `/iceberg/` 下提供 Apache Iceberg REST 目录规范的只读子集，供 Apache Spark、Trino、DuckDB 等外部引擎找到冷层表。以 `amberlog serve` 运行的实例总是提供它；也可以另起一个 `amberlog catalog` 进程，只提供目录，只需要冷层前缀的读权限。

| 方法与路径 | 作答 |
|---|---|
| `GET /iceberg/v1/config` | `{"defaults":{},"overrides":{"prefix":"<storage.prefix>"},"endpoints":[...]}` |
| `GET /iceberg/v1/{prefix}/namespaces` | `{"namespaces":[["default"]]}`；只列出至少有一张冷层表的命名空间 |
| `GET`、`HEAD /iceberg/v1/{prefix}/namespaces/{namespace}` | `{"namespace":["default"],"properties":{}}`；`HEAD` 回 204 |
| `GET /iceberg/v1/{prefix}/namespaces/{namespace}/tables` | `{"identifiers":[{"namespace":["default"],"name":"<stream>"}]}` |
| `GET`、`HEAD /iceberg/v1/{prefix}/namespaces/{namespace}/tables/{table}` | `{"metadata-location":"...","metadata":{...},"config":{}}`；`HEAD` 回 204 |

- 命名空间是组织名，表名是数据集名。数据进入冷层（入湖）以后，表才存在。
- `{prefix}` 必须等于配置项 `storage.prefix`。
- 目录**不下发存储凭据**（`config` 恒为 `{}`）。外部引擎要用自己配置的、只能读冷层前缀的存储凭据读数据文件。
- 错误体的形状与 `/api/` 不同，按 Iceberg REST 规范：`{"error":{"message":"...","type":"NoSuchTableException","code":404,"reason":"table_not_found"}}`。
- 不在七个端点之内的路径回 405，`Allow` 为空。
- 各外部引擎连这个目录的写法，见 [外部引擎直读指南](external-engines.zh-CN.md) 第 5 节。

```sh
curl -sS "$AMBERLOG/iceberg/v1/config" -H "Authorization: Bearer $TOKEN"
```

```json
{"defaults":{},"overrides":{"prefix":"amberlog"},"endpoints":["GET /v1/config","GET /v1/{prefix}/namespaces",
 "GET /v1/{prefix}/namespaces/{namespace}","HEAD /v1/{prefix}/namespaces/{namespace}","GET /v1/{prefix}/namespaces/{namespace}/tables",
 "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}","HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}"]}
```

```sh
curl -sS "$AMBERLOG/iceberg/v1/amberlog/namespaces/default/tables/app" -H "Authorization: Bearer $TOKEN"
```

```json
{"error":{"message":"the table does not exist","type":"NoSuchTableException","code":404,"reason":"table_not_found"}}
```

（这个数据集的数据还没有入湖，所以表还不存在。离例子的时间戳超过 72 小时再跑时，这个请求会返回表的元数据；见 1.2 节。）

## 10. 检索界面 `/ui`

在浏览器里打开 `<Amberlog 地址>/ui`，可以检索、查看台账与对账。页面不要求令牌，页面里输入令牌后，由它代你调用上面的 `_search` 与 `_fields` 接口。页面不向外部地址发请求。

```sh
curl -sS -o /dev/null -w '%{http_code} %{content_type}\n' "$AMBERLOG/ui"
```

```text
200 text/html; charset=utf-8
```

# 采集器接入示例

[English](README.md)

> 适用于 Amberlog 0.1.0。

本目录给出两种常见日志采集器接入 Amberlog 的配置：

| 采集器 | 配置文件 | 实测版本 |
|---|---|---|
| Vector | [`vector/vector.yaml`](vector/vector.yaml) | 0.51.1（镜像 `timberio/vector:0.51.1-debian`） |
| Fluent Bit | [`fluent-bit/fluent-bit.yaml`](fluent-bit/fluent-bit.yaml) | 4.1.1（镜像 `fluent/fluent-bit:4.1.1`） |

两份配置做同样的事：

1. 从文件尾读取 `/var/log/app/*.log` 中新写入的行。
2. 每行作为一条记录，原文放在字段 `message`，文件路径放在字段 `file`，读到这一行的时间写进 `_timestamp`。Vector 另外保留采集机的主机名 `host`。
3. 一批记录编码为一个 JSON 数组，用 gzip 压缩，经 HTTPS 发送到 Amberlog 的写入接口。
4. 请求带访问令牌和来源标识。
5. Amberlog 暂时不可用时重试。

## 使用前要改的地方

1. **地址。** 把 `amberlog.example.internal`、端口 `8800` 换成你的 Amberlog 地址和端口。
2. **数据集。** 把路径 `/api/default/app/_json` 里的 `app` 换成你的数据集名。数据集名只能包含小写字母、数字和下划线，以字母或数字开头。第一次写入时自动创建。
3. **日志路径。** 把 `/var/log/app/*.log` 换成你的日志文件。
4. **CA 证书。** Amberlog 用自签证书或内部 CA 签发的证书时，把 CA 证书放到配置里写的路径（Vector：`/etc/vector/amberlog-ca.crt`；Fluent Bit：`/fluent-bit/etc/amberlog-ca.crt`）。自签证书的 CA 证书就是服务端证书本身。证书的主题备用名称（SAN）要包含采集器连接时用的主机名或 IP 地址。使用公共 CA 签发的证书时，删掉这一项。
5. **两个环境变量。** 启动采集器前设置：
   - `INGEST_TOKEN`：Amberlog 的访问令牌，即配置项 `auth.token` 的值。
   - `SOURCE_ID`：这台机器的来源标识，例如 `web-1`。

**环境变量不要以 `AMBERLOG_` 开头。** Amberlog 把 `AMBERLOG_` 开头的环境变量当作自己的配置项。在同一个环境里启动 Amberlog 时，它不认识这类变量，会以退出码 2 拒绝启动。

## 运行

用 Docker 运行时，把配置、CA 证书和日志目录挂进容器：

```sh
# Vector
docker run -d --name vector \
  -e INGEST_TOKEN="$INGEST_TOKEN" -e SOURCE_ID=web-1 \
  -v "$PWD/vector/vector.yaml:/etc/vector/vector.yaml:ro" \
  -v /path/to/amberlog-ca.crt:/etc/vector/amberlog-ca.crt:ro \
  -v /var/log/app:/var/log/app:ro \
  -v vector-data:/var/lib/vector \
  timberio/vector:0.51.1-debian --config /etc/vector/vector.yaml

# Fluent Bit
docker run -d --name fluent-bit \
  -e INGEST_TOKEN="$INGEST_TOKEN" -e SOURCE_ID=web-1 \
  -v "$PWD/fluent-bit/fluent-bit.yaml:/fluent-bit/etc/fluent-bit.yaml:ro" \
  -v /path/to/amberlog-ca.crt:/fluent-bit/etc/amberlog-ca.crt:ro \
  -v /var/log/app:/var/log/app:ro \
  -v fluent-bit-data:/var/lib/fluent-bit \
  fluent/fluent-bit:4.1.1 -c /fluent-bit/etc/fluent-bit.yaml
```

`/var/lib/vector` 和 `/var/lib/fluent-bit` 保存读取位置。把它们放在持久卷上，采集器重启后从上次的位置接着读。

## 检查数据是否写入

```sh
NOW=$(($(date +%s) * 1000000))
curl --cacert /path/to/amberlog-ca.crt -H "Authorization: Bearer $INGEST_TOKEN" \
  -d "{\"query\":{\"sql\":\"SELECT count(*) AS n FROM app\",\"start_time\":$((NOW - 3600000000)),\"end_time\":$((NOW + 60000000))}}" \
  https://amberlog.example.internal:8800/api/default/_search
```

## 写入接口要点

两份配置都按下面的规则写。改配置时也要遵守。

- **地址与方法。** `POST /api/default/<数据集>/_json`。
- **请求体。** 一个 JSON 数组，每个元素是一条记录（一个 JSON 对象）。
- **认证。** 请求头 `Authorization: Bearer <令牌>`。
- **来源标识。** 请求头 `X-Amberlog-Source` 可选。取值只能由字母、数字和 `_.-` 组成，以字母或数字开头，最长 128 个字符。取值不合规时，整个请求以 HTTP 400 被拒。来源标识不写进记录，只用于按来源分开计数。
- **压缩。** 请求头 `Content-Encoding` 可以是 `gzip` 或 `zstd`，也可以不压缩。
- **大小。** 请求体（解压后）默认不超过 32 MiB，超过时以 HTTP 413 被拒。上限由 Amberlog 的配置项 `ingest.max_body_bytes` 决定。
- **记录时间 `_timestamp`。** 可以是数字（按大小自动识别为秒、毫秒、微秒或纳秒），也可以是带时区的 RFC 3339 字符串。缺失时，Amberlog 用接收时刻填充。默认不能晚于接收时刻 24 小时，由配置项 `ingest.max_future` 决定。
- **字段名。** 只能包含字母、数字和 `-_.`，以字母或数字开头和结尾。除 `_timestamp` 外，以 `_` 开头的字段名是保留字，含这类字段的记录会被拒收。

## 应答与重试

| Amberlog 的应答 | 含义 | 采集器的处置或你要做的事 |
|---|---|---|
| HTTP 200 | 这一批已受理。成功的记录已经落盘 | 继续发下一批 |
| HTTP 400 | 这一批整体不合格：例如不是合法 JSON、来源标识不合规、全部记录都被拒收 | 实测：Vector 和 Fluent Bit 都不重试，丢弃这一批，在自己的日志里记错误 |
| HTTP 401 | 令牌不对 | 检查 `INGEST_TOKEN` |
| HTTP 413 | 请求体太大 | 调小批次 |
| HTTP 503 | Amberlog 暂时不可用 | 实测：Vector 和 Fluent Bit 都按退避间隔重试整批。Vector 缺省不限重试次数，两次之间最长等 30 秒（`retry_max_duration_secs` 的缺省值），直到成功。Fluent Bit 最多重试 `retry_limit` 次，用尽后丢弃这一批，在自己的日志里记错误；示例配置是 10 次，按它的退避算法推算，10 次用完约需 1 分钟到 2 小时（推算，没有实测） |
| 连接失败 | Amberlog 没有在运行，或网络不通 | 实测：同 503 |

注意两点：

- **一批中部分记录被拒时，应答仍是 HTTP 200。** 应答体的 `errors` 列出被拒记录的序号和原因，但这两种采集器都不据此处理：Fluent Bit 把应答体原文写进自己的日志，Vector 不记。被拒的记录不会重试，采集器也不会报错。可以在 Amberlog 的摄取对账表 `_reconciliation` 里看到：被拒的记录计入 `received`，不计入 `durable`。
- **重试可能产生重复记录。** Amberlog 返回 503 的那一批不算写入。采集器重试整批时，如果前一次请求其实已经落盘（例如应答在网络上丢了），这批记录会保存两份。Amberlog 按追加保存，不去重。

## 已知差异与限制

- **记录时间是采集时间。** 两份配置的 `_timestamp` 都是采集器读到这一行的时间，不是日志行里写的时间。要用日志自己的时间，需要在采集器里解析日志行，再把解析出的时间写进 `_timestamp`（RFC 3339 字符串或数字）。
- **JSON 格式的日志行不会拆成字段。** 两份配置把整行原文放在 `message` 里。要把 JSON 日志拆成多个字段，需要在采集器里加解析步骤（Vector 用 `parse_json`，Fluent Bit 用 `json` 解析器）。拆出的字段名同样要遵守上面的字段名规则。
- **Vector 暂不读取很小的文件。** Vector 按文件开头的内容识别文件。文件小于 256 字节时，Vector 先忽略它，日志里有 `Currently ignoring file too small to fingerprint`，文件写到足够大后再读。
- **Fluent Bit 发现新文件有延迟。** Fluent Bit 默认每 60 秒扫描一次目录（`refresh_interval`）。启动之后新建的日志文件，最多要等这么久才开始读。
- **不上报发送条数。** Amberlog 的摄取对账可以接收采集端上报的每分钟发送条数（`POST /api/default/<数据集>/_sent`），做三方对账。这两种采集器没有现成的上报方式，示例不上报，对账表按两方对账呈现。

## 实测记录

两份配置在测试环境中按下面的步骤实测过：

1. Amberlog 启用 TLS（自签证书，主题备用名称为 `127.0.0.1`），对象存储为 MinIO。
2. 两种采集器各写一个数据集，分别用来源标识 `web-1`、`web-2`。
3. 向两个日志文件分别追加 50 行和 30 行。两个数据集都查到 80 条，`file` 字段区分了两个文件。
4. 停掉 Amberlog，再追加 20 行，然后重启 Amberlog。两种采集器都在重试后补发成功，两个数据集都查到 100 条。
5. 另用一个模拟服务端，分别以 HTTP 400 和 503 应答，得到上表「采集器的处置」一列的结果。又在测试副本里把重试次数调小（Fluent Bit 的 `retry_limit` 调为 2，Vector 的 `request.retry_attempts` 调为 3），一直回 503，测了重试用尽：两种采集器都丢弃这一批，在日志里记错误。

以 Amberlog 0.1.0 发布前的构建重测了第 1～4 步，结果相同。第 5 步只与采集器本身有关，2026-10-24 用同样的采集器版本重测，结果相同。

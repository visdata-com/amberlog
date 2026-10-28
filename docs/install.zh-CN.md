# 安装与部署指南

[English](install.md)

> 适用于 Amberlog 0.1.0。

本文写给要在自己的环境里部署 Amberlog 的人：怎样安装，怎样接对象存储，怎样给外部引擎发只读凭据，怎样配 TLS 与反向代理，内存怎样估。文中的命令都在测试环境里实际运行过（Linux，S3 兼容存储 MinIO），没有实测的地方逐处写明。

部署之前，请先读 [已知限制与路线图](known-limitations.zh-CN.md) 第 1 节。配置项见 [配置项参考](configuration.zh-CN.md)，日常运维见 [运维指南](operations.zh-CN.md)。只想先看一眼效果，用 [一键试用](../examples/quickstart/README.zh-CN.md)。

## 1. 先认识几个词

- **写入实例**：以 `amberlog serve` 运行的进程。它受理写入与检索，并在后台做提交、合并、入湖等工作。同一份存储（同一 `object_store.url` 与 `storage.prefix`）上同一时刻只能运行一个写入实例。
- **WAL**（预写日志）：`wal.dir` 下的本地文件。写入请求先写进 WAL 并 fsync，才回 200，之后后台把记录提交到对象存储。
- **热层**与**冷层**：记录先以 Amberlog 自己的格式存放在热层。一个小时桶结束 `lifecycle.lake_watermark`（默认 72 小时）以后**入湖**，成为冷层的标准 Apache Iceberg 表，外部引擎可以直接读。
- **只读目录模式**：以 `amberlog catalog` 运行的进程。它只给外部引擎提供 Iceberg REST 目录端点，不受理写入与检索，只用只读凭据。
- **明文声明**：配置项 `server.behind_tls_proxy = true`。它告诉 Amberlog：前面有反向代理负责 TLS。

## 2. 部署前要准备的

| 项 | 要求 |
|---|---|
| 操作系统 | Linux x86_64 或 aarch64，glibc 2.28 及以上（例如 RHEL 8 及其衍生版、麒麟 V10、Debian 10 及以上、Ubuntu 20.04 及以上）。用容器镜像时，宿主机只要能运行 Docker 或兼容的容器引擎 |
| 对象存储 | MinIO（实测过），或 AWS S3、Ceph RGW（按 S3 兼容的写法支持，还没有实测），见第 6 节。只做功能试用时也可以用本地磁盘 |
| 本地磁盘 | 放 WAL。积压时 WAL 会变大，大小约等于积压的记录量。磁盘要可靠：已确认、还没提交的记录只在这里 |
| 内存 | 见第 9 节。最大的一项是冷层合并，随单个数据集一天的数据量增长 |
| 网络 | 写入实例默认监听 8800 端口。只读目录模式另用一个端口 |

## 3. 三种安装方式

| 方式 | 适合 | 见 |
|---|---|---|
| 二进制 | 物理机与虚拟机，用 systemd 管理 | 第 4 节 |
| 容器镜像 | 已有容器平台 | 第 5 节 |
| 编排文件 | 在自己的电脑上试用，不用于生产 | [一键试用](../examples/quickstart/README.zh-CN.md) |

三种方式用的是同一个可执行文件 `amberlog`，配置项完全相同。

## 4. 用二进制安装

下面以 0.1.0 版、x86_64 为例。aarch64 把文件名里的 `x86_64` 换成 `aarch64`。命令要用 root 执行。

测试中，校验、解包、配置文件与启动检查都实际做过，用普通用户和同样的配置文件运行；建用户（`useradd`）与 `systemctl` 两步没有执行。

### 4.1 下载与校验

1. 从 GitHub Release 页面下载安装包和校验和文件：

   ```sh
   VERSION=0.1.0
   BASE=https://github.com/visdata-com/amberlog/releases/download/v$VERSION
   curl -LO "$BASE/amberlog-$VERSION-x86_64-unknown-linux-gnu.tar.gz"
   curl -LO "$BASE/SHA256SUMS"
   ```

2. 校验安装包。输出必须是 `OK`：

   ```sh
   sha256sum -c --ignore-missing SHA256SUMS
   ```

   ```text
   amberlog-0.1.0-x86_64-unknown-linux-gnu.tar.gz: OK
   ```

3. 解包。包里有 `amberlog` 可执行文件，以及 `README.md`、`README.zh-CN.md`、`LICENSE`、`NOTICE`、`THIRD-PARTY-NOTICES.txt`、`CHANGELOG.md`：

   ```sh
   tar -xzf amberlog-$VERSION-x86_64-unknown-linux-gnu.tar.gz
   ```

### 4.2 安装文件与建用户

1. 安装可执行文件，确认版本：

   ```sh
   install -m 0755 amberlog-$VERSION-x86_64-unknown-linux-gnu/amberlog /usr/local/bin/amberlog
   amberlog --version
   ```

2. 建一个专用的系统用户。Amberlog 不需要 root 权限：

   ```sh
   useradd --system --home-dir /var/lib/amberlog --shell /usr/sbin/nologin amberlog
   ```

3. 建 WAL 目录与配置目录：

   ```sh
   install -d -o amberlog -g amberlog -m 0700 /var/lib/amberlog /var/lib/amberlog/wal
   install -d -o root -g amberlog -m 0750 /etc/amberlog
   ```

### 4.3 生成令牌

令牌是访问 Amberlog 的唯一凭据，持有它的人能读写全部数据。用随机生成的长值，不要用示例里的值：

```sh
openssl rand -hex 32
```

令牌写进配置文件，文件只让运行用户读得到（第 4.4 节）。不要放在命令行参数里，也尽量不用环境变量：同一用户的其他进程能看到环境变量，容器里 `docker inspect` 也能看到。换令牌要重启进程。

### 4.4 写配置文件

写 `/etc/amberlog/amberlog.toml`。下面的例子用 MinIO、进程内 TLS；对象存储的其他写法见第 6 节，TLS 的其他做法见第 8 节。尖括号里的值换成你自己的：

```toml
[server]
http_addr = "0.0.0.0:8800"

[server.tls]
cert_file = "/etc/amberlog/tls/tls.crt"
key_file = "/etc/amberlog/tls/tls.key"

[auth]
token = "<第 4.3 节生成的令牌>"

[wal]
dir = "/var/lib/amberlog/wal"

[object_store]
url = "s3://logs"

[object_store.s3]
endpoint = "https://minio.example.com:9000"
force_path_style = true
conditional_put = "create_only"
access_key_id = "amberlog-writer"
secret_access_key = "<Amberlog 的访问密钥>"
```

设置文件权限：

```sh
chown root:amberlog /etc/amberlog/amberlog.toml
chmod 0640 /etc/amberlog/amberlog.toml
```

证书与私钥放在 `/etc/amberlog/tls/` 下，私钥同样设为 `root:amberlog`、`0640`。

试用时如果没有自己 CA 签的证书，可以用 openssl 做一个测试 CA 和服务端证书。客户端连接时用到的名字都要写进 `subjectAltName`。这样做的证书不要用于生产：

```sh
cd /etc/amberlog/tls
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 30 \
  -subj "/CN=Amberlog test CA" -keyout ca.key -out ca.crt
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -subj "/CN=amberlog.example.com" -keyout tls.key -out tls.csr
printf 'subjectAltName=DNS:amberlog.example.com,DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' > ext.cnf
openssl x509 -req -in tls.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 30 \
  -extfile ext.cnf -out tls.crt
rm -f tls.csr ext.cnf ca.srl
chown root:amberlog tls.crt tls.key && chmod 0640 tls.key && chmod 0600 ca.key
```

之后客户端要信任 `ca.crt`，例如 `curl --cacert /etc/amberlog/tls/ca.crt https://localhost:8800/readyz`。只在一台机器上试用时，也可以不用 TLS，改为只听 `127.0.0.1`（第 8.1 节）。

### 4.5 systemd 单元

写 `/etc/systemd/system/amberlog.service`：

```ini
[Unit]
Description=Amberlog
Documentation=https://github.com/visdata-com/amberlog
After=network-online.target
Wants=network-online.target

[Service]
Type=exec
User=amberlog
Group=amberlog
ExecStart=/usr/local/bin/amberlog serve --config /etc/amberlog/amberlog.toml
# 退出码 2 是配置错误，重启也没用。
Restart=on-failure
RestartPreventExitStatus=2
RestartSec=5s
# 大于 server.shutdown_timeout（默认 10 秒），留出把已确认记录提交完的时间。
TimeoutStopSec=60s
LimitNOFILE=65536
# 按机器内存与第 9 节的估算设置。
MemoryMax=40G

[Install]
WantedBy=multi-user.target
```

各行的理由：

- `LimitNOFILE=65536`：systemd 的默认软上限是 1024。同时约 1000 个连接时，WAL 打不开新的段文件，写入回 503。
- `MemoryMax`：给进程设内存上限，免得拖垮整台机器。

这份单元文件经 `systemd-analyze verify` 校验过，没有以系统服务的方式实际启动过。

### 4.6 启动与检查

1. 启动：

   ```sh
   systemctl daemon-reload
   systemctl enable --now amberlog
   ```

2. 看启动日志。日志写到 stderr，每行一个 JSON 对象：

   ```sh
   journalctl -u amberlog -n 20
   ```

   要看到这几行（输出示例，节选，每行只留了部分字段；systemd 下的启动没有实测过）：

   ```text
   {"level":"info","msg":"starting amberlog","event":"starting","version":"0.1.0"}
   {"level":"info","msg":"open file limit is 65536","soft":65536,"hard":65536}
   {"level":"info","msg":"amberlog is ready","event":"ready","version":"0.1.0","http_addr":"0.0.0.0:8800","object_store":"s3://logs","mode":"serve","tls":true}
   ```

   - `open file limit is` 的值不低于 65536。
   - `ready` 一行的 `tls` 为 `true`（用进程内 TLS 时）。

3. 检查就绪。客户端要信任签发证书的 CA：

   ```sh
   curl --cacert /etc/amberlog/tls/ca.crt https://amberlog.example.com:8800/readyz
   ```

   ```text
   {"status":"ready"}
   ```

4. 写一条记录，再查出来。`$TOKEN` 是令牌，检索的时间以微秒计：

   ```sh
   now=$(($(date +%s) * 1000))
   curl --cacert /etc/amberlog/tls/ca.crt -X POST \
     "https://amberlog.example.com:8800/api/default/app/_json" \
     -H "Authorization: Bearer $TOKEN" \
     -d "[{\"_timestamp\": $now, \"level\": \"info\", \"message\": \"installed\"}]"
   ```

   ```text
   {"code":200,"status":[{"name":"app","successful":1,"failed":0,"errors":[]}]}
   ```

   ```sh
   curl --cacert /etc/amberlog/tls/ca.crt -X POST \
     "https://amberlog.example.com:8800/api/default/_search" \
     -H "Authorization: Bearer $TOKEN" \
     -d "{\"query\": {\"sql\": \"SELECT level, message FROM app\",
          \"start_time\": $(( (now - 3600000) * 1000 )), \"end_time\": $(( (now + 60000) * 1000 ))}}"
   ```

   结果里的 `hits` 有刚写入的记录。

启动失败时，按退出码查原因：

| 退出码 | 含义 |
|---|---|
| 2 | 配置错误。stderr 指出出错的配置键 |
| 3 | WAL 目录或对象存储不可用，含对象存储证书不被信任、凭据被拒 |
| 4 | 监听地址无法监听，例如端口已被占用 |
| 1 | 其他启动失败 |

### 4.7 停机与升级

- `systemctl stop amberlog` 发 SIGTERM。Amberlog 停止受理新请求，等在途请求完成，把已确认的记录提交到对象存储，然后以退出码 0 退出。
- 升级：读新版本的变更记录，停机，替换 `/usr/local/bin/amberlog`，再启动。数据格式与升级的规则见 [已知限制与路线图](known-limitations.zh-CN.md) 第 6.2 节，步骤见 [运维指南](operations.zh-CN.md) 第 5 节。

## 5. 用容器镜像

镜像在 GitHub 容器镜像仓库，有 `linux/amd64` 与 `linux/arm64` 两种：

```text
ghcr.io/visdata-com/amberlog:<版本>
```

镜像的要点：

- 入口是 `amberlog`，默认命令是 `serve`。
- 以非 root 用户运行，用户与组的编号都是 10001。
- 工作目录 `/var/lib/amberlog`，属于用户 10001。WAL 放在它下面，用卷挂载。
- 声明 8800 端口。
- 镜像本身不设任何 `AMBERLOG_` 环境变量。

步骤：

1. 在宿主机上写配置文件（第 4.4 节），`wal.dir` 写 `/var/lib/amberlog/wal`，证书路径写容器里的路径，例如 `/etc/amberlog/tls/tls.crt`。
2. 把配置目录的属主改成 10001。否则容器里的用户读不了 `0600` 的文件，启动时报 `cannot read config file: Permission denied`：

   ```sh
   chown -R 10001:10001 /etc/amberlog
   chmod 0600 /etc/amberlog/amberlog.toml /etc/amberlog/tls/tls.key
   ```

3. 启动容器：

   ```sh
   docker run -d --name amberlog \
     -p 8800:8800 \
     --ulimit nofile=65536:65536 \
     --stop-timeout 60 \
     -v /etc/amberlog:/etc/amberlog:ro \
     -v amberlog-wal:/var/lib/amberlog \
     ghcr.io/visdata-com/amberlog:0.1.0 \
     serve --config /etc/amberlog/amberlog.toml
   ```

   - `--ulimit nofile`：理由同第 4.5 节。
   - `--stop-timeout 60`：`docker stop` 默认只等 10 秒就强行结束进程。
   - 令牌在配置文件里，不用 `-e` 传，免得在 `docker inspect` 里看得到。

4. 按第 4.6 节检查日志、就绪与写入：`docker logs amberlog`。

实测：以 10001 用户运行，`open file limit is 65536`，`ready` 的 `tls` 为 `true`，检索正常；`docker stop -t 30` 后退出码 0。

在 Kubernetes 上，配置文件与证书放进 Secret 挂载，WAL 用持久卷，`terminationGracePeriodSeconds` 设为 60 或更大。同一份存储只能有一个写入实例，所以副本数是 1，更新策略用 `Recreate`。这一段没有在 Kubernetes 上实测。

## 6. 对象存储

### 6.1 要求

- 支持 MinIO、AWS S3、Ceph RGW 与本地磁盘（`file://`）。MinIO 与本地磁盘实测过；AWS S3 与 Ceph RGW 按 S3 兼容的写法支持，还没有实测。华为云 OBS、阿里云 OSS、腾讯云 COS 不支持。
- 存储要支持以请求头 `If-None-Match: *` 实现的「仅当不存在时创建」。运维确认之后，在配置里写 `conditional_put = "create_only"`（`s3://` 下 `serve` 与 `retire` 必填）。
- 启动时 Amberlog 实际探测一次：在 `<storage.prefix>/_system/` 下对同一个新键先后写两次。存储不支持时，以退出码 2 退出。
- 端点用 `https://`。用 `http://` 时照常工作，但启动时输出一条 `object_store_plaintext` 警告。
- 对象存储的证书由企业内部 CA 签发时，把 CA 证书写进 `object_store.s3.ca_file`。Amberlog 总是验证对象存储的证书，没有跳过验证的配置项。

### 6.2 Amberlog 自己的凭据

给 Amberlog 的凭据只授权它自己的前缀。下面的策略用于桶 `logs`、默认的 `storage.prefix`（`amberlog`）。`object_store.url` 写成 `s3://logs/<路径>` 时，把策略里的 `amberlog/` 换成 `<路径>/amberlog/`。

```json
{"Version": "2012-10-17", "Statement": [
  {"Effect": "Allow",
   "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject",
              "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
   "Resource": ["arn:aws:s3:::logs/amberlog/*"]},
  {"Effect": "Allow", "Action": ["s3:ListBucket"],
   "Resource": ["arn:aws:s3:::logs"],
   "Condition": {"StringLike": {"s3:prefix": ["amberlog/*"]}}},
  {"Effect": "Allow", "Action": ["s3:GetBucketLocation", "s3:ListBucketMultipartUploads"],
   "Resource": ["arn:aws:s3:::logs"]}
]}
```

实测（MinIO）：用这份策略的凭据，写入实例启动、写入、检索、入湖、按期删除都正常；往前缀之外写、列举桶的根都被拒。

### 6.3 MinIO

测试用的是 MinIO `RELEASE.2025-09-07T16-13-09Z`。MinIO 项目已把代码仓库归档，不再发布镜像与二进制，这个版本现在只能从源码构建。

本节与第 6.2 节的步骤，又在 Silo `RELEASE.2026-10-18T00-00-00Z`（镜像 `pgsty/silo`，PGSTY 维护的 MinIO 社区分支）上用同样的命令测过一次。这次覆盖了启动、写入、入湖、前缀之外的写入与列举桶根被拒绝、版本控制、`mc ilm rule add`，没有覆盖到期删除。[一键试用](../examples/quickstart/README.zh-CN.md) 用的也是 Silo。

下面的命令用 MinIO 的客户端 `mc`。`pgsty/silo` 镜像里带有它，也可以用镜像 `pgsty/mc`。先用 MinIO 管理员凭据把你的 MinIO 登记为别名 `myminio`：

```sh
mc alias set myminio https://minio.example.com:9000 <管理员用户> '<管理员口令>'
```

1. 建桶，开版本控制（理由见第 6.6 节）：

   ```sh
   mc mb myminio/logs
   mc version enable myminio/logs
   ```

2. 建用户，把第 6.2 节的策略存为 `amberlog-writer.json`，授给这个用户：

   ```sh
   mc admin user add myminio amberlog-writer '<访问密钥>'
   mc admin policy create myminio amberlog-writer amberlog-writer.json
   mc admin policy attach myminio amberlog-writer --user amberlog-writer
   ```

3. 配置：

   ```toml
   [object_store]
   url = "s3://logs"

   [object_store.s3]
   endpoint = "https://minio.example.com:9000"
   force_path_style = true
   conditional_put = "create_only"
   access_key_id = "amberlog-writer"
   secret_access_key = "<访问密钥>"
   ```

### 6.4 AWS S3

1. 建桶，开版本控制。
2. 建一个 IAM 用户或角色，授予第 6.2 节的策略。
3. 配置。不写 `endpoint`，`region` 写桶所在的区域，`force_path_style` 保持默认的 `false`：

   ```toml
   [object_store]
   url = "s3://logs"

   [object_store.s3]
   region = "<桶所在的区域>"
   conditional_put = "create_only"
   ```

4. 凭据：在 EC2 等环境里，不写 `access_key_id` 与 `secret_access_key`，Amberlog 从 AWS 凭据环境变量与凭据链取得凭据（[配置项参考](configuration.zh-CN.md) 第 7 节）。

这一节没有在 AWS S3 上实测。

### 6.5 Ceph RGW

Ceph RGW 按 S3 兼容的写法支持，还没有实测。启动时的条件写探测（第 6.1 节）会在你自己的环境里实际核对存储是否支持 `If-None-Match: *`。

1. 建一个 RGW 用户与桶，开版本控制。
2. 用桶策略限制这个用户：把第 6.2 节的策略加上 `"Principal": {"AWS": ["arn:aws:iam:::user/<用户名>"]}`，以桶策略的形式设到桶上。
3. 配置同 MinIO（第 6.3 节），`endpoint` 写 RGW 的地址，`force_path_style = true`。

这一节没有在 Ceph RGW 上实测。

### 6.6 版本控制与生命周期规则

- 建议对桶开启版本控制。有人绕过 Amberlog 删改桶里的对象时，旧版本还在，可以找回；Amberlog 发现外部删改的时点有限（[已知限制与路线图](known-limitations.zh-CN.md) 第 6.1 节）。
- 实测（MinIO）：开了版本控制，Amberlog 照常启动、写入、删除。删除只留下删除标记，旧版本留在桶里。测试中当前对象 18 个，全部版本 43 个。
- 所以要配一条生命周期规则，按期清理非当前版本，例如 30 天。期限要长于你需要找回数据的时间。MinIO 的写法是 `mc ilm rule add --noncurrent-expire-days 30 myminio/logs`；AWS S3 与 Ceph 用各自的生命周期配置。这条 `mc` 命令在测试桶上执行成功，规则到期清理的效果没有等到。
- 对象存储的写权限只给 Amberlog（第 6.2 节）。

### 6.7 服务端加密

- `object_store.s3.server_side_encryption = "sse_s3"`：每个写入请求带 `x-amz-server-side-encryption: AES256`。
- `"sse_kms"`：带 `aws:kms`；另配 `sse_kms_key_id` 时指定密钥，不配时用存储方的默认密钥。
- 存储拒绝带加密头的写入时（例如密钥服务不可用），`serve` 启动时以退出码 3 退出。
- MinIO 与 Ceph 默认只在 TLS 连接上接受带加密头的请求，所以端点要用 `https://`。
- 用 SSE-KMS 时，外部引擎与审计用的只读凭据也要有该密钥的解密权限。
- 不支持 SSE-C。

本文的测试没有开服务端加密。

### 6.8 本地磁盘

- `object_store.url = "file:///var/lib/amberlog/data"`，必须是绝对路径。目录要事先建好，否则以退出码 3 退出。
- 只适合功能试用。存储目录在运行中被移走或所在的卷被卸载时，`/readyz` 不报未就绪（[运维指南](operations.zh-CN.md) 第 8 节）。

## 7. 外部引擎的只读凭据

外部引擎直接读对象存储里的冷层表，不经过 Amberlog。外部引擎必须用只读凭据：Amberlog 的目录端点不提供建表与删表，但挡不住引擎先把数据文件写进桶，只有只读凭据挡得住。这一步要你自己在对象存储上配置。各引擎怎样用这份凭据，见 [外部引擎直读指南](external-engines.zh-CN.md)。

### 7.1 授权范围

只读凭据只授权冷层表所在的前缀 `<storage.prefix>/_iceberg/`，不要授权热层：

```json
{"Version": "2012-10-17", "Statement": [
  {"Effect": "Allow", "Action": ["s3:GetObject"],
   "Resource": ["arn:aws:s3:::logs/amberlog/_iceberg",
                "arn:aws:s3:::logs/amberlog/_iceberg/*"]},
  {"Effect": "Allow", "Action": ["s3:ListBucket"],
   "Resource": ["arn:aws:s3:::logs"],
   "Condition": {"StringLike": {"s3:prefix": ["amberlog/_iceberg", "amberlog/_iceberg/", "amberlog/_iceberg/*"]}}},
  {"Effect": "Allow", "Action": ["s3:GetBucketLocation"],
   "Resource": ["arn:aws:s3:::logs"]}
]}
```

- 前缀键 `amberlog/_iceberg` 本身（不带尾斜杠）也要能读与列举。有的引擎的 hadoop 型目录要对它发请求，没有这一项时列不出命名空间。前缀键不是对象，授权它不多给任何内容。
- 用 SSE-KMS 时，另加该密钥的解密权限。

在 MinIO 上，把上面的策略存为 `lake-reader.json`，再建用户、授策略（`myminio` 是第 6.3 节登记的别名）：

```sh
mc admin user add myminio lake-reader '<访问密钥>'
mc admin policy create myminio lake-reader lake-reader.json
mc admin policy attach myminio lake-reader --user lake-reader
```

实测（MinIO）：用这份策略的凭据能列举冷层表的文件；列举热层的前缀、往冷层前缀写对象都被拒。

### 7.2 只读目录模式

交给外部引擎的令牌，在写入实例上同样能写入、能检索。要缩小权限，单独起一个只读目录模式的进程，给它另一个令牌与第 7.1 节的只读凭据：

1. 写 `/etc/amberlog/catalog.toml`：

   ```toml
   [server]
   http_addr = "0.0.0.0:8801"

   [server.tls]
   cert_file = "/etc/amberlog/tls/tls.crt"
   key_file = "/etc/amberlog/tls/tls.key"

   [auth]
   token = "<另生成的令牌>"

   [object_store]
   url = "s3://logs"

   [object_store.s3]
   endpoint = "https://minio.example.com:9000"
   force_path_style = true
   access_key_id = "lake-reader"
   secret_access_key = "<只读凭据的访问密钥>"
   ```

   `storage.prefix` 要与写入实例相同。不需要 `wal.dir` 与 `conditional_put`。

2. 启动：

   ```sh
   amberlog catalog --config /etc/amberlog/catalog.toml
   ```

   `ready` 一行的 `mode` 为 `catalog`。

3. 列出表：

   ```sh
   curl --cacert /etc/amberlog/tls/ca.crt \
     -H "Authorization: Bearer $CATALOG_TOKEN" \
     https://amberlog.example.com:8801/iceberg/v1/amberlog/namespaces/default/tables
   ```

   ```text
   {"identifiers":[{"namespace":["default"],"name":"app"}]}
   ```

实测：只读目录模式用只读凭据启动并列出表；用写入实例的令牌访问它回 401；向它发检索请求回 404。它可以与写入实例同时运行。

用 systemd 管理时，照第 4.5 节另写一个单元，`ExecStart` 换成 `amberlog catalog --config /etc/amberlog/catalog.toml`。

## 8. TLS 与安全部署

### 8.1 选一种监听方式

| 方式 | 配置 | 什么时候用 |
|---|---|---|
| 只监听本机 | `server.http_addr = "127.0.0.1:8800"` | 只在本机试用；或反向代理与 Amberlog 在同一台机器上 |
| 进程内 TLS | 配 `server.tls.cert_file` 与 `server.tls.key_file` | 客户端直接连 Amberlog |
| 反向代理终止 TLS | 不配证书，`server.behind_tls_proxy = true` | 前面已有反向代理或网关 |

- 默认监听地址 `0.0.0.0:8800` 不是回环地址。不配证书、也不写明文声明时，`serve` 与 `catalog` 以退出码 2 拒绝启动。
- 证书与明文声明同时配置，也以退出码 2 退出。
- 用明文声明时，启动日志有一条 `plaintext_listen` 警告。Amberlog 不检查前面是否真有反向代理。

### 8.2 进程内 TLS

- 只接受 TLS 1.2 与 TLS 1.3。配了证书以后，以明文 HTTP 发来的请求得不到任何 HTTP 响应。
- `/healthz`、`/readyz`、`/metrics` 也只经 TLS 作答。健康探针与 Prometheus 都要改用 `https://` 并信任签发证书的 CA；`/metrics` 要令牌，Prometheus 用 `authorization.credentials_file` 读令牌文件（[运维指南](operations.zh-CN.md) 第 3.1 节）。
- 证书文件是 PEM，叶证书在前，后面可以接中间证书。私钥不能有口令。接受的私钥算法见 [配置项参考](configuration.zh-CN.md) 的「入站 TLS」。
- 运行中替换证书与私钥不需要重启：进程至少每 60 秒读一次两个文件，最迟 120 秒后新连接用新证书。新内容无效时继续用旧证书，并输出 `tls_reload_failed`。替换时先写好两个新文件，再一起换名覆盖，免得读到一新一旧。
- 证书到期没有指标。用外部的探测监控到期时间。

### 8.3 反向代理

直接面向更大的网络时，前面放反向代理。现在的版本里，连接时限、连接数、请求速率与安全响应头都要由它负责（[已知限制与路线图](known-limitations.zh-CN.md) 第 2.2 节）。

Amberlog 与代理在同一台机器上时，Amberlog 只监听回环地址，不需要明文声明。不在同一台机器上时，Amberlog 监听内网地址并写明文声明；这一段仍是明文，客户端也不能绕过代理直连 Amberlog。

nginx 的例子（Amberlog 的 `server.http_addr` 设为 `127.0.0.1:8802`）：

```nginx
limit_req_zone  $binary_remote_addr zone=amberlog_req:10m rate=50r/s;
limit_conn_zone $binary_remote_addr zone=amberlog_conn:10m;

server {
    listen 443 ssl;
    server_name amberlog.example.com;

    ssl_certificate     /etc/nginx/tls/tls.crt;
    ssl_certificate_key /etc/nginx/tls/tls.key;
    ssl_protocols       TLSv1.2 TLSv1.3;

    # 与 ingest.max_body_bytes（默认 32 MiB）相同或更小。
    client_max_body_size  32m;
    # 请求头、请求体的读取时限与空闲连接的时限。
    client_header_timeout 30s;
    client_body_timeout   60s;
    keepalive_timeout     75s;
    # 每个来源地址的连接数与请求速率。按自己的采集器数量调整。
    limit_conn amberlog_conn 100;
    limit_req  zone=amberlog_req burst=200 nodelay;

    add_header X-Content-Type-Options nosniff always;
    add_header X-Frame-Options DENY always;
    add_header Referrer-Policy no-referrer always;
    add_header Strict-Transport-Security "max-age=31536000" always;

    location / {
        proxy_pass http://127.0.0.1:8802;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        # 时间范围大的检索可能要几分钟。
        proxy_read_timeout 300s;
    }
}
```

- `Authorization` 请求头要原样转发。nginx 默认就转发。
- 访问日志里不要记 `Authorization` 请求头。nginx 默认的日志格式不记。
- 速率与连接数的取值只是例子。采集器多、经同一个出口地址发来时，要放宽。

实测（nginx 1.31.2，监听 8443）：经代理就绪、写入、检索与 `/ui` 都正常；四个安全响应头都在应答里；不带令牌回 401；34 MB 的请求体由代理回 413；访问日志里没有 `Authorization`。

### 8.4 要设告警的日志事件

下面几条是稳定的日志事件，事件名在 JSON 日志的 `event` 字段里，不受 `log.level` 过滤。出现时说明部署偏离了安全的做法：

| 事件 | 含义 |
|---|---|
| `auth_disabled` | 关闭了认证（`auth.enabled = false`），任何人都能访问。只用于本机开发 |
| `plaintext_listen` | 在非回环地址上以明文监听 |
| `object_store_plaintext` | 对象存储端点是 `http://` |
| `tls_reload_failed` | 替换后的证书或私钥无效，仍在用旧证书 |

其余的告警见 [运维指南](operations.zh-CN.md) 第 3 节。

### 8.5 其他建议

- 不支持双向 TLS。需要客户端证书认证时，由反向代理校验。
- 不支持国密传输协议。需要时，由网络边界上的国密网关处理；网关到 Amberlog 之间用普通 TLS 或明文声明。
- 采集器、Prometheus 与外部引擎的令牌，都写在各自的配置文件里，不写进命令行参数。
- 检索界面 `/ui` 打开时不需要令牌，但页面本身不含数据，检索时要填令牌。

## 9. 容量规划

### 9.1 内存

进程的内存按下面几项相加来估，再留余量：

| 项 | 怎么估 |
|---|---|
| 摄取积压 | 不超过 `ingest.max_backlog_memory_bytes`，默认 256 MiB |
| 检索 | `query.max_concurrency`（默认 4）× 单个查询的峰值。实测一个 160 小时范围的全文检索约 1 GB，默认配置约留 4 GB |
| 封口合并 | 没有单独实测。参考：每秒 2 万条、4 个数据集的实测中，整个进程的匿名常驻内存最高约 740 MiB，出现在第一个小时桶封口时（[已知限制与路线图](known-limitations.zh-CN.md) 第 4.3 节） |
| 入湖 | 单个数据集一个小时原始数据的约 2.4 倍 |
| 冷层合并 | 单个数据集一天原始数据的约 2.4 倍（按入湖的倍数推算）。这一项最大 |

进程内存上限为 40 GB 时，单个数据集每天的原始数据约 15 GB 是上限（推算）。可用的内存越少，上限越低，约为「冷层合并可用的内存 ÷ 2.4」。

- 「原始数据」指采集器发来的 JSON 记录的字节数。
- 热层巡检的缓存随运行时长增长，要另留余量，并按天监控（[已知限制与路线图](known-limitations.zh-CN.md) 第 4.3 节）。
- 单个来源的数据量超过上限时，拆成几个数据集写入。上限是按单个数据集算的。

以上实测的数，都是在 0.1.0 发版之前的开发版本上、一台 16 核 46 GB 内存的虚拟机上各测一次得到的，对象存储是同一台机器上的单盘 MinIO。它们不是容量承诺。上线前在自己的机器、对象存储与日志上实测一次。

### 9.2 吞吐

实测能持续的最高摄取速率：4 个数据集同时写入约每秒 13 万条，只写 1 个数据集约每秒 8 万条（每条 500 字节）。这是实测值，不是承诺。测试环境与上一节相同，详见 [已知限制与路线图](known-limitations.zh-CN.md) 第 4.1 节。

### 9.3 对象存储容量

除了数据本身，还要算上热层的清单历史与冷层的元数据。清单历史在 `hot_retention.age`（默认 168 小时）之内不回收，见 [已知限制与路线图](known-limitations.zh-CN.md) 第 4.4、4.5 节。先按自己的写入方式写几个小时，量一下各数据集占用的字节数。

小时桶封口以后，Amberlog 把桶里大小相近的小文件合并。被合并掉的文件要再等 `storage.delete_delay`（默认 1 小时）才物理删除，所以封口后的一段时间里，热层数据文件占的空间会暂时变大。在 [已知限制与路线图](known-limitations.zh-CN.md) 第 4.3、4.4 节所写的大写入量实测中，热层数据文件的占用在运行中最高约为结束时的 1.65 倍（每 5 分钟读一次；把两次读数之间写出的量全部加上估算，也不超过 2 倍）。

### 9.4 文件描述符

把进程的文件描述符上限设为 65536 或更高（systemd 的 `LimitNOFILE`，容器的 `--ulimit nofile`）。Amberlog 启动时把软上限调到硬上限，并输出 `open file limit is <N>`；低于 65536 时另有一行警告。

## 10. 上线前检查

- [ ] 令牌是随机生成的长值，只写在权限为 `0640` 或 `0600` 的配置文件里。
- [ ] 用进程内 TLS，或放在终止 TLS 的反向代理后面；启动日志里没有意外的 `plaintext_listen`。
- [ ] 直接面向更大的网络时，反向代理负责连接时限、速率限制与安全响应头。
- [ ] 对象存储端点用 `https://`；日志里没有 `object_store_plaintext`。
- [ ] Amberlog 的凭据只授权它自己的前缀；桶开了版本控制，并配了清理非当前版本的生命周期规则。
- [ ] 外部引擎用只读凭据，只授权 `_iceberg/` 前缀；需要时另起只读目录模式。
- [ ] WAL 在可靠的本地磁盘上。
- [ ] `open file limit is` 不低于 65536。
- [ ] 内存按第 9 节估过，设了进程内存上限，并按天监控常驻内存。
- [ ] 按 [运维指南](operations.zh-CN.md) 第 3 节设好告警，包括第 8.4 节的四个事件。

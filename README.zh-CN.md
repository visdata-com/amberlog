# Amberlog

[English](README.md)

**Amberlog——数据主权优先的长期数据保存平台。** 数据以开放格式长期封存在你自己的对象存储上，低成本、审计完整性可证——供应商消失、产品退役，留存数据仍完整、可读、可审计。当前第一负载为企业日志（以开放表格式 Apache Iceberg 存放，任何标准引擎可直读）。

用 Rust 实现，按 Apache License 2.0 开源。

## 状态

这是 Amberlog 的第一个公开版本，处在早期阶段。部署之前，请先读 [已知限制与路线图](docs/known-limitations.zh-CN.md)，按自己的场景确定使用范围。

## 数据主权

- **格式**：长期留存的数据是标准的 Apache Iceberg 表，任何支持 Iceberg 的引擎都能直接读。DuckDB、Apache Spark 等引擎按桶内约定直接找到表，不需要 Amberlog 在运行；Trino 要经 Amberlog 的只读目录进程找表，这个进程只读对象存储，可以和写入实例分开运行（见[外部引擎直读指南](docs/external-engines.zh-CN.md)）。
- **存储**：数据在你自己的对象存储桶里（MinIO 上实测过；AWS S3 与 Ceph RGW 按 S3 兼容的写法支持，还没有实测）。对象存储是唯一的长期存放位置；本机只保留写入确认到提交之间的一份短暂缓冲（预写日志）。
- **代码**：Apache-2.0，可以审计、修改、自主维护。

开放直读的对象是留存数据，即已转入 Iceberg 表的数据。最近写入的数据先在 Amberlog 自己的热层里（默认在所在小时结束 72 小时后转入冷层），外部引擎读不到；不再使用 Amberlog 时，优雅退役把它们也全部转为 Iceberg 表。

## 设计：热冷双层

日志写入频繁、批次小，又要求写入后很快能查到；Iceberg 的提交面向批处理，高频的小提交会让元数据膨胀、提交互相冲突。Amberlog 用两层解决这对矛盾：

- **热层**用自己的轻量清单承接高频写入。在吞吐上限以内，写入确认后 10 秒内可以检索到；积压超过内存上限时，只留在 WAL 里的溢出记录要等读回内存才能查到（见[已知限制与路线图](docs/known-limitations.zh-CN.md) §4.1）。
- **冷层**按小时成批转入 Iceberg 表，一次重写同时完成文件合并与 Iceberg 提交。

冷层的 Iceberg 表本身就是数据的正本，不是私有存储之外的导出副本：不存两份，没有导出延迟带来的不一致，审计证据链因此完整。

## 快速上手

### 一键试用

需要 Docker 与 Compose 插件：

```sh
git clone https://github.com/visdata-com/amberlog.git
cd amberlog/examples/quickstart
docker compose up -d --wait
./demo.sh
```

脚本写入几条记录，用 Amberlog 检索，再用 DuckDB 直接读对象存储里的 Iceberg 表。说明见 [一键试用](examples/quickstart/README.zh-CN.md)。

### 单个二进制

从 [Releases](https://github.com/visdata-com/amberlog/releases) 下载对应平台的安装包（Linux x86_64 或 aarch64，要求 glibc 2.28 及以上），解包后用本地磁盘试一下。下面的命令以 x86_64 安装包为例，还要用到 `openssl`（生成令牌）和 `curl`：

```sh
tar -xzf amberlog-0.1.0-x86_64-unknown-linux-gnu.tar.gz
cd amberlog-0.1.0-x86_64-unknown-linux-gnu
TOKEN=$(openssl rand -hex 32)
echo "$TOKEN"    # 不能为空；第二个终端还要用它
mkdir -p /tmp/amberlog/store
AMBERLOG_OBJECT_STORE__URL=file:///tmp/amberlog/store \
AMBERLOG_WAL__DIR=/tmp/amberlog/wal \
AMBERLOG_AUTH__TOKEN="$TOKEN" \
AMBERLOG_SERVER__HTTP_ADDR=127.0.0.1:8800 \
  ./amberlog serve
```

在另一个终端里写入与检索。先在那里把 `TOKEN` 设成同一个值：

```sh
TOKEN=<第一个终端打印的令牌>
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

`start_time` 与 `end_time` 是 UTC 微秒，左闭右开。浏览器打开 `http://127.0.0.1:8800/ui`，填入令牌，也可以检索。

正式部署（对象存储、TLS、systemd、容器）见 [安装与部署指南](docs/install.zh-CN.md)。

## 文档

| 文档 | 内容 |
|---|---|
| [安装与部署指南](docs/install.zh-CN.md) | 二进制与容器安装、对象存储、外部引擎的只读凭据、TLS、容量规划 |
| [配置项参考](docs/configuration.zh-CN.md) | 全部配置项、环境变量、启动检查与退出码 |
| [API 参考](docs/api.zh-CN.md) | 写入、检索、系统表、错误、运维端点、Iceberg 只读目录 |
| [检索界面与告警](docs/ui-and-alerting.zh-CN.md) | 检索、台账、对账三屏，定时告警 |
| [运维指南](docs/operations.zh-CN.md) | 健康检查、指标与告警、停机与升级、审计证据、优雅退役 |
| [外部引擎直读指南](docs/external-engines.zh-CN.md) | 用 DuckDB、Spark、Trino、Apache Flink、Apache Doris、ClickHouse、StarRocks 读冷层表 |
| [已知限制与路线图](docs/known-limitations.zh-CN.md) | 这个版本还做不到的事，以及以后的方向 |
| [采集器接入示例](examples/collectors/README.zh-CN.md) | Vector 与 Fluent Bit 的配置 |
| [变更记录](CHANGELOG.md) | 每个版本的变化 |

## 参与

- 问题与建议：见 [获取帮助](SUPPORT.md)。
- 安全问题：请按 [安全策略](SECURITY.md) 私下报告，不要开公开 issue。
- 贡献代码：见 [贡献指南](CONTRIBUTING.md) 与 [行为准则](CODE_OF_CONDUCT.md)。

## 许可与致谢

Amberlog 按 [Apache License 2.0](LICENSE) 发布。它构建在许多开源项目之上，见 [致谢与商标声明](ACKNOWLEDGEMENTS.md)。

Apache、Apache Iceberg、Apache Spark、Apache Flink、Apache Doris 是 Apache 软件基金会在美国和其他国家的注册商标或商标。其他名称是其各自所有者的商标。Amberlog 与 Apache 软件基金会及这些所有者没有隶属关系，也未获其认可。

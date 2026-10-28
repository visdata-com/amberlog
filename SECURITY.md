# Security Policy / 安全策略

[中文](#中文)

## Reporting a vulnerability

**Do not report security vulnerabilities in public issues, Discussions or pull requests.**

Use GitHub private vulnerability reporting:

1. Open the [Security](https://github.com/visdata-com/amberlog/security) tab of this repository.
2. Click **Report a vulnerability**, or open <https://github.com/visdata-com/amberlog/security/advisories/new> directly.
3. Fill in the report. Only you and the maintainers can see it.

This is the only reporting channel. Reports in Chinese or English are both welcome.

Please include:

- The affected version (full output of `amberlog --version`).
- The type and impact of the vulnerability: what an attacker needs, and what they can do.
- Steps to reproduce: the configuration (tokens and keys removed), requests and commands.
- Whether you want to be credited in the security advisory.

## How we handle reports

Maintainers on duty handle reports.

| Stage | Time limit |
|---|---|
| Acknowledge receipt | Within 5 working days |
| Initial assessment: confirmed or not, severity | Within 14 days of the report |
| Public disclosure | By default within 90 days of the report; earlier or later by mutual agreement |

Process:

1. The maintainers confirm the issue, assess its severity and talk with you in the private report.
2. The maintainers prepare a fix in private.
3. When the fix is released, the maintainers publish a GitHub security advisory in this repository, and request a CVE when needed.
4. The advisory states the affected versions, the fixed version and any mitigation. Reporters who want credit are credited.

Please do not publish details of the vulnerability before the public disclosure.

## Supported versions

Amberlog is at the 0.x stage. Security fixes are released only for the latest minor series. For example, when the latest release is 0.3.2, the fix is released as 0.3.3, and no fix is released for 0.2.x.

| Version | Receives security fixes |
|---|---|
| Latest 0.y series | Yes |
| Earlier series | No, please upgrade |

## Deployment responsibilities

The following are treated as deployment configuration problems, not as vulnerabilities:

- The deployment turns authentication off, or leaks the access token to untrusted parties.
- The deployment declares a TLS reverse proxy with `server.behind_tls_proxy`, but actually serves plaintext HTTP on an untrusted network. Without that declaration, Amberlog refuses to start in plaintext on a non-loopback address; with it, Amberlog logs the warning event `plaintext_listen` at startup.
- The deployment connects to object storage over plaintext HTTP. Amberlog logs the warning event `object_store_plaintext` at startup.
- The deployment points Amberlog at an untrusted object storage endpoint.

If you think a default setting itself is insecure, report it as a vulnerability.

Protections that this version still lacks (minimum token length, connection time limits, security response headers, limits on failed authentication attempts) are listed in section 2.2 of [Known limitations and roadmap](docs/known-limitations.md). You do not need to report them.

## Known and assessed issues

The maintainers scan dependencies for advisories with cargo-deny (configuration in `deny.toml`) on every change in the development repository and before every release. The issues below are assessed, cannot be fixed yet, and are disclosed here.

### quick-xml: RUSTSEC-2026-0194, RUSTSEC-2026-0195

- **What.** Two denial-of-service vulnerabilities, CVSS 7.5, in the indirect dependency quick-xml 0.39.4. Amberlog pulls it in through object_store 0.13.2.
- **Exposure.** object_store uses quick-xml to parse the XML responses of the object storage service. To trigger the vulnerabilities, an attacker must control what the object storage endpoint returns: the endpoint itself is compromised, or responses are tampered with on a plaintext HTTP connection. Requests that clients send to Amberlog do not go through quick-xml.
- **Mitigation.**
  - Point Amberlog only at trusted object storage endpoints.
  - Connect to object storage over HTTPS.
- **When it will be fixed.** The fix needs a newer object_store, and the object_store version must match the one Apache DataFusion depends on. When DataFusion releases a version that depends on object_store 0.14.1 or later, we upgrade them together and remove this section.

### zstd: RUSTSEC-2026-0391, RUSTSEC-2026-0398, RUSTSEC-2026-0403

- **What.** Three advisories in zstd 0.13.3 and zstd-safe 7.3.0: data from an abandoned stream can leak into the next stream on a reused context (0391); `Seekable::decompress` can set a buffer length beyond its capacity (0398); a failed streaming decompression of a legacy frame can expose uninitialized memory (0403). Amberlog pulls this zstd line in indirectly, through Apache Arrow and Parquet 59, Apache DataFusion 55 and apache-avro.
- **Exposure.** The vulnerable functions are not called anywhere in Amberlog's dependency graph: the crates on this line use only the bulk API, the streaming read decoder, `decode_all` and the encoder constructor. None of them calls a `with_context` constructor or uses the streaming write decoder, and the `seekable` feature is off. Amberlog's own decompression of request bodies uses zstd 0.14.3, which contains the fixes.
- **When it will be fixed.** The 0.13 line has no fixed release. Arrow and Parquet 60 already use zstd 0.14, and their version must match the one DataFusion depends on. When DataFusion releases a version that depends on Arrow and Parquet 60 or later, and apache-avro moves to zstd 0.14.3 or later, we upgrade them together and remove this section.

## Trademarks

Apache and Apache DataFusion are registered trademarks or trademarks of the Apache Software Foundation in the United States and other countries. Amberlog is not affiliated with or endorsed by the Apache Software Foundation.

## 中文

### 怎样报告漏洞

**不要在公开的 issue、Discussions 或 PR 里报告安全漏洞。**

请用 GitHub 的私密漏洞报告：

1. 打开本仓库的 [Security](https://github.com/visdata-com/amberlog/security) 页面。
2. 点 **Report a vulnerability**，或直接打开 <https://github.com/visdata-com/amberlog/security/advisories/new>。
3. 填写报告。报告只有你和维护者能看到。

这是唯一的报告渠道。中文、英文都可以。

报告里请写明：

- 受影响的版本（`amberlog --version` 的完整输出）。
- 漏洞的类型和影响：攻击者需要什么前提，能做到什么。
- 复现步骤：用到的配置（删掉令牌和密钥）、请求和命令。
- 你是否希望在安全公告里署名。

### 我们怎样处理

报告由维护者值守处理。

| 阶段 | 时限 |
|---|---|
| 确认收到 | 5 个工作日内 |
| 初步评估：是否确认为漏洞、严重程度 | 收到报告后 14 天内 |
| 公开披露 | 默认在收到报告后 90 天内；双方可以协商提前或延后 |

处理过程：

1. 维护者确认问题，评估严重程度，在私密报告里和你沟通。
2. 维护者在私下准备修复。
3. 修复发布时，维护者在本仓库发布 GitHub 安全公告，需要时申请 CVE 编号。
4. 安全公告写明受影响的版本、修复的版本和缓解办法。你愿意署名的，公告里致谢。

请在公开披露之前不要公开漏洞细节。

### 支持的版本

Amberlog 处于 0.x 阶段。安全修复只发布在最新的次版本系列上。例如最新的版本是 0.3.2 时，修复发布为 0.3.3，不为 0.2.x 发布修复。

| 版本 | 是否接收安全修复 |
|---|---|
| 最新的 0.y 系列 | 是 |
| 更早的系列 | 否，请升级 |

### 部署方的责任

下面这些问题，按部署方的配置问题处理，不按漏洞处理：

- 部署方关闭了认证，或者把访问令牌泄露给了不可信的人。
- 部署方用 `server.behind_tls_proxy` 声明前面有 TLS 反向代理，实际却在不可信的网络上以明文 HTTP 提供服务。没有这项声明时，Amberlog 拒绝在非回环地址上以明文启动；有这项声明时，启动时输出告警事件 `plaintext_listen`。
- 部署方用明文 HTTP 连接对象存储。Amberlog 启动时输出告警事件 `object_store_plaintext`。
- 部署方把 Amberlog 指向不可信的对象存储端点。

如果你认为某项默认配置本身不安全，请按漏洞报告。

这个版本还缺的防护（令牌最小长度、连接时限、安全响应头、认证失败次数限制）列在[已知限制与路线图](docs/known-limitations.zh-CN.md)第 2.2 节，不必再报告。

### 已知并已评估的问题

维护者用 cargo-deny（配置在 `deny.toml`）扫描依赖漏洞：开发仓库每次改动时扫一次，每次发版前再扫一次。下面的问题已经评估，暂时不能修复，在这里公开说明。

#### quick-xml：RUSTSEC-2026-0194、RUSTSEC-2026-0195

- **是什么。** 两个拒绝服务漏洞，CVSS 7.5。它们在间接依赖 quick-xml 0.39.4 里。Amberlog 经由 object_store 0.13.2 引入它。
- **影响范围。** object_store 用 quick-xml 解析对象存储服务返回的 XML 应答。要触发漏洞，攻击者必须能控制对象存储端点返回的内容：端点本身被攻破，或者在明文 HTTP 连接上篡改应答。客户端发给 Amberlog 的请求不经过 quick-xml。
- **缓解办法。**
  - 只把 Amberlog 指向可信的对象存储端点。
  - 用 HTTPS 连接对象存储。
- **什么时候修。** 修复需要升级 object_store，而 object_store 的版本要和 Apache DataFusion 所依赖的版本一致。DataFusion 发布依赖 object_store 0.14.1 或更高版本的版本以后，我们一起升级并删掉本节。

#### zstd：RUSTSEC-2026-0391、RUSTSEC-2026-0398、RUSTSEC-2026-0403

- **是什么。** zstd 0.13.3 与 zstd-safe 7.3.0 里的三条漏洞通告：复用上下文时，上一段中途放弃的流的数据会带进下一段流（0391）；`Seekable::decompress` 可能把缓冲区长度设得超过容量（0398）；流式解压 legacy 帧失败时可能暴露未初始化的内存（0403）。Amberlog 经由 Apache Arrow 与 Parquet 59、Apache DataFusion 55 和 apache-avro 间接引入这一版 zstd。
- **影响范围。** Amberlog 的依赖图里没有调用这些有问题的函数：这一版 zstd 的使用者只用整块压缩接口、流式读解码器、`decode_all` 和编码器的构造函数，没有调用 `with_context` 构造函数，也没有用流式写解码器，`seekable` 特性也没有打开。Amberlog 自己解压请求体用的是已修复的 zstd 0.14.3。
- **什么时候修。** 0.13 这一版没有修复版本。Arrow 与 Parquet 60 已经改用 zstd 0.14，但它们的版本要和 DataFusion 所依赖的版本一致。DataFusion 发布依赖 Arrow 与 Parquet 60 或更高版本的版本、并且 apache-avro 换到 zstd 0.14.3 或更高版本以后，我们一起升级并删掉本节。

### 商标

Apache、Apache DataFusion 是 Apache 软件基金会在美国和其他国家的注册商标或商标。Amberlog 与 Apache 软件基金会没有隶属关系，也未获其认可。

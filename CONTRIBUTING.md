# Contributing / 贡献指南

[中文](#中文)

Thank you for helping with Amberlog. This project's process differs from most open-source projects in a few ways. Please read this guide before you start.

## 1. This repository is a snapshot

Amberlog is developed in an internal development repository. This repository is a snapshot of it:

- The maintainers regularly sync the publishable files of the development repository into this repository. Each sync creates one commit, titled `Snapshot <date>` or `Release v<version>`.
- Individual commits of the development repository do not appear in this repository's history.
- The `main` branch of this repository is updated only by syncs. **Pull requests are not merged in this repository.** Section 7 describes what happens instead.

## 2. Before you start

- **Bug fixes, documentation and internal changes**: open a pull request directly. For a bug fix, state the current behavior, the expected behavior, and the basis for the expected behavior.
- **Changes to externally observable behavior**: open an issue with the "Feature request" template first, and write code only after the maintainers agree. Externally observable behavior includes HTTP endpoints, request and response fields, error codes, log fields, metric names, configuration keys, command-line options, exit codes, and paths and file formats in object storage.

Every external behavior of Amberlog is specified in the project's internal requirement definitions, which are not public yet. The maintainers check your proposal against them: where a rule exists, it applies; where none exists, the rule is written first. A pull request that changes external behavior without that agreement is closed, with the reason stated.

## 3. Internal identifiers in the code

Code comments contain the following identifiers and paths:

| Form | Refers to |
|---|---|
| `REQ-AMB-…` | A requirement block: the rule for one external behavior |
| Numbers like `ADR-0001` | An architecture decision record |
| Paths under `knowledge/` | The directory of requirement blocks and architecture decisions |
| `块` or `块场景` on its own in Chinese comments | A requirement block, or one scenario in it |
| Upper-case hyphenated labels such as `TERM-NEEDED` or `MOVED` | One clause of the requirement block that the comment is about |

They refer to files in the internal development repository. These files are not in this repository yet and will be published over time. Until then:

- You do not need to understand these identifiers to change the code.
- Please do not remove or change existing identifiers. The maintainers use them to trace which rule a piece of code implements.
- New code does not need identifiers. The maintainers add them on import where needed.

## 4. Development environment

The full test suite runs only on Linux x86_64: the search UI tests use the linux64 headless browser, and some tests read `/proc`. On other platforms you can build the code and run clippy.

You need:

- Rust, installed with [rustup](https://rustup.rs/). In the repository directory, `rustup toolchain install` installs the pinned toolchain and components from `rust-toolchain.toml`.
- A C compiler and `libclang-dev`, needed to build the `pg_query` dependency. On Debian or Ubuntu: `sudo apt-get install build-essential libclang-dev`.
- Docker and Docker Compose (v2.1 or later, for `docker compose up --wait`). The object-storage test bench (Silo, a community fork of MinIO) runs in Docker.
- `openssl` and `curl` (7.75 or later, for `--aws-sigv4`): the encryption bench script uses them to create certificates and storage buckets.
- Node 24 and pnpm 10, if you change the search UI (`ui/`); see the end of section 5. You do not need them if you change only Rust code.
- Disk space: test build output is about 11 GB.

## 5. Local checks

Before opening a pull request, run these checks locally. This repository does not run CI. After the maintainers import your pull request, the development repository's CI runs the same commands and more (section 7).

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked --features amberlog-server/fault-injection -- -D warnings

# Tests need the object-storage bench and a headless browser
eval "$(tools/testbed.sh K up)"    # starts Silo in local Docker and exports TESTBED_* variables
eval "$(tools/testbed-encryption.sh K up)"   # TLS object storage and key service for the encryption tests; exports TESTBED_TLS_*
eval "$(tools/chrome.sh)"          # downloads the pinned headless browser for the search UI tests
cargo test --workspace --locked --features amberlog-server/fault-injection
tools/testbed.sh K down            # stops the bench
tools/testbed-encryption.sh K down
```

Notes:

- Run the tests as a normal user, not as root. Fault-injection tests depend on file permissions.
- The `fault-injection` feature is for tests only. Without it, some tests fail.
- A full `cargo test` run takes about two hours. While you work, run only the test files related to your change, for example `cargo test --locked --features amberlog-server/fault-injection -p amberlog-server --test cfg_startup`.
- Some tests wait on the real clock. On a heavily loaded machine, one of them may occasionally time out. If a failed test passes when you run it again on its own, it was the load, not your change; mention it in the pull request.
- Port 8800 must be free: two tests use the default listen address `0.0.0.0:8800`. Stop the quickstart (`docker compose down` in `examples/quickstart`) or any other Amberlog instance before you run the tests.
- Bench variables all start with `TESTBED_`. Do not rename them to `AMBERLOG_`: the program under test reads `AMBERLOG_*` variables as configuration keys and exits on unknown keys.
- `K` is a bench label. When several working directories run tests at the same time, give each its own label (`K`, `B`, `C`, `D`, `E`); their ports and storage buckets do not collide.
- The encryption tests need the bench started by `tools/testbed-encryption.sh`: one object store with TLS and a key service, and one whose key service has been stopped. Its output contains multi-line certificates, so import it with `eval`. Without this bench, these tests report that `TESTBED_TLS_*` is not set.

**Search UI (`ui/`).** The UI source is in `ui/`; its build output `crates/server/src/ui/index.html` is committed too, and the Rust build does not run Node. If you change files under `ui/`, rebuild in `ui/` and commit the output together with the source:

```sh
cd ui
corepack enable                    # gets pnpm 10 as pinned by packageManager in package.json
pnpm install --frozen-lockfile
pnpm typecheck && pnpm test
pnpm build                         # regenerates crates/server/src/ui/index.html
pnpm check                         # rebuilds and compares byte for byte with the committed output
cd .. && node tools/ui-deps-check.mjs   # licenses and sources of the front-end dependencies
```

If the build output conflicts in a merge, take either side, merge the source, then rebuild; do not merge the output by hand.

`ui/.npmrc` sets the default registry to the mirror `https://registry.npmmirror.com/` in China. To use the official npm registry, set the environment variable `npm_config_registry=https://registry.npmjs.org/`. The integrity values in the lockfile make sure both registries give the same packages.

**Long-running (soak) tests.** Some tests run continuously for a long time to check that the system stays correct under sustained load. Without the environment variable `TESTBED_SOAK_HOURS`, they run a shortened version, which is what `cargo test` runs. The full-length version runs only internally: your change goes through it after it is imported into the development repository.

## 6. Commits and sign-off (DCO)

This project uses the [Developer Certificate of Origin](https://developercertificate.org/) (DCO) instead of a contributor license agreement. Every commit message must end with a line:

```text
Signed-off-by: Your Name <your.email@example.com>
```

This line states that you have the right to submit the code and agree to license it under the project's license (Apache-2.0). You keep the copyright of your contribution.

- Commit with `git commit -s` and git adds the line for you.
- The email must match the commit author's email. The maintainers check this when they import the pull request.
- Forgot to sign off: run `git rebase --signoff <base of the PR>`, then force-push the PR branch.

## 7. How pull requests are handled

1. Fill in both sections of the PR template: section A states what the PR addresses; section B lists each externally observable change and maps it to section A.
2. The maintainers review. If changes are needed, they say so in the PR.
3. The maintainers import the PR's commits into the development repository. Your authorship, author date, commit message and `Signed-off-by` line are kept, and a line `Release-PR: github.com/visdata-com/amberlog#<PR number>` is added to the message.
4. Your commits go through the internal review and the full checks in the development repository. If a check fails because of the change, the maintainers say so in the PR. If tests or adjustments are needed, the maintainers add commits after yours instead of changing yours.
5. The next sync brings the change into this repository. The maintainers comment on the PR with the release or snapshot that contains it, then **close** the PR. The PR shows as "Closed", not "Merged"; this is expected.

For a user-visible change, you may add a line under `[Unreleased]` in `CHANGELOG.md`. You do not have to: the maintainers complete it before each release.

## 8. More

- Follow the [Code of Conduct](CODE_OF_CONDUCT.md) in all project discussions.
- Report security vulnerabilities privately, not in public issues or pull requests. See [SECURITY.md](SECURITY.md).
- Ask usage questions in Discussions. See [SUPPORT.md](SUPPORT.md).

## 中文

感谢你愿意为 Amberlog 出力。本项目的流程和一般的开源项目有几处不同，请先读完本文再动手。

### 1. 本仓库是快照

Amberlog 在一个内部开发仓库里开发。本仓库是它的快照：

- 维护者定期把开发仓库里可以公开的文件同步到本仓库。每次同步生成一个提交，标题是 `Snapshot <日期>` 或 `Release v<版本号>`。
- 开发仓库里的单个提交不出现在本仓库的历史里。
- 本仓库的 `main` 分支只由同步更新。**PR 不在本仓库合并**，处理办法见第 7 节。

### 2. 动手之前

- **修缺陷、改文档、改内部实现**：可以直接提 PR。修缺陷的 PR 请写清现在的行为、期望的行为，以及期望行为的依据。
- **改变对外可观察的行为**：先用「功能建议」模板开 issue，等维护者确认后再写代码。对外可观察的行为包括 HTTP 端点、请求与响应字段、错误码、日志字段、指标名、配置项、命令行参数、退出码，以及对象存储里的路径与文件格式。

Amberlog 的每一项对外行为，都由项目内部的需求定义规定。这套需求定义目前不公开。维护者会对照它判断你的提议：已有规定的照规定办，没有规定的先补规定。没有经过确认、却改变了对外行为的 PR，维护者会关闭，并说明原因。

### 3. 代码里的内部编号

代码注释里会出现下面这些编号和路径：

| 写法 | 指向 |
|---|---|
| `REQ-AMB-…` | 需求块：一条对外行为的规定 |
| `ADR-0001` 这样的编号 | 架构决策记录 |
| `knowledge/` 下的路径 | 需求块和架构决策所在的目录 |
| 注释里单说的「块」「块场景」 | 需求块，或需求块里的一个场景 |
| `TERM-NEEDED`、`MOVED` 这样的大写连字符短标签 | 注释所讲的那个需求块里的一条条款 |

它们指向内部开发仓库里的文件，这些文件还不在本仓库里，以后会逐步公开。在那之前：

- 你不需要读懂这些编号，也能修改代码。
- 请不要删除或改动已有的编号。它们是维护者追溯「这段代码实现哪条规定」的线索。
- 新写的代码不必加编号。需要的话，维护者导入时会补上。

### 4. 开发环境

完整的测试只在 Linux x86_64 上运行：检索界面的测试用 linux64 的无头浏览器，部分测试读取 `/proc`。在其他平台上可以编译和跑 clippy。

需要：

- Rust：用 [rustup](https://rustup.rs/) 安装。进入仓库目录后，`rustup toolchain install` 按 `rust-toolchain.toml` 装好固定版本的工具链和组件。
- C 编译器和 `libclang-dev`：依赖 `pg_query` 编译时要用。在 Debian 或 Ubuntu 上执行 `sudo apt-get install build-essential libclang-dev`。
- Docker 和 Docker Compose（v2.1 及以上，要支持 `docker compose up --wait`）：测试用的对象存储台架（Silo，MinIO 的社区分支）在 Docker 里运行。
- `openssl` 与 `curl`（7.75 及以上，要支持 `--aws-sigv4`）：加密场景的台架脚本用它们生成证书、建桶。
- 改检索界面（`ui/`）时另需 Node 24 与 pnpm 10，见第 5 节末尾。只改 Rust 代码时不需要。
- 磁盘空间：测试的编译产物大约 11 GB。

### 5. 本地门禁

提 PR 之前，请在本地跑下面这些检查。本仓库不跑 CI：维护者导入你的 PR 以后，开发仓库的 CI 会跑同一组命令和更多检查（第 7 节）。

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked --features amberlog-server/fault-injection -- -D warnings

# 测试要用对象存储台架和无头浏览器
eval "$(tools/testbed.sh K up)"    # 在本机 Docker 里起 Silo，导出 TESTBED_* 变量
eval "$(tools/testbed-encryption.sh K up)"   # 加密场景用的 TLS 对象存储与密钥服务，导出 TESTBED_TLS_* 变量
eval "$(tools/chrome.sh)"          # 下载固定版本的无头浏览器，供检索界面的测试使用
cargo test --workspace --locked --features amberlog-server/fault-injection
tools/testbed.sh K down            # 停掉台架
tools/testbed-encryption.sh K down
```

注意：

- 请以普通用户运行测试，不要用 root。故障注入类的测试依赖文件权限。
- `fault-injection` 特性只给测试用。不带它时，一部分测试会失败。
- 全量 `cargo test` 大约要两个小时。改代码的过程中，只跑与改动相关的测试文件即可，例如 `cargo test --locked --features amberlog-server/fault-injection -p amberlog-server --test cfg_startup`。
- 有些测试要等真实时间。机器负载很高时，其中个别测试可能偶尔超时。失败的测试单独重跑能通过，就是负载造成的，与你的改动无关；在 PR 里说明一下即可。
- 8800 端口要空着：有两个测试使用默认监听地址 `0.0.0.0:8800`。跑测试前，先停掉一键试用（在 `examples/quickstart` 下执行 `docker compose down`）或其他正在运行的 Amberlog。
- 台架的环境变量都以 `TESTBED_` 开头。不要把它们改成 `AMBERLOG_` 开头：被测程序会把 `AMBERLOG_*` 当成配置项读取，遇到不认识的配置项就退出。
- `K` 是台架的标签。几个工作目录要同时跑测试时，各用一个标签（`K`、`B`、`C`、`D`、`E`），它们的端口和桶互不冲突。
- 加密相关的测试要用 `tools/testbed-encryption.sh` 起的台架：一台开 TLS、接密钥服务的对象存储，另一台的密钥服务已停掉。它打印的变量含多行证书，要用 `eval` 导入。没起这套台架时，这些测试报 `TESTBED_TLS_*` 未设置。

**检索界面（`ui/`）。** 界面的源码在 `ui/`，构建产物是 `crates/server/src/ui/index.html`，也提交在仓库里；编译 Rust 时不调用 Node。改了 `ui/` 下的文件，要在 `ui/` 目录里重新构建，并把产物一起提交：

```sh
cd ui
corepack enable                    # 按 package.json 的 packageManager 取 pnpm 10
pnpm install --frozen-lockfile
pnpm typecheck && pnpm test
pnpm build                         # 重新生成 crates/server/src/ui/index.html
pnpm check                         # 重新构建并与提交的产物逐字节比对
cd .. && node tools/ui-deps-check.mjs   # 前端依赖的许可与来源
```

产物合并冲突时，取任一边，合完源码后重新构建，不要手工合并。

`ui/.npmrc` 把默认注册表设为国内镜像 `https://registry.npmmirror.com/`。要用 npm 官方注册表时，设环境变量 `npm_config_registry=https://registry.npmjs.org/`。锁文件里的完整性校验值保证两边取到的是同一个包。

**长时间测试（浸泡测试）。** 有几个测试会长时间连续运行，检查系统在持续负载下是否出错。不设置环境变量 `TESTBED_SOAK_HOURS` 时，它们按缩短的时长运行，`cargo test` 跑的就是缩短版。完整时长的版本只在内部运行：你的改动导入开发仓库以后，会经过它。

### 6. 提交与签名（DCO）

本项目用 [开发者原创声明](https://developercertificate.org/)（Developer Certificate of Origin，DCO），不需要签贡献者协议。每个提交的说明末尾要有一行：

```text
Signed-off-by: 你的名字 <你的邮箱>
```

这一行表示：你有权提交这段代码，并同意按项目的许可（Apache-2.0）授权。你贡献的部分，版权仍归你所有。

- 用 `git commit -s` 提交，git 会自动加上这一行。
- 邮箱要和提交的作者邮箱一致。维护者导入 PR 时会检查。
- 忘了签：执行 `git rebase --signoff <PR 的基点>`，再强制推送 PR 分支。

### 7. PR 怎样被处理

1. 按 PR 模板填写两栏：A 栏写这个 PR 解决什么，B 栏逐项列出对外可观察的变化，每一项对应到 A 栏。
2. 维护者审阅。需要修改时，维护者在 PR 里说明。
3. 维护者把 PR 的提交导入开发仓库。导入时保留你的作者署名、作者时间、提交说明和 `Signed-off-by` 行，并在说明末尾加一行 `Release-PR: github.com/visdata-com/amberlog#<PR 编号>`。
4. 你的提交在开发仓库里按内部流程评审，并跑完整的检查。检查因这次改动失败时，维护者在 PR 里说明。需要补测试或调整写法时，维护者在你的提交之后追加提交，不改你的提交。
5. 下一次同步把改动带到本仓库。维护者在 PR 里说明它进了哪个版本或哪次快照，然后**关闭** PR。PR 显示为「已关闭」，不是「已合并」，这是正常的。

对使用者可见的变化，可以在 `CHANGELOG.md` 的 `[Unreleased]` 下加一行。不加也可以，维护者发版前会补齐。

### 8. 其他

- 参与本项目的讨论，请遵守[行为准则](CODE_OF_CONDUCT.md)。
- 安全漏洞请私下报告，不要提公开的 issue 或 PR。见 [SECURITY.md](SECURITY.md)。
- 使用中的问题请到 Discussions 提问。见 [SUPPORT.md](SUPPORT.md)。

<!--
Thanks for contributing. This repository is a snapshot, so pull requests are not merged here: the maintainers import the change into the development repository, release it with the next sync, then close the PR and say which release contains it. Authorship and sign-off are kept. See CONTRIBUTING.md.
感谢贡献。本仓库是快照，PR 不在这里合并：维护者把它导入开发仓库，随下一次同步发布，然后关闭 PR 并说明发布到了哪个版本。作者和签名都会保留。详见 CONTRIBUTING.md。

Chinese and English are both welcome. / 中文、英文都可以。
-->

## A. What this PR addresses / 这个 PR 解决什么

<!--
Link the issue (for example, Fixes #12). For a change to external behavior, the issue must already have the maintainers' agreement.
For a small fix without an issue: state the current behavior, the expected behavior, and the basis for the expected behavior (a documentation link or quote).
关联的 issue（例如 Fixes #12）。改变对外行为的 PR，issue 里要已经有维护者的确认。
没有 issue 的小修复：写清现在的行为、期望的行为，以及期望行为的依据（文档链接或原文）。
-->



## B. Externally observable changes / 对外可观察的变化

<!--
List each item: HTTP endpoints, request and response fields, error codes, log fields, metric names, configuration keys, command-line options, exit codes, paths and file formats in object storage.
Map each item to an entry in A. A PR with an item in B that maps to nothing in A cannot be accepted. Write "None" if only internals, tests or documentation change.
逐项列出：HTTP 端点、请求与响应字段、错误码、日志字段、指标名、配置项、命令行参数、退出码、对象存储里的路径与文件格式。
每一项写明对应 A 的哪一条。B 中有对应不到 A 的项，这个 PR 就不能收。只改内部实现、测试或文档时，写「无」。
-->

| Change / 变化 | Maps to A / 对应 A |
|---|---|
| | |

## Checklist / 自查

- [ ] Every commit has a `Signed-off-by` line (`git commit -s`) matching the author's email. / 每个提交都有 `Signed-off-by` 行（`git commit -s`），与提交作者的邮箱一致。
- [ ] `cargo fmt --all --check` passes. / 通过。
- [ ] `cargo clippy --workspace --all-targets --locked --features amberlog-server/fault-injection -- -D warnings` passes. / 通过。
- [ ] `cargo test --workspace --locked --features amberlog-server/fault-injection` passes (needs the test bench and takes about two hours; see CONTRIBUTING.md section 5). / 通过（需要测试台架，约两小时，见 CONTRIBUTING.md 第 5 节）。
- [ ] A bug fix comes with a test that fails before the fix and passes after it. / 修缺陷的 PR 带一个修复前失败、修复后通过的测试。
- [ ] The commits contain no tokens, keys or internal addresses. / 提交里没有令牌、密钥或内部地址。

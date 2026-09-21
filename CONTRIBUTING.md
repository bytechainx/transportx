# CONTRIBUTING.md — 贡献指南（transportx）

本文件面向贡献者，汇总本地门禁与提交约定。
AI Agent 的工作约定另见 [`AGENTS.md`](./AGENTS.md)；术语与领域语言见 [`CONTEXT.md`](./CONTEXT.md)。

## 开发流程

- 本仓库是**独立的单 crate 仓库**，不依赖 `xhyper.rs` 主工程及其内部 crate（`kernel` / `contracts` 等）；
  **没有任何 path 依赖**，可被任意 Rust 工程复用。
- substantial 变更走 feature branch → PR → review → merge，**禁止直接 push `main`**。
- `main` 已启用分支保护：要求 PR + 必需检查 `fmt / clippy / test`，
  `required_approving_review_count = 0`（单人也能合并），禁止强推与删除。
- 合并方式固定为 **create a merge commit**。注意仓库设置是
  `merge_commit_title = MERGE_MESSAGE` + `merge_commit_message = PR_TITLE`，因此
  `gh pr merge` 必须显式传 `--subject` 与 `--body`，否则会产出通用
  `Merge pull request #N from …` 标题。
- 提交信息遵循 Conventional Commits（`feat:` / `fix:` / `docs:` / `ci:` / `chore:` / `refactor:`），
  描述用简体中文。

## 本地门禁（P0 三件套）

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

本 crate 没有 feature 开关（`[features]` 只有空的 `default`），因此无需 `--all-features`。

元数据完整性门禁（**不发布 crates.io**，此命令只校验打包元数据）：

```bash
cargo package --no-verify --allow-dirty
```

本 crate 无 path 依赖，package 不需要额外的 `--config patch` 覆盖。

## 复用口径（不发布 crates.io）

- 本 crate **不发布到 crates.io**，仅以 GitHub 源码 / git 依赖形式复用。
- 文档与元数据中不得出现「可独立发布」「可直接 `cargo publish`」等表述，
  也不得放置 crates.io / docs.rs 徽章与外链。
- `Cargo.toml` 的 `documentation` 指向 `https://github.com/bytechainx/transportx#readme`。
- 消费方引入方式（README「安装」小节为准）：

  ```toml
  [dependencies]
  transportx = { git = "https://github.com/bytechainx/transportx" }
  ```

## 开发约定

- 注释、文档、错误消息使用**简体中文**；标识符保持英文。
- MSRV 为 Rust 1.77、edition 2021；不得使用高于该下界的语言特性或 std API。
- 错误模型：`TransportError`（thiserror 风格枚举）；
  禁止公共 API 返回 `String` / `anyhow::Error`。
- 不在库代码里裸 `unwrap()`（`[lints.clippy]` 已 `deny` `unwrap_used` / `expect_used` / `panic`）。
- 所有 `pub` 项必须有中文 `///` 文档（`missing_docs` 已 `deny`）。
- 集成测试**必须离线运行**，全部走 loopback（`127.0.0.1:0`），不触碰真实网络。
- 生产默认必须保持：`HttpRequest` / `HttpResponse` 的 `Debug` **脱敏**（URL 仅 origin、
  敏感 header 不打印、body 只报长度）；30s 总超时 + 请求/响应体大小上限；
  超限走 `TransportError::PayloadTooLarge`（fail-closed）。
- 出站请求必须有超时与体积上限，禁止无界读取。
- TLS 默认校验开启，禁止默认跳过证书验证；`TlsMode::InsecureDevOnly` 仅限本地开发。
- 驱动私有类型（`reqwest::Client`、tungstenite stream）必须保持 crate-private，不得出现在边界上。

## 提交前自检清单

- [ ] `cargo fmt --all -- --check` 通过
- [ ] `cargo clippy --all-targets -- -D warnings` 通过
- [ ] `cargo test --all-targets` 通过
- [ ] `cargo package --no-verify --allow-dirty` 通过
- [ ] 新增 `pub` 项都有中文 `///` 文档
- [ ] 文档中无「可独立发布」/ crates.io / docs.rs 表述
- [ ] 新的默认值仍 fail-closed，且 `Debug` 未泄漏 URL path/query、凭据或 body 原文

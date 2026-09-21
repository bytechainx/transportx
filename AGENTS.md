# transportx Agent 指南

> 本文件为 AI Agent 在本仓库工作时的入口指南。

## 项目定位

驱动无关的 HTTP/WebSocket 传输边界：TLS 模式、客户端连接池与代理配置，附 reqwest / tungstenite 默认驱动。只承载传输，不承载业务契约；不实现重试 / 熔断 / 限流 / 调度，不成为应用的组合根。

## 技术栈

- Rust edition 2021, rust-version 1.77
- 关键依赖: `async-trait`、`bytes`、`futures-util`、`httpdate`、`reqwest` 0.12（rustls-tls, json，default-features = false）、`thiserror` 2、`tokio` 1、`tokio-tungstenite` 0.29（rustls-tls-native-roots）
- 无 path 依赖；零业务耦合，不依赖 kernel/contracts 等私有 crate，可被任意 Rust 工程复用
- crate 级 lint：`unwrap_used` / `expect_used` / `panic` / `unreachable` / `todo` / `unimplemented` 全部 `deny`（测试代码经 `cfg_attr(test)` 豁免）

## 代码结构

```text
src/
├── lib.rs  # 传输边界核心：TransportError / HttpRequest / HttpResponse DTO、
│           # HttpDriver / WsConnector / WsConnection trait、
│           # 默认驱动 ReqwestHttpDriver / TungsteniteWsConnector、测试用 MockHttpTransport
├── pool.rs # HttpClientPool / SharedHttpClientPool / HttpClientLease / PoolConfig
├── proxy.rs# ProxyConfig / build_reqwest_proxy（Debug 脱敏）
└── tls.rs  # TlsConfig / TlsMode
```

- 驱动私有类型（`reqwest::Client`、tungstenite stream）封装在 crate 内部，不外泄
- `#![deny(missing_docs)]`、`#![deny(unreachable_pub)]`

## 开发约定

- 注释与文档使用简体中文；标识符保持英文
- 错误：`TransportError`（thiserror 风格）+ `#[non_exhaustive]`；禁止公共 API 返回 `String` / `anyhow::Error`
- 禁止裸 `unwrap()`（库代码；lint 已 deny）
- 生产默认必须保持：`HttpRequest` / `HttpResponse` 的 `Debug` **脱敏**（URL 仅 origin、敏感 header 不打印、body 只报长度）；30s 总超时 + 请求/响应体大小上限；超限走 `TransportError::PayloadTooLarge`（fail-closed）
- 出站请求必须有超时与体积上限，禁止无界读取
- TLS 默认校验开启，禁止默认跳过证书验证

## 门禁三件套（P0）

```bash
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
```

## 相关文档

- 组织 Rust 规范：`~/org-config/rulesets/rust/RULES.md`
- API 文档：`docs/API.md`
- 标准与验收：`docs/标准.md`
- 术语与领域语言：`CONTEXT.md`
- 贡献指南：`CONTRIBUTING.md`
- 变更记录：`CHANGELOG.md`
- 基准测试：`benches/hot_path.rs`

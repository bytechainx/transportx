# Changelog — transportx

本文件记录 `transportx` 的用户可见变更，遵循 [Keep a Changelog](https://keepachangelog.com/)
与 [Semantic Versioning](https://semver.org/)。

本仓库代码自 `xhyper.rs` 的 `crates/infra/transport` 抽取而来（抽取时点为 `0.1.6`）。
该工程内的版本线不在本文件中延续，本仓库从 `0.1.0` 重新起算。

## [Unreleased]

## [0.1.1] - 2026-09-22

### 新增

- 特性 002：`tests/tdd_contracts.rs`（覆盖公开接口契约全部 10 个入口的行为契约 +
  TDD-PROBE 变异探测表）、`tests/sdd_spec.rs`（`docs/标准.md` 全部 3 个 `##` 章节的
  可执行断言）、`tests/aidd_boundary.rs`（8 条经复核的 AI 生成对抗 / 边界用例）。

### 变更

- **内部结构改写（公开 API 与可观察契约均不变）**：按 `docs/module-rules.md` §5.5 的手法，
  把两个默认驱动从 `src/lib.rs` 下沉为独立子模块 —— reqwest 驱动 → `src/http.rs`、
  tungstenite 驱动 → `src/ws.rs`。门面 `src/lib.rs` 保留边界类型 / trait / 常量 / 内存 Mock
  与**原有内联测试**；`ReqwestHttpDriver` / `TungsteniteWsConnector` 经 crate 根 `pub use`
  导出，公开路径与 `Debug` 输出均不变。`src/lib.rs` 生产段由 **787 → 403** 行。
  动机：`module-rules` 已是元仓库必需检查，而它审计各仓默认分支，故 `lib.rs` 生产段距
  `MR-STRUCT-007` 的 800 行 ERROR 阈值只剩 13 行时，任一仓的任意改动都可能卡住元仓库的
  全部 PR。属纯搬移，全部 97 项测试与 doctest 结果不变。

## [0.1.0] - 2026-09-21

### 新增

- 从 `xhyper.rs` 抽取为独立 crate，移除对内部 crate `kernel` 的依赖。
- `HttpDriver` 边界：带 method / headers / body 的 typed HTTP 请求，响应按 chunk 流式累计，
  未知长度越界时立即中止。
- `WsConnector` / `WsConnection`：帧级 WebSocket 生命周期边界；tungstenite 在解码与聚合前
  设置入站 frame / message 上限。
- `ReqwestHttpDriver` 与 `TungsteniteWsConnector` 真实驱动，驱动私有类型保持 crate-private。
- `HttpClientPool` / `HttpClientLease` / `PoolConfig`：有界对象池、配置校验与 RAII 租约；
  `try_new` 返回可恢复错误，兼容构造 `new` 对无效配置 fail-fast。
- `TlsConfig` / `TlsMode::{SystemRoots, CustomCa, InsecureDevOnly}`；未接线的 `sni = false`
  被明确拒绝而非静默忽略。
- `ProxyConfig` / `build_reqwest_proxy`，凭据在 `Debug` 中脱敏。
- `MockHttpTransport`：`set_get` / `set_post` 预置响应并实现 `HttpDriver`。
- `parse_retry_after_at`：RFC 9110 `Retry-After` 解析（delay-seconds 与 HTTP-date）。
- `TransportError`：超时、连接关闭（区分 clean）、限流（携带 `retry_after`）、载荷超限、
  协议违背与 I/O。

### 移除

- 已废弃的 legacy `HttpTransport` trait 及其 `MockHttpTransport` 实现。它是 monorepo 内的
  staged compatibility 面，在独立 crate 首版中不再提供；`MockHttpTransport` 仍然可用，
  只需通过 `HttpDriver::execute` 调用。

### 变更

- `ReqwestHttpDriver::execute` 中的 let-chain 改写为等价的嵌套 `if let`，
  使代码可在 edition 2021 下编译。

### 说明

测试全部走 loopback（`127.0.0.1:0`），不访问外网。企业 PKI / mTLS、WS 企业 TLS 与
完整业务 live 均未实现。

# transportx 上下文

本文件定义 `transportx` 与其使用方共享的核心词汇。它只记录领域含义与能力边界，
不记录具体实现、API 签名、存储或部署决定。

## 角色与边界

**传输边界**：把 HTTP / WebSocket 的报文收发收敛成稳定 Rust 类型的一层；
它只承载传输，不承载业务契约，也不解析交易所、对象存储等具体协议。
_Avoid_: 客户端 SDK（SDK 通常含重试策略、业务模型与遥测后端，超出本 crate 边界）

**驱动无关**：上层只依赖 `HttpDriver` / `WsConnector` / `WsConnection` 三个 trait，
具体客户端类型不出现在边界上，因此可替换驱动而不改动调用方。
_Avoid_: 抽象层（本 crate 同时提供可用的默认驱动，不是只留接口的空壳）

**默认驱动**：基于 `reqwest` 与 `tokio-tungstenite` 的可直接使用的实现；
其内部类型（`reqwest::Client`、tungstenite stream）保持 crate-private。
_Avoid_: 唯一实现（驱动是默认可选项，不是本 crate 的定位本身）

**组合根**：负责编排重试、熔断、限流、调度与业务依赖装配的上层；
本 crate **不是**组合根，不替调用方做这些决策。
_Avoid_: 基础设施 crate（容易让人误以为可以在此装配全局策略）

## 资源与限额

**fail-closed 超限**：请求体、响应体、WebSocket 帧任一超过配置上限时立即返回
`TransportError::PayloadTooLarge`，而不是截断或继续读取。
_Avoid_: 截断（本 crate 不返回部分载荷）

**响应流式累计**：HTTP 响应按 chunk 累计，首次越界即中止；不预先信任 `Content-Length`。
_Avoid_: 整体读取（无界读取正是上限机制要防的情形）

**Debug 脱敏**：`HttpRequest` / `HttpResponse` / `ProxyConfig` 的 `Debug` 只输出
URL 的 scheme/host/port、敏感 header 的 `***` 与 body 长度；非法 URL 不回显原文。
_Avoid_: 日志安全（脱敏是类型自身的默认行为，不依赖调用方是否记得过滤）

**客户端池与 RAII 租约**：有界对象池 + `HttpClientLease`，离开作用域自动归还对象与许可；
`PoolConfig` 在构造期校验。
_Avoid_: 连接池（本池按泛型槽位管理借出对象，不代表底层 TCP 连接的复用策略）

## TLS 与代理

**TLS 模式**（`TlsMode`）：`SystemRoots`（生产默认）、`CustomCa`（指定 PEM 文件）、
`InsecureDevOnly`（仅开发，显式关闭证书校验）三选一。
_Avoid_: TLS 支持（企业 PKI / mTLS 与 WS 企业 TLS 均未实现）

**未接线拒绝**：`sni = false` 当前没有实现，驱动构造时明确报错而非静默忽略。
_Avoid_: 默认关闭（本 crate 不会替调用方悄悄降级安全配置）

**代理配置**（`ProxyConfig`）：代理 URL 与可选基本认证；密码只在 `Debug` 中以 `***` 出现，
由 `build_reqwest_proxy` 转成驱动可用的代理。
_Avoid_: 凭据透传（把带认证的代理原样打进日志会泄漏口令，本 crate 明确脱敏）

## 连接与错误

**TransportError**：传输层失败的统一分类，取值为 `ConnectTimeout` / `ReadTimeout` /
`ConnectionClosed { clean }` / `RateLimited { retry_after }` / `PayloadTooLarge` /
`ProtocolViolation` / `Io`；它保留足够语义供上层决定是否重连。
_Avoid_: 通用错误（分类的存在意义就是让上层不必靠字符串判断）

**clean 关闭**：`ConnectionClosed { clean }` 中 `clean` 表示是否完成协议层关闭握手，
用于区分正常结束与异常断开。
_Avoid_: 断开（不区分 clean 的「断开」无法支撑重连策略）

**确定性 Retry-After 解析**（`parse_retry_after_at`）：按 RFC 9110 解析 delay-seconds
或 HTTP-date，`now` 由调用方显式传入以便测试可复现。
_Avoid_: 自动重试（本 crate 只解析提示值，不据此发起重试）

**帧级生命周期**（`WsConnection`）：`next_frame` / `send_frame` / `close` 三件套；
text / binary 作为载荷返回，Ping / Pong 跳过，Close 返回 `Ok(None)`，流自然结束报
`ConnectionClosed { clean: false }`。
_Avoid_: 消息流（本边界不保留 Close 的 code / reason）

**loopback 测试边界**：全部测试走 `127.0.0.1:0`，只证明受控传输实现面，
不构成公网或业务 live 证据。
_Avoid_: 集成测试通过（通过 loopback 不等于真实网络与企业 TLS 场景已验证）

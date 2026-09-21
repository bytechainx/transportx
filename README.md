# transportx

`transportx` 是一层**驱动无关**的 HTTP / WebSocket 传输边界，并把 `reqwest` /
`tokio-tungstenite` 默认驱动与它们的私有类型（`reqwest::Client`、tungstenite stream）
封装在 crate 内部。

- 只承载传输，不承载业务契约；零内部耦合
- 默认 fail-closed：请求 / 响应体与 WebSocket 帧都有硬上限，超限直接报错
- `Debug` 默认**脱敏**：URL 只保留 scheme/host/port，敏感 header 与 body 只输出长度
- 有界客户端池 + RAII 租约，Drop 自动归还
- 统一错误模型：`TransportError`

## 安装

本 crate **不发布到 crates.io**，通过 git 依赖引入：

```toml
[dependencies]
transportx = { git = "https://github.com/bytechainx/transportx" }
```

## 最小可运行示例

```rust,no_run
use bytes::Bytes;
use transportx::{HttpDriver, HttpRequest, MockHttpTransport, TransportError};

#[tokio::main]
async fn main() -> Result<(), TransportError> {
    let mock = MockHttpTransport::new();
    mock.set_get("https://api/ping", Bytes::from_static(b"{}"));

    let response = mock
        .execute(HttpRequest {
            method: "GET".into(),
            url: "https://api/ping".into(),
            headers: vec![],
            body: None,
        })
        .await?;

    assert_eq!(response.status, 200);
    Ok(())
}
```

真实驱动：

```rust,no_run
use bytes::Bytes;
use transportx::{HttpDriver, HttpRequest, ReqwestHttpDriver, TlsConfig, TransportError};

#[tokio::main]
async fn main() -> Result<(), TransportError> {
    // 默认 30s 总超时 + 16 MiB 请求/响应体上限
    let driver = ReqwestHttpDriver::with_tls(TlsConfig::system_roots())?;
    let response = driver
        .execute(HttpRequest {
            method: "GET".into(),
            url: "https://example.com/".into(),
            headers: vec![],
            body: Some(Bytes::new()),
        })
        .await?;
    println!("status = {}", response.status);
    Ok(())
}
```

## 主要内容

| 类型 | 作用 |
| --- | --- |
| `HttpDriver` | 带 method / headers / body 的 typed HTTP 边界 |
| `WsConnector` / `WsConnection` | 帧级 WebSocket 生命周期边界 |
| `ReqwestHttpDriver` / `TungsteniteWsConnector` | 真实驱动（内部客户端类型 crate-private） |
| `HttpClientPool` / `HttpClientLease` / `PoolConfig` | 有界对象池与 Drop 自动归还的 RAII 租约 |
| `TlsConfig` / `TlsMode` | `SystemRoots` / `CustomCa` / `InsecureDevOnly`（后者仅限开发） |
| `ProxyConfig` / `build_reqwest_proxy` | 代理配置（`Debug` 脱敏） |
| `MockHttpTransport` | `set_get` / `set_post` 预置响应，实现 `HttpDriver` |
| `parse_retry_after_at` | RFC 9110 `Retry-After` 解析（delay-seconds 与 HTTP-date） |
| `TransportError` | 统一错误类型，覆盖超时、连接关闭、限流、超限、协议违背与 I/O |

## 默认超时与资源上限

| 项 | 默认 | 逃生口 |
| --- | --- | --- |
| HTTP 总超时 | 30s（`ReqwestHttpDriver::new`） | `with_timeout(None)` 显式关闭 |
| HTTP 请求 / 响应体 | 16 MiB | `with_limits(.., 0, 0)` |
| WS 连接超时 | 30s | `TungsteniteWsConnector::with_limits` |
| WS 入站 frame / message | 4 MiB，在解码与聚合前下沉 | `max_frame_bytes = 0` |
| `Debug` | header / body 脱敏；URL 仅保留 scheme/host/port | — |

HTTP 响应按 chunk 累计，首次越界立即中止；超限返回 `TransportError::PayloadTooLarge`。

Pool 新代码应使用可恢复的 `try_new` + `checkout_lease_with`；兼容构造 `new` 对无效
`PoolConfig` 采取 fail-fast，旧的手动 checkout / return 接口仅为兼容保留。

## 生产误用红线

| 禁止 | 原因 |
| --- | --- |
| 宣称企业 PKI / mTLS / WS 企业 TLS 已完成 | 未实现，配置会 fail-closed 拒绝 |
| 把 `TlsMode::InsecureDevOnly` 用于生产 | 它显式关闭证书校验，仅供本地开发 |
| 把 loopback 测试当作公网 / 业务 live 证据 | 只证明受控传输实现面 |
| 在日志里输出完整 `Authorization` 头或私钥材料 | `Debug` 已脱敏，不要绕过它自行打印原始值 |

## 非目标

- 不解析具体业务协议（交易所、对象存储等由各自 adapter 负责）
- 不实现重试 / 熔断 / 限流 / 调度
- 不成为应用的组合根
- 不承诺企业 PKI / mTLS、WS 企业 TLS、gRPC 或完整故障恢复矩阵

## 门禁

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo package --no-verify
```

测试全部走 loopback（`127.0.0.1:0`），不访问外网。

## 许可

MIT OR Apache-2.0

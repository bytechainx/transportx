#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]
#![forbid(unsafe_code)]
#![deny(missing_docs)]
#![deny(unreachable_pub)]

//! # transportx — 统一网络客户端抽象
//!
//! 提供驱动无关的 HTTP / WebSocket 传输边界，以及基于 `reqwest` / `tokio-tungstenite`
//! 的默认驱动。驱动私有类型（`reqwest::Client`、tungstenite stream）封装在 crate 内部。
//!
//! ## 职责
//!
//! - 统一 HTTP / WebSocket 客户端侧传输边界
//! - 只承载传输，不承载业务契约
//! - 零内部耦合：可被任意 Rust 工程复用
//!
//! ## 非目标
//!
//! - 不实现重试 / 熔断 / 限流 / 调度
//! - 不成为应用的组合根
//!
//! ## 生产默认
//!
//! - [`HttpRequest`] / [`HttpResponse`] 默认 [`Debug`] **脱敏**（URL 仅 origin、敏感 header、body 长度）
//! - [`ReqwestHttpDriver::new`]：30s 总超时 + 16 MiB 请求/响应体上限
//! - [`TungsteniteWsConnector::new`]：30s 连接超时 + 4 MiB 单帧上限
//! - 超限 → [`TransportError::PayloadTooLarge`]（fail-closed）
//! - TLS：[`TlsConfig`] / [`TlsMode`]；池：[`HttpClientPool`]；代理：[`ProxyConfig`]（Debug 脱敏）
//!
//! # 最小示例
//!
//! ```
//! use bytes::Bytes;
//! use transportx::{HttpDriver, HttpRequest, MockHttpTransport, TransportError};
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), TransportError> {
//! let mock = MockHttpTransport::new();
//! mock.set_get("https://api/ping", Bytes::from_static(b"{}"));
//!
//! let response = mock
//!     .execute(HttpRequest {
//!         method: "GET".into(),
//!         url: "https://api/ping".into(),
//!         headers: vec![],
//!         body: None,
//!     })
//!     .await?;
//!
//! assert_eq!(response.status, 200);
//! # Ok(())
//! # }
//! ```

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, Method};
use std::collections::HashMap;
use std::fmt;
use std::sync::RwLock;
use std::time::{Duration, SystemTime};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{protocol::WebSocketConfig, Message},
    MaybeTlsStream, WebSocketStream,
};

mod pool;
mod proxy;
mod tls;
pub use pool::{HttpClientLease, HttpClientPool, PoolConfig, SharedHttpClientPool};
pub use proxy::{build_reqwest_proxy, ProxyConfig};
pub use tls::{TlsConfig, TlsMode};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Transport failures retain enough semantics for reconnect policy decisions.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// TCP / TLS 握手超时。
    #[error("connect timeout")]
    ConnectTimeout,
    /// 读响应 / 帧超时。
    #[error("read timeout")]
    ReadTimeout,
    /// 连接已关闭；`clean` 表示是否为协议层正常关闭。
    #[error("connection closed ({clean})")]
    ConnectionClosed {
        /// `true` 表示对端/本端已完成协议关闭握手。
        clean: bool,
    },
    /// HTTP 429；可选 RFC 9110 `Retry-After`（整数秒或 HTTP-date）。
    #[error("rate limited{retry_after:?}")]
    RateLimited {
        /// 建议等待时长（来自 delay-seconds 或相对当前时间的 HTTP-date）。
        retry_after: Option<Duration>,
    },
    /// 请求/响应/帧超过资源上限（fail-closed）。
    #[error("载荷过大: {kind} 上限 {limit} 字节，实际 {got} 字节")]
    PayloadTooLarge {
        /// 资源类别（request_body / response_body / ws_frame / ws_message）。
        kind: &'static str,
        /// 配置上限（字节）。
        limit: usize,
        /// 实际大小（字节）。
        got: usize,
    },
    /// 协议 / 方法 / URL 等不可恢复语义错误。
    #[error("protocol violation: {0}")]
    ProtocolViolation(String),
    /// 底层 I/O 或客户端构建失败。
    #[error("I/O error: {0}")]
    Io(#[source] Box<dyn std::error::Error + Send + Sync>),
}

// ---------------------------------------------------------------------------
// HTTP boundary
// ---------------------------------------------------------------------------

/// Request passed to an HTTP driver. Concrete HTTP clients stay behind this boundary.
///
/// 默认 [`Debug`] **脱敏**：URL 只显示 scheme/host/port，敏感 header 值显示为
/// `***`，body 仅显示字节长度。
#[derive(Clone, PartialEq, Eq)]
pub struct HttpRequest {
    /// HTTP method（如 `"GET"` / `"POST"`）。
    pub method: String,
    /// 完整请求 URL。
    pub url: String,
    /// 请求头（name, value）。
    pub headers: Vec<(String, String)>,
    /// 可选请求体。
    pub body: Option<Bytes>,
}

impl fmt::Debug for HttpRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpRequest")
            .field("method", &self.method)
            .field("url", &RedactedUrl(&self.url))
            .field("headers", &RedactedHeaders(&self.headers))
            .field("body", &BodyDebug(self.body.as_ref().map(|b| b.len())))
            .finish()
    }
}

/// Transport-neutral HTTP response.
///
/// 当前只保留 status/body，不保留 response headers（SSOT §4.4）。
/// 默认 [`Debug`] 不打印 body 原文，仅长度。
#[derive(Clone, PartialEq, Eq)]
pub struct HttpResponse {
    /// HTTP 状态码。
    pub status: u16,
    /// 响应体。
    pub body: Bytes,
}

impl fmt::Debug for HttpResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpResponse")
            .field("status", &self.status)
            .field("body", &BodyDebug(Some(self.body.len())))
            .finish()
    }
}

/// Debug 辅助：body 仅长度。
struct BodyDebug(Option<usize>);

impl fmt::Debug for BodyDebug {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            None => write!(f, "None"),
            Some(n) => write!(f, "Some(<{n} bytes>)"),
        }
    }
}

/// Debug 辅助：敏感 header 脱敏。
struct RedactedHeaders<'a>(&'a [(String, String)]);

/// Debug 辅助：只保留 URL 的安全 origin（scheme/host/port）。
///
/// path、query 名和值、userinfo、fragment 一律不输出；解析失败或没有 host 时
/// fail-closed，且不回显原文。
pub(crate) struct RedactedUrl<'a>(pub(crate) &'a str);

impl fmt::Debug for RedactedUrl<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Ok(url) = reqwest::Url::parse(self.0) else {
            return f.write_str("\"<invalid-url-redacted>\"");
        };
        if url.cannot_be_a_base() || url.host().is_none() {
            return f.write_str("\"<invalid-url-redacted>\"");
        }
        fmt::Debug::fmt(&url.origin().ascii_serialization(), f)
    }
}

impl fmt::Debug for RedactedHeaders<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut list = f.debug_list();
        for (name, value) in self.0 {
            if is_sensitive_header_name(name) {
                list.entry(&(name.as_str(), "***"));
            } else {
                list.entry(&(name.as_str(), value.as_str()));
            }
        }
        list.finish()
    }
}

/// 判断 header 名是否敏感（Authorization / Cookie / *token* / *secret* / *api-key* 等）。
#[must_use]
pub fn is_sensitive_header_name(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    matches!(
        n.as_str(),
        "authorization"
            | "proxy-authorization"
            | "cookie"
            | "set-cookie"
            | "x-api-key"
            | "x-auth-token"
            // OKX v5 鉴权头（勿把 passphrase/key 打进 Debug）
            | "ok-access-key"
            | "ok-access-sign"
            | "ok-access-timestamp"
            | "ok-access-passphrase"
    ) || n.contains("token")
        || n.contains("secret")
        || n.contains("password")
        || n.contains("api-key")
        || n.contains("apikey")
        || n.contains("passphrase")
}

/// Typed HTTP driver boundary.
#[async_trait]
pub trait HttpDriver: Send + Sync {
    /// 执行一次 HTTP 请求。
    ///
    /// - HTTP 429 → [`TransportError::RateLimited`]（delay-seconds / HTTP-date）
    /// - 其他 4xx/5xx → [`Ok`]`(`[`HttpResponse`]`)`
    /// - 体超限 → [`TransportError::PayloadTooLarge`]
    async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, TransportError>;
}

/// 按 RFC 9110 解析 `Retry-After` 的 delay-seconds 或 HTTP-date。
///
/// 显式传入 `now` 以便调用方和测试获得确定性结果；过去日期钳制为零。
#[must_use]
pub fn parse_retry_after_at(value: &str, now: SystemTime) -> Option<Duration> {
    let value = value.trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    httpdate::parse_http_date(value)
        .ok()
        .map(|deadline| deadline.duration_since(now).unwrap_or(Duration::ZERO))
}

// ---------------------------------------------------------------------------
// WebSocket boundary
// ---------------------------------------------------------------------------

/// WebSocket 连接器边界。
#[async_trait]
pub trait WsConnector: Send + Sync {
    /// 建立到 `url` 的 WebSocket 连接。
    async fn connect(&self, url: &str) -> Result<Box<dyn WsConnection>, TransportError>;
}

/// 已建立的 WebSocket 连接（帧级生命周期）。
#[async_trait]
pub trait WsConnection: Send + Sync {
    /// 读取下一帧。
    ///
    /// - text / binary → `Some(Bytes)`
    /// - Ping / Pong / Frame → 跳过
    /// - Close → `Ok(None)`（不保留 code/reason）
    /// - 流自然结束 → [`TransportError::ConnectionClosed`] `{ clean: false }`
    /// - 帧超限 → [`TransportError::PayloadTooLarge`]
    async fn next_frame(&mut self) -> Result<Option<Bytes>, TransportError>;
    /// 发送一帧 binary payload。
    async fn send_frame(&mut self, frame: Bytes) -> Result<(), TransportError>;
    /// 发送 Close 并结束发送侧。
    async fn close(&mut self) -> Result<(), TransportError>;
}

// ---------------------------------------------------------------------------
// Limits & defaults（生产默认 fail-closed；0 = 关闭对应上限）
// ---------------------------------------------------------------------------

/// `ReqwestHttpDriver::new` 默认请求总超时。
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// 默认 HTTP 响应体上限（16 MiB）。
pub const DEFAULT_MAX_RESPONSE_BODY_BYTES: usize = 16 * 1024 * 1024;

/// 默认 HTTP 请求体上限（16 MiB）。
pub const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 16 * 1024 * 1024;

/// `TungsteniteWsConnector::new` 默认连接超时。
pub const DEFAULT_WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// 默认 WebSocket 单帧上限（4 MiB）。
pub const DEFAULT_MAX_WS_FRAME_BYTES: usize = 4 * 1024 * 1024;

// ---------------------------------------------------------------------------
// reqwest HTTP driver
// ---------------------------------------------------------------------------

/// reqwest-backed HTTP driver. The reqwest type remains private to this crate.
///
/// 默认 [`Debug`] **不**展开内部 `Client`。
#[derive(Clone)]
pub struct ReqwestHttpDriver {
    client: Client,
    max_response_body_bytes: usize,
    max_request_body_bytes: usize,
}

impl fmt::Debug for ReqwestHttpDriver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReqwestHttpDriver")
            .field("max_response_body_bytes", &self.max_response_body_bytes)
            .field("max_request_body_bytes", &self.max_request_body_bytes)
            .finish_non_exhaustive()
    }
}

impl ReqwestHttpDriver {
    /// 构建驱动：默认 **30s** 总超时 + 16 MiB 请求/响应体上限。
    pub fn new() -> Result<Self, TransportError> {
        Self::with_timeout(Some(DEFAULT_REQUEST_TIMEOUT))
    }

    /// 构建驱动：可选总超时；体上限仍为默认 16 MiB。
    ///
    /// `timeout = None` 表示**显式**关闭超时（测试/特殊路径）；生产请优先 [`Self::new`]。
    pub fn with_timeout(timeout: Option<Duration>) -> Result<Self, TransportError> {
        Self::with_limits(
            timeout,
            DEFAULT_MAX_RESPONSE_BODY_BYTES,
            DEFAULT_MAX_REQUEST_BODY_BYTES,
        )
    }

    /// 完整限制构造。
    ///
    /// - `max_*_bytes == 0`：关闭对应体上限（仅测试逃生口）。
    pub fn with_limits(
        timeout: Option<Duration>,
        max_response_body_bytes: usize,
        max_request_body_bytes: usize,
    ) -> Result<Self, TransportError> {
        Self::builder(
            timeout,
            max_response_body_bytes,
            max_request_body_bytes,
            None,
            None,
        )
    }

    /// 带 TLS 配置。
    pub fn with_tls(tls: TlsConfig) -> Result<Self, TransportError> {
        Self::builder(
            Some(DEFAULT_REQUEST_TIMEOUT),
            DEFAULT_MAX_RESPONSE_BODY_BYTES,
            DEFAULT_MAX_REQUEST_BODY_BYTES,
            Some(tls),
            None,
        )
    }

    /// 带代理配置。
    pub fn with_proxy(proxy: ProxyConfig) -> Result<Self, TransportError> {
        Self::builder(
            Some(DEFAULT_REQUEST_TIMEOUT),
            DEFAULT_MAX_RESPONSE_BODY_BYTES,
            DEFAULT_MAX_REQUEST_BODY_BYTES,
            None,
            Some(proxy),
        )
    }

    /// 完整构造：超时 / 体限 / TLS / 代理。
    pub fn builder(
        timeout: Option<Duration>,
        max_response_body_bytes: usize,
        max_request_body_bytes: usize,
        tls: Option<TlsConfig>,
        proxy: Option<ProxyConfig>,
    ) -> Result<Self, TransportError> {
        let mut builder = Client::builder();
        if let Some(timeout) = timeout {
            builder = builder.timeout(timeout);
        }
        if let Some(tls) = tls {
            if !tls.sni {
                return Err(TransportError::ProtocolViolation(
                    "SNI=false 当前未接线，拒绝静默忽略".into(),
                ));
            }
            match tls.mode {
                TlsMode::SystemRoots => {}
                TlsMode::CustomCa { path } => {
                    let pem = std::fs::read(&path).map_err(|e| {
                        TransportError::Io(Box::new(std::io::Error::new(
                            e.kind(),
                            format!("read custom CA {}: {e}", path.display()),
                        )))
                    })?;
                    // 当前 reqwest TLS 后端的 from_pem 对多数非法输入返回 Ok，
                    // 真正的编码错误往往在 Client::build。先做 PEM 头校验以便可测失败路径。
                    let pem_text = std::str::from_utf8(&pem).map_err(|_| {
                        TransportError::ProtocolViolation("invalid CA pem: not utf-8".into())
                    })?;
                    if !pem_text.contains("-----BEGIN CERTIFICATE-----") {
                        return Err(TransportError::ProtocolViolation(
                            "invalid CA pem: missing BEGIN CERTIFICATE".into(),
                        ));
                    }
                    // from_pem 在现后端通常延迟到 build 才报告编码错误；若后端在此
                    // 提前拒绝，也必须返回结构化错误，不能在库代码中 panic。
                    let cert =
                        reqwest::Certificate::from_pem(&pem).map_err(map_client_build_error)?;
                    builder = builder.add_root_certificate(cert);
                }
                TlsMode::InsecureDevOnly => {
                    builder = builder.danger_accept_invalid_certs(true);
                }
            }
        }
        if let Some(proxy_cfg) = proxy {
            builder = builder.proxy(build_reqwest_proxy(&proxy_cfg)?);
        }
        let client = builder.build().map_err(map_client_build_error)?;
        Ok(Self {
            client,
            max_response_body_bytes,
            max_request_body_bytes,
        })
    }
}

/// ClientBuilder::build 失败映射（构建期错误统一为 Io）。
pub(crate) fn map_client_build_error(error: reqwest::Error) -> TransportError {
    TransportError::Io(Box::new(error))
}

/// 将 reqwest 错误映射为 [`TransportError`]。
///
/// 仅在 crate 内复用，保持构建请求、执行请求和读取响应的映射一致。
pub(crate) fn map_reqwest_error(error: reqwest::Error) -> TransportError {
    if let Some(timeout) = classify_reqwest_timeout(error.is_timeout(), error.is_connect()) {
        timeout
    } else if error.is_builder() {
        TransportError::ProtocolViolation(error.to_string())
    } else {
        TransportError::Io(Box::new(error))
    }
}

fn classify_reqwest_timeout(is_timeout: bool, is_connect: bool) -> Option<TransportError> {
    if !is_timeout {
        None
    } else if is_connect {
        Some(TransportError::ConnectTimeout)
    } else {
        Some(TransportError::ReadTimeout)
    }
}

#[async_trait]
impl HttpDriver for ReqwestHttpDriver {
    async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        if let Some(ref body) = request.body {
            if self.max_request_body_bytes > 0 && body.len() > self.max_request_body_bytes {
                return Err(TransportError::PayloadTooLarge {
                    kind: "request_body",
                    limit: self.max_request_body_bytes,
                    got: body.len(),
                });
            }
        }
        let method = Method::from_bytes(request.method.as_bytes())
            .map_err(|error| TransportError::ProtocolViolation(error.to_string()))?;
        let mut builder = self.client.request(method, &request.url);
        for (name, value) in request.headers {
            builder = builder.header(name, value);
        }
        if let Some(body) = request.body {
            builder = builder.body(body);
        }
        let request = builder.build().map_err(map_reqwest_error)?;
        let mut response = self
            .client
            .execute(request)
            .await
            .map_err(map_reqwest_error)?;
        let status = response.status();
        if status.as_u16() == 429 {
            let retry_after = response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| parse_retry_after_at(value, SystemTime::now()));
            return Err(TransportError::RateLimited { retry_after });
        }
        if let Some(cl) = response.content_length() {
            let cl = usize::try_from(cl).unwrap_or(usize::MAX);
            if self.max_response_body_bytes > 0 && cl > self.max_response_body_bytes {
                return Err(TransportError::PayloadTooLarge {
                    kind: "response_body",
                    limit: self.max_response_body_bytes,
                    got: cl,
                });
            }
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(map_reqwest_error)? {
            let got = body.len().saturating_add(chunk.len());
            if self.max_response_body_bytes > 0 && got > self.max_response_body_bytes {
                return Err(TransportError::PayloadTooLarge {
                    kind: "response_body",
                    limit: self.max_response_body_bytes,
                    got,
                });
            }
            body.extend_from_slice(&chunk);
        }
        Ok(HttpResponse {
            status: status.as_u16(),
            body: Bytes::from(body),
        })
    }
}

// ---------------------------------------------------------------------------
// Tungstenite WebSocket driver
// ---------------------------------------------------------------------------

/// tokio-tungstenite-backed WebSocket connector. Driver-specific stream types
/// stay private behind [`WsConnection`].
///
/// 默认：连接超时 30s、单帧上限 4 MiB。
#[derive(Debug, Clone, Copy)]
pub struct TungsteniteWsConnector {
    connect_timeout: Duration,
    max_frame_bytes: usize,
}

impl Default for TungsteniteWsConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl TungsteniteWsConnector {
    /// 创建默认连接器（连接超时 + 帧上限见 `DEFAULT_WS_*`）。
    pub const fn new() -> Self {
        Self {
            connect_timeout: DEFAULT_WS_CONNECT_TIMEOUT,
            max_frame_bytes: DEFAULT_MAX_WS_FRAME_BYTES,
        }
    }

    /// 自定义连接超时与单帧上限；`max_frame_bytes == 0` 关闭帧上限。
    pub const fn with_limits(connect_timeout: Duration, max_frame_bytes: usize) -> Self {
        Self {
            connect_timeout,
            max_frame_bytes,
        }
    }
}

type TungsteniteStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct TungsteniteWsConnection {
    stream: TungsteniteStream,
    max_frame_bytes: usize,
}

/// 将 tungstenite 错误映射为 [`TransportError`]。
pub(crate) fn map_tungstenite_error(
    error: tokio_tungstenite::tungstenite::Error,
) -> TransportError {
    use tokio_tungstenite::tungstenite::Error;
    match error {
        Error::ConnectionClosed | Error::AlreadyClosed => {
            TransportError::ConnectionClosed { clean: true }
        }
        Error::Io(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::BrokenPipe
            ) =>
        {
            TransportError::ConnectionClosed { clean: false }
        }
        Error::Io(error) => TransportError::Io(Box::new(error)),
        Error::Protocol(
            tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
        ) => TransportError::ConnectionClosed { clean: false },
        Error::Protocol(error) => TransportError::ProtocolViolation(error.to_string()),
        Error::Url(error) => TransportError::ProtocolViolation(error.to_string()),
        Error::Capacity(tokio_tungstenite::tungstenite::error::CapacityError::MessageTooLong {
            size,
            max_size,
        }) => TransportError::PayloadTooLarge {
            kind: "ws_message",
            limit: max_size,
            got: size,
        },
        other => TransportError::ProtocolViolation(other.to_string()),
    }
}

fn enforce_frame_limit(max_frame_bytes: usize, payload: Bytes) -> Result<Bytes, TransportError> {
    if max_frame_bytes > 0 && payload.len() > max_frame_bytes {
        return Err(TransportError::PayloadTooLarge {
            kind: "ws_frame",
            limit: max_frame_bytes,
            got: payload.len(),
        });
    }
    Ok(payload)
}

#[async_trait]
impl WsConnector for TungsteniteWsConnector {
    async fn connect(&self, url: &str) -> Result<Box<dyn WsConnection>, TransportError> {
        let inbound_limit = (self.max_frame_bytes > 0).then_some(self.max_frame_bytes);
        let config = WebSocketConfig::default()
            .max_frame_size(inbound_limit)
            .max_message_size(inbound_limit);
        let fut = connect_async_with_config(url, Some(config), false);
        let (stream, _) = tokio::time::timeout(self.connect_timeout, fut)
            .await
            .map_err(|_| TransportError::ConnectTimeout)?
            .map_err(map_tungstenite_error)?;
        Ok(Box::new(TungsteniteWsConnection {
            stream,
            max_frame_bytes: self.max_frame_bytes,
        }))
    }
}

#[async_trait]
impl WsConnection for TungsteniteWsConnection {
    async fn next_frame(&mut self) -> Result<Option<Bytes>, TransportError> {
        while let Some(message) = self.stream.next().await {
            match message.map_err(map_tungstenite_error)? {
                Message::Text(text) => {
                    let bytes = Bytes::copy_from_slice(text.as_bytes());
                    return Ok(Some(enforce_frame_limit(self.max_frame_bytes, bytes)?));
                }
                Message::Binary(bytes) => {
                    return Ok(Some(enforce_frame_limit(self.max_frame_bytes, bytes)?));
                }
                Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => continue,
                Message::Close(_) => return Ok(None),
            }
        }
        Err(TransportError::ConnectionClosed { clean: false })
    }

    async fn send_frame(&mut self, frame: Bytes) -> Result<(), TransportError> {
        let frame = enforce_frame_limit(self.max_frame_bytes, frame)?;
        self.stream
            .send(Message::Binary(frame))
            .await
            .map_err(map_tungstenite_error)
    }

    async fn close(&mut self) -> Result<(), TransportError> {
        self.stream
            .send(Message::Close(None))
            .await
            .map_err(map_tungstenite_error)
    }
}

// ---------------------------------------------------------------------------
// 内存 Mock
// ---------------------------------------------------------------------------

/// 内存模拟 HTTP 传输（不依赖任何 HTTP 驱动）。
///
/// 通过 [`MockHttpTransport::set_get`] / [`MockHttpTransport::set_post`]
/// 预置 `(url, response)` 映射；未预置的 url 返回
/// [`TransportError::ProtocolViolation`]，不支持的方法同样返回 `ProtocolViolation`。
/// POST 的 `body` 参数在 mock 中被忽略（不校验请求体）。
#[derive(Debug, Default)]
pub struct MockHttpTransport {
    gets: RwLock<HashMap<String, Bytes>>,
    posts: RwLock<HashMap<String, Bytes>>,
}

impl MockHttpTransport {
    /// 创建空 mock。
    pub fn new() -> Self {
        Self::default()
    }

    /// 预置 GET `url` 的响应。
    pub fn set_get(&self, url: &str, response: Bytes) {
        // 锁中毒（此前持写锁线程 panic）时恢复内层数据；mock 辅助路径不回传错误。
        self.gets
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(url.to_string(), response);
    }

    /// 预置 POST `url` 的响应。
    pub fn set_post(&self, url: &str, response: Bytes) {
        // 锁中毒（此前持写锁线程 panic）时恢复内层数据；mock 辅助路径不回传错误。
        self.posts
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(url.to_string(), response);
    }
}

#[async_trait]
impl HttpDriver for MockHttpTransport {
    async fn execute(&self, request: HttpRequest) -> Result<HttpResponse, TransportError> {
        let response = match request.method.to_ascii_uppercase().as_str() {
            "GET" => self
                .gets
                .read()
                .map_err(|_| TransportError::Io(Box::new(std::io::Error::other("mock lock"))))?
                .get(&request.url)
                .cloned(),
            "POST" => self
                .posts
                .read()
                .map_err(|_| TransportError::Io(Box::new(std::io::Error::other("mock lock"))))?
                .get(&request.url)
                .cloned(),
            method => {
                return Err(TransportError::ProtocolViolation(format!(
                    "mock method unsupported: {method}"
                )));
            }
        };
        response
            .map(|body| HttpResponse { status: 200, body })
            .ok_or_else(|| {
                TransportError::ProtocolViolation(format!(
                    "mock response missing for {} {}",
                    request.method, request.url
                ))
            })
    }
}

#[cfg(test)]
mod builder_tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn with_tls_system_and_insecure() {
        let _ = ReqwestHttpDriver::with_tls(TlsConfig::system_roots()).expect("system roots");
        let _ = ReqwestHttpDriver::with_tls(TlsConfig::insecure_dev_only()).expect("insecure");
    }

    #[test]
    fn with_tls_custom_ca_missing_and_valid() {
        let missing = TlsConfig::custom_ca("/tmp/definitely-no-such-ca-file-transportx.pem");
        assert!(ReqwestHttpDriver::with_tls(missing).is_err());

        let dir = std::env::temp_dir().join(format!("transportx-ca-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        // 缺 PEM 头 → ProtocolViolation
        {
            let bad = dir.join("bad.pem");
            let mut f = std::fs::File::create(&bad).unwrap();
            write!(f, "not-a-pem-at-all").unwrap();
            let err = ReqwestHttpDriver::with_tls(TlsConfig::custom_ca(&bad)).unwrap_err();
            assert!(
                matches!(err, TransportError::ProtocolViolation(_)),
                "expected missing PEM header, got {err:?}"
            );
        }
        // 非 utf-8 → ProtocolViolation
        {
            let bad = dir.join("bin.pem");
            std::fs::write(&bad, [0xff, 0xfe, 0xfd]).unwrap();
            let err = ReqwestHttpDriver::with_tls(TlsConfig::custom_ca(&bad)).unwrap_err();
            assert!(
                matches!(err, TransportError::ProtocolViolation(_)),
                "expected non-utf8 pem, got {err:?}"
            );
        }
        // 源码内固定 PEM fixture 验证正向路径，不依赖发行版 CA 安装位置。
        {
            let path = dir.join("ca.pem");
            std::fs::write(
                &path,
                include_bytes!("../tests/fixtures/test-ca.pem.fixture"),
            )
            .expect("可写入固定 CA fixture");
            let _ = ReqwestHttpDriver::with_tls(TlsConfig::custom_ca(&path)).expect("有效 CA");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn with_proxy_and_builder_combo() {
        let _ =
            ReqwestHttpDriver::with_proxy(ProxyConfig::new("http://127.0.0.1:9")).expect("proxy");
        let _ = ReqwestHttpDriver::builder(
            Some(Duration::from_secs(1)),
            1024,
            1024,
            Some(TlsConfig::system_roots()),
            Some(ProxyConfig::with_auth(
                "http://127.0.0.1:9",
                "u",
                format!("{}{}", "p", "w"),
            )),
        )
        .expect("builder combo");
    }

    #[test]
    fn private_reqwest_mappers_cover_builder_and_io_branches() {
        let invalid_header = || {
            reqwest::Client::new()
                .request(reqwest::Method::GET, "http://example.com")
                .header("X-Bad", "\n")
                .build()
                .expect_err("非法 header 必须产生 reqwest 错误")
        };

        assert!(matches!(
            map_reqwest_error(invalid_header()),
            TransportError::ProtocolViolation(_)
        ));
        assert!(matches!(
            map_client_build_error(invalid_header()),
            TransportError::Io(_)
        ));
    }

    #[test]
    fn private_reqwest_timeout_classifier_is_deterministic() {
        assert!(matches!(
            classify_reqwest_timeout(true, true),
            Some(TransportError::ConnectTimeout)
        ));
        assert!(matches!(
            classify_reqwest_timeout(true, false),
            Some(TransportError::ReadTimeout)
        ));
        assert!(classify_reqwest_timeout(false, true).is_none());
    }

    #[test]
    fn private_tungstenite_mapper_covers_error_variants() {
        use tokio_tungstenite::tungstenite::error::{Error, ProtocolError, UrlError};

        assert!(matches!(
            map_tungstenite_error(Error::ConnectionClosed),
            TransportError::ConnectionClosed { clean: true }
        ));
        assert!(matches!(
            map_tungstenite_error(Error::AlreadyClosed),
            TransportError::ConnectionClosed { clean: true }
        ));
        assert!(matches!(
            map_tungstenite_error(Error::Io(std::io::Error::from(
                std::io::ErrorKind::UnexpectedEof
            ))),
            TransportError::ConnectionClosed { clean: false }
        ));
        assert!(matches!(
            map_tungstenite_error(Error::Io(std::io::Error::other("e"))),
            TransportError::Io(_)
        ));
        assert!(matches!(
            map_tungstenite_error(Error::Protocol(ProtocolError::ResetWithoutClosingHandshake)),
            TransportError::ConnectionClosed { clean: false }
        ));
        assert!(matches!(
            map_tungstenite_error(Error::Protocol(ProtocolError::InvalidOpcode(3))),
            TransportError::ProtocolViolation(_)
        ));
        assert!(matches!(
            map_tungstenite_error(Error::Url(UrlError::UnableToConnect("x".into()))),
            TransportError::ProtocolViolation(_)
        ));
        assert!(matches!(
            map_tungstenite_error(Error::Utf8("invalid utf8".into())),
            TransportError::ProtocolViolation(_)
        ));
    }

    #[tokio::test]
    async fn private_mock_lock_poison_paths_map_to_io() {
        let driver = MockHttpTransport::new();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = driver.gets.write().expect("可取得 GET mock 锁");
            panic!("投毒 GET mock 锁");
        }));
        let get_error = driver
            .execute(HttpRequest {
                method: "GET".into(),
                url: "u".into(),
                headers: Vec::new(),
                body: None,
            })
            .await
            .expect_err("中毒 GET 锁必须映射为错误");
        assert!(matches!(get_error, TransportError::Io(_)));

        let driver = MockHttpTransport::new();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = driver.posts.write().expect("可取得 POST mock 锁");
            panic!("投毒 POST mock 锁");
        }));
        let post_error = driver
            .execute(HttpRequest {
                method: "POST".into(),
                url: "u".into(),
                headers: Vec::new(),
                body: None,
            })
            .await
            .expect_err("中毒 POST 锁必须映射为错误");
        assert!(matches!(post_error, TransportError::Io(_)));
    }
}

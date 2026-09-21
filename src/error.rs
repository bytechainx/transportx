//! 传输层错误类型：驱动无关的错误面，保留足够的语义供重连策略决策。

use std::time::Duration;

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

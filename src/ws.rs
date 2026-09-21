//! WebSocket 驱动：`tokio-tungstenite` 实现（驱动私有流类型封装在 crate 内部）。
//!
//! 默认连接超时 30s、单帧上限 4 MiB（见 [`crate::DEFAULT_WS_CONNECT_TIMEOUT`] /
//! [`crate::DEFAULT_MAX_WS_FRAME_BYTES`]）；帧超限 → [`TransportError::PayloadTooLarge`]。
//! 驱动无关的边界 trait 见 [`crate::WsConnector`] / [`crate::WsConnection`]。

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{protocol::WebSocketConfig, Message},
    MaybeTlsStream, WebSocketStream,
};

use crate::TransportError;

// ---------------------------------------------------------------------------
// Tungstenite WebSocket driver
// ---------------------------------------------------------------------------

/// tokio-tungstenite-backed WebSocket connector. Driver-specific stream types
/// stay private behind [`crate::WsConnection`].
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
            connect_timeout: crate::DEFAULT_WS_CONNECT_TIMEOUT,
            max_frame_bytes: crate::DEFAULT_MAX_WS_FRAME_BYTES,
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
impl crate::WsConnector for TungsteniteWsConnector {
    async fn connect(&self, url: &str) -> Result<Box<dyn crate::WsConnection>, TransportError> {
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
impl crate::WsConnection for TungsteniteWsConnection {
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

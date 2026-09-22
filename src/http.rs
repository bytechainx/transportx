//! reqwest 默认 HTTP 驱动。
//!
//! 公开类型 [`ReqwestHttpDriver`] 经 crate 根 `pub use` 导出，路径与拆分前一致；
//! 本模块只承载实现，不含边界 trait（见 crate 根的 [`crate::HttpDriver`]）。

use std::fmt;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use reqwest::{Client, Method};

use crate::{
    build_reqwest_proxy, parse_retry_after_at, HttpDriver, HttpRequest, HttpResponse, ProxyConfig,
    TlsConfig, TlsMode, TransportError, DEFAULT_MAX_REQUEST_BODY_BYTES,
    DEFAULT_MAX_RESPONSE_BODY_BYTES, DEFAULT_REQUEST_TIMEOUT,
};

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

pub(crate) fn classify_reqwest_timeout(
    is_timeout: bool,
    is_connect: bool,
) -> Option<TransportError> {
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

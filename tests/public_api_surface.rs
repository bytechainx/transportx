#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! transportx 公开面：错误、Mock、Reqwest 构造、Tungstenite connector。

use bytes::Bytes;
use transportx::{
    HttpDriver, HttpRequest, MockHttpTransport, ReqwestHttpDriver, TransportError,
    TungsteniteWsConnector,
};

#[tokio::test]
async fn mock_http_get_post_and_miss() {
    let mock = MockHttpTransport::default();
    mock.set_get("https://x/a", Bytes::from_static(b"A"));
    mock.set_post("https://x/b", Bytes::from_static(b"B"));

    let get = mock
        .execute(HttpRequest {
            method: "GET".into(),
            url: "https://x/a".into(),
            headers: vec![("h".into(), "v".into())],
            body: None,
        })
        .await
        .expect("get");
    assert_eq!(get.status, 200);
    assert_eq!(get.body.as_ref(), b"A");

    let post = mock
        .execute(HttpRequest {
            method: "POST".into(),
            url: "https://x/b".into(),
            headers: vec![],
            body: Some(Bytes::from_static(b"{}")),
        })
        .await
        .expect("post");
    assert_eq!(post.body.as_ref(), b"B");

    let miss = mock
        .execute(HttpRequest {
            method: "GET".into(),
            url: "https://x/missing".into(),
            headers: vec![],
            body: None,
        })
        .await;
    assert!(miss.is_err());

    assert!(!TransportError::ConnectTimeout.to_string().is_empty());
    assert!(!TransportError::ReadTimeout.to_string().is_empty());
    assert!(!TransportError::ConnectionClosed { clean: true }
        .to_string()
        .is_empty());
    assert!(!TransportError::RateLimited { retry_after: None }
        .to_string()
        .is_empty());
    assert!(!TransportError::ProtocolViolation("x".into())
        .to_string()
        .is_empty());
    assert!(!TransportError::PayloadTooLarge {
        kind: "response_body",
        limit: 1,
        got: 2
    }
    .to_string()
    .is_empty());
}

#[test]
fn drivers_construct() {
    let d = ReqwestHttpDriver::new().expect("reqwest");
    let _ = ReqwestHttpDriver::with_timeout(None).expect("timeout none");
    let _ = format!("{d:?}");
    let ws = TungsteniteWsConnector::new();
    let _ = format!("{ws:?}");
}

/// 对抗审查 Top10 #3 修复面：`PoolExhausted` 变体的 Display 与
/// `is_retryable()` 可重试 / 永久错误分类契约。
#[test]
fn pool_exhausted_display_and_retryable_classification() {
    // 新变体 Display 非空且携带上限上下文。
    let exhausted = TransportError::PoolExhausted { limit: 8 };
    let text = exhausted.to_string();
    assert!(!text.is_empty());
    assert!(text.contains('8'), "文案应包含池上限：{text}");
    assert!(exhausted.is_retryable(), "池耗尽必须分类为可重试");

    // 临时状况类：可重试。
    assert!(TransportError::ConnectTimeout.is_retryable());
    assert!(TransportError::ReadTimeout.is_retryable());
    assert!(TransportError::ConnectionClosed { clean: true }.is_retryable());
    assert!(TransportError::ConnectionClosed { clean: false }.is_retryable());
    assert!(TransportError::RateLimited { retry_after: None }.is_retryable());
    assert!(TransportError::Io(Box::new(std::io::Error::other("io"))).is_retryable());

    // 永久错误类（fail-closed）：不可重试。
    assert!(!TransportError::PayloadTooLarge {
        kind: "response_body",
        limit: 1,
        got: 2
    }
    .is_retryable());
    assert!(!TransportError::ProtocolViolation("x".into()).is_retryable());
}

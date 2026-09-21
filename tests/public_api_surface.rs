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

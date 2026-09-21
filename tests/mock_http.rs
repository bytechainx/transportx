#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! `MockHttpTransport` 与 `HttpDriver` 行为测试（纯内存，无网络）。

use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;
use transportx::{HttpDriver, HttpRequest, HttpResponse, MockHttpTransport, TransportError};

fn get_request(url: &str) -> HttpRequest {
    HttpRequest {
        method: "GET".into(),
        url: url.into(),
        headers: Vec::new(),
        body: None,
    }
}

#[tokio::test]
async fn driver_get_returns_preset_response() {
    let driver = MockHttpTransport::new();
    driver.set_get("https://api/ping", Bytes::from_static(b"{}"));
    let response = driver
        .execute(get_request("https://api/ping"))
        .await
        .unwrap();
    assert_eq!(response.body, Bytes::from_static(b"{}"));
}

#[tokio::test]
async fn driver_missing_url_returns_protocol_violation() {
    let driver = MockHttpTransport::new();
    let err = driver
        .execute(get_request("https://api/nope"))
        .await
        .unwrap_err();
    match err {
        TransportError::ProtocolViolation(msg) => {
            assert!(msg.contains("missing"), "应说明响应缺失: {msg}");
            assert!(msg.contains("https://api/nope"), "应回显 url: {msg}");
        }
        other => panic!("unexpected: {other:?}"),
    }
}

#[tokio::test]
async fn driver_post_returns_preset_response_ignoring_body() {
    let driver = MockHttpTransport::new();
    driver.set_post("https://api/order", Bytes::from_static(b"ack"));
    let response = driver
        .execute(HttpRequest {
            method: "POST".into(),
            url: "https://api/order".into(),
            headers: Vec::new(),
            body: Some(Bytes::from_static(b"body-ignored")),
        })
        .await
        .unwrap();
    assert_eq!(response.body, Bytes::from_static(b"ack"));
}

#[tokio::test]
async fn set_get_overwrites_previous_response() {
    let driver = MockHttpTransport::new();
    driver.set_get("u", Bytes::from_static(b"a"));
    driver.set_get("u", Bytes::from_static(b"b"));
    assert_eq!(
        driver.execute(get_request("u")).await.unwrap().body,
        Bytes::from_static(b"b")
    );
}

#[tokio::test]
async fn set_post_overwrites_previous_response() {
    let driver = MockHttpTransport::new();
    driver.set_post("p", Bytes::from_static(b"1"));
    driver.set_post("p", Bytes::from_static(b"2"));
    let response = driver
        .execute(HttpRequest {
            method: "POST".into(),
            url: "p".into(),
            headers: Vec::new(),
            body: Some(Bytes::new()),
        })
        .await
        .unwrap();
    assert_eq!(response.body.as_ref(), b"2");
}

#[tokio::test]
async fn get_and_post_are_isolated() {
    let driver = MockHttpTransport::new();
    driver.set_get("u", Bytes::from_static(b"g"));
    driver.set_post("u", Bytes::from_static(b"p"));
    assert_eq!(
        driver.execute(get_request("u")).await.unwrap().body,
        Bytes::from_static(b"g")
    );
    let response = driver
        .execute(HttpRequest {
            method: "POST".into(),
            url: "u".into(),
            headers: Vec::new(),
            body: Some(Bytes::new()),
        })
        .await
        .unwrap();
    assert_eq!(response.body, Bytes::from_static(b"p"));
}

#[test]
fn transport_error_keeps_reconnect_semantics() {
    let err = TransportError::ConnectionClosed { clean: false };
    assert_eq!(err.to_string(), "connection closed (false)");
    let err = TransportError::RateLimited {
        retry_after: Some(Duration::from_secs(2)),
    };
    assert!(err.to_string().contains("2s"));
    assert_eq!(
        TransportError::ConnectTimeout.to_string(),
        "connect timeout"
    );
    assert_eq!(TransportError::ReadTimeout.to_string(), "read timeout");
    assert!(TransportError::ProtocolViolation("bad".into())
        .to_string()
        .contains("bad"));
    assert!(TransportError::Io(Box::new(std::io::Error::other("x")))
        .to_string()
        .contains("x"));
}

#[test]
fn request_headers_are_part_of_the_boundary() {
    let request = HttpRequest {
        method: "POST".into(),
        url: "https://api/order".into(),
        headers: vec![("X-Token".into(), "redacted".into())],
        body: Some(Bytes::from_static(b"{}")),
    };
    assert_eq!(request.headers[0].0, "X-Token");
    assert_eq!(request.body.as_ref().unwrap().as_ref(), b"{}");
    let response = HttpResponse {
        status: 200,
        body: Bytes::from_static(b"x"),
    };
    assert_eq!(response, response.clone());
    let _ = format!("{request:?}{response:?}");
}

#[tokio::test]
async fn mock_http_driver_returns_transport_response() {
    let driver = MockHttpTransport::new();
    driver.set_get("https://api/ping", Bytes::from_static(b"{}"));
    let response = driver
        .execute(get_request("https://api/ping"))
        .await
        .unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(response.body, Bytes::from_static(b"{}"));
}

#[tokio::test]
async fn mock_http_driver_post_and_case_insensitive_method() {
    let driver = MockHttpTransport::new();
    driver.set_post("https://api/order", Bytes::from_static(b"ok"));
    let response = driver
        .execute(HttpRequest {
            method: "post".into(),
            url: "https://api/order".into(),
            headers: Vec::new(),
            body: Some(Bytes::from_static(b"ignored")),
        })
        .await
        .unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(response.body, Bytes::from_static(b"ok"));
}

#[tokio::test]
async fn mock_http_driver_unsupported_method() {
    let driver = MockHttpTransport::new();
    let err = driver
        .execute(HttpRequest {
            method: "PUT".into(),
            url: "https://api/x".into(),
            headers: Vec::new(),
            body: None,
        })
        .await
        .unwrap_err();
    match err {
        TransportError::ProtocolViolation(msg) => assert!(msg.contains("unsupported")),
        other => panic!("unexpected: {other:?}"),
    }
}

#[tokio::test]
async fn mock_as_arc_dyn_http_driver() {
    let mock = Arc::new(MockHttpTransport::new());
    mock.set_get("u", Bytes::from_static(b"v"));
    let driver: Arc<dyn HttpDriver> = mock;
    let r = driver.execute(get_request("u")).await.unwrap();
    assert_eq!(r.body.as_ref(), b"v");
}

#[test]
fn mock_debug() {
    let m = MockHttpTransport::default();
    let _ = format!("{m:?}");
}

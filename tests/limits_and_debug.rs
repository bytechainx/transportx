#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 敏感 Debug 脱敏、deadline 默认、body/frame 上限 fail-closed。

use bytes::Bytes;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::thread;
use std::time::Duration;
use transportx::{
    is_sensitive_header_name, HttpDriver, HttpRequest, HttpResponse, ProxyConfig,
    ReqwestHttpDriver, TlsConfig, TlsMode, TransportError, TungsteniteWsConnector, WsConnector,
    DEFAULT_MAX_REQUEST_BODY_BYTES, DEFAULT_MAX_RESPONSE_BODY_BYTES, DEFAULT_REQUEST_TIMEOUT,
};

fn spawn_stalling_chunked_server(
    first_chunk: &'static [u8],
    second_chunk: &'static [u8],
) -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = [0_u8; 4096];
        let _ = stream.read(&mut buf);
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
            .unwrap();
        stream
            .write_all(format!("{:X}\r\n", first_chunk.len()).as_bytes())
            .unwrap();
        stream.write_all(first_chunk).unwrap();
        stream.write_all(b"\r\n").unwrap();
        stream.flush().unwrap();
        thread::sleep(Duration::from_millis(20));
        stream
            .write_all(format!("{:X}\r\n", second_chunk.len()).as_bytes())
            .unwrap();
        stream.write_all(second_chunk).unwrap();
        stream.write_all(b"\r\n").unwrap();
        stream.flush().unwrap();
        thread::sleep(Duration::from_secs(2));
        let _ = stream.write_all(b"0\r\n\r\n");
    });
    addr
}

fn spawn_http_server(
    status_line: &'static str,
    headers: &'static [(&'static str, &'static str)],
    body: &'static [u8],
) -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = [0u8; 4096];
        let _ = stream.read(&mut buf);
        let mut response = format!("{status_line}\r\n");
        for (k, v) in headers {
            response.push_str(&format!("{k}: {v}\r\n"));
        }
        response.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
        stream.write_all(response.as_bytes()).unwrap();
        stream.write_all(body).unwrap();
    });
    addr
}

#[test]
fn sensitive_header_names_detected() {
    assert!(is_sensitive_header_name("Authorization"));
    assert!(is_sensitive_header_name("cookie"));
    assert!(is_sensitive_header_name("X-Api-Key"));
    assert!(is_sensitive_header_name("x-session-token"));
    // OKX v5 鉴权头必须脱敏
    assert!(is_sensitive_header_name("OK-ACCESS-KEY"));
    assert!(is_sensitive_header_name("OK-ACCESS-PASSPHRASE"));
    assert!(is_sensitive_header_name("OK-ACCESS-SIGN"));
    assert!(is_sensitive_header_name("ok-access-timestamp"));
    assert!(!is_sensitive_header_name("Accept"));
    assert!(!is_sensitive_header_name("Content-Type"));
}

#[test]
fn http_request_debug_redacts_secrets_and_body() {
    let req = HttpRequest {
        method: "POST".into(),
        url: "https://api.example/v1".into(),
        headers: vec![
            ("Authorization".into(), "Bearer super-secret-token".into()),
            ("Accept".into(), "application/json".into()),
        ],
        body: Some(Bytes::from_static(b"{\"password\":\"hunter2\"}")),
    };
    let dbg = format!("{req:?}");
    assert!(dbg.contains("***"), "auth must be redacted: {dbg}");
    assert!(!dbg.contains("super-secret-token"), "token leaked: {dbg}");
    assert!(!dbg.contains("hunter2"), "body leaked: {dbg}");
    assert!(
        dbg.contains("<32 bytes>") || dbg.contains("bytes"),
        "body len: {dbg}"
    );
    assert!(dbg.contains("application/json"));
}

#[test]
fn request_and_proxy_debug_only_expose_safe_url_origin() {
    let secret_url = "https://alice:login-secret@api.example:8443/path-credential/query-secret?no-value-query-key&token=query-secret#fragment-secret";
    let request = HttpRequest {
        method: "GET".into(),
        url: secret_url.into(),
        headers: vec![],
        body: None,
    };
    let request_debug = format!("{request:?}");
    assert!(request_debug.contains("https://api.example:8443"));
    for leaked in [
        "alice",
        "login-secret",
        "path-credential",
        "no-value-query-key",
        "query-secret",
        "fragment-secret",
    ] {
        assert!(
            !request_debug.contains(leaked),
            "请求 URL 泄漏敏感值：{request_debug}"
        );
    }

    let proxy = ProxyConfig::new(secret_url);
    let proxy_debug = format!("{proxy:?}");
    assert!(proxy_debug.contains("https://api.example:8443"));
    for leaked in [
        "alice",
        "login-secret",
        "path-credential",
        "no-value-query-key",
        "query-secret",
        "fragment-secret",
    ] {
        assert!(
            !proxy_debug.contains(leaked),
            "代理 URL 泄漏敏感值：{proxy_debug}"
        );
    }
}

#[test]
fn invalid_url_debug_is_fail_closed() {
    let raw = "not a url?token=must-not-leak";
    let request = HttpRequest {
        method: "GET".into(),
        url: raw.into(),
        headers: vec![],
        body: None,
    };
    let debug = format!("{request:?}");
    assert!(
        !debug.contains("must-not-leak"),
        "无效 URL 泄漏敏感值：{debug}"
    );
    assert!(debug.contains("<invalid-url-redacted>"));

    let opaque = HttpRequest {
        method: "GET".into(),
        url: "mailto:alice@example.test?token=opaque-secret".into(),
        headers: vec![],
        body: None,
    };
    let debug = format!("{opaque:?}");
    assert!(
        !debug.contains("opaque-secret"),
        "不透明 URL 泄漏敏感值：{debug}"
    );
    assert!(debug.contains("<invalid-url-redacted>"));
}

#[test]
fn disabled_sni_is_rejected_instead_of_silently_ignored() {
    let tls = TlsConfig {
        mode: TlsMode::SystemRoots,
        sni: false,
    };
    let error = ReqwestHttpDriver::with_tls(tls).expect_err("未接线的 SNI=false 必须关闭失败");
    assert!(
        matches!(error, TransportError::ProtocolViolation(_)),
        "实际错误：{error:?}"
    );
    assert!(error.to_string().contains("SNI"));
}

#[test]
fn http_response_debug_hides_body_bytes() {
    let resp = HttpResponse {
        status: 200,
        body: Bytes::from_static(b"secret-payload"),
    };
    let dbg = format!("{resp:?}");
    assert!(!dbg.contains("secret-payload"), "body leaked: {dbg}");
    assert!(
        dbg.contains("14") || dbg.contains("bytes"),
        "len missing: {dbg}"
    );
}

#[test]
fn reqwest_new_uses_production_defaults() {
    let d = ReqwestHttpDriver::new().expect("new");
    let dbg = format!("{d:?}");
    assert!(dbg.contains(&DEFAULT_MAX_RESPONSE_BODY_BYTES.to_string()));
    assert!(dbg.contains(&DEFAULT_MAX_REQUEST_BODY_BYTES.to_string()));
    assert!(
        !dbg.to_lowercase().contains("client {"),
        "must not dump client: {dbg}"
    );
    // 常量文档契约：默认 30s（实现侧 with_timeout(Some(DEFAULT))）
    assert_eq!(DEFAULT_REQUEST_TIMEOUT, Duration::from_secs(30));
}

#[tokio::test]
async fn request_body_over_limit_fail_closed() {
    let driver =
        ReqwestHttpDriver::with_limits(Some(Duration::from_secs(2)), 1024, 8).expect("driver");
    let err = driver
        .execute(HttpRequest {
            method: "POST".into(),
            url: "http://127.0.0.1:1/never".into(),
            headers: vec![],
            body: Some(Bytes::from(vec![0u8; 16])),
        })
        .await
        .expect_err("must reject oversize request body before network");
    match err {
        TransportError::PayloadTooLarge { kind, limit, got } => {
            assert_eq!(kind, "request_body");
            assert_eq!(limit, 8);
            assert_eq!(got, 16);
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn response_body_over_limit_fail_closed() {
    let body = b"0123456789abcdef"; // 16 bytes
    let addr = spawn_http_server("HTTP/1.1 200 OK", &[], body);
    let driver =
        ReqwestHttpDriver::with_limits(Some(Duration::from_secs(5)), 8, 1024).expect("driver");
    let err = driver
        .execute(HttpRequest {
            method: "GET".into(),
            url: format!("http://{addr}/big"),
            headers: vec![],
            body: None,
        })
        .await
        .expect_err("must reject oversize response");
    match err {
        TransportError::PayloadTooLarge { kind, limit, got } => {
            assert_eq!(kind, "response_body");
            assert_eq!(limit, 8);
            assert!(got >= 8, "got={got}");
        }
        other => panic!("unexpected {other:?}"),
    }
}

#[tokio::test]
async fn chunked_response_stops_at_first_cumulative_overflow() {
    let addr = spawn_stalling_chunked_server(b"12345", b"6789");
    let driver =
        ReqwestHttpDriver::with_limits(Some(Duration::from_secs(5)), 8, 1024).expect("驱动可构造");
    let result = tokio::time::timeout(
        Duration::from_millis(500),
        driver.execute(HttpRequest {
            method: "GET".into(),
            url: format!("http://{addr}/chunked"),
            headers: vec![],
            body: None,
        }),
    )
    .await
    .expect("累计越界后必须立即中止，不能等待 chunked 响应结束");
    let error = result.expect_err("chunked 响应体必须被拒绝");
    assert!(
        matches!(
            error,
            TransportError::PayloadTooLarge {
                kind: "response_body",
                limit: 8,
                got: 9
            }
        ),
        "实际错误：{error:?}"
    );
}

#[tokio::test]
async fn ws_send_frame_over_limit_fail_closed() {
    // 本地 echo 服务：accept + websocket handshake 太重；只测 connect 前 send 路径
    // 通过已连接 mock 不可用 → 使用真实 loopback 最小 WS 服务。
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        // 保持打开直到对端 close
        while let Some(Ok(_)) = futures_util::StreamExt::next(&mut ws).await {}
    });

    // 等服务就绪
    tokio::time::sleep(Duration::from_millis(50)).await;

    let connector = TungsteniteWsConnector::with_limits(Duration::from_secs(2), 8);
    let mut conn = connector
        .connect(&format!("ws://{addr}/"))
        .await
        .expect("connect");
    let err = conn
        .send_frame(Bytes::from(vec![0u8; 32]))
        .await
        .expect_err("frame limit");
    match err {
        TransportError::PayloadTooLarge { kind, limit, got } => {
            assert_eq!(kind, "ws_frame");
            assert_eq!(limit, 8);
            assert_eq!(got, 32);
        }
        other => panic!("unexpected {other:?}"),
    }
    let _ = conn.close().await;
}

#[tokio::test]
async fn ws_connect_timeout_maps_to_connect_timeout() {
    use tokio::net::TcpListener;

    // TCP 已在本地建立，但服务端永不返回 WebSocket 握手，精确覆盖握手 deadline。
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.unwrap();
        let _ = accepted_tx.send(());
        tokio::time::sleep(Duration::from_secs(1)).await;
    });

    let connector = TungsteniteWsConnector::with_limits(Duration::from_millis(100), 1024);
    let result = connector.connect(&format!("ws://{addr}/")).await;
    let err = match result {
        Ok(_) => panic!("expected connect failure"),
        Err(e) => e,
    };
    accepted_rx.await.expect("服务端必须已接受本地 TCP 连接");
    server.abort();
    assert!(
        matches!(err, TransportError::ConnectTimeout),
        "实际错误：{err:?}"
    );
}

#[test]
fn http_request_debug_none_body_branch() {
    let req = HttpRequest {
        method: "GET".into(),
        url: "https://api.example/".into(),
        headers: vec![],
        body: None,
    };
    let dbg = format!("{req:?}");
    assert!(
        dbg.contains("body: None"),
        "expected None body branch: {dbg}"
    );
}

#[test]
fn tungstenite_default_matches_new() {
    let a = TungsteniteWsConnector::default();
    let b = TungsteniteWsConnector::new();
    assert_eq!(format!("{a:?}"), format!("{b:?}"));
}

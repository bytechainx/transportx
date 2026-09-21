#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! TDD 行为契约（特性 002）。
//!
//! 入口集合 = `specs/002-*/contracts/public-api-contract.md` 中 `transportx` 全部入口。
//! 下表每个入口先在变异副本上观测应红、再在本树观测绿；实际执行的变异与红绿结果
//! 见 PR 描述（变异描述 + 失败用例名 + 复现命令）。
//!
//! 全部用例离线：HTTP 侧走内存 mock，WebSocket 侧走 `127.0.0.1:0` loopback。
//!
//! // TDD-PROBE: HttpDriver::execute | 变异：mock 未命中 URL 时返回空成功响应 | 红=http_driver_execute_returns_response_and_rejects_unknown | 绿=http_driver_execute_returns_response_and_rejects_unknown
//! // TDD-PROBE: WsConnector::connect | 变异：非法 URL 也返回 Ok | 红=ws_connector_connect_establishes_connection | 绿=ws_connector_connect_establishes_connection
//! // TDD-PROBE: WsConnection::next_frame | 变异：Ping/Pong 控制帧被当作应用帧返回 | 红=ws_connection_next_frame_skips_control_frames | 绿=ws_connection_next_frame_skips_control_frames
//! // TDD-PROBE: WsConnection::send_frame | 变异：发送不落到对端（丢帧） | 红=ws_connection_send_frame_reaches_peer | 绿=ws_connection_send_frame_reaches_peer
//! // TDD-PROBE: ReqwestHttpDriver::new | 变异：默认体上限被置零（关闭 fail-closed） | 红=reqwest_driver_new_constructs_with_documented_limits | 绿=reqwest_driver_new_constructs_with_documented_limits
//! // TDD-PROBE: HttpClientPool::try_new | 变异：跳过配置校验直接构造 | 红=pool_try_new_validates_config_recoverably | 绿=pool_try_new_validates_config_recoverably
//! // TDD-PROBE: HttpClientPool::checkout_lease_with | 变异：lease drop 不归还许可 | 红=pool_checkout_lease_with_returns_object_on_drop | 绿=pool_checkout_lease_with_returns_object_on_drop
//! // TDD-PROBE: parse_retry_after_at | 变异：过去 HTTP-date 返回负值 / panic | 红=parse_retry_after_at_handles_seconds_and_http_date | 绿=parse_retry_after_at_handles_seconds_and_http_date
//! // TDD-PROBE: is_sensitive_header_name | 变异：判定改为大小写敏感 | 红=sensitive_header_names_are_case_insensitive | 绿=sensitive_header_names_are_case_insensitive
//! // TDD-PROBE: MockHttpTransport::set_get | 变异：set_get 与 set_post 落入同一张表 | 红=mock_set_get_is_isolated_from_post | 绿=mock_set_get_is_isolated_from_post

use std::net::SocketAddr;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::{accept_async, tungstenite::Message};
use transportx::{
    is_sensitive_header_name, parse_retry_after_at, HttpClientPool, HttpDriver, HttpRequest,
    MockHttpTransport, PoolConfig, ReqwestHttpDriver, TransportError, TungsteniteWsConnector,
    WsConnector, DEFAULT_MAX_REQUEST_BODY_BYTES, DEFAULT_MAX_RESPONSE_BODY_BYTES,
    DEFAULT_REQUEST_TIMEOUT,
};

/// 启动 loopback WebSocket 对端：先发 Ping + 文本，收到二进制后原样回显并关闭。
async fn spawn_ws_peer() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("绑定本地端口");
    let addr = listener.local_addr().expect("读取本地地址");
    tokio::spawn(async move {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let Ok(mut ws) = accept_async(stream).await else {
            return;
        };
        let _ = ws.send(Message::Ping(Bytes::from_static(b"p"))).await;
        let _ = ws.send(Message::Text("greeting".into())).await;
        while let Some(message) = ws.next().await {
            match message {
                Ok(Message::Binary(payload)) => {
                    let _ = ws.send(Message::Binary(payload)).await;
                    let _ = ws.send(Message::Close(None)).await;
                    break;
                }
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(_) => continue,
            }
        }
    });
    addr
}

fn get_request(url: &str) -> HttpRequest {
    HttpRequest {
        method: "GET".into(),
        url: url.into(),
        headers: Vec::new(),
        body: None,
    }
}

/// `HttpDriver::execute`：命中预置响应返回 `HttpResponse`；未命中 fail-closed。
#[tokio::test]
async fn http_driver_execute_returns_response_and_rejects_unknown() {
    let driver = MockHttpTransport::new();
    driver.set_get("https://api/ok", Bytes::from_static(b"{}"));
    let response = driver
        .execute(get_request("https://api/ok"))
        .await
        .expect("命中预置响应");
    assert_eq!(response.status, 200);
    assert_eq!(response.body, Bytes::from_static(b"{}"));

    let error = driver
        .execute(get_request("https://api/missing"))
        .await
        .expect_err("未预置 URL 必须拒绝");
    assert!(
        matches!(error, TransportError::ProtocolViolation(_)),
        "实际错误：{error:?}"
    );
}

/// `WsConnector::connect`：loopback 建连成功；非法 URL 拒绝为 `ProtocolViolation`。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_connector_connect_establishes_connection() {
    let addr = spawn_ws_peer().await;
    let connector = TungsteniteWsConnector::new();
    let mut connection = connector
        .connect(&format!("ws://{addr}/"))
        .await
        .expect("loopback 建连成功");
    let _ = connection.close().await;

    // `Box<dyn WsConnection>` 未实现 Debug，故不用 expect_err。
    let error = match connector.connect("not-a-url").await {
        Ok(_) => panic!("非法 URL 必须拒绝"),
        Err(error) => error,
    };
    assert!(
        matches!(error, TransportError::ProtocolViolation(_)),
        "实际错误：{error:?}"
    );
}

/// `WsConnection::next_frame`：跳过 Ping/Pong 控制帧，交付首个应用帧。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_connection_next_frame_skips_control_frames() {
    let addr = spawn_ws_peer().await;
    let mut connection = TungsteniteWsConnector::new()
        .connect(&format!("ws://{addr}/"))
        .await
        .expect("建连");
    let first = connection
        .next_frame()
        .await
        .expect("读取首帧")
        .expect("应有应用帧");
    assert_eq!(first.as_ref(), b"greeting", "Ping 控制帧必须被跳过");
    let _ = connection.close().await;
}

/// `WsConnection::send_frame`：发送的二进制帧确实到达对端（对端回显）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_connection_send_frame_reaches_peer() {
    let addr = spawn_ws_peer().await;
    let mut connection = TungsteniteWsConnector::new()
        .connect(&format!("ws://{addr}/"))
        .await
        .expect("建连");
    assert!(connection.next_frame().await.expect("读帧").is_some());
    connection
        .send_frame(Bytes::from_static(b"echo-me"))
        .await
        .expect("发送二进制帧");
    let echoed = connection
        .next_frame()
        .await
        .expect("读取回显")
        .expect("回显帧");
    assert_eq!(echoed.as_ref(), b"echo-me");
    let _ = connection.close().await;
}

/// `ReqwestHttpDriver::new`：默认 30s 超时 + 16 MiB 请求/响应体上限（fail-closed 默认）。
#[test]
fn reqwest_driver_new_constructs_with_documented_limits() {
    assert_eq!(DEFAULT_REQUEST_TIMEOUT, Duration::from_secs(30));
    let driver = ReqwestHttpDriver::new().expect("默认驱动可构造");
    let debug = format!("{driver:?}");
    assert!(
        debug.contains(&DEFAULT_MAX_RESPONSE_BODY_BYTES.to_string()),
        "响应体上限须为默认值: {debug}"
    );
    assert!(
        debug.contains(&DEFAULT_MAX_REQUEST_BODY_BYTES.to_string()),
        "请求体上限须为默认值: {debug}"
    );
}

/// `HttpClientPool::try_new`：非法配置返回可恢复错误；合法配置保留原样。
#[test]
fn pool_try_new_validates_config_recoverably() {
    let error =
        HttpClientPool::<u32>::try_new(PoolConfig::new(0, 0)).expect_err("零池大小必须拒绝");
    assert!(matches!(error, TransportError::ProtocolViolation(_)));
    let error = HttpClientPool::<u32>::try_new(PoolConfig::new(1, 2))
        .expect_err("max_idle 超池大小必须拒绝");
    assert!(matches!(error, TransportError::ProtocolViolation(_)));
    let pool = HttpClientPool::<u32>::try_new(PoolConfig::new(2, 1)).expect("合法配置");
    assert_eq!(pool.config(), PoolConfig::new(2, 1));
}

/// `HttpClientPool::checkout_lease_with`：RAII lease 归还对象与许可；factory 失败回滚许可。
#[test]
fn pool_checkout_lease_with_returns_object_on_drop() {
    let pool = HttpClientPool::try_new(PoolConfig::new(1, 1)).expect("合法配置");
    {
        let lease = pool.checkout_lease_with(|| Ok(7u32)).expect("借出");
        assert_eq!(lease.get(), Some(&7));
        assert_eq!(pool.checked_out(), 1);
    }
    assert_eq!(pool.checked_out(), 0, "lease drop 必须归还许可");
    assert_eq!(pool.idle_len(), 1, "lease drop 必须归还对象");

    let reused = pool.checkout_lease_with(|| Ok(99u32)).expect("复用 idle");
    assert_eq!(reused.get(), Some(&7), "空闲对象优先，不调用 factory");

    let failing = HttpClientPool::<u32>::try_new(PoolConfig::new(1, 1)).expect("合法配置");
    assert!(failing
        .checkout_lease_with(|| Err(TransportError::ProtocolViolation("factory".into())))
        .is_err());
    assert_eq!(failing.checked_out(), 0, "factory 失败必须回滚许可");
}

/// `parse_retry_after_at`：delay-seconds、HTTP-date（含过去日期钳制）、非法值。
#[test]
fn parse_retry_after_at_handles_seconds_and_http_date() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    assert_eq!(
        parse_retry_after_at("120", now),
        Some(Duration::from_secs(120))
    );
    assert_eq!(
        parse_retry_after_at("  120  ", now),
        Some(Duration::from_secs(120)),
        "两端空白须被忽略"
    );
    assert_eq!(
        parse_retry_after_at("Tue, 14 Nov 2023 22:14:20 GMT", now),
        Some(Duration::from_secs(60)),
        "未来 HTTP-date 相对 now 计算"
    );
    assert_eq!(
        parse_retry_after_at("Thu, 01 Jan 1970 00:00:00 GMT", now),
        Some(Duration::ZERO),
        "过去日期钳制为零"
    );
    assert_eq!(parse_retry_after_at("not-a-value", now), None);
}

/// `is_sensitive_header_name`：大小写无关，含已知头与子串命中。
#[test]
fn sensitive_header_names_are_case_insensitive() {
    for name in [
        "Authorization",
        "AUTHORIZATION",
        "Proxy-Authorization",
        "Cookie",
        "Set-Cookie",
        "X-API-Key",
        "x-api-key",
        "X-Auth-Token",
        "Ok-Access-Passphrase",
        "X-Custom-Token",
        "x-my-secret",
        "user-password",
    ] {
        assert!(is_sensitive_header_name(name), "{name} 应判为敏感");
    }
    for name in ["Content-Type", "Accept", "User-Agent", "X-Request-Id"] {
        assert!(!is_sensitive_header_name(name), "{name} 不应判为敏感");
    }
}

/// `MockHttpTransport::set_get`：GET 与 POST 表相互隔离，不串味。
#[tokio::test]
async fn mock_set_get_is_isolated_from_post() {
    let driver = MockHttpTransport::new();
    driver.set_get("https://api/resource", Bytes::from_static(b"get-body"));
    let post = HttpRequest {
        method: "POST".into(),
        url: "https://api/resource".into(),
        headers: Vec::new(),
        body: Some(Bytes::from_static(b"ignored")),
    };
    let error = driver
        .execute(post)
        .await
        .expect_err("GET 预置不得被 POST 命中");
    assert!(matches!(error, TransportError::ProtocolViolation(_)));

    let response = driver
        .execute(get_request("https://api/resource"))
        .await
        .expect("GET 命中");
    assert_eq!(response.body, Bytes::from_static(b"get-body"));
}

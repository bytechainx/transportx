#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! TungsteniteWsConnector / WsConnection 本地 loopback 测试。

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::{
    accept_async,
    tungstenite::{
        protocol::frame::{
            coding::{Data, OpCode},
            Frame,
        },
        Message,
    },
};
use transportx::{TransportError, TungsteniteWsConnector, WsConnector};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_text_binary_ping_pong_close_lifecycle() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        let _ = ready_tx.send(());
        ws.send(Message::Ping(Bytes::from_static(b"p")))
            .await
            .unwrap();
        ws.send(Message::Text("hello-text".into())).await.unwrap();
        ws.send(Message::Pong(Bytes::from_static(b"p")))
            .await
            .unwrap();
        ws.send(Message::Binary(Bytes::from_static(b"bin-payload")))
            .await
            .unwrap();
        loop {
            match ws.next().await {
                Some(Ok(Message::Binary(b))) => {
                    assert_eq!(b.as_ref(), b"from-client");
                    break;
                }
                Some(Ok(Message::Ping(_) | Message::Pong(_))) => continue,
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(_)) => continue,
                Some(Err(_)) => break,
            }
        }
        ws.send(Message::Close(None)).await.unwrap();
    });

    let connector = TungsteniteWsConnector::new();
    let mut conn = connector
        .connect(&format!("ws://{addr}/"))
        .await
        .expect("connect");
    let _ = ready_rx.await;

    let frame = conn.next_frame().await.unwrap().expect("text frame");
    assert_eq!(frame.as_ref(), b"hello-text");

    let frame = conn.next_frame().await.unwrap().expect("binary frame");
    assert_eq!(frame.as_ref(), b"bin-payload");

    conn.send_frame(Bytes::from_static(b"from-client"))
        .await
        .unwrap();

    let end = conn.next_frame().await.unwrap();
    assert!(end.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_close_then_send_maps_connection_closed() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        while let Some(msg) = ws.next().await {
            if let Ok(Message::Close(_)) = msg {
                break;
            }
        }
    });
    let connector = TungsteniteWsConnector::new();
    let mut conn = connector.connect(&format!("ws://{addr}/")).await.unwrap();
    conn.close().await.unwrap();
    let err = conn.send_frame(Bytes::from_static(b"x")).await.unwrap_err();
    assert!(
        matches!(
            err,
            TransportError::ConnectionClosed { .. } | TransportError::ProtocolViolation(_)
        ),
        "got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_stream_end_without_close_frame_is_unclean() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let _ws = accept_async(stream).await.unwrap();
    });
    let connector = TungsteniteWsConnector::new();
    let mut conn = connector.connect(&format!("ws://{addr}/")).await.unwrap();
    let err = conn.next_frame().await.unwrap_err();
    assert!(
        matches!(err, TransportError::ConnectionClosed { clean: false }),
        "实际错误：{err:?}"
    );
}

#[tokio::test]
async fn ws_connect_invalid_url_protocol_violation() {
    let connector = TungsteniteWsConnector::new();
    let err = match connector.connect("not-a-url").await {
        Ok(_) => panic!("expected error"),
        Err(e) => e,
    };
    assert!(
        matches!(err, TransportError::ProtocolViolation(_)),
        "got {err:?}"
    );
}

#[tokio::test]
async fn ws_connect_refused_maps_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let connector = TungsteniteWsConnector::new();
    let err = match connector.connect(&format!("ws://{addr}/")).await {
        Ok(_) => panic!("expected error"),
        Err(e) => e,
    };
    assert!(
        matches!(
            err,
            TransportError::Io(_) | TransportError::ProtocolViolation(_)
        ),
        "got {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_server_sends_only_ping_then_text_skips_control() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();
        let _ = ready_tx.send(());
        // 控制帧后跟应用文本；客户端 next_frame 应跳过 Ping
        let _ = ws.send(Message::Ping(Bytes::from_static(b"x"))).await;
        let _ = ws.send(Message::Ping(Bytes::from_static(b"y"))).await;
        let _ = ws.send(Message::Text("after-pings".into())).await;
        while let Some(msg) = ws.next().await {
            if matches!(msg, Ok(Message::Close(_)) | Err(_)) {
                break;
            }
        }
    });
    let mut conn = TungsteniteWsConnector::new()
        .connect(&format!("ws://{addr}/"))
        .await
        .unwrap();
    let _ = ready_rx.await;
    let frame = conn
        .next_frame()
        .await
        .expect("next_frame ok")
        .expect("application frame");
    assert_eq!(frame.as_ref(), b"after-pings");
    let _ = conn.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_inbound_limit_is_enforced_by_decoder_before_delivery() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(stream).await.unwrap();
        websocket
            .send(Message::Binary(Bytes::from(vec![0_u8; 32])))
            .await
            .unwrap();
    });

    let connector = TungsteniteWsConnector::with_limits(std::time::Duration::from_secs(2), 8);
    let mut connection = connector.connect(&format!("ws://{addr}/")).await.unwrap();
    let error = connection
        .next_frame()
        .await
        .expect_err("入站超限不得交付 payload");
    assert!(
        matches!(
            error,
            TransportError::PayloadTooLarge {
                kind: "ws_message",
                limit: 8,
                got: 32
            }
        ),
        "底层 decoder 必须报告入站 message 上限，实际错误：{error:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ws_fragmented_message_limit_is_cumulative_before_delivery() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(stream).await.unwrap();
        websocket
            .send(Message::Frame(Frame::message(
                Bytes::from_static(b"12345"),
                OpCode::Data(Data::Binary),
                false,
            )))
            .await
            .unwrap();
        websocket
            .send(Message::Frame(Frame::message(
                Bytes::from_static(b"6789"),
                OpCode::Data(Data::Continue),
                true,
            )))
            .await
            .unwrap();
    });

    let connector = TungsteniteWsConnector::with_limits(std::time::Duration::from_secs(2), 8);
    let mut connection = connector.connect(&format!("ws://{addr}/")).await.unwrap();
    let error = connection
        .next_frame()
        .await
        .expect_err("碎片累计超限不得交付 payload");
    assert!(
        matches!(
            error,
            TransportError::PayloadTooLarge {
                kind: "ws_message",
                limit: 8,
                got: 9
            }
        ),
        "每个 frame 均未超限时仍必须检查聚合 message，实际错误：{error:?}"
    );
}

#[test]
fn connector_debug_default() {
    let c = TungsteniteWsConnector::new();
    let _ = format!("{c:?}");
    let _ = TungsteniteWsConnector::new();
}

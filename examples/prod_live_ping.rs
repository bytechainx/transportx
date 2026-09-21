#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 内部生产 live 消费者示例：loopback 真实 HTTP 路径（非 mock、非 dry-run）。
//! 使用 `ReqwestHttpDriver` 经真实 TCP/HTTP 栈访问本机临时端口，
//! 验证真实网络传输与错误/响应映射（Q-2=D · 纯 library 最低证据）。
//!
//! 运行：`TRANSPORTX_LIVE_PROFILE=production cargo run -p transportx --example prod_live_ping`

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use transportx::{HttpDriver, HttpRequest, ReqwestHttpDriver};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.expect("accept");
        let mut buf = [0u8; 1024];
        let n = sock.read(&mut buf).await.expect("read request");
        let head = String::from_utf8_lossy(&buf[..n]).to_string();
        let body = Bytes::from_static(b"{\"ok\":true}");
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            String::from_utf8_lossy(&body),
        );
        sock.write_all(response.as_bytes())
            .await
            .expect("write response");
        head
    });

    let driver = ReqwestHttpDriver::new().expect("driver");
    let response = driver
        .execute(HttpRequest {
            method: "GET".into(),
            url: format!("http://{addr}/ping"),
            headers: vec![("Accept".into(), "application/json".into())],
            body: None,
        })
        .await
        .expect("real loopback GET");

    let served = server.await.expect("server task");
    assert_eq!(response.status, 200);
    assert_eq!(response.body.as_ref(), b"{\"ok\":true}");
    assert!(served.starts_with("GET /ping"));
    let profile = std::env::var("TRANSPORTX_LIVE_PROFILE").unwrap_or_else(|_| "development".into());
    println!(
        "transportx-consumer: ok profile={profile} status={} body={} driver=reqwest loopback={addr}",
        response.status,
        String::from_utf8_lossy(&response.body),
    );
}

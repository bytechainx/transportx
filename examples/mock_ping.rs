#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! 消费方示例：使用 MockHttpTransport 执行一次真实公开 API 调用并断言返回体。

use bytes::Bytes;
use transportx::{HttpDriver, HttpRequest, MockHttpTransport};

#[tokio::main]
async fn main() {
    let mock = MockHttpTransport::new();
    mock.set_get(
        "https://api.example/ping",
        Bytes::from_static(b"{\"ok\":true}"),
    );

    let response = mock
        .execute(HttpRequest {
            method: "GET".into(),
            url: "https://api.example/ping".into(),
            headers: vec![("Accept".into(), "application/json".into())],
            body: None,
        })
        .await
        .expect("mock execute");

    assert_eq!(response.status, 200);
    assert_eq!(response.body.as_ref(), b"{\"ok\":true}");
    println!(
        "consumer_ok status={} body={}",
        response.status,
        String::from_utf8_lossy(&response.body)
    );
}

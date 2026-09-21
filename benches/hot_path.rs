#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! transportx 热路径：MockHttpTransport execute。
use std::hint::black_box;
use std::time::Instant;

use bytes::Bytes;
use transportx::{HttpDriver, HttpRequest, MockHttpTransport};

fn iters() -> u32 {
    if std::env::args().any(|a| a == "--quick") {
        200
    } else {
        5_000
    }
}

fn main() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt");
    let n = iters();
    let mock = MockHttpTransport::new();
    mock.set_get("https://bench.local/p", Bytes::from_static(b"ok"));
    let req = HttpRequest {
        method: "GET".into(),
        url: "https://bench.local/p".into(),
        headers: vec![],
        body: None,
    };
    for _ in 0..n.min(10) {
        let _ = rt.block_on(mock.execute(req.clone()));
    }
    let start = Instant::now();
    let mut bytes = 0usize;
    for _ in 0..n {
        let resp = rt.block_on(mock.execute(req.clone())).expect("exec");
        bytes = bytes.wrapping_add(resp.body.len());
        black_box(resp.status);
    }
    let elapsed = start.elapsed();
    println!(
        "bench_transportx_mock_http: iters={n} total={elapsed:?} per_iter={:?} bytes={}",
        elapsed / n,
        black_box(bytes)
    );
}

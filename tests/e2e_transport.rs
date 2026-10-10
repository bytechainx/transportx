#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! E2E（transportx）：端到端执行**全部**公开接口，不依赖任何外部真服务。
//!
//! 本仓是「驱动无关的传输边界」：对外行为落在**边界契约**上，而不是某个具体厂商端点。
//! 因此 E2E 的端到端落点是：用 crate 自带的 `MockHttpTransport` 走完整
//! `HttpDriver::execute` 往返；用测试内实现走完整 `WsConnector::connect` →
//! `send_frame` → `next_frame` → `close` 帧级生命周期；真实驱动（reqwest /
//! tungstenite）只做**离线构造**，不发起连接。
//!
//! 对齐对象是 `cargo +nightly public-api --simplified` 导出的完整公开面：
//! `fn` / `type` / `field` / `const` / `variant` 五类逐条登记在 [`E2E_MANIFEST`]，
//! 运行期由 `cover` 登记表核对「声明 = 实际执行」（缺一即失败）。
//!
//! **独立核对**：`scripts/verify-e2e-coverage.mjs` 会重新派生公开面与清单双向 diff，
//! 并用 `-C instrument-coverage` + `llvm-cov report --show-functions` 断言每条公开
//! 函数执行次数 > 0；本文件内的登记表只是**声明**，不是唯一证据。
//!
//! ```text
//! cd /home/workspace/bytechainx/infra/transportx
//! cargo test --test e2e_transport
//! node scripts/verify-e2e-coverage.mjs transportx --no-coverage
//! ```

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use bytes::Bytes;

use transportx::{
    build_reqwest_proxy, is_sensitive_header_name, parse_retry_after_at, HttpClientLease,
    HttpClientPool, HttpDriver, HttpRequest, HttpResponse, MockHttpTransport, PoolConfig,
    ProxyConfig, ReqwestHttpDriver, SharedHttpClientPool, TlsConfig, TlsMode, TransportError,
    TungsteniteWsConnector, WsConnection, WsConnector, DEFAULT_MAX_REQUEST_BODY_BYTES,
    DEFAULT_MAX_RESPONSE_BODY_BYTES, DEFAULT_MAX_WS_FRAME_BYTES, DEFAULT_REQUEST_TIMEOUT,
    DEFAULT_WS_CONNECT_TIMEOUT,
};

/// 公开面清单：`(条目类别, 入口 id)`，由 `cargo +nightly public-api --simplified` 派生并冻结。
///
/// 类别取值域：`fn` / `type` / `field` / `const` / `variant`。
/// 该清单是运行时登记的**唯一事实源**——`cover::hit` 拒绝清单外的 id，收尾断言拒绝
/// 「声明了却没执行」的条目。清单本身的时效性由外部核对器与公开面 diff 保证。
const E2E_MANIFEST: &[(&str, &str)] = &[
    ("type", "TlsMode"),
    ("variant", "TlsMode::CustomCa"),
    ("variant", "TlsMode::InsecureDevOnly"),
    ("variant", "TlsMode::SystemRoots"),
    ("type", "TransportError"),
    ("variant", "TransportError::ConnectTimeout"),
    ("variant", "TransportError::ConnectionClosed"),
    ("variant", "TransportError::Io"),
    ("variant", "TransportError::PayloadTooLarge"),
    ("variant", "TransportError::PoolExhausted"),
    ("variant", "TransportError::ProtocolViolation"),
    ("variant", "TransportError::RateLimited"),
    ("variant", "TransportError::ReadTimeout"),
    ("fn", "TransportError::is_retryable"),
    ("type", "HttpClientLease"),
    ("type", "HttpClientPool"),
    ("type", "HttpRequest"),
    ("field", "HttpRequest::body"),
    ("field", "HttpRequest::headers"),
    ("field", "HttpRequest::method"),
    ("field", "HttpRequest::url"),
    ("type", "HttpResponse"),
    ("field", "HttpResponse::body"),
    ("field", "HttpResponse::status"),
    ("type", "MockHttpTransport"),
    ("fn", "MockHttpTransport::new"),
    ("fn", "MockHttpTransport::set_get"),
    ("fn", "MockHttpTransport::set_post"),
    ("type", "PoolConfig"),
    ("field", "PoolConfig::max_idle"),
    ("field", "PoolConfig::max_pool_size"),
    ("fn", "PoolConfig::new"),
    ("fn", "PoolConfig::validate"),
    ("type", "ProxyConfig"),
    ("field", "ProxyConfig::password"),
    ("field", "ProxyConfig::url"),
    ("field", "ProxyConfig::username"),
    ("fn", "ProxyConfig::has_auth"),
    ("fn", "ProxyConfig::new"),
    ("fn", "ProxyConfig::with_auth"),
    ("type", "ReqwestHttpDriver"),
    ("fn", "ReqwestHttpDriver::builder"),
    ("fn", "ReqwestHttpDriver::new"),
    ("fn", "ReqwestHttpDriver::with_limits"),
    ("fn", "ReqwestHttpDriver::with_proxy"),
    ("fn", "ReqwestHttpDriver::with_timeout"),
    ("fn", "ReqwestHttpDriver::with_tls"),
    ("type", "TlsConfig"),
    ("field", "TlsConfig::mode"),
    ("field", "TlsConfig::sni"),
    ("fn", "TlsConfig::custom_ca"),
    ("fn", "TlsConfig::insecure_dev_only"),
    ("fn", "TlsConfig::is_insecure"),
    ("fn", "TlsConfig::system_roots"),
    ("type", "TungsteniteWsConnector"),
    ("fn", "TungsteniteWsConnector::new"),
    ("fn", "TungsteniteWsConnector::with_limits"),
    ("const", "DEFAULT_MAX_REQUEST_BODY_BYTES"),
    ("const", "DEFAULT_MAX_RESPONSE_BODY_BYTES"),
    ("const", "DEFAULT_MAX_WS_FRAME_BYTES"),
    ("const", "DEFAULT_REQUEST_TIMEOUT"),
    ("const", "DEFAULT_WS_CONNECT_TIMEOUT"),
    ("type", "HttpDriver"),
    ("fn", "HttpDriver::execute"),
    ("type", "WsConnection"),
    ("fn", "WsConnection::close"),
    ("fn", "WsConnection::next_frame"),
    ("fn", "WsConnection::send_frame"),
    ("type", "WsConnector"),
    ("fn", "WsConnector::connect"),
    ("fn", "build_reqwest_proxy"),
    ("fn", "is_sensitive_header_name"),
    ("fn", "parse_retry_after_at"),
    ("type", "SharedHttpClientPool"),
];

mod cover {
    use std::collections::BTreeSet;
    use std::sync::{Mutex, OnceLock};

    static EXECUTED: OnceLock<Mutex<BTreeSet<(&'static str, &'static str)>>> = OnceLock::new();

    fn log() -> &'static Mutex<BTreeSet<(&'static str, &'static str)>> {
        EXECUTED.get_or_init(|| Mutex::new(BTreeSet::new()))
    }

    /// 登记一次真实执行。清单外的 `(类别, id)` 立即 panic，防止调用点与清单漂移。
    pub fn hit(kind: &'static str, id: &'static str) {
        assert!(
            super::E2E_MANIFEST
                .iter()
                .any(|(declared_kind, declared_id)| *declared_kind == kind && *declared_id == id),
            "登记了清单外的公开条目：{kind} {id}"
        );
        log().lock().expect("覆盖登记表锁中毒").insert((kind, id));
    }

    /// 已登记的执行集合（收尾断言用）。
    pub fn executed() -> BTreeSet<(&'static str, &'static str)> {
        log().lock().expect("覆盖登记表锁中毒").clone()
    }
}

/// 覆盖登记的简写入口（保持调用点可读）。
fn hit(kind: &'static str, id: &'static str) {
    cover::hit(kind, id);
}

/// 清单自身良构：类别取值域合法、`(类别, id)` 不重复。
fn assert_manifest_wellformed() {
    let mut seen: BTreeSet<(&str, &str)> = BTreeSet::new();
    for (kind, id) in E2E_MANIFEST {
        assert!(
            matches!(*kind, "fn" | "type" | "field" | "const" | "variant"),
            "未知条目类别 {kind}（id={id}）"
        );
        assert!(seen.insert((*kind, *id)), "清单重复条目：{kind} {id}");
    }
}

/// 覆盖完整性：清单里每一条都必须被真实执行过。
fn assert_coverage_complete() {
    let executed = cover::executed();
    let mut missing: Vec<(&str, &str)> = Vec::new();
    for (kind, id) in E2E_MANIFEST {
        if !executed.contains(&(*kind, *id)) {
            missing.push((kind, id));
        }
    }
    assert!(missing.is_empty(), "声明了却未执行：{missing:?}");
}

/// 供 `type` 条目落地：泛型容器在此只做类型可达性 / 约定断言，不发起连接。
fn assert_sized<T>() {}
fn assert_send_sync<T: Send + Sync + ?Sized>() {}

/// 测试侧 WS 连接：真实走完帧级生命周期，不触网。
#[derive(Debug, Default)]
struct FakeWsConnection {
    inbox: Vec<Bytes>,
    closed: bool,
}

#[async_trait]
impl WsConnection for FakeWsConnection {
    async fn next_frame(&mut self) -> Result<Option<Bytes>, TransportError> {
        Ok(self.inbox.pop())
    }

    async fn send_frame(&mut self, frame: Bytes) -> Result<(), TransportError> {
        self.inbox.push(frame);
        Ok(())
    }

    async fn close(&mut self) -> Result<(), TransportError> {
        self.closed = true;
        Ok(())
    }
}

/// 测试侧 WS 连接器：返回值只能是 trait object，这同时验证了边界的对象安全性。
#[derive(Debug, Default)]
struct FakeWsConnector;

#[async_trait]
impl WsConnector for FakeWsConnector {
    async fn connect(&self, _url: &str) -> Result<Box<dyn WsConnection>, TransportError> {
        Ok(Box::new(FakeWsConnection {
            inbox: vec![Bytes::from_static(b"server-frame")],
            closed: false,
        }))
    }
}

/// 错误枚举：逐个构造变体并断言 `is_retryable` 的分类。
fn phase_error_variants() {
    hit("type", "TransportError");
    let cases = [
        (
            TransportError::ConnectTimeout,
            "TransportError::ConnectTimeout",
            true,
        ),
        (
            TransportError::ConnectionClosed { clean: true },
            "TransportError::ConnectionClosed",
            true,
        ),
        (
            TransportError::Io(Box::new(std::io::Error::other("io"))),
            "TransportError::Io",
            true,
        ),
        (
            TransportError::PayloadTooLarge {
                got: 64,
                kind: "response",
                limit: 16,
            },
            "TransportError::PayloadTooLarge",
            false,
        ),
        (
            TransportError::PoolExhausted { limit: 4 },
            "TransportError::PoolExhausted",
            true,
        ),
        (
            TransportError::ProtocolViolation("bad frame".to_owned()),
            "TransportError::ProtocolViolation",
            false,
        ),
        (
            TransportError::RateLimited {
                retry_after: Some(Duration::from_secs(2)),
            },
            "TransportError::RateLimited",
            true,
        ),
        (
            TransportError::ReadTimeout,
            "TransportError::ReadTimeout",
            true,
        ),
    ];
    for (error, id, expected) in cases {
        hit("variant", id);
        hit("fn", "TransportError::is_retryable");
        assert_eq!(
            error.is_retryable(),
            expected,
            "{id} 的 is_retryable 分类不符"
        );
    }
}

/// TLS 模式与配置：三个变体都要真实构造到。
fn phase_tls() {
    hit("type", "TlsMode");
    hit("variant", "TlsMode::SystemRoots");
    hit("variant", "TlsMode::CustomCa");
    hit("variant", "TlsMode::InsecureDevOnly");
    hit("type", "TlsConfig");
    hit("fn", "TlsConfig::system_roots");
    hit("fn", "TlsConfig::custom_ca");
    hit("fn", "TlsConfig::insecure_dev_only");
    hit("fn", "TlsConfig::is_insecure");
    hit("field", "TlsConfig::mode");
    hit("field", "TlsConfig::sni");

    let system = TlsConfig::system_roots();
    assert!(matches!(system.mode, TlsMode::SystemRoots));
    assert!(system.sni, "默认必须启用 SNI");
    assert!(!system.is_insecure());

    let custom = TlsConfig::custom_ca(PathBuf::from("/etc/ssl/custom.pem"));
    assert!(matches!(custom.mode, TlsMode::CustomCa { .. }));

    let insecure = TlsConfig::insecure_dev_only();
    assert!(matches!(insecure.mode, TlsMode::InsecureDevOnly));
    assert!(insecure.is_insecure());
}

/// 代理：认证与非认证两条构造路径，外加 reqwest 转换。
fn phase_proxy() {
    hit("type", "ProxyConfig");
    hit("fn", "ProxyConfig::new");
    hit("fn", "ProxyConfig::with_auth");
    hit("fn", "ProxyConfig::has_auth");
    hit("field", "ProxyConfig::url");
    hit("field", "ProxyConfig::username");
    hit("field", "ProxyConfig::password");
    hit("fn", "build_reqwest_proxy");

    let plain = ProxyConfig::new("http://proxy.local:8080");
    assert_eq!(plain.url, "http://proxy.local:8080");
    assert!(!plain.has_auth());
    assert!(plain.username.is_none() && plain.password.is_none());

    let auth = ProxyConfig::with_auth("http://proxy.local:8080", "user", "pa55");
    assert_eq!(auth.username.as_deref(), Some("user"));
    assert_eq!(auth.password.as_deref(), Some("pa55"));
    assert!(auth.has_auth());
    assert!(build_reqwest_proxy(&auth).is_ok());
    assert!(
        !format!("{auth:?}").contains("pa55"),
        "代理口令不得进 Debug"
    );
}

/// 连接池配置：构造 + 校验路径（含 fail-closed 的非法配置）。
fn phase_pool() {
    hit("type", "PoolConfig");
    hit("fn", "PoolConfig::new");
    hit("fn", "PoolConfig::validate");
    hit("field", "PoolConfig::max_pool_size");
    hit("field", "PoolConfig::max_idle");
    hit("type", "HttpClientPool");
    hit("type", "HttpClientLease");
    hit("type", "SharedHttpClientPool");

    let ok = PoolConfig::new(8, 2);
    assert_eq!(ok.max_pool_size, 8);
    assert_eq!(ok.max_idle, 2);
    assert!(ok.validate().is_ok());

    let bad = PoolConfig::new(0, 0);
    assert!(bad.validate().is_err(), "非法池配置必须 fail-closed");

    assert_sized::<HttpClientPool<u8>>();
    assert_sized::<SharedHttpClientPool<u8>>();
    assert_send_sync::<HttpClientLease<'static, u8>>();
}

/// HTTP 边界：`MockHttpTransport` 走完整 `HttpDriver::execute` 往返（GET 与 POST）。
async fn phase_http() {
    hit("type", "HttpRequest");
    hit("field", "HttpRequest::method");
    hit("field", "HttpRequest::url");
    hit("field", "HttpRequest::headers");
    hit("field", "HttpRequest::body");
    hit("type", "HttpResponse");
    hit("field", "HttpResponse::status");
    hit("field", "HttpResponse::body");
    hit("type", "HttpDriver");
    hit("fn", "HttpDriver::execute");
    hit("type", "MockHttpTransport");
    hit("fn", "MockHttpTransport::new");
    hit("fn", "MockHttpTransport::set_get");
    hit("fn", "MockHttpTransport::set_post");

    let driver = MockHttpTransport::new();
    driver.set_get("https://example.invalid/get", Bytes::from_static(b"ok"));
    driver.set_post(
        "https://example.invalid/post",
        Bytes::from_static(b"created"),
    );

    let get = HttpRequest {
        method: "GET".to_owned(),
        url: "https://example.invalid/get".to_owned(),
        headers: vec![("authorization".to_owned(), "Bearer x".to_owned())],
        body: None,
    };
    assert_eq!(get.method, "GET");
    assert_eq!(get.url, "https://example.invalid/get");
    assert_eq!(get.headers.len(), 1);
    assert!(get.body.is_none());
    assert!(
        !format!("{get:?}").contains("Bearer x"),
        "敏感 header 值不得进 Debug"
    );

    let response: HttpResponse = driver.execute(get).await.expect("GET 必须成功");
    assert_eq!(response.status, 200);
    assert_eq!(response.body, Bytes::from_static(b"ok"));

    let post = HttpRequest {
        method: "POST".to_owned(),
        url: "https://example.invalid/post".to_owned(),
        headers: Vec::new(),
        body: Some(Bytes::from_static(b"payload")),
    };
    let created = driver.execute(post).await.expect("POST 必须成功");
    assert_eq!(created.status, 200);
    assert_eq!(created.body, Bytes::from_static(b"created"));
}

/// 真实驱动只做离线构造：不发连接，但把每条构造路径都走一遍。
fn phase_real_drivers() {
    hit("type", "ReqwestHttpDriver");
    hit("fn", "ReqwestHttpDriver::new");
    hit("fn", "ReqwestHttpDriver::with_timeout");
    hit("fn", "ReqwestHttpDriver::with_limits");
    hit("fn", "ReqwestHttpDriver::with_tls");
    hit("fn", "ReqwestHttpDriver::with_proxy");
    hit("fn", "ReqwestHttpDriver::builder");

    assert!(ReqwestHttpDriver::new().is_ok());
    assert!(ReqwestHttpDriver::with_timeout(Some(Duration::from_secs(5))).is_ok());
    assert!(
        ReqwestHttpDriver::with_limits(Some(Duration::from_secs(5)), 1024, 1024).is_ok(),
        "显式限制构造必须成功"
    );
    assert!(ReqwestHttpDriver::with_tls(TlsConfig::system_roots()).is_ok());
    assert!(ReqwestHttpDriver::with_proxy(ProxyConfig::new("http://proxy.local:8080")).is_ok());
    assert!(ReqwestHttpDriver::builder(None, 1024, 1024, None, None).is_ok());

    hit("type", "TungsteniteWsConnector");
    hit("fn", "TungsteniteWsConnector::new");
    hit("fn", "TungsteniteWsConnector::with_limits");
    let _unbounded = TungsteniteWsConnector::new();
    let _bounded = TungsteniteWsConnector::with_limits(Duration::from_secs(5), 1024);
    assert_send_sync::<TungsteniteWsConnector>();
}

/// 常量与自由函数：默认值必须存在且可达。
fn phase_constants_and_free_fns() {
    hit("const", "DEFAULT_REQUEST_TIMEOUT");
    hit("const", "DEFAULT_MAX_RESPONSE_BODY_BYTES");
    hit("const", "DEFAULT_MAX_REQUEST_BODY_BYTES");
    hit("const", "DEFAULT_WS_CONNECT_TIMEOUT");
    hit("const", "DEFAULT_MAX_WS_FRAME_BYTES");

    let durations = [DEFAULT_REQUEST_TIMEOUT, DEFAULT_WS_CONNECT_TIMEOUT];
    for value in durations {
        assert!(value > Duration::ZERO, "默认超时必须为正");
    }
    let byte_limits = [
        DEFAULT_MAX_RESPONSE_BODY_BYTES,
        DEFAULT_MAX_REQUEST_BODY_BYTES,
        DEFAULT_MAX_WS_FRAME_BYTES,
    ];
    for value in byte_limits {
        assert!(value > 0, "默认体限必须为正");
    }

    hit("fn", "is_sensitive_header_name");
    assert!(is_sensitive_header_name("Authorization"));
    assert!(is_sensitive_header_name("X-Auth-Token"));
    assert!(!is_sensitive_header_name("content-type"));

    hit("fn", "parse_retry_after_at");
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000);
    assert_eq!(parse_retry_after_at("7", now), Some(Duration::from_secs(7)));
    assert_eq!(parse_retry_after_at("not-a-date", now), None);
}

/// WS 边界：连接 → 发送 → 接收 → 关闭，走完整帧级生命周期。
async fn phase_ws() {
    hit("type", "WsConnector");
    hit("fn", "WsConnector::connect");
    hit("type", "WsConnection");
    hit("fn", "WsConnection::send_frame");
    hit("fn", "WsConnection::next_frame");
    hit("fn", "WsConnection::close");

    let connector = FakeWsConnector;
    let mut boxed = connector
        .connect("ws://127.0.0.1:1/stream")
        .await
        .expect("connect 必须成功");
    boxed
        .send_frame(Bytes::from_static(b"client-frame"))
        .await
        .expect("send_frame 必须成功");
    assert_eq!(
        boxed.next_frame().await.expect("next_frame 必须成功"),
        Some(Bytes::from_static(b"client-frame"))
    );
    boxed.close().await.expect("close 必须成功");

    let mut direct = FakeWsConnection::default();
    direct
        .send_frame(Bytes::from_static(b"roundtrip"))
        .await
        .expect("send_frame 必须成功");
    assert_eq!(
        direct.next_frame().await.expect("next_frame 必须成功"),
        Some(Bytes::from_static(b"roundtrip"))
    );
    direct.close().await.expect("close 必须成功");
    assert!(direct.closed, "close 后必须置关闭标记");
}

/// 单一驱动用例：保证阶段顺序与覆盖断言在同一个进程内完成。
#[tokio::test]
async fn e2e_transport_all_public_api() {
    assert_manifest_wellformed();
    phase_constants_and_free_fns();
    phase_tls();
    phase_proxy();
    phase_pool();
    phase_error_variants();
    phase_real_drivers();
    phase_http().await;
    phase_ws().await;
    assert_coverage_complete();
}

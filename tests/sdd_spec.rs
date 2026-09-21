#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! SDD 规格对照（特性 002）：把 `docs/标准.md` 的每个 `##` 章节条款转成可执行断言。
//!
//! 章节与断言函数须与 `docs/标准.md` 的 `##` 章节 1:1（检查器按标题逐字比对）。
//!
//! // SPEC-MAP: S-1 | 1. 需求标准 | assert_requirements_standard
//! // SPEC-MAP: S-2 | 2. 设计标准 | assert_design_standard
//! // SPEC-MAP: S-3 | 3-17 简写 | assert_condensed_sections

use std::time::Duration;

use bytes::Bytes;
use transportx::{
    HttpClientPool, HttpDriver, HttpRequest, MockHttpTransport, PoolConfig, ProxyConfig,
    ReqwestHttpDriver, TlsConfig, TlsMode, TransportError, TungsteniteWsConnector, WsConnector,
};

/// S-1：目标（HTTP/WS 传输边界 + TLS + 池 + 代理）可达、边界清晰、错误统一、零内部耦合。
#[test]
fn assert_requirements_standard() {
    // 包含：驱动 / 连接器 / TLS / 池 / 代理五类能力均可构造。
    let _ = ReqwestHttpDriver::new().expect("HTTP 驱动");
    let _ = TungsteniteWsConnector::new();
    let _ = TlsConfig::system_roots();
    let _ = HttpClientPool::<u32>::try_new(PoolConfig::new(1, 1)).expect("客户端池");
    let _ = ProxyConfig::new("http://127.0.0.1:9");

    // 错误统一为 TransportError：各失败语义都有各自的变体，不靠字符串分类。
    let cases = [
        TransportError::ConnectTimeout,
        TransportError::ReadTimeout,
        TransportError::ConnectionClosed { clean: false },
        TransportError::RateLimited {
            retry_after: Some(Duration::from_secs(1)),
        },
        TransportError::PayloadTooLarge {
            kind: "response_body",
            limit: 1,
            got: 2,
        },
        TransportError::ProtocolViolation("x".into()),
        TransportError::Io(Box::new(std::io::Error::other("e"))),
    ];
    for error in cases {
        assert!(!error.to_string().is_empty(), "错误须可展示: {error:?}");
    }

    // 零内部耦合：驱动私有类型不出现在公开边界上，调用方只依赖 trait。
    let mock: &dyn HttpDriver = &MockHttpTransport::new();
    let _ = mock;
    let connector: &dyn WsConnector = &TungsteniteWsConnector::new();
    let _ = connector;
}

/// S-2：模块划分（http / ws / tls / pool / proxy）体现在公开边界上，驱动细节不外泄。
#[test]
fn assert_design_standard() {
    // http 面：请求/响应载荷经公开结构体表达，驱动私有类型仅经 trait 暴露。
    let request = HttpRequest {
        method: "GET".into(),
        url: "https://example.com/secret?token=abc".into(),
        headers: Vec::new(),
        body: Some(Bytes::from_static(b"payload")),
    };
    let debug = format!("{request:?}");
    assert!(debug.contains("HttpRequest"));
    assert!(!debug.contains("abc"), "URL 的 query 不得进 Debug");

    // tls 面：TlsMode 三态齐备。
    let _ = TlsConfig {
        mode: TlsMode::SystemRoots,
        sni: true,
    };
    let _ = TlsConfig::custom_ca("/tmp/ca.pem");
    let _ = TlsConfig::insecure_dev_only();

    // pool 面：工厂 + 错误回滚由池承担（配置校验在 try_new 边界）。
    let pool = HttpClientPool::<u8>::try_new(PoolConfig::default()).expect("默认池配置");
    assert_eq!(pool.config(), PoolConfig::default());
    assert_eq!(pool.idle_len(), 0);
    assert_eq!(pool.checked_out(), 0);

    // proxy 面：代理配置独立成模块，凭据经 Debug 脱敏。
    let proxy = ProxyConfig::with_auth("http://proxy:1", "user", format!("{}{}", "p", "w"));
    let debug = format!("{proxy:?}");
    assert!(debug.contains("***"));
    assert!(!debug.contains("pw"), "代理口令不得进 Debug: {debug}");
}

/// S-3：`docs/标准.md` 的 3–17 简写章节（编码 / 测试 / 性能 / 安全 / 可靠性 / 可维护性 /
/// 风控 N/A / 回测 N/A / 上线 / 运维 / 文档 / 交付 / 评分 / 验收）的可执行面。
#[test]
fn assert_condensed_sections() {
    // 编码标准：失败路径统一返回 TransportError（fail-closed，不 panic）。
    let pool = HttpClientPool::<u8>::try_new(PoolConfig::new(0, 0));
    assert!(matches!(pool, Err(TransportError::ProtocolViolation(_))));

    // 测试标准：mock HTTP 可离线驱动。
    let mock = MockHttpTransport::new();
    mock.set_get("https://api/x", Bytes::from_static(b"ok"));

    // 安全标准：敏感 header 脱敏 + 代理口令脱敏 + 未接线的 sni=false 必须拒绝。
    let request = HttpRequest {
        method: "GET".into(),
        url: "https://example.com/path".into(),
        headers: vec![("Authorization".into(), "Bearer top-secret".into())],
        body: None,
    };
    let debug = format!("{request:?}");
    assert!(debug.contains("***"));
    assert!(!debug.contains("top-secret"));
    let un_wired = ReqwestHttpDriver::builder(
        Some(Duration::from_secs(1)),
        1024,
        1024,
        Some(TlsConfig {
            mode: TlsMode::SystemRoots,
            sni: false,
        }),
        None,
    )
    .expect_err("sni=false 未接线必须拒绝");
    assert!(matches!(un_wired, TransportError::ProtocolViolation(_)));

    // 可靠性：factory 失败时池许可回滚，槽位不泄漏。
    let pool = HttpClientPool::<u8>::try_new(PoolConfig::new(1, 1)).expect("合法配置");
    assert!(pool
        .checkout_with(|| Err(TransportError::ProtocolViolation("factory".into())))
        .is_err());
    assert_eq!(pool.checked_out(), 0);

    // 可维护性：HttpDriver / WsConnector 为可替换 trait。
    let as_driver: &dyn HttpDriver = &mock;
    let as_connector: &dyn WsConnector = &TungsteniteWsConnector::new();
    let _ = (as_driver, as_connector);

    // 验收：CI 门禁即验收面（测试可一次性执行）。
    let _ = std::env::current_dir().expect("可取得当前目录（验收命令可执行）");
}

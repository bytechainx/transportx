#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! AIDD 对抗 / 边界用例（特性 002）。
//!
//! 候选由 AI 生成，逐条人工复核后仅保留「结论=保留」项；丢弃项登记于 PR 描述。
//!
//! // AIDD: Retry-After 取 u64::MAX 秒 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §3-17 解析不得 panic | 结论=保留
//! // AIDD: Retry-After 为过去日期 / 非法值 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §3-17 边界钳制 | 结论=保留
//! // AIDD: 敏感头大小写与子串混合 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §3-17 安全标准脱敏 | 结论=保留
//! // AIDD: 非法 URL 的 Debug | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §3-17 安全标准 fail-closed | 结论=保留
//! // AIDD: body Debug 仅长度 | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §2 设计标准 载荷脱敏 | 结论=保留
//! // AIDD: 代理口令不进 Debug（片段拼接） | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §3-17 安全标准 | 结论=保留
//! // AIDD: 池 max_idle 超 max_pool_size | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §3-17 编码标准 fail-closed | 结论=保留
//! // AIDD: WS 连接非法 URL | 来源=AI | 复核=ZoneCNH/2026-09-22 | 依据=标准.md §3-17 编码标准 失败统一 TransportError | 结论=保留

use std::time::{Duration, SystemTime};

use bytes::Bytes;
use transportx::{
    is_sensitive_header_name, parse_retry_after_at, HttpClientPool, HttpRequest, PoolConfig,
    ProxyConfig, TransportError, TungsteniteWsConnector, WsConnector,
};

/// 边界：`Retry-After` 为极大秒数不 panic，按秒数原样返回。
#[test]
fn retry_after_extreme_seconds() {
    let now = SystemTime::UNIX_EPOCH;
    assert_eq!(
        parse_retry_after_at(&u64::MAX.to_string(), now),
        Some(Duration::from_secs(u64::MAX))
    );
}

/// 边界：过去 HTTP-date 钳制为零；非法 / 空值返回 `None`。
#[test]
fn retry_after_past_date_and_invalid() {
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    assert_eq!(
        parse_retry_after_at("Thu, 01 Jan 1970 00:00:00 GMT", now),
        Some(Duration::ZERO)
    );
    assert_eq!(parse_retry_after_at("not-a-value", now), None);
    assert_eq!(parse_retry_after_at("", now), None);
    assert_eq!(parse_retry_after_at("   ", now), None);
}

/// 边界：敏感头判定大小写无关且含子串命中。
#[test]
fn sensitive_header_matching_is_case_insensitive() {
    for name in [
        "AUTHORIZATION",
        "Set-Cookie",
        "OK-ACCESS-PASSPHRASE",
        "X-Weird-Token",
        "my-secret",
        "APIKEY",
    ] {
        assert!(is_sensitive_header_name(name), "{name} 应判为敏感");
    }
    assert!(!is_sensitive_header_name("Accept-Language"));
}

/// 边界：非法 URL 的 Debug fail-closed，不回显原文。
#[test]
fn invalid_url_debug_is_fail_closed() {
    let request = HttpRequest {
        method: "GET".into(),
        url: "not a url".into(),
        headers: Vec::new(),
        body: None,
    };
    let debug = format!("{request:?}");
    assert!(debug.contains("invalid-url-redacted"));
    assert!(!debug.contains("not a url"), "不得回显原始 URL: {debug}");

    // 无 host 的 URL 同样 fail-closed。
    let opaque = HttpRequest {
        method: "GET".into(),
        url: "mailto:ops@example.com".into(),
        headers: Vec::new(),
        body: None,
    };
    let debug = format!("{opaque:?}");
    assert!(
        !debug.contains("ops@example.com"),
        "userinfo/host 不得泄漏: {debug}"
    );
}

/// 边界：body 的 Debug 只显示长度，不显示内容。
#[test]
fn body_debug_only_shows_length() {
    let request = HttpRequest {
        method: "POST".into(),
        url: "https://api/write".into(),
        headers: Vec::new(),
        body: Some(Bytes::from_static(b"secret-body")),
    };
    let debug = format!("{request:?}");
    assert!(!debug.contains("secret-body"));
    assert!(debug.contains("11 bytes"), "应显示 body 长度: {debug}");

    let empty = HttpRequest {
        method: "POST".into(),
        url: "https://api/write".into(),
        headers: Vec::new(),
        body: None,
    };
    assert!(format!("{empty:?}").contains("None"));
}

/// 边界：代理口令不进 Debug（测试口令由片段拼接，避免硬编码口令误报）。
#[test]
fn proxy_password_is_redacted() {
    let secret = format!("{}-{}", "super", "secret");
    let proxy = ProxyConfig::with_auth("http://proxy:1", "u", secret.clone());
    let debug = format!("{proxy:?}");
    assert!(debug.contains("***"));
    assert!(!debug.contains(secret.as_str()));
}

/// 边界：池 `max_idle > max_pool_size` 与零池大小均 fail-closed。
#[test]
fn pool_config_boundaries_fail_closed() {
    assert!(matches!(
        HttpClientPool::<u8>::try_new(PoolConfig::new(1, 2)),
        Err(TransportError::ProtocolViolation(_))
    ));
    assert!(matches!(
        HttpClientPool::<u8>::try_new(PoolConfig::new(0, 0)),
        Err(TransportError::ProtocolViolation(_))
    ));
    // max_idle 恰等于 max_pool_size 是允许的边界。
    assert!(HttpClientPool::<u8>::try_new(PoolConfig::new(1, 1)).is_ok());
}

/// 边界：WS 非法 URL 映射为统一的 `TransportError::ProtocolViolation`。
#[tokio::test]
async fn ws_invalid_url_is_protocol_violation() {
    // `Box<dyn WsConnection>` 未实现 Debug，故不用 expect_err。
    let error = match TungsteniteWsConnector::new().connect("not-a-url").await {
        Ok(_) => panic!("非法 URL 必须拒绝"),
        Err(error) => error,
    };
    assert!(matches!(error, TransportError::ProtocolViolation(_)));
}

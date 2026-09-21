#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]
//! `HttpClientPool` 公共配置与 RAII lease 合同。

use transportx::{HttpClientPool, PoolConfig, TransportError};

#[test]
fn invalid_pool_config_is_rejected() {
    let zero =
        HttpClientPool::<u32>::try_new(PoolConfig::new(0, 0)).expect_err("大小为零的池必须被拒绝");
    assert!(matches!(zero, TransportError::ProtocolViolation(_)));

    let idle_over_size = HttpClientPool::<u32>::try_new(PoolConfig::new(1, 2))
        .expect_err("空闲上限超过池大小时必须被拒绝");
    assert!(matches!(
        idle_over_size,
        TransportError::ProtocolViolation(_)
    ));
}

#[test]
#[should_panic(
    expected = "HttpClientPool::new 收到无效配置: protocol violation: HTTP 客户端池 max_pool_size 必须大于 0"
)]
fn compatibility_new_fail_fast_uses_the_same_validation() {
    let _ = HttpClientPool::<u32>::new(PoolConfig::new(0, 0));
}

#[test]
fn lease_drop_returns_object_and_recovers_permit() {
    let pool = HttpClientPool::try_new(PoolConfig::new(1, 1)).expect("池配置有效");
    {
        let mut lease = pool
            .checkout_lease_with(|| Ok(7_u32))
            .expect("可借出 lease");
        assert_eq!(lease.get(), Some(&7));
        *lease.get_mut().expect("lease 内对象存在") = 9;
        assert_eq!(pool.checked_out(), 1);
    }
    assert_eq!(pool.checked_out(), 0, "Drop 必须释放许可");
    assert_eq!(pool.idle_len(), 1, "Drop 必须归还对象");

    let lease = pool.checkout_lease_with(|| Ok(99)).expect("可复用 lease");
    assert_eq!(lease.get(), Some(&9), "必须复用上一 lease 归还的对象");
}

#[test]
fn taking_item_releases_permit_exactly_once_without_returning_idle() {
    let pool = HttpClientPool::try_new(PoolConfig::new(1, 1)).expect("池配置有效");
    let lease = pool
        .checkout_lease_with(|| Ok(5_u32))
        .expect("可借出 lease");
    let item = lease.into_inner();
    assert_eq!(item, Some(5));
    assert_eq!(pool.checked_out(), 0);
    assert_eq!(pool.idle_len(), 0);
    let next = pool
        .checkout_lease_with(|| Ok(6_u32))
        .expect("许可仅释放一次");
    assert_eq!(next.get(), Some(&6));
}

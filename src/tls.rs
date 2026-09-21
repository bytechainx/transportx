//! TLS 模式与配置（构建 reqwest Client 时消费）。

use std::path::PathBuf;

/// TLS 校验模式。
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum TlsMode {
    /// 使用系统根证书（生产默认）。
    #[default]
    SystemRoots,
    /// 自定义 CA 证书文件路径（PEM）。
    CustomCa {
        /// PEM 文件路径。
        path: PathBuf,
    },
    /// **仅开发**：跳过证书校验（危险）。
    InsecureDevOnly,
}

/// TLS 相关客户端配置。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsConfig {
    /// 模式。
    pub mode: TlsMode,
    /// 是否启用 SNI（默认 true）；当前设为 false 会被驱动构造明确拒绝。
    pub sni: bool,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self::system_roots()
    }
}

impl TlsConfig {
    /// 系统根证书默认。
    #[must_use]
    pub fn system_roots() -> Self {
        Self {
            mode: TlsMode::SystemRoots,
            sni: true,
        }
    }

    /// 自定义 CA。
    #[must_use]
    pub fn custom_ca(path: impl Into<PathBuf>) -> Self {
        Self {
            mode: TlsMode::CustomCa { path: path.into() },
            sni: true,
        }
    }

    /// 开发用 insecure。
    #[must_use]
    pub fn insecure_dev_only() -> Self {
        Self {
            mode: TlsMode::InsecureDevOnly,
            sni: true,
        }
    }

    /// 是否为危险 insecure 模式。
    #[must_use]
    pub fn is_insecure(&self) -> bool {
        matches!(self.mode, TlsMode::InsecureDevOnly)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tls_defaults() {
        let d = TlsConfig::default();
        assert_eq!(d.mode, TlsMode::SystemRoots);
        assert!(d.sni);
        assert!(!d.is_insecure());
        assert!(TlsConfig::system_roots().sni);
        assert!(TlsConfig::insecure_dev_only().is_insecure());
        let ca = TlsConfig::custom_ca("/tmp/ca.pem");
        assert!(matches!(ca.mode, TlsMode::CustomCa { .. }));
    }
}

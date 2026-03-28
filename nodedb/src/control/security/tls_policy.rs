//! TLS enforcement policy: minimum version, reject cleartext.

use serde::{Deserialize, Serialize};

/// TLS enforcement configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsPolicy {
    /// Minimum TLS version ("1.2" or "1.3"). Default: "1.2".
    #[serde(default = "default_min_tls")]
    pub min_tls_version: String,
    /// Reject cleartext (non-TLS) connections for non-superusers.
    #[serde(default)]
    pub reject_cleartext: bool,
}

fn default_min_tls() -> String {
    "1.2".into()
}

impl Default for TlsPolicy {
    fn default() -> Self {
        Self {
            min_tls_version: default_min_tls(),
            reject_cleartext: false,
        }
    }
}

impl TlsPolicy {
    /// Check if a connection should be rejected based on TLS policy.
    ///
    /// `is_tls` = whether the connection is encrypted.
    /// `is_superuser` = superusers are exempt from cleartext rejection.
    /// Returns `Ok(())` if allowed, `Err(reason)` if rejected.
    pub fn check_connection(&self, is_tls: bool, is_superuser: bool) -> crate::Result<()> {
        if !is_tls && self.reject_cleartext && !is_superuser {
            return Err(crate::Error::RejectedAuthz {
                tenant_id: crate::types::TenantId::new(0),
                resource: format!(
                    "cleartext connections rejected by policy (min_tls_version={})",
                    self.min_tls_version
                ),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleartext_rejected_for_non_superuser() {
        let policy = TlsPolicy {
            reject_cleartext: true,
            min_tls_version: "1.3".into(),
        };
        assert!(policy.check_connection(false, false).is_err());
        assert!(policy.check_connection(false, true).is_ok()); // Superuser exempt.
        assert!(policy.check_connection(true, false).is_ok()); // TLS connection OK.
    }

    #[test]
    fn default_allows_cleartext() {
        let policy = TlsPolicy::default();
        assert!(policy.check_connection(false, false).is_ok());
    }
}

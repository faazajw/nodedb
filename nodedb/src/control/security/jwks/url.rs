//! JWKS URL validation: SSRF defense for the JWKS fetch path.
//!
//! A [`JwksPolicy`] captures the effective allow-list for a running
//! process. The default (`strict`) policy forbids `http://`, IP-literal
//! hosts, and any resolved address outside global unicast — the correct
//! default for public-IdP deployments.
//!
//! For self-hosted IdPs (in-cluster Keycloak, internal Auth0 on a VPC),
//! a relaxed policy can be built from config:
//!
//! ```toml
//! [auth.jwt]
//! allow_http_jwks  = true
//! allow_jwks_hosts = ["keycloak.internal"]
//! allow_jwks_cidrs = ["10.42.0.0/16"]
//! ```
//!
//! Relaxations are **scoped**:
//!
//! - `allow_http_jwks` only applies to URLs whose host is in `hosts`.
//! - A resolved private IP is only accepted if the host is in `hosts`
//!   AND the IP falls in one of the `cidrs`.
//! - IP-literal URLs are never permitted.
//!
//! This keeps the invariant — every relaxation is tied to a named host —
//! so a typo or forgotten flag can't open the whole private IP space.
//!
//! Error messages do not echo the URL back; external input flowing into
//! log lines is its own exfil surface.

use std::collections::HashSet;
use std::net::IpAddr;

use reqwest::Url;

/// URL validation errors for JWKS endpoints.
#[derive(Debug, thiserror::Error)]
pub enum UrlValidationError {
    #[error("JWKS URL is malformed")]
    Malformed,
    #[error("JWKS URL must use https scheme")]
    NonHttps,
    #[error("JWKS URL has no host component")]
    NoHost,
    #[error("JWKS URL must use a DNS name, not an IP literal")]
    IpLiteral,
    #[error("JWKS URL resolves to a non-global address not covered by allow_jwks_cidrs")]
    PrivateAddress,
    #[error("invalid CIDR in allow_jwks_cidrs: {0}")]
    BadCidr(String),
}

/// Effective JWKS allow-list policy.
#[derive(Debug, Clone, Default)]
pub struct JwksPolicy {
    /// Allow `http://` scheme for hosts in `hosts`.
    pub allow_http: bool,
    /// Hostnames (exact, lowercase) that may resolve to private addresses
    /// covered by `cidrs` and, if `allow_http`, may use http scheme.
    pub hosts: HashSet<String>,
    /// CIDR blocks that `hosts` are allowed to resolve into, in addition
    /// to global unicast.
    pub cidrs: Vec<Cidr>,
}

impl JwksPolicy {
    /// Strict default: no relaxations. Correct for public-IdP deployments.
    pub fn strict() -> Self {
        Self::default()
    }

    /// Build a policy from parsed strings. Returns an error if any CIDR
    /// fails to parse.
    pub fn from_parts(
        allow_http: bool,
        hosts: &[String],
        cidrs: &[String],
    ) -> Result<Self, UrlValidationError> {
        let hosts = hosts.iter().map(|h| h.to_ascii_lowercase()).collect();
        let cidrs = cidrs
            .iter()
            .map(|s| Cidr::parse(s).ok_or_else(|| UrlValidationError::BadCidr(s.clone())))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            allow_http,
            hosts,
            cidrs,
        })
    }

    /// Validate a URL string (used initially and for each redirect hop).
    pub fn check_url(&self, url: &str) -> Result<(), UrlValidationError> {
        let parsed = Url::parse(url).map_err(|_| UrlValidationError::Malformed)?;
        self.check_parsed(&parsed)
    }

    /// Validate an already-parsed URL.
    pub fn check_parsed(&self, parsed: &Url) -> Result<(), UrlValidationError> {
        let host = match parsed.domain() {
            Some(d) if !d.is_empty() => d.to_ascii_lowercase(),
            Some(_) => return Err(UrlValidationError::NoHost),
            None if parsed.host_str().is_some() => return Err(UrlValidationError::IpLiteral),
            None => return Err(UrlValidationError::NoHost),
        };
        match parsed.scheme() {
            "https" => Ok(()),
            "http" if self.allow_http && self.hosts.contains(&host) => Ok(()),
            _ => Err(UrlValidationError::NonHttps),
        }
    }

    /// Validate every resolved IP for a hostname. ALL addresses must pass
    /// (AAAA/A split defense).
    pub fn check_resolved(&self, host: &str, addrs: &[IpAddr]) -> Result<(), UrlValidationError> {
        if addrs.is_empty() {
            return Err(UrlValidationError::PrivateAddress);
        }
        let host_lc = host.to_ascii_lowercase();
        let host_allowlisted = self.hosts.contains(&host_lc);
        for ip in addrs {
            if is_global_unicast(ip) {
                continue;
            }
            if host_allowlisted && self.cidrs.iter().any(|c| c.contains(ip)) {
                continue;
            }
            return Err(UrlValidationError::PrivateAddress);
        }
        Ok(())
    }
}

/// Back-compat helper: strict URL validation with no allow-list.
pub fn validate_jwks_url(url: &str) -> Result<(), UrlValidationError> {
    JwksPolicy::strict().check_url(url)
}

/// Back-compat helper: strict resolved-addr validation.
pub fn validate_resolved_addrs(addrs: &[IpAddr]) -> Result<(), UrlValidationError> {
    JwksPolicy::strict().check_resolved("", addrs)
}

/// Back-compat helper: strict parsed-URL validation (used by the redirect
/// policy when no policy-aware closure is in scope).
pub fn validate_parsed_url(parsed: &Url) -> Result<(), UrlValidationError> {
    JwksPolicy::strict().check_parsed(parsed)
}

/// Is this IP a routable, global unicast address?
fn is_global_unicast(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            !(v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_broadcast()
                || v4.is_multicast()
                || v4.is_unspecified()
                || v4.is_documentation()
                || v4.octets()[0] == 0
                || (v4.octets()[0] == 100 && (v4.octets()[1] & 0xC0) == 64)
                || (v4.octets()[0] == 169 && v4.octets()[1] == 254)
                || (v4.octets()[0] == 192 && v4.octets()[1] == 0 && v4.octets()[2] == 0)
                || (v4.octets()[0] == 198 && (v4.octets()[1] & 0xFE) == 18)
                || v4.octets()[0] >= 240)
        }
        IpAddr::V6(v6) => {
            let seg = v6.segments();
            !(v6.is_loopback()
                || v6.is_multicast()
                || v6.is_unspecified()
                || (seg[0] & 0xffc0) == 0xfe80
                || (seg[0] & 0xfe00) == 0xfc00
                || v6
                    .to_ipv4_mapped()
                    .is_some_and(|v4| !is_global_unicast(&IpAddr::V4(v4)))
                || (seg[0] == 0x2001 && seg[1] == 0x0db8))
        }
    }
}

// ── Minimal CIDR type (v4 + v6) ─────────────────────────────────────────

/// A small CIDR block with `contains` support for IPv4 and IPv6.
#[derive(Debug, Clone, Copy)]
pub struct Cidr {
    base: u128,
    mask: u128,
    is_v4: bool,
}

impl Cidr {
    /// Parse a CIDR like `"10.42.0.0/16"` or `"fd00::/8"`. Returns `None`
    /// if the input doesn't match `IP/prefix` with a valid prefix width.
    pub fn parse(s: &str) -> Option<Self> {
        let (ip_str, prefix_str) = s.split_once('/')?;
        let prefix: u8 = prefix_str.parse().ok()?;
        let ip: IpAddr = ip_str.parse().ok()?;
        let (bits, is_v4) = match ip {
            IpAddr::V4(v4) => (u32::from(v4) as u128, true),
            IpAddr::V6(v6) => (u128::from(v6), false),
        };
        let width: u8 = if is_v4 { 32 } else { 128 };
        if prefix > width {
            return None;
        }
        let mask = build_mask(prefix, is_v4);
        Some(Self {
            base: bits & mask,
            mask,
            is_v4,
        })
    }

    /// Does this CIDR contain `ip`?
    pub fn contains(&self, ip: &IpAddr) -> bool {
        match (self.is_v4, ip) {
            (true, IpAddr::V4(v4)) => (u32::from(*v4) as u128) & self.mask == self.base,
            (false, IpAddr::V6(v6)) => u128::from(*v6) & self.mask == self.base,
            _ => false,
        }
    }
}

fn build_mask(prefix: u8, is_v4: bool) -> u128 {
    let width: u8 = if is_v4 { 32 } else { 128 };
    let prefix = prefix.min(width);
    if prefix == 0 {
        0
    } else {
        let shift = width - prefix;
        let full: u128 = if is_v4 { u32::MAX as u128 } else { u128::MAX };
        (full >> shift) << shift & full
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn https_public_dns_passes_strict() {
        validate_jwks_url("https://auth.example.com/.well-known/jwks.json").unwrap();
    }

    #[test]
    fn http_scheme_rejected_strict() {
        assert!(matches!(
            validate_jwks_url("http://example.com/"),
            Err(UrlValidationError::NonHttps)
        ));
    }

    #[test]
    fn ip_literal_always_rejected() {
        let relaxed = JwksPolicy::from_parts(
            true,
            &["ignored".into()],
            &["0.0.0.0/0".into(), "::/0".into()],
        )
        .unwrap();
        for u in [
            "https://127.0.0.1/",
            "https://169.254.169.254/",
            "https://[::1]/",
        ] {
            assert!(matches!(
                relaxed.check_url(u),
                Err(UrlValidationError::IpLiteral)
            ));
        }
    }

    #[test]
    fn http_allowed_only_for_allowlisted_host() {
        let p = JwksPolicy::from_parts(true, &["keycloak.internal".into()], &[]).unwrap();
        p.check_url("http://keycloak.internal/jwks.json").unwrap();
        assert!(p.check_url("http://evil.example.com/").is_err());
    }

    #[test]
    fn private_addrs_rejected_unless_host_and_cidr_match() {
        let p = JwksPolicy::from_parts(
            false,
            &["keycloak.internal".into()],
            &["10.42.0.0/16".into()],
        )
        .unwrap();
        // Allowed host + allowed CIDR.
        p.check_resolved(
            "keycloak.internal",
            &[IpAddr::V4(Ipv4Addr::new(10, 42, 1, 5))],
        )
        .unwrap();
        // Allowed host but CIDR outside list.
        assert!(
            p.check_resolved(
                "keycloak.internal",
                &[IpAddr::V4(Ipv4Addr::new(10, 43, 0, 1))]
            )
            .is_err()
        );
        // Non-allowlisted host — private IP refused.
        assert!(
            p.check_resolved(
                "evil.example.com",
                &[IpAddr::V4(Ipv4Addr::new(10, 42, 0, 1))]
            )
            .is_err()
        );
    }

    #[test]
    fn mixed_addrs_fail_if_any_outside_allowed_set() {
        let p =
            JwksPolicy::from_parts(false, &["h.internal".into()], &["10.0.0.0/8".into()]).unwrap();
        // One public + one private-but-allowed: OK.
        p.check_resolved(
            "h.internal",
            &[
                IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34)),
                IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3)),
            ],
        )
        .unwrap();
        // One public + one loopback (not in cidr): fails.
        assert!(
            p.check_resolved(
                "h.internal",
                &[
                    IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34)),
                    IpAddr::V4(Ipv4Addr::LOCALHOST),
                ],
            )
            .is_err()
        );
    }

    #[test]
    fn v6_unique_local_with_cidr() {
        let p =
            JwksPolicy::from_parts(false, &["v6.internal".into()], &["fd00::/8".into()]).unwrap();
        p.check_resolved(
            "v6.internal",
            &[IpAddr::V6(Ipv6Addr::new(0xfd12, 0, 0, 0, 0, 0, 0, 1))],
        )
        .unwrap();
        // Link-local outside the CIDR stays rejected.
        assert!(
            p.check_resolved(
                "v6.internal",
                &[IpAddr::V6(Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1))]
            )
            .is_err()
        );
    }

    #[test]
    fn bad_cidr_string_errors_at_build_time() {
        assert!(JwksPolicy::from_parts(false, &[], &["not-a-cidr".into()]).is_err());
        assert!(JwksPolicy::from_parts(false, &[], &["10.0.0.0/40".into()]).is_err());
    }

    #[test]
    fn cidr_contains_basic() {
        let c4 = Cidr::parse("10.0.0.0/8").unwrap();
        assert!(c4.contains(&IpAddr::V4(Ipv4Addr::new(10, 99, 0, 1))));
        assert!(!c4.contains(&IpAddr::V4(Ipv4Addr::new(11, 0, 0, 1))));
        let c6 = Cidr::parse("fd00::/8").unwrap();
        assert!(c6.contains(&IpAddr::V6(Ipv6Addr::new(0xfd12, 0, 0, 0, 0, 0, 0, 1))));
        assert!(!c6.contains(&IpAddr::V6(Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1))));
    }
}

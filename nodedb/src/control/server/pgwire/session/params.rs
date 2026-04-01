//! Session parameter methods (SET/SHOW) on SessionStore.

use std::net::SocketAddr;

use super::store::SessionStore;

impl SessionStore {
    /// Set a session parameter.
    pub fn set_parameter(&self, addr: &SocketAddr, key: String, value: String) {
        self.write_session(addr, |session| {
            session.parameters.insert(key, value);
        });
    }

    /// Get a session parameter.
    pub fn get_parameter(&self, addr: &SocketAddr, key: &str) -> Option<String> {
        self.read_session(addr, |s| s.parameters.get(key).cloned())?
    }

    /// Get all session parameters.
    pub fn all_parameters(&self, addr: &SocketAddr) -> Vec<(String, String)> {
        self.read_session(addr, |s| {
            let mut params: Vec<_> = s
                .parameters
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            params.sort_by(|a, b| a.0.cmp(&b.0));
            params
        })
        .unwrap_or_default()
    }
}

/// Parse a SET command: `SET [SESSION|LOCAL] key = value` or `SET key TO value`.
///
/// Returns (key, value) on success, or None if not a valid SET command.
pub fn parse_set_command(sql: &str) -> Option<(String, String)> {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    // Strip SET prefix.
    let rest = if upper.starts_with("SET SESSION ") {
        &trimmed[12..]
    } else if upper.starts_with("SET LOCAL ") {
        &trimmed[10..]
    } else if upper.starts_with("SET ") {
        &trimmed[4..]
    } else {
        return None;
    };

    let rest = rest.trim();

    // Split on = or TO.
    let (key, value) = if let Some(eq_pos) = rest.find('=') {
        let k = rest[..eq_pos].trim();
        let v = rest[eq_pos + 1..].trim();
        (k, v)
    } else {
        // Try TO separator.
        let upper_rest = rest.to_uppercase();
        if let Some(to_pos) = upper_rest.find(" TO ") {
            let k = rest[..to_pos].trim();
            let v = rest[to_pos + 4..].trim();
            (k, v)
        } else {
            return None;
        }
    };

    if key.is_empty() {
        return None;
    }

    // Strip quotes from value.
    let value = value.trim_matches('\'').trim_matches('"').to_string();

    Some((key.to_lowercase(), value))
}

/// Parse a SHOW command: `SHOW <parameter>` or `SHOW ALL`.
///
/// Returns the parameter name, or "all" for SHOW ALL.
pub fn parse_show_command(sql: &str) -> Option<String> {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    if !upper.starts_with("SHOW ") {
        return None;
    }

    let param = trimmed[5..].trim().to_lowercase();
    if param.is_empty() {
        return None;
    }

    Some(param)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_set_equals() {
        let (k, v) = parse_set_command("SET client_encoding = 'UTF8'").unwrap();
        assert_eq!(k, "client_encoding");
        assert_eq!(v, "UTF8");
    }

    #[test]
    fn parse_set_to() {
        let (k, v) = parse_set_command("SET search_path TO public").unwrap();
        assert_eq!(k, "search_path");
        assert_eq!(v, "public");
    }

    #[test]
    fn parse_set_session() {
        let (k, v) = parse_set_command("SET SESSION nodedb.consistency = 'eventual'").unwrap();
        assert_eq!(k, "nodedb.consistency");
        assert_eq!(v, "eventual");
    }

    #[test]
    fn parse_set_nodedb_tenant() {
        let (k, v) = parse_set_command("SET nodedb.tenant_id = 5").unwrap();
        assert_eq!(k, "nodedb.tenant_id");
        assert_eq!(v, "5");
    }

    #[test]
    fn parse_show() {
        assert_eq!(
            parse_show_command("SHOW client_encoding"),
            Some("client_encoding".into())
        );
        assert_eq!(parse_show_command("SHOW ALL"), Some("all".into()));
        assert_eq!(parse_show_command("SHOW"), None);
    }
}

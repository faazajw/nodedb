//! `ALTER ALERT` DDL handler.
//!
//! Syntax:
//! ```sql
//! ALTER ALERT <name> ENABLE
//! ALTER ALERT <name> DISABLE
//! ```

use pgwire::api::results::{Response, Tag};
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::types::{require_admin, sqlstate_error};

pub fn alter_alert(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> PgWireResult<Vec<Response>> {
    require_admin(identity, "alter alerts")?;

    let upper = sql.to_uppercase();
    let tenant_id = identity.tenant_id.as_u32();

    // Extract name: "ALTER ALERT <name> ..."
    let prefix = "ALTER ALERT ";
    if !upper.starts_with(prefix) {
        return Err(sqlstate_error("42601", "expected ALTER ALERT"));
    }
    let rest = &sql[prefix.len()..];
    let name = rest
        .split_whitespace()
        .next()
        .ok_or_else(|| sqlstate_error("42601", "missing alert name"))?
        .to_lowercase();

    let mut def = state
        .alert_registry
        .get(tenant_id, &name)
        .ok_or_else(|| sqlstate_error("42704", &format!("alert '{name}' does not exist")))?;

    if upper.contains(" ENABLE") && !upper.contains("DISABLE") {
        def.enabled = true;
    } else if upper.contains(" DISABLE") {
        def.enabled = false;
    } else {
        return Err(sqlstate_error("42601", "expected ENABLE or DISABLE"));
    }

    let catalog = state
        .credentials
        .catalog()
        .as_ref()
        .ok_or_else(|| sqlstate_error("XX000", "system catalog not available"))?;

    catalog
        .put_alert_rule(&def)
        .map_err(|e| sqlstate_error("XX000", &format!("catalog write: {e}")))?;

    state.alert_registry.update(def);

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(identity.tenant_id),
        &identity.username,
        &format!("ALTER ALERT {name}"),
    );

    Ok(vec![Response::Execution(Tag::new("ALTER ALERT"))])
}

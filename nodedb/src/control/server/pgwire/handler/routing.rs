//! Query routing: consistency selection, leader detection, SQL forwarding,
//! and the execute_planned_sql entry point for DML/query dispatch.

use pgwire::api::results::{Response, Tag};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::planner::physical::PhysicalTask;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::types::{ReadConsistency, TenantId};

use super::super::types::{error_to_sqlstate, response_status_to_sqlstate};
use super::core::NodeDbPgHandler;
use super::plan::{PlanKind, describe_plan, extract_collection, payload_to_response};

impl NodeDbPgHandler {
    /// Plan and dispatch SQL after quota and DDL checks have passed.
    ///
    /// When in a transaction block (BEGIN..COMMIT), write operations are
    /// buffered instead of dispatched. Read operations execute immediately.
    /// The buffer is dispatched atomically on COMMIT.
    pub(super) async fn execute_planned_sql(
        &self,
        identity: &AuthenticatedIdentity,
        sql: &str,
        tenant_id: TenantId,
        addr: &std::net::SocketAddr,
    ) -> PgWireResult<Vec<Response>> {
        // Resolve opaque session handle if SET LOCAL nodedb.auth_session is set.
        let mut auth_ctx = if let Some(handle) =
            self.sessions.get_parameter(addr, "nodedb.auth_session")
            && let Some(cached) = self.state.session_handles.resolve(&handle)
        {
            cached
        } else {
            crate::control::server::session_auth::build_auth_context_with_session(
                identity,
                &self.sessions,
                addr,
            )
        };

        // Extract per-query ON DENY override.
        let clean_sql =
            crate::control::server::session_auth::extract_and_apply_on_deny(sql, &mut auth_ctx);

        // Strip RETURNING clause before DataFusion planning (DataFusion doesn't
        // support RETURNING for DML). The flag is passed per-query through the
        // planning call chain — no shared mutable state.
        let (clean_sql, has_returning) = super::returning::strip_returning(&clean_sql);

        // Check plan cache before full planning. Cache key includes schema_version
        // for automatic invalidation on DDL. RLS policies and permissions are still
        // validated per-query after cache lookup — caching does not bypass security.
        let schema_ver = self.state.schema_version.current();
        let cached_tasks = self.sessions.get_cached_plan(addr, &clean_sql, schema_ver);

        let tasks = if let Some(tasks) = cached_tasks {
            tasks
        } else {
            let perm_cache = self.state.permission_cache.read().await;
            let sec = crate::control::planner::context::PlanSecurityContext {
                identity,
                auth: &auth_ctx,
                rls_store: &self.state.rls,
                permissions: &self.state.permissions,
                roles: &self.state.roles,
                permission_cache: Some(&*perm_cache),
            };
            let planned = self
                .query_ctx
                .plan_sql_with_rls_returning(&clean_sql, tenant_id, &sec, has_returning)
                .await
                .map_err(|e| {
                    let (severity, code, message) = error_to_sqlstate(&e);
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        severity.to_owned(),
                        code.to_owned(),
                        message,
                    )))
                })?;

            // Cache the result for future identical queries.
            self.sessions
                .put_cached_plan(addr, &clean_sql, planned.clone(), schema_ver);

            planned
        };

        if tasks.is_empty() {
            return Ok(vec![Response::Execution(Tag::new("OK"))]);
        }

        let consistency = self.consistency_for_tasks(&tasks);

        if let Some(leader) = self.remote_leader_for_tasks(&tasks, consistency) {
            return self.forward_sql(sql, tenant_id, leader).await;
        }

        let needs_dedup = tasks.iter().any(|t| t.post_dedup);
        let mut dedup_payloads: Vec<Vec<u8>> = Vec::new();
        let mut dedup_plan_kind = None;
        let mut responses = Vec::with_capacity(tasks.len());
        for mut task in tasks {
            if task.tenant_id != tenant_id {
                tracing::error!(
                    expected = %tenant_id,
                    actual = %task.tenant_id,
                    "SECURITY: task tenant_id mismatch — rejecting"
                );
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "42501".to_owned(),
                    "tenant isolation violation: task targets wrong tenant".to_owned(),
                ))));
            }

            self.check_permission(identity, &task.plan)?;

            if self.sessions.transaction_state(addr)
                == crate::control::server::pgwire::session::TransactionState::InBlock
            {
                let is_write = crate::control::wal_replication::to_replicated_entry(
                    task.tenant_id,
                    task.vshard_id,
                    &task.plan,
                )
                .is_some();
                if is_write {
                    self.sessions.buffer_write(addr, task);
                    responses.push(Response::Execution(Tag::new("OK")));
                    continue;
                }
            }

            let plan_kind = describe_plan(&task.plan);
            let collection_for_si = extract_collection(&task.plan).map(String::from);
            let resp_post_dedup = task.post_dedup;

            // --- Trigger interception for DML writes ---
            let dml_info = crate::control::trigger::dml_hook::classify_dml_write(&task.plan);

            // Fetch OLD row and fire BEFORE/INSTEAD OF triggers if applicable.
            let old_row = if let Some(ref info) = dml_info
                && info.document_id.is_some()
                && matches!(
                    info.event,
                    crate::control::trigger::DmlEvent::Update
                        | crate::control::trigger::DmlEvent::Delete
                ) {
                let doc_id = info.document_id.as_deref().unwrap_or("");
                let row = crate::control::trigger::dml_hook::fetch_old_row(
                    &self.state,
                    tenant_id,
                    &info.collection,
                    doc_id,
                )
                .await;
                if row.is_empty() { None } else { Some(row) }
            } else {
                None
            };

            if let Some(ref info) = dml_info {
                use crate::control::trigger::dml_hook::PreDispatchResult;
                let result = crate::control::trigger::dml_hook::fire_pre_dispatch_triggers(
                    &self.state,
                    identity,
                    tenant_id,
                    info,
                    &old_row,
                    0,
                )
                .await
                .map_err(|e| {
                    let (severity, code, message) = error_to_sqlstate(&e);
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        severity.to_owned(),
                        code.to_owned(),
                        message,
                    )))
                })?;

                match result {
                    PreDispatchResult::Handled => {
                        // INSTEAD OF trigger handled the write — skip dispatch.
                        responses.push(Response::Execution(Tag::new("OK")));
                        continue;
                    }
                    PreDispatchResult::Proceed {
                        mutated_fields: Some(ref fields),
                    } => {
                        // BEFORE trigger mutated the row — patch the task.
                        crate::control::trigger::dml_hook::patch_task_with_mutated_fields(
                            &mut task, fields,
                        );
                    }
                    PreDispatchResult::Proceed {
                        mutated_fields: None,
                    } => {}
                }
            }

            // --- Normal dispatch ---
            let resp = self.dispatch_task(task).await.map_err(|e| {
                let (severity, code, message) = error_to_sqlstate(&e);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            })?;

            if let Some((severity, code, message)) =
                response_status_to_sqlstate(resp.status, &resp.error_code)
            {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                ))));
            }

            // --- SYNC AFTER triggers ---
            if let Some(ref info) = dml_info {
                crate::control::trigger::dml_hook::fire_post_dispatch_triggers(
                    &self.state,
                    identity,
                    tenant_id,
                    info,
                    &old_row,
                    0,
                )
                .await
                .map_err(|e| {
                    let (severity, code, message) = error_to_sqlstate(&e);
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        severity.to_owned(),
                        code.to_owned(),
                        message,
                    )))
                })?;

                // Track DML for auto-ANALYZE threshold.
                self.state
                    .dml_counter
                    .record_dml(tenant_id.as_u32(), &info.collection);
            }

            // Track reads for snapshot isolation conflict detection.
            if self.sessions.transaction_state(addr)
                == crate::control::server::pgwire::session::TransactionState::InBlock
                && let Some(collection) = collection_for_si
            {
                self.sessions
                    .record_read(addr, collection, String::new(), resp.watermark_lsn);
            }

            if needs_dedup && resp_post_dedup {
                dedup_payloads.push(resp.payload.to_vec());
                dedup_plan_kind = Some(plan_kind);
            } else {
                responses.push(payload_to_response(&resp.payload, plan_kind));
            }
        }

        // UNION DISTINCT: merge all sub-query payloads and deduplicate rows.
        if needs_dedup && !dedup_payloads.is_empty() {
            let kind = dedup_plan_kind.unwrap_or(PlanKind::MultiRow);
            let merged = dedup_union_payloads(&dedup_payloads);
            responses.push(payload_to_response(&merged, kind));
        }

        Ok(responses)
    }

    /// Determine read consistency for a set of tasks.
    fn consistency_for_tasks(&self, tasks: &[PhysicalTask]) -> ReadConsistency {
        let has_writes = tasks.iter().any(|t| {
            crate::control::wal_replication::to_replicated_entry(t.tenant_id, t.vshard_id, &t.plan)
                .is_some()
        });

        if has_writes {
            ReadConsistency::Strong
        } else {
            ReadConsistency::BoundedStaleness(std::time::Duration::from_secs(5))
        }
    }

    /// Check if all tasks target a single remote leader.
    fn remote_leader_for_tasks(
        &self,
        tasks: &[PhysicalTask],
        consistency: ReadConsistency,
    ) -> Option<u64> {
        let routing = self.state.cluster_routing.as_ref()?;
        let routing = routing.read().unwrap_or_else(|p| p.into_inner());
        let my_node = self.state.node_id;

        let mut remote_leader: Option<u64> = None;

        for task in tasks {
            let vshard_id = task.vshard_id.as_u16();
            let group_id = routing.group_for_vshard(vshard_id).ok()?;
            let info = routing.group_info(group_id)?;
            let leader = info.leader;

            if leader == my_node {
                return None;
            }
            if !consistency.requires_leader() && info.members.contains(&my_node) {
                return None;
            }
            if leader == 0 {
                return None;
            }

            match remote_leader {
                None => remote_leader = Some(leader),
                Some(prev) if prev != leader => return None,
                _ => {}
            }
        }

        remote_leader
    }

    /// Forward a SQL query to a remote leader node via QUIC.
    async fn forward_sql(
        &self,
        sql: &str,
        tenant_id: TenantId,
        leader: u64,
    ) -> PgWireResult<Vec<Response>> {
        let transport = match &self.state.cluster_transport {
            Some(t) => t,
            None => {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "55000".to_owned(),
                    "cluster transport not available".to_owned(),
                ))));
            }
        };

        let req = nodedb_cluster::rpc_codec::RaftRpc::ForwardRequest(
            nodedb_cluster::rpc_codec::ForwardRequest {
                sql: sql.to_owned(),
                tenant_id: tenant_id.as_u32(),
                deadline_remaining_ms: std::time::Duration::from_secs(
                    self.state.tuning.network.default_deadline_secs,
                )
                .as_millis() as u64,
                trace_id: 0,
            },
        );

        let leader_addr = self
            .state
            .cluster_topology
            .as_ref()
            .and_then(|t| {
                let topo = t.read().unwrap_or_else(|p| p.into_inner());
                topo.get_node(leader).map(|n| n.addr.clone())
            })
            .unwrap_or_else(|| format!("node-{leader}"));

        let resp = transport.send_rpc(leader, req).await.map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "01R01".to_owned(),
                format!("not leader; redirect to {leader_addr} (forward failed: {e})"),
            )))
        })?;

        match resp {
            nodedb_cluster::rpc_codec::RaftRpc::ForwardResponse(fwd) => {
                if !fwd.success {
                    return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_owned(),
                        "XX000".to_owned(),
                        format!("remote execution failed: {}", fwd.error_message),
                    ))));
                }

                let mut responses = Vec::with_capacity(fwd.payloads.len());
                for payload in &fwd.payloads {
                    responses.push(payload_to_response(payload, PlanKind::MultiRow));
                }
                if responses.is_empty() {
                    responses.push(Response::Execution(Tag::new("OK")));
                }
                Ok(responses)
            }
            other => Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("unexpected response from leader: {other:?}"),
            )))),
        }
    }
}

/// Merge multiple Data Plane response payloads and deduplicate rows (UNION DISTINCT).
///
/// Each payload is a msgpack-encoded array of rows. Deduplication is performed
/// at the binary level: each row's raw msgpack bytes serve as the canonical key,
/// eliminating the decode → JSON string → re-encode round-trip.
///
/// Output: a single msgpack array containing all unique rows in encounter order.
fn dedup_union_payloads(payloads: &[Vec<u8>]) -> Vec<u8> {
    use nodedb_query::msgpack_scan;

    let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    // Collect raw byte slices of unique rows before writing output.
    let mut unique_row_bytes: Vec<Vec<u8>> = Vec::new();

    for payload in payloads {
        if payload.is_empty() {
            continue;
        }

        let bytes = payload.as_slice();
        let first = bytes[0];

        // Decode msgpack array header to get element count and header length.
        let (count, hdr_len) = if (0x90..=0x9f).contains(&first) {
            ((first & 0x0f) as usize, 1)
        } else if first == 0xdc && bytes.len() >= 3 {
            (u16::from_be_bytes([bytes[1], bytes[2]]) as usize, 3)
        } else if first == 0xdd && bytes.len() >= 5 {
            (
                u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize,
                5,
            )
        } else {
            // Not a msgpack array — treat the whole payload as one row.
            tracing::warn!(
                payload_len = bytes.len(),
                "dedup_union_payloads: payload is not a msgpack array; treating as single row"
            );
            let key = bytes.to_vec();
            if seen.insert(key.clone()) {
                unique_row_bytes.push(key);
            }
            continue;
        };

        let mut pos = hdr_len;
        for _ in 0..count {
            if pos >= bytes.len() {
                break;
            }
            let elem_start = pos;
            match msgpack_scan::skip_value(bytes, pos) {
                Some(next_pos) => {
                    let row_bytes = bytes[elem_start..next_pos].to_vec();
                    if seen.insert(row_bytes.clone()) {
                        unique_row_bytes.push(row_bytes);
                    }
                    pos = next_pos;
                }
                None => {
                    tracing::warn!(
                        pos,
                        payload_len = bytes.len(),
                        "dedup_union_payloads: could not skip msgpack element; stopping early"
                    );
                    break;
                }
            }
        }
    }

    // Write output: msgpack array header + concatenated unique row bytes.
    let row_count = unique_row_bytes.len();
    let total_data: usize = unique_row_bytes.iter().map(|r| r.len()).sum();
    let mut out = Vec::with_capacity(total_data + 5);

    // Write array header.
    if row_count < 16 {
        out.push(0x90 | row_count as u8);
    } else if row_count <= u16::MAX as usize {
        out.push(0xdc);
        out.extend_from_slice(&(row_count as u16).to_be_bytes());
    } else {
        out.push(0xdd);
        out.extend_from_slice(&(row_count as u32).to_be_bytes());
    }

    for row in unique_row_bytes {
        out.extend_from_slice(&row);
    }

    out
}

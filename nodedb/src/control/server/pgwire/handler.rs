use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::sink::Sink;
use futures::stream;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use pgwire::api::stmt::NoopQueryParser;
use pgwire::api::{ClientInfo, ClientPortalStore};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use crate::bridge::envelope::{PhysicalPlan, Priority, Request};
use crate::config::auth::AuthMode;
use crate::control::planner::context::QueryContext;
use crate::control::planner::physical::PhysicalTask;
use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::{
    AuthMethod, AuthenticatedIdentity, Role, required_permission, role_grants_permission,
};
use crate::control::state::SharedState;
use crate::types::{ReadConsistency, RequestId, TenantId};

use super::types::{error_to_sqlstate, response_status_to_sqlstate, text_field};

/// Default request deadline: 30 seconds.
const DEFAULT_DEADLINE: Duration = Duration::from_secs(30);

/// PostgreSQL wire protocol handler for NodeDB.
///
/// Implements `SimpleQueryHandler` + `ExtendedQueryHandler`.
/// Receives SQL strings from clients, resolves the authenticated identity,
/// checks permissions, plans via DataFusion, dispatches to the Data Plane
/// via SPSC, and returns results.
///
/// Lives on the Control Plane (Send + Sync).
pub struct NodeDbPgHandler {
    pub(crate) state: Arc<SharedState>,
    query_ctx: QueryContext,
    next_request_id: AtomicU64,
    query_parser: Arc<NoopQueryParser>,
    auth_mode: AuthMode,
}

impl NodeDbPgHandler {
    pub fn new(state: Arc<SharedState>, auth_mode: AuthMode) -> Self {
        Self {
            state,
            query_ctx: QueryContext::new(),
            next_request_id: AtomicU64::new(1_000_000),
            query_parser: Arc::new(NoopQueryParser::new()),
            auth_mode,
        }
    }

    fn next_request_id(&self) -> RequestId {
        RequestId::new(self.next_request_id.fetch_add(1, Ordering::Relaxed))
    }

    /// Resolve the authenticated identity from pgwire client metadata.
    fn resolve_identity<C: ClientInfo>(&self, client: &C) -> PgWireResult<AuthenticatedIdentity> {
        let username = client
            .metadata()
            .get("user")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());

        match self.auth_mode {
            AuthMode::Trust => {
                if let Some(identity) = self
                    .state
                    .credentials
                    .to_identity(&username, AuthMethod::Trust)
                {
                    Ok(identity)
                } else {
                    Ok(AuthenticatedIdentity {
                        user_id: 0,
                        username,
                        tenant_id: TenantId::new(1),
                        auth_method: AuthMethod::Trust,
                        roles: vec![Role::Superuser],
                        is_superuser: true,
                    })
                }
            }
            AuthMode::Password | AuthMode::Certificate => self
                .state
                .credentials
                .to_identity(&username, AuthMethod::ScramSha256)
                .ok_or_else(|| {
                    PgWireError::UserError(Box::new(ErrorInfo::new(
                        "FATAL".to_owned(),
                        "28000".to_owned(),
                        format!("authenticated user '{username}' not found in credential store"),
                    )))
                }),
        }
    }

    /// Check if the identity has permission for the given plan.
    ///
    /// Uses the full permission resolution stack:
    /// 1. Superuser → always allowed
    /// 2. Ownership → owner has all permissions on their objects
    /// 3. Built-in role grants (ReadWrite → Read+Write, etc.)
    /// 4. Explicit collection-level grants (GRANT ON <collection>)
    /// 5. Inherited custom role grants (via RoleStore)
    fn check_permission(
        &self,
        identity: &AuthenticatedIdentity,
        plan: &PhysicalPlan,
    ) -> PgWireResult<()> {
        let required = required_permission(plan);

        // Extract collection name from the plan for collection-level checks.
        let collection = extract_collection(plan);

        let allowed = match collection {
            Some(coll) => {
                // Collection-specific check: uses PermissionStore with full resolution.
                self.state
                    .permissions
                    .check(identity, required, coll, &self.state.roles)
            }
            None => {
                // No collection context (e.g. WAL append, cancel) — fall back to role check.
                identity.is_superuser
                    || identity
                        .roles
                        .iter()
                        .any(|role| role_grants_permission(role, required))
            }
        };

        if allowed {
            Ok(())
        } else {
            self.state.audit_record(
                AuditEvent::AuthzDenied,
                Some(identity.tenant_id),
                &identity.username,
                &format!(
                    "permission {:?} denied on {}",
                    required,
                    collection.unwrap_or("<none>")
                ),
            );

            Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42501".to_owned(),
                format!(
                    "permission denied: user '{}' lacks {:?} permission{}",
                    identity.username,
                    required,
                    collection.map(|c| format!(" on '{c}'")).unwrap_or_default()
                ),
            ))))
        }
    }

    /// Dispatch a single physical task and wait for the response.
    async fn dispatch_task(
        &self,
        task: PhysicalTask,
    ) -> crate::Result<crate::bridge::envelope::Response> {
        let request_id = self.next_request_id();
        let request = Request {
            request_id,
            tenant_id: task.tenant_id,
            vshard_id: task.vshard_id,
            plan: task.plan,
            deadline: Instant::now() + DEFAULT_DEADLINE,
            priority: Priority::Normal,
            trace_id: 0,
            consistency: ReadConsistency::Strong,
        };

        let rx = self.state.tracker.register(request_id);

        match self.state.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request)?,
            Err(poisoned) => poisoned.into_inner().dispatch(request)?,
        };

        tokio::time::timeout(DEFAULT_DEADLINE, rx)
            .await
            .map_err(|_| crate::Error::DeadlineExceeded { request_id })?
            .map_err(|_| crate::Error::Dispatch {
                detail: "response channel closed".into(),
            })
    }

    /// Execute a SQL query: identity → DDL check → quota → plan → perms → dispatch.
    async fn execute_sql(
        &self,
        identity: &AuthenticatedIdentity,
        sql: &str,
    ) -> PgWireResult<Vec<Response>> {
        let sql_trimmed = sql.trim();

        // Handle SET commands that pgwire clients send during connection setup.
        if sql_trimmed.to_uppercase().starts_with("SET ") {
            return Ok(vec![Response::Execution(Tag::new("SET"))]);
        }
        if sql_trimmed.eq_ignore_ascii_case("DISCARD ALL") {
            return Ok(vec![Response::Execution(Tag::new("DISCARD ALL"))]);
        }
        if sql_trimmed.is_empty() || sql_trimmed == ";" {
            return Ok(vec![Response::EmptyQuery]);
        }

        // Try Control Plane DDL commands (CREATE USER, GRANT, SHOW, etc.).
        if let Some(result) = super::ddl::dispatch(&self.state, identity, sql_trimmed) {
            return result;
        }

        // Tenant derived from authenticated identity — never from client.
        let tenant_id = identity.tenant_id;

        // Tenant quota check before planning.
        self.state.check_tenant_quota(tenant_id).map_err(|e| {
            let (severity, code, message) = error_to_sqlstate(&e);
            PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                code.to_owned(),
                message,
            )))
        })?;

        self.state.tenant_request_start(tenant_id);
        let result = self.execute_planned_sql(identity, sql, tenant_id).await;
        self.state.tenant_request_end(tenant_id);

        result
    }

    /// Plan and dispatch SQL after quota and DDL checks have passed.
    async fn execute_planned_sql(
        &self,
        identity: &AuthenticatedIdentity,
        sql: &str,
        tenant_id: TenantId,
    ) -> PgWireResult<Vec<Response>> {
        let tasks = self.query_ctx.plan_sql(sql, tenant_id).await.map_err(|e| {
            let (severity, code, message) = error_to_sqlstate(&e);
            PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                code.to_owned(),
                message,
            )))
        })?;

        if tasks.is_empty() {
            return Ok(vec![Response::Execution(Tag::new("OK"))]);
        }

        // In cluster mode, check if all tasks target a remote leader.
        // If so, forward the entire SQL to the leader node.
        if let Some(leader) = self.remote_leader_for_tasks(&tasks) {
            return self.forward_sql(sql, tenant_id, leader).await;
        }

        let mut responses = Vec::with_capacity(tasks.len());
        for task in tasks {
            self.check_permission(identity, &task.plan)?;

            let plan_kind = describe_plan(&task.plan);
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

            responses.push(payload_to_response(&resp.payload, plan_kind));
        }

        Ok(responses)
    }

    /// Check if all tasks target a single remote leader. Returns the leader's
    /// node_id if forwarding is needed, None if all tasks are local.
    fn remote_leader_for_tasks(&self, tasks: &[PhysicalTask]) -> Option<u64> {
        let routing = self.state.cluster_routing.as_ref()?;
        let routing = routing.read().unwrap_or_else(|p| p.into_inner());
        let my_node = self.state.node_id;

        let mut remote_leader: Option<u64> = None;

        for task in tasks {
            let vshard_id = task.vshard_id.as_u16();
            let group_id = routing.group_for_vshard(vshard_id).ok()?;
            let info = routing.group_info(group_id)?;
            let leader = info.leader;

            if leader == 0 || leader == my_node {
                // Local or no leader — dispatch locally.
                return None;
            }

            match remote_leader {
                None => remote_leader = Some(leader),
                Some(prev) if prev != leader => {
                    // Tasks target different remote leaders — can't forward as one.
                    // Fall back to local dispatch (scatter-gather is a later feature).
                    return None;
                }
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
                deadline_remaining_ms: DEFAULT_DEADLINE.as_millis() as u64,
                trace_id: 0,
            },
        );

        let resp = transport.send_rpc(leader, req).await.map_err(|e| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "08006".to_owned(),
                format!("forward to leader node {leader} failed: {e}"),
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

                // Convert forwarded payloads back to pgwire Responses.
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

// ── Plan classification ─────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
enum PlanKind {
    SingleDocument,
    MultiRow,
    Execution,
}

/// Extract the collection name from a physical plan (if applicable).
fn extract_collection(plan: &PhysicalPlan) -> Option<&str> {
    match plan {
        PhysicalPlan::PointGet { collection, .. }
        | PhysicalPlan::VectorSearch { collection, .. }
        | PhysicalPlan::RangeScan { collection, .. }
        | PhysicalPlan::CrdtRead { collection, .. }
        | PhysicalPlan::CrdtApply { collection, .. }
        | PhysicalPlan::VectorInsert { collection, .. }
        | PhysicalPlan::PointPut { collection, .. }
        | PhysicalPlan::PointDelete { collection, .. }
        | PhysicalPlan::GraphRagFusion { collection, .. }
        | PhysicalPlan::SetCollectionPolicy { collection, .. } => Some(collection.as_str()),
        PhysicalPlan::EdgePut { .. }
        | PhysicalPlan::EdgeDelete { .. }
        | PhysicalPlan::GraphHop { .. }
        | PhysicalPlan::GraphNeighbors { .. }
        | PhysicalPlan::GraphPath { .. }
        | PhysicalPlan::GraphSubgraph { .. }
        | PhysicalPlan::WalAppend { .. }
        | PhysicalPlan::Cancel { .. } => None,
    }
}

fn describe_plan(plan: &PhysicalPlan) -> PlanKind {
    match plan {
        PhysicalPlan::PointGet { .. } | PhysicalPlan::CrdtRead { .. } => PlanKind::SingleDocument,
        PhysicalPlan::VectorSearch { .. }
        | PhysicalPlan::RangeScan { .. }
        | PhysicalPlan::GraphHop { .. }
        | PhysicalPlan::GraphNeighbors { .. }
        | PhysicalPlan::GraphPath { .. }
        | PhysicalPlan::GraphSubgraph { .. }
        | PhysicalPlan::GraphRagFusion { .. } => PlanKind::MultiRow,
        _ => PlanKind::Execution,
    }
}

fn payload_to_response(payload: &[u8], kind: PlanKind) -> Response {
    match kind {
        PlanKind::Execution => Response::Execution(Tag::new("OK")),
        PlanKind::SingleDocument | PlanKind::MultiRow => {
            let col_name = if matches!(kind, PlanKind::SingleDocument) {
                "document"
            } else {
                "result"
            };
            let schema = Arc::new(vec![text_field(col_name)]);
            if payload.is_empty() {
                Response::Query(QueryResponse::new(schema, stream::empty()))
            } else {
                let text = String::from_utf8_lossy(payload).into_owned();
                let mut encoder = DataRowEncoder::new(schema.clone());
                if let Err(e) = encoder.encode_field(&text) {
                    tracing::error!(error = %e, "failed to encode field");
                    return Response::Execution(Tag::new("ERROR"));
                }
                let row = encoder.take_row();
                Response::Query(QueryResponse::new(schema, stream::iter(vec![Ok(row)])))
            }
        }
    }
}

// ── SimpleQueryHandler ──────────────────────────────────────────────

#[async_trait]
impl SimpleQueryHandler for NodeDbPgHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let identity = self.resolve_identity(client)?;
        self.execute_sql(&identity, query).await
    }
}

// ── ExtendedQueryHandler ────────────────────────────────────────────

#[async_trait]
impl ExtendedQueryHandler for NodeDbPgHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.query_parser.clone()
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &pgwire::api::portal::Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let identity = self.resolve_identity(client)?;
        let query = &portal.statement.statement;
        let mut results = self.execute_sql(&identity, query).await?;
        Ok(results.pop().unwrap_or(Response::EmptyQuery))
    }
}

// Trust mode: NoopStartupHandler (no authentication).
impl NoopStartupHandler for NodeDbPgHandler {}

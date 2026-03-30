//! Scheduler executor: Tokio task that evaluates cron expressions every second.
//!
//! For each due schedule, dispatches the SQL body through the Control Plane
//! query path using a system identity (SECURITY DEFINER).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::watch;
use tracing::{debug, info, trace, warn};

use crate::control::planner::procedural::executor::bindings::RowBindings;
use crate::control::planner::procedural::executor::core::StatementExecutor;
use crate::control::security::identity::{AuthMethod, AuthenticatedIdentity, Role};
use crate::control::state::SharedState;
use crate::types::TenantId;

use super::cron::CronExpr;
use super::history::JobHistoryStore;
use super::registry::ScheduleRegistry;
use super::types::{JobRun, ScheduleDef};

/// Spawn the scheduler loop as a background Tokio task.
pub fn spawn_scheduler(
    state: Arc<SharedState>,
    registry: Arc<ScheduleRegistry>,
    history: Arc<JobHistoryStore>,
    shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        scheduler_loop(state, registry, history, shutdown).await;
    })
}

/// The main scheduler loop. Runs every second.
async fn scheduler_loop(
    state: Arc<SharedState>,
    registry: Arc<ScheduleRegistry>,
    history: Arc<JobHistoryStore>,
    mut shutdown: watch::Receiver<bool>,
) {
    info!("scheduler started");

    // Track currently running jobs (for ALLOW_OVERLAP = false enforcement).
    // Shared with spawned job tasks so they remove themselves on completion.
    let running: Arc<std::sync::Mutex<HashSet<(u32, String)>>> =
        Arc::new(std::sync::Mutex::new(HashSet::new()));

    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    debug!("scheduler shutting down");
                    return;
                }
            }
        }

        if *shutdown.borrow() {
            return;
        }

        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Get all enabled schedules.
        let schedules = registry.list_all_enabled();
        if schedules.is_empty() {
            continue;
        }

        for sched in &schedules {
            // Parse cron expression.
            let cron = match CronExpr::parse(&sched.cron_expr) {
                Ok(c) => c,
                Err(e) => {
                    warn!(
                        schedule = %sched.name,
                        error = %e,
                        "invalid cron expression, skipping"
                    );
                    continue;
                }
            };

            // Check if this second matches the cron expression.
            // We check at second-level granularity but cron is minute-level,
            // so only fire at second 0 of each minute to prevent duplicate fires.
            if !now_secs.is_multiple_of(60) {
                continue;
            }

            if !cron.matches_epoch(now_secs) {
                continue;
            }

            // Check overlap policy.
            let key = (sched.tenant_id, sched.name.clone());
            if !sched.allow_overlap {
                let guard = running.lock().unwrap_or_else(|p| p.into_inner());
                if guard.contains(&key) {
                    trace!(
                        schedule = %sched.name,
                        "skipping: previous run still active (ALLOW_OVERLAP = false)"
                    );
                    continue;
                }
            }

            // Mark as running before spawning (prevents race with next scheduler tick).
            {
                let mut guard = running.lock().unwrap_or_else(|p| p.into_inner());
                guard.insert(key.clone());
            }

            debug!(schedule = %sched.name, "firing scheduled job");

            let state_clone = Arc::clone(&state);
            let history_clone = Arc::clone(&history);
            let running_clone = Arc::clone(&running);
            let sched_clone = sched.clone();

            // Spawn each job as a separate task so the scheduler loop doesn't block.
            // The task removes itself from `running` on completion.
            tokio::spawn(async move {
                let result = execute_job(&state_clone, &sched_clone).await;
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;

                let run = match result {
                    Ok(duration_ms) => JobRun {
                        schedule_name: sched_clone.name.clone(),
                        tenant_id: sched_clone.tenant_id,
                        started_at: now_ms.saturating_sub(duration_ms),
                        duration_ms,
                        success: true,
                        error: None,
                    },
                    Err(e) => {
                        warn!(
                            schedule = %sched_clone.name,
                            error = %e,
                            "scheduled job failed"
                        );
                        JobRun {
                            schedule_name: sched_clone.name.clone(),
                            tenant_id: sched_clone.tenant_id,
                            started_at: now_ms,
                            duration_ms: 0,
                            success: false,
                            error: Some(e.to_string()),
                        }
                    }
                };

                if let Err(e) = history_clone.record(run) {
                    warn!(error = %e, "failed to record job history");
                }

                // Remove from running set — allows next scheduled fire.
                let key = (sched_clone.tenant_id, sched_clone.name.clone());
                let mut guard = running_clone.lock().unwrap_or_else(|p| p.into_inner());
                guard.remove(&key);
            });
        }
    }
}

/// Execute a single scheduled job.
///
/// Returns the duration in milliseconds on success.
async fn execute_job(state: &SharedState, sched: &ScheduleDef) -> crate::Result<u64> {
    let start = std::time::Instant::now();
    let identity = scheduler_identity(TenantId::new(sched.tenant_id), &sched.owner);

    // Parse and execute the body as procedural SQL.
    let block = crate::control::planner::procedural::parse_block(&sched.body_sql).map_err(|e| {
        crate::Error::BadRequest {
            detail: format!("schedule '{}' body parse error: {e}", sched.name),
        }
    })?;

    let executor = StatementExecutor::new(state, identity, TenantId::new(sched.tenant_id), 0);
    let bindings = RowBindings::empty();
    executor.execute_block(&block, &bindings).await?;

    Ok(start.elapsed().as_millis() as u64)
}

/// Build a system identity for scheduled job execution (SECURITY DEFINER).
fn scheduler_identity(tenant_id: TenantId, owner: &str) -> AuthenticatedIdentity {
    AuthenticatedIdentity {
        user_id: 0,
        username: format!("_scheduler:{owner}"),
        tenant_id,
        auth_method: AuthMethod::Trust,
        roles: vec![Role::Superuser],
        is_superuser: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheduler_identity_is_superuser() {
        let id = scheduler_identity(TenantId::new(1), "admin");
        assert!(id.is_superuser);
        assert_eq!(id.username, "_scheduler:admin");
    }
}

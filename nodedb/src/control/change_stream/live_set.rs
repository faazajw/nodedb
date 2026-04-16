//! Per-connection set of live CDC subscription forwarder tasks.
//!
//! WS `LIVE SELECT` subscriptions spawn a Tokio task per subscription that
//! forwards filtered change events to the connection's sender. Without
//! lifecycle tracking, a client that RST-s the TCP socket leaves the
//! forwarder tasks alive — each pinning a `Subscription` (broadcast
//! receiver + counter) until the broadcast channel closes.
//!
//! `LiveSubscriptionSet` owns a `JoinSet` of those tasks. Dropping the set
//! aborts every task; each abort causes the `Subscription` owned by the
//! task to drop, whose `Drop` decrements `active_subscriptions`. The WS
//! route moves this set into the connection's scope so disconnect tears
//! down every subscription opened by that connection.

use std::future::Future;

use tokio::task::JoinSet;

use super::stream::{ChangeEvent, Subscription};

/// Tracks forwarder tasks for every LIVE subscription opened on a single
/// connection. Aborts all tasks on drop.
pub struct LiveSubscriptionSet {
    tasks: JoinSet<()>,
}

impl LiveSubscriptionSet {
    pub fn new() -> Self {
        Self {
            tasks: JoinSet::new(),
        }
    }

    /// Spawn a forwarder task that pulls events from `sub` and calls
    /// `on_event` for each. The task is owned by this set — aborting the
    /// set drops the `Subscription`, which decrements the active counter.
    pub fn spawn_forwarder<F>(&mut self, sub: Subscription, mut on_event: F)
    where
        F: FnMut(&ChangeEvent) + Send + 'static,
    {
        let mut sub = sub;
        self.tasks.spawn(async move {
            while let Ok(event) = sub.recv_filtered().await {
                on_event(&event);
            }
        });
    }

    /// Spawn an arbitrary forwarder future into the set. The WS route
    /// uses this to capture an async forwarder that owns the `Subscription`
    /// and sends notifications onto the connection's live-notification
    /// channel — aborting the set drops the captured `Subscription`.
    pub fn spawn_task<F>(&mut self, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.tasks.spawn(fut);
    }

    /// Abort every spawned forwarder. Used by the shutdown bus to drain
    /// all LIVE subscriptions cluster-wide without waiting for the
    /// broadcast channel to close.
    pub fn abort_all(&mut self) {
        self.tasks.abort_all();
    }

    /// Number of live forwarder tasks still outstanding.
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
}

impl Default for LiveSubscriptionSet {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for LiveSubscriptionSet {
    fn drop(&mut self) {
        self.tasks.abort_all();
    }
}

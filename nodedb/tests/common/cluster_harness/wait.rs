//! Poll a predicate with a deadline. Same shape as
//! `nodedb-cluster/tests/common/mod.rs::wait_for`, copied rather than
//! shared because test harnesses cross crate boundaries.

use std::time::{Duration, Instant};

pub async fn wait_for<F: FnMut() -> bool>(
    desc: &str,
    deadline: Duration,
    step: Duration,
    mut pred: F,
) {
    let start = Instant::now();
    while start.elapsed() < deadline {
        if pred() {
            return;
        }
        tokio::time::sleep(step).await;
    }
    panic!("timed out after {:?} waiting for: {}", deadline, desc);
}

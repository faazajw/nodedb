//! Lock recovery utilities with observability.
//!
//! Centralizes the poisoned-lock recovery pattern used across the Control
//! Plane. Recovers from panics that occurred while holding the lock and
//! logs a warning so the incident is visible in observability.

use std::sync::{MutexGuard, PoisonError, RwLockReadGuard, RwLockWriteGuard};

/// Recover a poisoned `RwLock` write guard, logging a warning.
pub fn write_or_recover<'a, T>(
    result: Result<RwLockWriteGuard<'a, T>, PoisonError<RwLockWriteGuard<'a, T>>>,
    lock_name: &str,
) -> RwLockWriteGuard<'a, T> {
    result.unwrap_or_else(|p| {
        tracing::warn!(lock = lock_name, "RwLock poisoned (write), recovering");
        p.into_inner()
    })
}

/// Recover a poisoned `RwLock` read guard, logging a warning.
pub fn read_or_recover<'a, T>(
    result: Result<RwLockReadGuard<'a, T>, PoisonError<RwLockReadGuard<'a, T>>>,
    lock_name: &str,
) -> RwLockReadGuard<'a, T> {
    result.unwrap_or_else(|p| {
        tracing::warn!(lock = lock_name, "RwLock poisoned (read), recovering");
        p.into_inner()
    })
}

/// Recover a poisoned `Mutex` guard, logging a warning.
pub fn lock_or_recover<'a, T>(
    result: Result<MutexGuard<'a, T>, PoisonError<MutexGuard<'a, T>>>,
    lock_name: &str,
) -> MutexGuard<'a, T> {
    result.unwrap_or_else(|p| {
        tracing::warn!(lock = lock_name, "Mutex poisoned, recovering");
        p.into_inner()
    })
}

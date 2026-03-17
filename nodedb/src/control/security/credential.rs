use std::collections::HashMap;
use std::path::Path;
use std::sync::RwLock;
use std::time::Instant;

use tracing::info;

use crate::types::TenantId;

use super::catalog::{StoredUser, SystemCatalog};
use super::identity::{AuthMethod, AuthenticatedIdentity, Role};

/// Tracks failed login attempts for lockout enforcement.
#[derive(Debug, Clone)]
struct LoginAttemptTracker {
    /// Number of consecutive failed attempts.
    failed_count: u32,
    /// When the lockout expires (if locked out).
    locked_until: Option<Instant>,
}

/// A stored user record (in-memory cache).
#[derive(Debug, Clone)]
pub struct UserRecord {
    pub user_id: u64,
    pub username: String,
    pub tenant_id: TenantId,
    /// Argon2id password hash (PHC string format).
    pub password_hash: String,
    /// Salt used for SCRAM-SHA-256 (16 bytes).
    pub scram_salt: Vec<u8>,
    /// SCRAM-SHA-256 salted password (for pgwire auth).
    pub scram_salted_password: Vec<u8>,
    pub roles: Vec<Role>,
    pub is_superuser: bool,
    pub is_active: bool,
}

impl UserRecord {
    fn to_stored(&self) -> StoredUser {
        StoredUser {
            user_id: self.user_id,
            username: self.username.clone(),
            tenant_id: self.tenant_id.as_u32(),
            password_hash: self.password_hash.clone(),
            scram_salt: self.scram_salt.clone(),
            scram_salted_password: self.scram_salted_password.clone(),
            roles: self.roles.iter().map(|r| r.to_string()).collect(),
            is_superuser: self.is_superuser,
            is_active: self.is_active,
        }
    }

    fn from_stored(s: StoredUser) -> Self {
        let roles: Vec<Role> = s
            .roles
            .iter()
            .map(|r| r.parse().unwrap_or(Role::ReadOnly))
            .collect();
        Self {
            user_id: s.user_id,
            username: s.username,
            tenant_id: TenantId::new(s.tenant_id),
            password_hash: s.password_hash,
            scram_salt: s.scram_salt,
            scram_salted_password: s.scram_salted_password,
            is_superuser: s.is_superuser,
            is_active: s.is_active,
            roles,
        }
    }
}

/// Credential store with in-memory cache and redb persistence.
///
/// Reads hit the in-memory cache (fast). Writes go to redb first (ACID),
/// then update the cache. On startup, all records are loaded from redb.
///
/// Lives on the Control Plane (Send + Sync).
pub struct CredentialStore {
    users: RwLock<HashMap<String, UserRecord>>,
    next_user_id: RwLock<u64>,
    catalog: Option<SystemCatalog>,
    /// Failed login tracking (in-memory only — clears on restart).
    login_attempts: RwLock<HashMap<String, LoginAttemptTracker>>,
    /// Max failed logins before lockout (0 = disabled).
    max_failed_logins: u32,
    /// Lockout duration.
    lockout_duration: std::time::Duration,
}

impl Default for CredentialStore {
    fn default() -> Self {
        Self::new()
    }
}

fn read_lock<T>(lock: &RwLock<T>) -> crate::Result<std::sync::RwLockReadGuard<'_, T>> {
    lock.read().map_err(|e| {
        tracing::error!("credential store read lock poisoned: {e}");
        crate::Error::Internal {
            detail: "credential store lock poisoned".into(),
        }
    })
}

fn write_lock<T>(lock: &RwLock<T>) -> crate::Result<std::sync::RwLockWriteGuard<'_, T>> {
    lock.write().map_err(|e| {
        tracing::error!("credential store write lock poisoned: {e}");
        crate::Error::Internal {
            detail: "credential store lock poisoned".into(),
        }
    })
}

impl CredentialStore {
    /// Create an in-memory-only credential store (for tests).
    pub fn new() -> Self {
        Self {
            users: RwLock::new(HashMap::new()),
            next_user_id: RwLock::new(1),
            catalog: None,
            login_attempts: RwLock::new(HashMap::new()),
            max_failed_logins: 0, // Disabled in tests.
            lockout_duration: std::time::Duration::from_secs(300),
        }
    }

    /// Open a persistent credential store backed by redb.
    ///
    /// `path` is the system catalog file (e.g. `{data_dir}/system.redb`).
    /// Loads all existing users into the in-memory cache.
    pub fn open(path: &Path) -> crate::Result<Self> {
        let catalog = SystemCatalog::open(path)?;

        // Load existing users.
        let stored_users = catalog.load_all_users()?;
        let next_id = catalog.load_next_user_id()?;

        let mut users = HashMap::with_capacity(stored_users.len());
        for stored in stored_users {
            let record = UserRecord::from_stored(stored);
            users.insert(record.username.clone(), record);
        }

        let count = users.len();
        if count > 0 {
            info!(count, "loaded users from system catalog");
        }

        Ok(Self {
            users: RwLock::new(users),
            next_user_id: RwLock::new(next_id),
            catalog: Some(catalog),
            login_attempts: RwLock::new(HashMap::new()),
            max_failed_logins: 0,
            lockout_duration: std::time::Duration::from_secs(300),
        })
    }

    /// Configure lockout policy. Called after construction with values from AuthConfig.
    pub fn set_lockout_policy(&mut self, max_failed: u32, lockout_secs: u64) {
        self.max_failed_logins = max_failed;
        self.lockout_duration = std::time::Duration::from_secs(lockout_secs);
    }

    /// Check if a user is currently locked out.
    /// Returns Ok(()) if not locked out, Err if locked out.
    pub fn check_lockout(&self, username: &str) -> crate::Result<()> {
        if self.max_failed_logins == 0 {
            return Ok(()); // Lockout disabled.
        }

        let attempts = match read_lock(&self.login_attempts) {
            Ok(a) => a,
            Err(_) => {
                tracing::error!(
                    "login_attempts lock poisoned in check_lockout, allowing access as fallback"
                );
                return Ok(());
            }
        };

        if let Some(tracker) = attempts.get(username) {
            if let Some(locked_until) = tracker.locked_until {
                if Instant::now() < locked_until {
                    return Err(crate::Error::RejectedAuthz {
                        tenant_id: TenantId::new(0),
                        resource: format!(
                            "user '{username}' is locked out ({} failed attempts)",
                            tracker.failed_count
                        ),
                    });
                }
            }
        }

        Ok(())
    }

    /// Record a failed login attempt. May trigger lockout.
    pub fn record_login_failure(&self, username: &str) {
        if self.max_failed_logins == 0 {
            return;
        }

        let mut attempts = match write_lock(&self.login_attempts) {
            Ok(a) => a,
            Err(_) => {
                tracing::error!("login_attempts lock poisoned in record_login_failure");
                return;
            }
        };

        let tracker = attempts
            .entry(username.to_string())
            .or_insert(LoginAttemptTracker {
                failed_count: 0,
                locked_until: None,
            });

        tracker.failed_count += 1;

        if tracker.failed_count >= self.max_failed_logins {
            tracker.locked_until = Some(Instant::now() + self.lockout_duration);
            tracing::warn!(
                username,
                failed_count = tracker.failed_count,
                lockout_secs = self.lockout_duration.as_secs(),
                "user locked out due to failed login attempts"
            );
        }
    }

    /// Reset failed login counter on successful authentication.
    pub fn record_login_success(&self, username: &str) {
        if self.max_failed_logins == 0 {
            return;
        }

        let mut attempts = match write_lock(&self.login_attempts) {
            Ok(a) => a,
            Err(_) => {
                tracing::error!("login_attempts lock poisoned in record_login_success");
                return;
            }
        };

        attempts.remove(username);
    }

    /// Persist a user record to the catalog (if persistent).
    fn persist_user(&self, record: &UserRecord) -> crate::Result<()> {
        if let Some(ref catalog) = self.catalog {
            catalog.put_user(&record.to_stored())?;
        }
        Ok(())
    }

    /// Persist the next_user_id counter (if persistent).
    fn persist_next_id(&self, id: u64) -> crate::Result<()> {
        if let Some(ref catalog) = self.catalog {
            catalog.save_next_user_id(id)?;
        }
        Ok(())
    }

    fn alloc_user_id(&self) -> crate::Result<u64> {
        let mut next = write_lock(&self.next_user_id)?;
        let id = *next;
        *next += 1;
        self.persist_next_id(*next)?;
        Ok(id)
    }

    /// Bootstrap the superuser from config. Called once on startup.
    /// If the user already exists (loaded from catalog), updates the password.
    pub fn bootstrap_superuser(&self, username: &str, password: &str) -> crate::Result<()> {
        let salt = generate_scram_salt();
        let scram_salted_password = compute_scram_salted_password(password, &salt);
        let password_hash = hash_password_argon2(password)?;

        let mut users = write_lock(&self.users)?;

        if let Some(existing) = users.get_mut(username) {
            // User exists from catalog — update password and ensure active + superuser.
            existing.password_hash = password_hash;
            existing.scram_salt = salt;
            existing.scram_salted_password = scram_salted_password;
            existing.is_superuser = true;
            existing.is_active = true;
            if !existing.roles.contains(&Role::Superuser) {
                existing.roles.push(Role::Superuser);
            }
            self.persist_user(existing)?;
        } else {
            let user_id = self.alloc_user_id()?;
            let record = UserRecord {
                user_id,
                username: username.to_string(),
                tenant_id: TenantId::new(0),
                password_hash,
                scram_salt: salt,
                scram_salted_password,
                roles: vec![Role::Superuser],
                is_superuser: true,
                is_active: true,
            };
            self.persist_user(&record)?;
            users.insert(username.to_string(), record);
        }

        Ok(())
    }

    /// Create a new user. Returns the user_id.
    pub fn create_user(
        &self,
        username: &str,
        password: &str,
        tenant_id: TenantId,
        roles: Vec<Role>,
    ) -> crate::Result<u64> {
        let mut users = write_lock(&self.users)?;
        if users.contains_key(username) {
            return Err(crate::Error::BadRequest {
                detail: format!("user '{username}' already exists"),
            });
        }

        let salt = generate_scram_salt();
        let scram_salted_password = compute_scram_salted_password(password, &salt);
        let password_hash = hash_password_argon2(password)?;
        let user_id = self.alloc_user_id()?;

        let is_superuser = roles.contains(&Role::Superuser);
        let record = UserRecord {
            user_id,
            username: username.to_string(),
            tenant_id,
            password_hash,
            scram_salt: salt,
            scram_salted_password,
            roles,
            is_superuser,
            is_active: true,
        };

        self.persist_user(&record)?;
        users.insert(username.to_string(), record);
        Ok(user_id)
    }

    /// Look up a user by username. Returns None if not found or inactive.
    pub fn get_user(&self, username: &str) -> Option<UserRecord> {
        let users = read_lock(&self.users).ok()?;
        users.get(username).filter(|u| u.is_active).cloned()
    }

    /// Get the SCRAM salt and salted password for pgwire SCRAM auth.
    pub fn get_scram_credentials(&self, username: &str) -> Option<(Vec<u8>, Vec<u8>)> {
        let users = read_lock(&self.users).ok()?;
        users
            .get(username)
            .filter(|u| u.is_active)
            .map(|u| (u.scram_salt.clone(), u.scram_salted_password.clone()))
    }

    /// Verify a cleartext password against the stored Argon2 hash.
    pub fn verify_password(&self, username: &str, password: &str) -> bool {
        let users = match read_lock(&self.users) {
            Ok(u) => u,
            Err(_) => {
                let _ = hash_password_argon2(password);
                return false;
            }
        };
        match users.get(username).filter(|u| u.is_active) {
            Some(record) => verify_argon2(&record.password_hash, password),
            None => {
                let _ = hash_password_argon2(password);
                false
            }
        }
    }

    /// Build an `AuthenticatedIdentity` for a verified user.
    pub fn to_identity(&self, username: &str, method: AuthMethod) -> Option<AuthenticatedIdentity> {
        self.get_user(username).map(|record| AuthenticatedIdentity {
            user_id: record.user_id,
            username: record.username,
            tenant_id: record.tenant_id,
            auth_method: method,
            roles: record.roles,
            is_superuser: record.is_superuser,
        })
    }

    /// Deactivate a user (soft delete). Persists the change.
    pub fn deactivate_user(&self, username: &str) -> crate::Result<bool> {
        let mut users = write_lock(&self.users)?;
        if let Some(record) = users.get_mut(username) {
            record.is_active = false;
            self.persist_user(record)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Update a user's password. Recomputes both Argon2 hash and SCRAM credentials.
    pub fn update_password(&self, username: &str, password: &str) -> crate::Result<()> {
        let mut users = write_lock(&self.users)?;
        let record = users
            .get_mut(username)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: format!("user '{username}' not found"),
            })?;
        if !record.is_active {
            return Err(crate::Error::BadRequest {
                detail: format!("user '{username}' is inactive"),
            });
        }
        let salt = generate_scram_salt();
        record.scram_salted_password = compute_scram_salted_password(password, &salt);
        record.scram_salt = salt;
        record.password_hash = hash_password_argon2(password)?;
        self.persist_user(record)?;
        Ok(())
    }

    /// Replace all roles for a user.
    pub fn update_roles(&self, username: &str, roles: Vec<Role>) -> crate::Result<()> {
        let mut users = write_lock(&self.users)?;
        let record = users
            .get_mut(username)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: format!("user '{username}' not found"),
            })?;
        record.is_superuser = roles.contains(&Role::Superuser);
        record.roles = roles;
        self.persist_user(record)?;
        Ok(())
    }

    /// Add a role to a user (if not already present).
    pub fn add_role(&self, username: &str, role: Role) -> crate::Result<()> {
        let mut users = write_lock(&self.users)?;
        let record = users
            .get_mut(username)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: format!("user '{username}' not found"),
            })?;
        if !record.roles.contains(&role) {
            record.roles.push(role.clone());
            if matches!(role, Role::Superuser) {
                record.is_superuser = true;
            }
        }
        self.persist_user(record)?;
        Ok(())
    }

    /// Remove a role from a user.
    pub fn remove_role(&self, username: &str, role: &Role) -> crate::Result<()> {
        let mut users = write_lock(&self.users)?;
        let record = users
            .get_mut(username)
            .ok_or_else(|| crate::Error::BadRequest {
                detail: format!("user '{username}' not found"),
            })?;
        record.roles.retain(|r| r != role);
        if matches!(role, Role::Superuser) {
            record.is_superuser = false;
        }
        self.persist_user(record)?;
        Ok(())
    }

    /// List all active users with full details (for SHOW USERS).
    pub fn list_user_details(&self) -> Vec<UserRecord> {
        let users = match read_lock(&self.users) {
            Ok(u) => u,
            Err(_) => return Vec::new(),
        };
        users.values().filter(|u| u.is_active).cloned().collect()
    }

    /// List all active usernames.
    pub fn list_users(&self) -> Vec<String> {
        let users = match read_lock(&self.users) {
            Ok(u) => u,
            Err(_) => return Vec::new(),
        };
        users
            .values()
            .filter(|u| u.is_active)
            .map(|u| u.username.clone())
            .collect()
    }

    /// Check if any users exist.
    pub fn is_empty(&self) -> bool {
        read_lock(&self.users).map(|u| u.is_empty()).unwrap_or(true)
    }

    /// Access the underlying system catalog (for API key persistence).
    pub fn catalog(&self) -> &Option<SystemCatalog> {
        &self.catalog
    }
}

// ── Password hashing ───────────────────────────────────────────────

use argon2::Argon2;
use argon2::password_hash::{
    PasswordHash, PasswordHasher, PasswordVerifier, SaltString, rand_core::OsRng,
};

fn generate_scram_salt() -> Vec<u8> {
    use argon2::password_hash::rand_core::RngCore;
    let mut salt = vec![0u8; 16];
    OsRng.fill_bytes(&mut salt);
    salt
}

fn compute_scram_salted_password(password: &str, salt: &[u8]) -> Vec<u8> {
    pgwire::api::auth::sasl::scram::gen_salted_password(password, salt, 4096)
}

fn hash_password_argon2(password: &str) -> crate::Result<String> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let hash = argon2
        .hash_password(password.as_bytes(), &salt)
        .map_err(|e| crate::Error::Internal {
            detail: format!("argon2 hashing failed: {e}"),
        })?;
    Ok(hash.to_string())
}

fn verify_argon2(stored_hash: &str, password: &str) -> bool {
    let parsed = match PasswordHash::new(stored_hash) {
        Ok(h) => h,
        Err(_) => return false,
    };
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_memory_create_and_verify() {
        let store = CredentialStore::new();
        store.bootstrap_superuser("admin", "secret").unwrap();
        assert!(store.verify_password("admin", "secret"));
        assert!(!store.verify_password("admin", "wrong"));
    }

    #[test]
    fn persistent_create_and_reload() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");

        // Create user in persistent store.
        {
            let store = CredentialStore::open(&path).unwrap();
            store
                .create_user("alice", "pass123", TenantId::new(1), vec![Role::ReadWrite])
                .unwrap();
            store.bootstrap_superuser("admin", "secret").unwrap();
        }

        // Reopen — users should survive.
        {
            let store = CredentialStore::open(&path).unwrap();
            let alice = store.get_user("alice").unwrap();
            assert_eq!(alice.tenant_id, TenantId::new(1));
            assert!(alice.roles.contains(&Role::ReadWrite));

            // Verify password still works after reload.
            assert!(store.verify_password("alice", "pass123"));

            // Superuser should also exist.
            let admin = store.get_user("admin").unwrap();
            assert!(admin.is_superuser);
        }
    }

    #[test]
    fn persistent_deactivate_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");

        {
            let store = CredentialStore::open(&path).unwrap();
            store
                .create_user("bob", "pass", TenantId::new(1), vec![Role::ReadOnly])
                .unwrap();
            store.deactivate_user("bob").unwrap();
        }

        {
            let store = CredentialStore::open(&path).unwrap();
            // Deactivated user should not be visible.
            assert!(store.get_user("bob").is_none());
        }
    }

    #[test]
    fn persistent_role_changes_survive_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");

        {
            let store = CredentialStore::open(&path).unwrap();
            store
                .create_user("carol", "pass", TenantId::new(1), vec![Role::ReadOnly])
                .unwrap();
            store.add_role("carol", Role::ReadWrite).unwrap();
            store.remove_role("carol", &Role::ReadOnly).unwrap();
        }

        {
            let store = CredentialStore::open(&path).unwrap();
            let carol = store.get_user("carol").unwrap();
            assert!(carol.roles.contains(&Role::ReadWrite));
            assert!(!carol.roles.contains(&Role::ReadOnly));
        }
    }

    #[test]
    fn persistent_password_change_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");

        {
            let store = CredentialStore::open(&path).unwrap();
            store
                .create_user("dave", "old_pass", TenantId::new(1), vec![Role::ReadWrite])
                .unwrap();
            store.update_password("dave", "new_pass").unwrap();
        }

        {
            let store = CredentialStore::open(&path).unwrap();
            assert!(store.verify_password("dave", "new_pass"));
            assert!(!store.verify_password("dave", "old_pass"));
        }
    }

    #[test]
    fn lockout_after_max_failures() {
        let mut store = CredentialStore::new();
        store.set_lockout_policy(3, 300);
        store.bootstrap_superuser("admin", "secret").unwrap();

        // First 2 failures — not locked out yet.
        store.record_login_failure("admin");
        store.record_login_failure("admin");
        assert!(store.check_lockout("admin").is_ok());

        // Third failure — locked out.
        store.record_login_failure("admin");
        assert!(store.check_lockout("admin").is_err());
    }

    #[test]
    fn lockout_resets_on_success() {
        let mut store = CredentialStore::new();
        store.set_lockout_policy(3, 300);
        store.bootstrap_superuser("admin", "secret").unwrap();

        store.record_login_failure("admin");
        store.record_login_failure("admin");

        // Successful login resets counter.
        store.record_login_success("admin");

        // Next failure is first again, not third.
        store.record_login_failure("admin");
        assert!(store.check_lockout("admin").is_ok());
    }

    #[test]
    fn lockout_disabled_when_zero() {
        let store = CredentialStore::new(); // max_failed_logins = 0
        store.bootstrap_superuser("admin", "secret").unwrap();

        for _ in 0..100 {
            store.record_login_failure("admin");
        }
        assert!(store.check_lockout("admin").is_ok());
    }

    #[test]
    fn user_id_counter_persists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.redb");

        let first_id;
        {
            let store = CredentialStore::open(&path).unwrap();
            first_id = store
                .create_user("u1", "p", TenantId::new(1), vec![])
                .unwrap();
            store
                .create_user("u2", "p", TenantId::new(1), vec![])
                .unwrap();
        }

        {
            let store = CredentialStore::open(&path).unwrap();
            let next_id = store
                .create_user("u3", "p", TenantId::new(1), vec![])
                .unwrap();
            // Should continue from where we left off, not restart at 1.
            assert!(next_id > first_id + 1);
        }
    }
}

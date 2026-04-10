# Security

NodeDB has a defense-in-depth security model covering authentication, authorization, encryption, and audit.

## Guides

- [Authentication](auth.md) — Users, passwords, API keys, JWKS, mTLS
- [Roles & Permissions (RBAC)](rbac.md) — CREATE ROLE, GRANT, REVOKE, permission hierarchy
- [Row-Level Security (RLS)](rls.md) — Per-row filtering based on auth context
- [Audit Log](audit.md) — Hash-chained audit trail, change tracking, SIEM export
- [Multi-Tenancy](tenants.md) — Tenant isolation, quotas, purge

## Encryption

- **At rest** — AES-256-XTS for data volumes, AES-256-GCM for WAL segments, per-file data encryption keys
- **In transit** — TLS for all protocols (pgwire, HTTP, WebSocket, native)
- **Lite devices** — AES-256-GCM + Argon2id key derivation for on-device encryption

## Quick Reference

```sql
-- Create a user
CREATE USER alice WITH PASSWORD 'secret' ROLE readwrite;

-- Row-level security
CREATE RLS POLICY own_data ON orders FOR ALL
    USING (customer_id = $auth.id);

-- View audit log
SHOW AUDIT LOG LIMIT 50;

-- Typeguard-based change tracking (schemaless)
CREATE TYPEGUARD ON users (
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP VALUE now()
);
```

[Back to docs](../README.md)

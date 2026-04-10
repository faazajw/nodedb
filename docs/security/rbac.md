# Roles & Permissions (RBAC)

Role-based access control with fine-grained permissions on collections, functions, and procedures.

## Roles

```sql
CREATE ROLE analyst;
CREATE ROLE data_engineer;
```

### Built-in Roles

| Role           | Permissions                    |
| -------------- | ------------------------------ |
| `readonly`     | SELECT on all collections      |
| `readwrite`    | SELECT, INSERT, UPDATE, DELETE |
| `admin`        | All operations + DDL           |
| `tenant_admin` | Admin within a tenant          |
| `superuser`    | Unrestricted (cross-tenant)    |

## Granting Permissions

```sql
-- Collection-level
GRANT SELECT ON orders TO analyst;
GRANT INSERT, UPDATE ON orders TO data_engineer;
GRANT ALL ON orders TO admin;

-- Function/procedure execute
GRANT EXECUTE ON FUNCTION full_name TO analyst;
GRANT EXECUTE ON PROCEDURE transfer_funds TO data_engineer;

-- Tenant backup
GRANT BACKUP ON TENANT acme TO ops_user;
```

## Revoking Permissions

```sql
REVOKE INSERT ON orders FROM analyst;
REVOKE EXECUTE ON FUNCTION full_name FROM analyst;
```

## Introspection

```sql
SHOW GRANTS FOR analyst;
SHOW PERMISSIONS;
```

## SECURITY DEFINER

Functions and triggers can execute with the owner's permissions instead of the caller's:

```sql
CREATE FUNCTION admin_count() RETURNS INT
    SECURITY DEFINER
    AS BEGIN
        RETURN (SELECT COUNT(*) FROM audit_log);
    END;
```

Use with caution — this is intentional privilege escalation.

## Permission Hierarchy

```
superuser
  └── tenant_admin (scoped to one tenant)
        └── admin (DDL + DML)
              └── readwrite (DML only)
                    └── readonly (SELECT only)
```

Higher roles inherit all permissions of lower roles.

[Back to security](README.md)

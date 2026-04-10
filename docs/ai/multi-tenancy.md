# Multi-Tenancy for AI SaaS

NodeDB isolates tenants at the WAL level — not as an application-layer filter. Tenant ID is embedded in every WAL record, every vShard routing decision, and every RLS predicate. A customer's embeddings are never even read by another customer's query.

## WAL-Level Tenant Isolation

Every write in NodeDB carries a `tenant_id` in the WAL record header. This is enforced by the database, not by application code. There is no way to bypass it — even internal system queries (triggers, CRDT sync, scheduled jobs) carry the originating tenant context.

```sql
-- Create tenants
CREATE TENANT acme;
CREATE TENANT globex;

-- Each tenant's data is hermetically separated
-- Tenant acme's session:
INSERT INTO chunks {
    id: 'chunk-001',
    content: 'ACME product documentation...',
    embedding: [0.12, -0.34, ...]
};

-- Tenant globex cannot see acme's data, even with the same collection name.
-- The WAL, vShard routing, and storage are all scoped by tenant_id.
```

**What this means for AI:**

- Tenant A's embeddings never appear in tenant B's vector search results
- Tenant A's graph edges are invisible to tenant B's MATCH queries
- BM25 term frequencies are computed per-tenant (no cross-tenant IDF pollution)
- CDC change streams are tenant-scoped — a consumer group only sees its own tenant's events

## RLS Predicates During Vector Search

Row-Level Security predicates are evaluated during HNSW traversal, not as a post-filter. This means filtered vector search returns exactly `top_k` results that the user is authorized to see — without over-fetching or leaking unauthorized result counts.

```sql
-- RLS policy: users only see documents in their department
CREATE RLS POLICY dept_filter ON chunks FOR read
    USING (department = $auth.department);

-- RLS policy: users only see their own or public documents
CREATE RLS POLICY visibility ON chunks FOR read
    USING (visibility = 'public' OR author_id = $auth.id);

-- Vector search automatically applies RLS
-- User in "engineering" department sees only engineering chunks
SELECT id, content, vector_distance(embedding, $query_embedding) AS score
FROM chunks
WHERE embedding <-> $query_embedding
LIMIT 10;
-- Returns 10 results, all from the engineering department.
-- No post-filter truncation, no need to over-fetch.
```

**How it works internally:**

1. The Control Plane injects RLS predicates into the query plan at parse time
2. The Data Plane evaluates RLS predicates on each HNSW candidate after distance computation
3. Candidates that fail RLS are skipped (not counted toward top_k)
4. HNSW continues traversal until top_k authorized results are found or the graph is exhausted
5. The over-fetch factor automatically adjusts based on the estimated selectivity

## Per-Tenant Vector Index Budgets

Each tenant can have memory and storage budgets for their vector indexes. When a tenant exceeds their RAM budget, new sealed segments spill to NVMe (L1 tier) instead of staying in RAM — preserving search correctness while bounding resource usage.

```sql
-- Set tenant quotas
ALTER TENANT acme SET QUOTA
    max_vectors = 10000000,
    max_storage_gb = 50;

ALTER TENANT globex SET QUOTA
    max_vectors = 1000000,
    max_storage_gb = 10;

-- Monitor tenant usage
SHOW TENANT USAGE FOR acme;
-- Returns: vector_count, storage_gb, query_count_24h, etc.

SHOW TENANT QUOTA FOR acme;
-- Returns: max_vectors, max_storage_gb, current usage percentages
```

**Budget enforcement:**

- When a tenant's vector count approaches `max_vectors`, new inserts return a quota exceeded error
- When RAM usage exceeds the per-tenant budget, new sealed HNSW segments use mmap (L1 NVMe) instead of RAM — search still works, just with slightly higher latency for reranking
- Monitoring via `SHOW TENANT USAGE` lets you track growth and right-size quotas

## Tenant-Scoped Embedding Model Metadata

Track which embedding model each tenant uses for each vector column. This is critical when different tenants onboard at different times and use different model versions.

```sql
-- Tenant acme uses text-embedding-3-large
ALTER COLLECTION chunks SET VECTOR METADATA ON embedding (
    model = 'text-embedding-3-large',
    dimensions = 1536
);

-- Tenant globex uses an older model
-- (In globex's session context)
ALTER COLLECTION chunks SET VECTOR METADATA ON embedding (
    model = 'text-embedding-ada-002',
    dimensions = 1536
);

-- Audit all tenants' embedding models
-- (Superuser query across all tenants)
SHOW VECTOR MODELS;
-- Returns: tenant_id, collection, column, model, dimensions, created_at

-- Identify tenants still using the old model
-- (Plan migration: re-embed chunks for tenants on ada-002)
```

**Migration pattern:** When upgrading embedding models across tenants:

1. Query `SHOW VECTOR MODELS` to identify tenants on the old model
2. For each tenant, add a new vector column (`embedding_v2`) with the new model's metadata
3. Re-embed documents using the new model, writing to `embedding_v2`
4. Update the vector index to point to the new column
5. Drop the old column after verifying search quality

## RBAC for AI Endpoints

Control which users and service accounts can perform AI operations.

```sql
-- Create roles for different AI access levels
CREATE ROLE ai_reader;
CREATE ROLE ai_writer;
CREATE ROLE ai_admin;

-- ai_reader: can search but not modify
GRANT SELECT ON chunks TO ai_reader;
GRANT SELECT ON entities TO ai_reader;

-- ai_writer: can insert embeddings and documents
GRANT SELECT, INSERT, UPDATE ON chunks TO ai_writer;
GRANT SELECT, INSERT, UPDATE ON entities TO ai_writer;

-- ai_admin: can manage indexes and model metadata
GRANT ALL ON chunks TO ai_admin;
GRANT ALL ON entities TO ai_admin;

-- Service account for the embedding pipeline
CREATE SERVICE ACCOUNT embedding_service;
GRANT ai_writer TO embedding_service;

-- API key for the embedding service
CREATE API KEY FOR embedding_service;
```

This ensures your embedding pipeline can write vectors but can't drop collections, and your read-only search API can't modify data.

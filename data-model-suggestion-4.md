# Data Model Suggestion 4: Graph-Relational (Lineage-First)

> Project: Feature Store · Created: 2026-05-20

## Philosophy

This model places the feature lineage DAG (directed acyclic graph) at the centre of the data architecture. Every registry object — data source, entity, feature view, feature, feature service, model — is a node in a property graph. Every relationship between them — "feeds into", "derived from", "consumed by", "trained with" — is an edge with typed properties. The graph is implemented using relational tables (`graph_nodes` and `graph_edges`) with recursive CTE support, avoiding the need for a separate graph database while enabling full DAG traversal.

Feature stores are fundamentally graph problems: data flows from sources through transformations to feature views, which are composed into feature services, which feed models. Understanding this graph is critical for impact analysis ("if this data source goes down, which models are affected?"), cost attribution ("how much compute does feature X consume across all its downstream consumers?"), and compliance ("what is the full provenance chain of this prediction?"). Traditional relational models treat lineage as an afterthought; this model makes it the primary organising principle.

The design borrows from property graph databases (Neo4j, Apache AGE) but implements entirely in PostgreSQL using an adjacency list with typed edges, combined with conventional relational tables for operational data that benefits from strict schemas (materialisation jobs, access control, monitoring metrics).

**Best for:** Teams where lineage, impact analysis, and dependency management are primary use cases; organisations with complex multi-team feature sharing; environments where understanding the full data flow graph is a regulatory or operational requirement.

**Trade-offs:**
- (+) Lineage queries are first-class: "give me everything upstream/downstream of X" is a single recursive CTE
- (+) Impact analysis is trivial: traverse the graph to find all affected objects before a change
- (+) Natural model for feature discovery: graph traversal finds related features by connectivity
- (+) Extensible: new node and edge types require no schema changes
- (+) AI-powered recommendations can walk the graph to suggest features
- (-) Simple registry CRUD requires joining through the graph layer (more complex writes)
- (-) Property graph queries via SQL recursive CTEs are less ergonomic than Cypher/GQL
- (-) Graph traversal performance degrades with very deep/wide graphs without careful indexing
- (-) Two-layer architecture (graph + relational) increases cognitive complexity
- (-) More storage overhead per entity due to node/edge metadata

---

## Standards Alignment

| Standard | How It's Used |
|----------|---------------|
| Feast Data Model | Entity, FeatureView, DataSource, FeatureService are node types in the graph |
| Feast Type System (Value.proto) | Feature value types stored as node properties |
| GQL (ISO/IEC 39075) | Graph query patterns align with the emerging SQL/GQL standard; ready for PostgreSQL AGE or future native graph support |
| Apache Parquet / Delta Lake / Iceberg | Referenced in DataSource node properties |
| OpenTelemetry | Graph edges carry tracing metadata for distributed lineage |
| NIST AI RMF | Full provenance graph satisfies data lineage, bias documentation, and model governance requirements |
| GDPR | Graph traversal identifies all nodes containing PII for erasure workflows |

---

## Graph Layer

### Nodes

```sql
CREATE TABLE graph_nodes (
    node_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL,
    node_type       VARCHAR(50) NOT NULL,
    -- Node types:
    --   data_source, entity, feature_view, feature, feature_service,
    --   stream_feature_view, on_demand_feature_view,
    --   transformation, model, dataset
    name            VARCHAR(255) NOT NULL,
    -- Properties bag: all node-type-specific attributes
    properties      JSONB NOT NULL DEFAULT '{}',
    -- Status
    is_active       BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (project_id, node_type, name)
);

CREATE INDEX idx_nodes_project ON graph_nodes (project_id);
CREATE INDEX idx_nodes_type ON graph_nodes (node_type);
CREATE INDEX idx_nodes_project_type ON graph_nodes (project_id, node_type) WHERE is_active;
CREATE INDEX idx_nodes_properties ON graph_nodes USING GIN (properties);
CREATE INDEX idx_nodes_name_trgm ON graph_nodes USING GIN (name gin_trgm_ops);
-- Requires: CREATE EXTENSION IF NOT EXISTS pg_trgm;
```

### Edges

```sql
CREATE TABLE graph_edges (
    edge_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_node_id  UUID NOT NULL REFERENCES graph_nodes(node_id) ON DELETE CASCADE,
    target_node_id  UUID NOT NULL REFERENCES graph_nodes(node_id) ON DELETE CASCADE,
    edge_type       VARCHAR(50) NOT NULL,
    -- Edge types:
    --   has_entity        (feature_view -> entity)
    --   has_feature       (feature_view -> feature)
    --   reads_from        (feature_view -> data_source)
    --   includes_view     (feature_service -> feature_view)
    --   derived_from      (on_demand_feature_view -> feature_view)
    --   trained_with      (model -> feature_service)
    --   materialised_to   (feature_view -> online_store_config)
    --   produced_dataset  (feature_service -> dataset)
    --   transforms        (transformation -> feature_view)
    -- Properties bag: edge-specific attributes
    properties      JSONB NOT NULL DEFAULT '{}',
    -- properties example for 'reads_from':
    -- {
    --   "timestamp_column": "event_timestamp",
    --   "freshness_dependency": true
    -- }
    -- properties example for 'trained_with':
    -- {
    --   "model_version": "v3.2",
    --   "training_date": "2026-05-15",
    --   "feature_view_version": 2
    -- }
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_node_id, target_node_id, edge_type)
);

CREATE INDEX idx_edges_source ON graph_edges (source_node_id);
CREATE INDEX idx_edges_target ON graph_edges (target_node_id);
CREATE INDEX idx_edges_type ON graph_edges (edge_type);
CREATE INDEX idx_edges_source_type ON graph_edges (source_node_id, edge_type);
CREATE INDEX idx_edges_target_type ON graph_edges (target_node_id, edge_type);
```

---

## Node Property Examples

```sql
-- DataSource node properties:
-- {
--   "source_type": "BIGQUERY",
--   "table": "my-project.ml_features.driver_stats",
--   "timestamp_column": "event_timestamp",
--   "format": null,
--   "owner": "data-eng@example.com",
--   "description": "Raw driver telemetry from BigQuery",
--   "tags": ["production"]
-- }

-- Entity node properties:
-- {
--   "join_key": "driver_id",
--   "value_type": "INT64",
--   "description": "Unique driver identifier",
--   "owner": "ml-platform@example.com"
-- }

-- FeatureView node properties:
-- {
--   "description": "Hourly aggregated driver statistics",
--   "ttl_seconds": 86400,
--   "online_enabled": true,
--   "version": 2,
--   "owner": "ml-platform@example.com",
--   "tags": ["production", "driver"]
-- }

-- Feature node properties:
-- {
--   "value_type": "FLOAT64",
--   "description": "Driver conversion rate (accepted / offered)",
--   "tags": ["kpi"],
--   "statistics": {
--     "mean": 0.42,
--     "stddev": 0.15,
--     "null_fraction": 0.0001
--   }
-- }

-- FeatureService node properties:
-- {
--   "description": "All features for driver ETA model",
--   "owner": "ml-platform@example.com",
--   "serving_endpoint": "/v1/features/driver-eta",
--   "tags": ["production"]
-- }

-- Model node properties:
-- {
--   "model_name": "driver-eta-v3",
--   "framework": "xgboost",
--   "mlflow_run_id": "abc123",
--   "training_date": "2026-05-15",
--   "metrics": {"rmse": 2.3, "mae": 1.8}
-- }
```

---

## Projects Table (Relational)

```sql
CREATE TABLE projects (
    project_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(255) NOT NULL UNIQUE,
    description     TEXT,
    owner_email     VARCHAR(255),
    settings        JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

## Materialisation (Relational)

```sql
CREATE TABLE materialisation_jobs (
    job_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    feature_view_node_id UUID NOT NULL REFERENCES graph_nodes(node_id),
    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    records_written BIGINT,
    error_message   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_mat_jobs_node ON materialisation_jobs (feature_view_node_id);
CREATE INDEX idx_mat_jobs_status ON materialisation_jobs (status);

CREATE TABLE materialisation_state (
    feature_view_node_id UUID PRIMARY KEY REFERENCES graph_nodes(node_id),
    last_materialised_at TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

## Access Control (Relational)

```sql
CREATE TABLE users (
    user_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email           VARCHAR(255) NOT NULL UNIQUE,
    display_name    VARCHAR(255),
    auth_provider   VARCHAR(50),
    auth_subject    VARCHAR(255),
    is_active       BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE user_project_roles (
    user_id         UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    project_id      UUID NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    role            VARCHAR(100) NOT NULL,
    PRIMARY KEY (user_id, project_id, role)
);

-- Node-level access control: grants on specific graph nodes
CREATE TABLE node_grants (
    grant_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    node_id         UUID NOT NULL REFERENCES graph_nodes(node_id) ON DELETE CASCADE,
    actions         TEXT[] NOT NULL,         -- ['read', 'write', 'materialize', 'serve']
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, node_id)
);

CREATE INDEX idx_node_grants_node ON node_grants (node_id);

CREATE TABLE api_keys (
    api_key_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    project_id      UUID REFERENCES projects(project_id),
    key_hash        VARCHAR(255) NOT NULL UNIQUE,
    name            VARCHAR(255) NOT NULL,
    expires_at      TIMESTAMPTZ,
    is_active       BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

## Monitoring (Relational with Graph References)

```sql
CREATE TABLE feature_statistics (
    stat_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    feature_node_id UUID NOT NULL REFERENCES graph_nodes(node_id),
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    row_count       BIGINT,
    null_count      BIGINT,
    null_fraction   DOUBLE PRECISION,
    mean            DOUBLE PRECISION,
    stddev          DOUBLE PRECISION,
    min_value       TEXT,
    max_value       TEXT,
    unique_count    BIGINT,
    percentiles     JSONB,                  -- {"p50": 0.41, "p95": 0.72, "p99": 0.89}
    drift_score     DOUBLE PRECISION,
    baseline_stat_id UUID REFERENCES feature_statistics(stat_id)
);

CREATE INDEX idx_stats_node ON feature_statistics (feature_node_id, computed_at DESC);

CREATE TABLE alerts (
    alert_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    node_id         UUID NOT NULL REFERENCES graph_nodes(node_id),
    alert_type      VARCHAR(50) NOT NULL,
    severity        VARCHAR(20) NOT NULL,
    message         TEXT,
    details         JSONB,
    acknowledged    BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_alerts_node ON alerts (node_id);
CREATE INDEX idx_alerts_unack ON alerts (severity) WHERE NOT acknowledged;
```

---

## Audit Log

```sql
CREATE TABLE audit_log (
    log_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID,
    actor_type      VARCHAR(20) NOT NULL,
    actor_id        UUID,
    action          VARCHAR(100) NOT NULL,
    node_id         UUID REFERENCES graph_nodes(node_id),
    edge_id         UUID REFERENCES graph_edges(edge_id),
    details         JSONB,
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_audit_project ON audit_log (project_id, occurred_at DESC);
CREATE INDEX idx_audit_node ON audit_log (node_id) WHERE node_id IS NOT NULL;
```

---

## Graph Traversal Query Examples

```sql
-- Prerequisite: CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- 1. Full upstream lineage: "What feeds into this feature view?"
WITH RECURSIVE upstream AS (
    -- Start from the target feature view
    SELECT e.source_node_id AS node_id, e.edge_type, 1 AS depth
    FROM graph_edges e
    WHERE e.target_node_id = $1  -- feature_view_node_id
      AND e.edge_type IN ('reads_from', 'derived_from', 'has_entity')

    UNION ALL

    -- Walk upstream recursively
    SELECT e.source_node_id, e.edge_type, u.depth + 1
    FROM graph_edges e
    JOIN upstream u ON e.target_node_id = u.node_id
    WHERE u.depth < 10  -- prevent infinite loops (should not happen in a DAG)
)
SELECT n.node_id, n.node_type, n.name, n.properties, u.depth
FROM upstream u
JOIN graph_nodes n ON n.node_id = u.node_id
ORDER BY u.depth;

-- 2. Full downstream impact: "What breaks if this data source goes down?"
WITH RECURSIVE downstream AS (
    SELECT e.target_node_id AS node_id, e.edge_type, 1 AS depth
    FROM graph_edges e
    WHERE e.source_node_id = $1  -- data_source_node_id

    UNION ALL

    SELECT e.target_node_id, e.edge_type, d.depth + 1
    FROM graph_edges e
    JOIN downstream d ON e.source_node_id = d.node_id
    WHERE d.depth < 10
)
SELECT n.node_id, n.node_type, n.name, d.depth
FROM downstream d
JOIN graph_nodes n ON n.node_id = d.node_id
ORDER BY d.depth;

-- 3. Feature discovery: "What other features share data sources with feature view X?"
SELECT DISTINCT sibling.node_id, sibling.name, sibling.properties
FROM graph_edges e1
JOIN graph_edges e2 ON e1.source_node_id = e2.source_node_id
    AND e2.edge_type = 'reads_from'
    AND e2.target_node_id != e1.target_node_id
JOIN graph_nodes sibling ON sibling.node_id = e2.target_node_id
WHERE e1.target_node_id = $1  -- feature_view_node_id
  AND e1.edge_type = 'reads_from';

-- 4. Model provenance: "What is the complete data lineage for model predictions?"
WITH RECURSIVE provenance AS (
    SELECT e.source_node_id AS node_id, e.edge_type, 1 AS depth
    FROM graph_edges e
    WHERE e.target_node_id = $1  -- model_node_id

    UNION ALL

    SELECT e.source_node_id, e.edge_type, p.depth + 1
    FROM graph_edges e
    JOIN provenance p ON e.target_node_id = p.node_id
    WHERE p.depth < 20
)
SELECT n.node_type, n.name, n.properties, p.depth
FROM provenance p
JOIN graph_nodes n ON n.node_id = p.node_id
ORDER BY p.depth;

-- 5. Fuzzy feature search by name
SELECT node_id, name, node_type, properties,
       similarity(name, 'driver conversion') AS sim
FROM graph_nodes
WHERE node_type = 'feature'
  AND name % 'driver conversion'
ORDER BY sim DESC
LIMIT 20;

-- 6. GDPR: Find all nodes containing PII for a given entity value
WITH RECURSIVE pii_trail AS (
    SELECT node_id, node_type, name FROM graph_nodes
    WHERE node_type = 'entity'
      AND properties->>'join_key' = 'user_id'

    UNION ALL

    SELECT n.node_id, n.node_type, n.name
    FROM graph_edges e
    JOIN pii_trail p ON e.source_node_id = p.node_id
    JOIN graph_nodes n ON n.node_id = e.target_node_id
)
SELECT * FROM pii_trail;
```

---

## Table Count Summary

| Category | Tables | Notes |
|----------|--------|-------|
| Graph Layer | 2 | graph_nodes, graph_edges (core of the model) |
| Projects | 1 | projects (relational, not in graph) |
| Materialisation | 2 | materialisation_jobs, materialisation_state |
| Access Control | 4 | users, user_project_roles, node_grants, api_keys |
| Monitoring | 2 | feature_statistics, alerts |
| Audit | 1 | audit_log |
| **Total** | **12** | 2 graph tables + 10 relational tables |

---

## Key Design Decisions

1. **Two-table property graph** (`graph_nodes` + `graph_edges`) rather than a separate graph database — keeps everything in PostgreSQL, avoids operational complexity of a second database, and leverages PostgreSQL's recursive CTE support for graph traversal.

2. **All registry objects are graph nodes** — data sources, entities, feature views, individual features, feature services, and even downstream models are nodes. This means lineage is not an afterthought but the primary organising principle.

3. **Individual features are separate nodes** (not embedded in feature views) — this enables feature-level lineage, where you can trace a single feature's provenance from raw data source column through transformations to serving.

4. **Node-level access control** via `node_grants` — goes beyond project-scoped RBAC by allowing fine-grained permissions on individual feature views or data sources. Combined with graph traversal, you can implement "grant read access to this feature service and everything upstream of it."

5. **Operational data remains relational** — materialisation jobs, users, API keys, and statistics use conventional relational tables because they benefit from strict schemas, foreign keys, and direct queries without graph traversal overhead.

6. **Trigram index for fuzzy feature search** — the `pg_trgm` extension enables similarity-based feature discovery ("find features whose names are similar to 'driver conversion'"), supporting the AI-powered feature recommendation use case.

7. **Edge properties enable rich relationship metadata** — edges are not just connections but carry context: which version of a feature view was used to train a model, what timestamp column a feature view reads from a data source, etc.

8. **Models as first-class graph nodes** — unlike the other data model suggestions, this model tracks downstream ML models as nodes connected to feature services. This enables full "data source to prediction" lineage, which is critical for NIST AI RMF compliance and bias documentation.

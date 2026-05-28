# Data Model Suggestion 3: Hybrid Relational + JSONB

> Project: Feature Store · Created: 2026-05-20

## Philosophy

This model uses relational tables for the structural backbone — projects, entities, feature views, and their relationships — but stores variable, source-type-specific, and extensible metadata in JSONB columns. The core identity and relationships of each registry object are queryable via standard SQL joins, while configuration details, transformation logic, tags, and provider-specific settings live in flexible JSONB fields that can evolve without schema migrations.

This is the approach PostgreSQL was designed for: combine the referential integrity and query planning of a relational engine with the schema-flexibility of a document store. It mirrors how modern SaaS products (Stripe, GitHub, Notion) store core entity relationships relationally while using JSONB for variable attributes. In the feature store domain, this is especially valuable because data source configurations vary wildly by provider (BigQuery needs project/dataset/table; Kafka needs bootstrap servers and topic; S3 needs bucket/path/format), and feature metadata needs change as the platform evolves.

The table count is significantly lower than the fully normalised model because related but variable data is collapsed into JSONB columns rather than spread across child tables and junction tables.

**Best for:** Teams building a rapid MVP that needs to evolve quickly, supporting many data source types without table-per-type proliferation, and environments where multi-provider or multi-jurisdiction configurations create high field variability.

**Trade-offs:**
- (+) Far fewer tables than the normalised model (lower cognitive overhead)
- (+) Adding new metadata fields requires no schema migration — just add to the JSONB
- (+) JSONB GIN indexes enable fast containment and key-exists queries
- (+) Natural fit for provider-specific configuration that varies by source type
- (+) Easy to serialise/deserialise to Python dataclass hierarchies
- (-) JSONB fields are not individually constrained by the database (validation must happen in application code)
- (-) Referential integrity within JSONB is not enforced (e.g., entity references inside JSONB)
- (-) Complex JSONB queries can be slower than normalised joins for certain access patterns
- (-) Schema documentation requires external tooling (JSONB columns are opaque to standard ER diagrams)

---

## Standards Alignment

| Standard | How It's Used |
|----------|---------------|
| Feast Data Model | Entity, FeatureView, DataSource, FeatureService map to tables; features stored as JSONB arrays within feature views |
| Feast Type System (Value.proto) | Feature `value_type` uses Feast enum values within the JSONB `features` array |
| JSON Schema Draft 2020-12 | Can optionally validate JSONB columns against JSON Schemas stored in the `schema_registry` table |
| Apache Parquet / Delta Lake / Iceberg | Referenced in data source `config` JSONB for offline store formats |
| AsyncAPI 2.6 / 3.0 | Streaming source configurations in JSONB follow AsyncAPI channel patterns |
| OpenTelemetry / Prometheus | Monitoring config stored as JSONB, metric names follow OTel semantic conventions |
| OAuth 2.0 / OIDC | Auth configuration in JSONB supports multiple identity providers |

---

## Core Registry Tables

### Projects

```sql
CREATE TABLE projects (
    project_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(255) NOT NULL UNIQUE,
    description     TEXT,
    owner_email     VARCHAR(255),
    settings        JSONB NOT NULL DEFAULT '{}',
    -- settings example:
    -- {
    --   "default_offline_store": "bigquery",
    --   "default_online_store": "redis",
    --   "materialisation_schedule": "0 */6 * * *",
    --   "alerting_webhook": "https://hooks.slack.com/services/..."
    -- }
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### Entities

```sql
CREATE TABLE entities (
    entity_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    join_key        VARCHAR(255) NOT NULL,
    value_type      VARCHAR(50) NOT NULL,   -- INT64, STRING, etc. (Feast type enum)
    metadata        JSONB NOT NULL DEFAULT '{}',
    -- metadata example:
    -- {
    --   "description": "Unique driver identifier",
    --   "owner": "ml-platform@example.com",
    --   "tags": ["production", "driver"],
    --   "pii": false,
    --   "data_classification": "internal"
    -- }
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (project_id, name)
);

CREATE INDEX idx_entities_project ON entities (project_id);
CREATE INDEX idx_entities_metadata ON entities USING GIN (metadata);
```

### Data Sources

```sql
CREATE TABLE data_sources (
    data_source_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    source_type     VARCHAR(50) NOT NULL,   -- FILE, BIGQUERY, SNOWFLAKE, REDSHIFT, KAFKA, KINESIS, PUSH, POSTGRESQL
    config          JSONB NOT NULL,
    -- config varies by source_type:
    --
    -- FILE:
    -- {
    --   "path": "s3://my-bucket/features/driver_stats.parquet",
    --   "format": "PARQUET",
    --   "s3_endpoint_override": null,
    --   "timestamp_column": "event_timestamp",
    --   "created_timestamp_column": "created_timestamp"
    -- }
    --
    -- BIGQUERY:
    -- {
    --   "table": "my-gcp-project.ml_features.driver_stats",
    --   "query": null,
    --   "timestamp_column": "event_timestamp",
    --   "created_timestamp_column": "created_timestamp"
    -- }
    --
    -- KAFKA:
    -- {
    --   "bootstrap_servers": "kafka-broker:9092",
    --   "topic": "driver-events",
    --   "message_format": "AVRO",
    --   "schema_registry_url": "http://schema-registry:8081",
    --   "timestamp_column": "event_timestamp",
    --   "watermark_delay_threshold_seconds": 300
    -- }
    --
    -- SNOWFLAKE:
    -- {
    --   "database": "ML_FEATURES",
    --   "schema": "PUBLIC",
    --   "table": "DRIVER_STATS",
    --   "warehouse": "COMPUTE_WH",
    --   "timestamp_column": "event_timestamp"
    -- }
    metadata        JSONB NOT NULL DEFAULT '{}',
    -- metadata example:
    -- {
    --   "description": "Driver statistics from the ride platform",
    --   "owner": "data-eng@example.com",
    --   "tags": ["production"],
    --   "freshness_sla_seconds": 3600
    -- }
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (project_id, name)
);

CREATE INDEX idx_data_sources_project ON data_sources (project_id);
CREATE INDEX idx_data_sources_type ON data_sources (source_type);
CREATE INDEX idx_data_sources_config ON data_sources USING GIN (config);
```

### Feature Views

```sql
CREATE TABLE feature_views (
    feature_view_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    view_type       VARCHAR(30) NOT NULL DEFAULT 'batch',  -- batch, stream, on_demand
    data_source_id  UUID REFERENCES data_sources(data_source_id),
    entity_ids      UUID[] NOT NULL DEFAULT '{}',          -- array of entity UUIDs
    -- Features embedded as JSONB array (the key hybrid design choice)
    features        JSONB NOT NULL DEFAULT '[]',
    -- features example:
    -- [
    --   {"name": "conv_rate", "value_type": "FLOAT64", "description": "Conversion rate", "tags": ["kpi"]},
    --   {"name": "acc_rate", "value_type": "FLOAT64", "description": "Acceptance rate"},
    --   {"name": "avg_daily_trips", "value_type": "INT32", "description": "Average trips per day"},
    --   {"name": "surge_multiplier", "value_type": "FLOAT64", "description": "Surge pricing multiplier"}
    -- ]
    -- Materialisation config
    ttl_seconds     BIGINT,
    online_enabled  BOOLEAN NOT NULL DEFAULT true,
    version         INTEGER NOT NULL DEFAULT 1,
    -- Stream-specific config (only populated when view_type = 'stream')
    stream_config   JSONB,
    -- stream_config example:
    -- {
    --   "aggregation_slide_interval_seconds": 3600,
    --   "aggregation_window_seconds": 86400,
    --   "transformation_code": "def transform(df): return df.groupby('driver_id').agg(...)"
    -- }
    -- On-demand-specific config (only populated when view_type = 'on_demand')
    on_demand_config JSONB,
    -- on_demand_config example:
    -- {
    --   "transformation_code": "def transform(inputs): ...",
    --   "transformation_mode": "python",
    --   "sources": [
    --     {"type": "feature_view", "feature_view_id": "...", "alias": "driver_stats"},
    --     {"type": "request_data", "schema": {"lat": "FLOAT64", "lon": "FLOAT64"}, "alias": "request"}
    --   ]
    -- }
    -- General metadata
    metadata        JSONB NOT NULL DEFAULT '{}',
    -- metadata example:
    -- {
    --   "description": "Hourly aggregated driver statistics",
    --   "owner": "ml-platform@example.com",
    --   "tags": ["production", "driver"],
    --   "documentation_url": "https://wiki.example.com/driver-features"
    -- }
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (project_id, name, version)
);

CREATE INDEX idx_fv_project ON feature_views (project_id);
CREATE INDEX idx_fv_type ON feature_views (view_type);
CREATE INDEX idx_fv_data_source ON feature_views (data_source_id);
CREATE INDEX idx_fv_entities ON feature_views USING GIN (entity_ids);
CREATE INDEX idx_fv_features ON feature_views USING GIN (features);
CREATE INDEX idx_fv_metadata ON feature_views USING GIN (metadata);
```

### Feature Services

```sql
CREATE TABLE feature_services (
    feature_service_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    feature_view_ids UUID[] NOT NULL DEFAULT '{}',  -- array of feature view IDs included
    metadata        JSONB NOT NULL DEFAULT '{}',
    -- metadata example:
    -- {
    --   "description": "All features needed for driver ETA prediction",
    --   "owner": "ml-platform@example.com",
    --   "tags": ["production", "eta-model"],
    --   "serving_endpoint": "/v1/features/driver-eta"
    -- }
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (project_id, name)
);

CREATE INDEX idx_fs_project ON feature_services (project_id);
CREATE INDEX idx_fs_views ON feature_services USING GIN (feature_view_ids);
```

---

## Materialisation & Operations

```sql
CREATE TABLE materialisation_jobs (
    job_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    feature_view_id UUID NOT NULL REFERENCES feature_views(feature_view_id),
    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    result          JSONB,
    -- result example:
    -- {
    --   "records_written": 1247832,
    --   "duration_seconds": 142,
    --   "bytes_processed": 52428800,
    --   "error_message": null,
    --   "warnings": ["3 null values in conv_rate column"]
    -- }
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_mat_jobs_fv ON materialisation_jobs (feature_view_id);
CREATE INDEX idx_mat_jobs_status ON materialisation_jobs (status);
CREATE INDEX idx_mat_jobs_time ON materialisation_jobs (created_at);
```

---

## Access Control

```sql
CREATE TABLE users (
    user_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email           VARCHAR(255) NOT NULL UNIQUE,
    display_name    VARCHAR(255),
    auth_config     JSONB NOT NULL DEFAULT '{}',
    -- auth_config example:
    -- {
    --   "provider": "oidc",
    --   "subject": "auth0|abc123",
    --   "mfa_enabled": true
    -- }
    is_active       BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Simplified RBAC: roles and permissions in a single table
-- with JSONB permissions array rather than 3 normalised tables
CREATE TABLE user_project_roles (
    user_id         UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    project_id      UUID NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    role            VARCHAR(100) NOT NULL,  -- admin, editor, viewer, feature_owner
    permissions     JSONB NOT NULL DEFAULT '[]',
    -- permissions example (overrides for this specific assignment):
    -- [
    --   {"resource_type": "feature_view", "actions": ["create", "read", "update", "delete", "materialize"]},
    --   {"resource_type": "data_source", "actions": ["read"]},
    --   {"resource_type": "feature_service", "actions": ["create", "read", "update", "serve"]}
    -- ]
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, project_id, role)
);

CREATE TABLE api_keys (
    api_key_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    project_id      UUID REFERENCES projects(project_id),
    key_hash        VARCHAR(255) NOT NULL UNIQUE,
    name            VARCHAR(255) NOT NULL,
    scopes          JSONB NOT NULL DEFAULT '["read"]',
    -- scopes example: ["read", "write", "materialize", "serve"]
    expires_at      TIMESTAMPTZ,
    is_active       BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

## Monitoring & Data Quality

```sql
CREATE TABLE feature_statistics (
    stat_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    feature_view_id UUID NOT NULL REFERENCES feature_views(feature_view_id),
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    stats           JSONB NOT NULL,
    -- stats example:
    -- {
    --   "features": {
    --     "conv_rate": {
    --       "row_count": 125000,
    --       "null_count": 12,
    --       "null_fraction": 0.000096,
    --       "mean": 0.42,
    --       "stddev": 0.15,
    --       "min": 0.0,
    --       "max": 1.0,
    --       "p50": 0.41,
    --       "p95": 0.72,
    --       "p99": 0.89,
    --       "unique_count": 48231
    --     },
    --     "avg_daily_trips": {
    --       "row_count": 125000,
    --       "null_count": 0,
    --       "mean": 14.2,
    --       "stddev": 8.7,
    --       "min": 0,
    --       "max": 89
    --     }
    --   },
    --   "drift": {
    --     "conv_rate": {"psi": 0.012, "ks_statistic": 0.034, "status": "ok"},
    --     "avg_daily_trips": {"psi": 0.087, "ks_statistic": 0.091, "status": "warning"}
    --   }
    -- }
    baseline_stat_id UUID REFERENCES feature_statistics(stat_id)  -- reference distribution
);

CREATE INDEX idx_stats_fv ON feature_statistics (feature_view_id, computed_at DESC);

CREATE TABLE alerts (
    alert_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(project_id),
    source_type     VARCHAR(50) NOT NULL,   -- feature_view, materialisation, data_source
    source_id       UUID NOT NULL,
    alert_data      JSONB NOT NULL,
    -- alert_data example:
    -- {
    --   "type": "distribution_drift",
    --   "severity": "warning",
    --   "feature_name": "avg_daily_trips",
    --   "threshold": 0.05,
    --   "observed_value": 0.087,
    --   "message": "PSI drift detected for avg_daily_trips: 0.087 > threshold 0.05"
    -- }
    acknowledged    BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_alerts_project ON alerts (project_id);
CREATE INDEX idx_alerts_source ON alerts (source_type, source_id);
CREATE INDEX idx_alerts_unack ON alerts (created_at) WHERE NOT acknowledged;
```

---

## Lineage Tracking

```sql
CREATE TABLE lineage_edges (
    edge_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_type     VARCHAR(50) NOT NULL,
    source_id       UUID NOT NULL,
    target_type     VARCHAR(50) NOT NULL,
    target_id       UUID NOT NULL,
    relationship    VARCHAR(50) NOT NULL,   -- feeds_into, derived_from, consumed_by
    edge_metadata   JSONB NOT NULL DEFAULT '{}',
    -- edge_metadata example:
    -- {
    --   "transformation": "SQL aggregation",
    --   "freshness_dependency": true
    -- }
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_type, source_id, target_type, target_id, relationship)
);

CREATE INDEX idx_lineage_source ON lineage_edges (source_type, source_id);
CREATE INDEX idx_lineage_target ON lineage_edges (target_type, target_id);
```

---

## Saved Datasets & Audit

```sql
CREATE TABLE saved_datasets (
    dataset_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    feature_service_id UUID REFERENCES feature_services(feature_service_id),
    config          JSONB NOT NULL,
    -- config example:
    -- {
    --   "storage_path": "s3://ml-datasets/driver-training-2026-05-20.parquet",
    --   "format": "PARQUET",
    --   "row_count": 5000000,
    --   "feature_view_versions": {"driver_hourly_stats": 3, "driver_profile": 1},
    --   "entity_filter": {"city_id": [1, 2, 5]},
    --   "time_range": {"start": "2026-01-01", "end": "2026-05-01"}
    -- }
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (project_id, name)
);

CREATE TABLE audit_log (
    log_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID,
    actor_type      VARCHAR(20) NOT NULL,   -- user, api_key, system
    actor_id        UUID,
    action          VARCHAR(100) NOT NULL,  -- e.g. feature_view.created, materialisation.started
    resource_type   VARCHAR(50) NOT NULL,
    resource_id     UUID,
    details         JSONB,
    -- details example:
    -- {
    --   "feature_view_name": "driver_hourly_stats",
    --   "changes": {"online_enabled": {"old": false, "new": true}}
    -- }
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_audit_project ON audit_log (project_id, occurred_at DESC);
CREATE INDEX idx_audit_resource ON audit_log (resource_type, resource_id);
CREATE INDEX idx_audit_actor ON audit_log (actor_id);
```

---

## Optional: JSONB Schema Validation Registry

```sql
-- Store JSON Schemas to validate JSONB columns at the application layer
CREATE TABLE schema_registry (
    schema_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    schema_name     VARCHAR(255) NOT NULL UNIQUE,  -- e.g. 'data_source.config.kafka', 'feature_view.features'
    json_schema     JSONB NOT NULL,                -- JSON Schema Draft 2020-12
    version         INTEGER NOT NULL DEFAULT 1,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

## JSONB Query Examples

```sql
-- Find all feature views that contain a feature named 'conv_rate'
SELECT feature_view_id, name
FROM feature_views
WHERE features @> '[{"name": "conv_rate"}]';

-- Find all data sources of type KAFKA with a specific topic
SELECT name, config
FROM data_sources
WHERE source_type = 'KAFKA'
  AND config->>'topic' = 'driver-events';

-- Find all feature views tagged 'production'
SELECT name, metadata
FROM feature_views
WHERE metadata->'tags' ? 'production';

-- Count features per value_type across the entire registry
SELECT f->>'value_type' AS value_type, COUNT(*)
FROM feature_views, jsonb_array_elements(features) AS f
GROUP BY f->>'value_type';

-- Find all users with admin role in any project
SELECT u.email, upr.project_id
FROM users u
JOIN user_project_roles upr ON u.user_id = upr.user_id
WHERE upr.role = 'admin';
```

---

## Table Count Summary

| Category | Tables | Notes |
|----------|--------|-------|
| Core Registry | 5 | projects, entities, data_sources, feature_views, feature_services |
| Materialisation | 1 | materialisation_jobs |
| Access Control | 3 | users, user_project_roles, api_keys |
| Monitoring | 2 | feature_statistics, alerts |
| Lineage | 1 | lineage_edges |
| Datasets & Audit | 2 | saved_datasets, audit_log |
| Schema Validation | 1 | schema_registry (optional) |
| **Total** | **15** | (14 without optional schema_registry) |

---

## Key Design Decisions

1. **Features embedded as JSONB array in feature_views** rather than a separate `features` table — this is the defining hybrid choice. It reduces the table count and makes feature view retrieval a single-row read, but means cross-view feature queries require `jsonb_array_elements()`.

2. **Single feature_views table with `view_type` discriminator** instead of separate tables for batch/stream/on-demand — type-specific configuration lives in JSONB columns (`stream_config`, `on_demand_config`) that are only populated for the relevant type.

3. **Entity references stored as UUID arrays** (`entity_ids UUID[]`) rather than junction tables — leverages PostgreSQL array types and GIN indexing. Trade-off: no foreign key enforcement on array elements.

4. **Data source configuration in JSONB** rather than nullable typed columns — each source type has a completely different configuration shape. JSONB handles this cleanly without dozens of nullable columns.

5. **Permissions as JSONB arrays** rather than normalised permission tables — reduces 3 tables (roles, permissions, role_permissions) to inline JSONB. Acceptable for the moderate cardinality typical of feature store RBAC.

6. **Statistics stored as nested JSONB** with per-feature metrics — enables storing arbitrary percentiles, histograms, and drift metrics without schema changes. New statistical measures can be added without migration.

7. **Audit log as a separate append-only table** rather than event sourcing — provides accountability without the full complexity of event-sourced projections. Simpler to implement but does not support temporal state reconstruction.

8. **Optional JSON Schema validation registry** — since JSONB columns lack database-level schema enforcement, storing JSON Schemas allows application-layer validation without hardcoding schema rules in code.

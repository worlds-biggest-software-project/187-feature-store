# Data Model Suggestion 1: Entity-Centric Normalized Relational

> Project: Feature Store · Created: 2026-05-20

## Philosophy

This model follows classical relational database design: every concept in the feature store domain gets its own table with well-defined columns, foreign keys enforce referential integrity, and junction tables model many-to-many relationships. The registry, feature definitions, data sources, materialisation state, and access control are all represented as first-class relational entities.

The design mirrors how Feast's SQL registry works conceptually — each object type (entity, feature view, data source, feature service) has its own storage — but goes further by fully normalising metadata that Feast stores as serialised protobuf blobs. This means every field is queryable via standard SQL without deserialisation.

This approach is battle-tested in systems where data integrity, complex cross-entity queries, and regulatory audit requirements are paramount. It trades flexibility (adding new metadata fields requires schema migration) for queryability and constraint enforcement.

**Best for:** Teams that value data integrity, need complex SQL queries across the registry, and operate in environments where schema changes follow a controlled migration process.

**Trade-offs:**
- (+) Full referential integrity — broken references are impossible
- (+) Every field is independently queryable and indexable
- (+) Well understood by any engineer familiar with relational databases
- (+) Easy to add standard reporting and BI tools on top
- (-) Schema migrations required for every new metadata field
- (-) Higher table count increases join complexity for some queries
- (-) Less flexible for rapid prototyping or multi-jurisdiction variations
- (-) Serialising/deserialising to Python objects requires more mapping code

---

## Standards Alignment

| Standard | How It's Used |
|----------|---------------|
| Feast Data Model | Entity, FeatureView, DataSource, FeatureService concepts map directly to tables |
| Feast Type System (Value.proto) | `value_type` column uses Feast's type enum: INT32, INT64, FLOAT32, FLOAT64, STRING, BYTES, BOOL, UNIX_TIMESTAMP, plus array variants |
| Apache Parquet / Delta Lake / Iceberg | Referenced in `data_sources.format` for offline store configurations |
| OpenTelemetry / Prometheus | Monitoring tables store metrics compatible with OTel semantic conventions |
| OAuth 2.0 / OIDC | `users` and `api_keys` tables support token-based auth |
| GDPR | `entity_erasure_requests` table tracks right-to-erasure compliance |

---

## Core Registry Tables

### Projects & Tenancy

```sql
CREATE TABLE projects (
    project_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(255) NOT NULL UNIQUE,
    description     TEXT,
    owner_email     VARCHAR(255),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_projects_name ON projects (name);
```

### Entities

```sql
CREATE TABLE entities (
    entity_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    description     TEXT,
    join_key        VARCHAR(255) NOT NULL,
    value_type      VARCHAR(50) NOT NULL,  -- e.g. INT64, STRING (Feast type system)
    owner           VARCHAR(255),
    tags            TEXT[],                -- PostgreSQL array for simple tag lists
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (project_id, name)
);

CREATE INDEX idx_entities_project ON entities (project_id);
CREATE INDEX idx_entities_join_key ON entities (join_key);
```

### Data Sources

```sql
CREATE TABLE data_sources (
    data_source_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    source_type     VARCHAR(50) NOT NULL,  -- FILE, BIGQUERY, SNOWFLAKE, REDSHIFT, KAFKA, KINESIS, PUSH
    format          VARCHAR(50),           -- PARQUET, DELTA, ICEBERG, AVRO, CSV
    -- Connection details (only relevant columns populated per source_type)
    file_path       TEXT,
    table_ref       TEXT,                  -- fully qualified table: project.dataset.table
    query           TEXT,                  -- custom SQL query for source
    topic           VARCHAR(255),          -- Kafka/Kinesis topic
    bootstrap_servers TEXT,                -- Kafka bootstrap servers
    stream_arn      TEXT,                  -- Kinesis stream ARN
    -- Timestamp handling
    timestamp_column    VARCHAR(255),
    created_timestamp_column VARCHAR(255),
    event_timestamp_column   VARCHAR(255),
    -- Metadata
    description     TEXT,
    owner           VARCHAR(255),
    tags            TEXT[],
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (project_id, name)
);

CREATE INDEX idx_data_sources_project ON data_sources (project_id);
CREATE INDEX idx_data_sources_type ON data_sources (source_type);
```

### Feature Views

```sql
CREATE TABLE feature_views (
    feature_view_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    description     TEXT,
    data_source_id  UUID REFERENCES data_sources(data_source_id),
    -- Materialisation settings
    ttl_seconds     BIGINT,                -- time-to-live for online store records
    online_enabled  BOOLEAN NOT NULL DEFAULT true,
    -- Versioning
    version         INTEGER NOT NULL DEFAULT 1,
    -- Metadata
    owner           VARCHAR(255),
    tags            TEXT[],
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (project_id, name, version)
);

CREATE INDEX idx_feature_views_project ON feature_views (project_id);
CREATE INDEX idx_feature_views_data_source ON feature_views (data_source_id);
```

### Feature View Entities (Junction)

```sql
CREATE TABLE feature_view_entities (
    feature_view_id UUID NOT NULL REFERENCES feature_views(feature_view_id) ON DELETE CASCADE,
    entity_id       UUID NOT NULL REFERENCES entities(entity_id) ON DELETE CASCADE,
    PRIMARY KEY (feature_view_id, entity_id)
);
```

### Features (Columns within Feature Views)

```sql
CREATE TABLE features (
    feature_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    feature_view_id UUID NOT NULL REFERENCES feature_views(feature_view_id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    value_type      VARCHAR(50) NOT NULL,  -- INT32, INT64, FLOAT32, FLOAT64, STRING, BYTES, BOOL, UNIX_TIMESTAMP, ARRAY_*
    description     TEXT,
    tags            TEXT[],
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (feature_view_id, name)
);

CREATE INDEX idx_features_view ON features (feature_view_id);
```

---

## Feature Services

```sql
CREATE TABLE feature_services (
    feature_service_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    description     TEXT,
    owner           VARCHAR(255),
    tags            TEXT[],
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (project_id, name)
);

-- Junction: which feature views are included in each feature service
CREATE TABLE feature_service_views (
    feature_service_id UUID NOT NULL REFERENCES feature_services(feature_service_id) ON DELETE CASCADE,
    feature_view_id    UUID NOT NULL REFERENCES feature_views(feature_view_id) ON DELETE CASCADE,
    PRIMARY KEY (feature_service_id, feature_view_id)
);
```

---

## On-Demand Feature Views

```sql
CREATE TABLE on_demand_feature_views (
    odfv_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    description     TEXT,
    transformation_code TEXT NOT NULL,      -- Python or SQL transformation source
    transformation_mode VARCHAR(20) NOT NULL DEFAULT 'python', -- python, sql
    owner           VARCHAR(255),
    tags            TEXT[],
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (project_id, name)
);

-- Sources for on-demand feature views (can reference feature views or request data)
CREATE TABLE odfv_sources (
    odfv_id         UUID NOT NULL REFERENCES on_demand_feature_views(odfv_id) ON DELETE CASCADE,
    source_type     VARCHAR(20) NOT NULL,  -- feature_view, request_data
    feature_view_id UUID REFERENCES feature_views(feature_view_id),
    request_schema  TEXT,                  -- JSON schema for request data sources
    alias           VARCHAR(255),
    PRIMARY KEY (odfv_id, alias)
);

-- Output features of on-demand feature views
CREATE TABLE odfv_features (
    feature_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    odfv_id         UUID NOT NULL REFERENCES on_demand_feature_views(odfv_id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    value_type      VARCHAR(50) NOT NULL,
    description     TEXT,
    UNIQUE (odfv_id, name)
);
```

---

## Stream Feature Views

```sql
CREATE TABLE stream_feature_views (
    sfv_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    description     TEXT,
    data_source_id  UUID REFERENCES data_sources(data_source_id),
    -- Aggregation settings
    aggregation_slide_interval_seconds BIGINT,
    aggregation_window_seconds         BIGINT,
    -- Materialisation
    ttl_seconds     BIGINT,
    online_enabled  BOOLEAN NOT NULL DEFAULT true,
    -- Transformation
    transformation_code TEXT,
    -- Metadata
    owner           VARCHAR(255),
    tags            TEXT[],
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (project_id, name)
);

CREATE TABLE stream_feature_view_entities (
    sfv_id          UUID NOT NULL REFERENCES stream_feature_views(sfv_id) ON DELETE CASCADE,
    entity_id       UUID NOT NULL REFERENCES entities(entity_id) ON DELETE CASCADE,
    PRIMARY KEY (sfv_id, entity_id)
);

CREATE TABLE stream_features (
    feature_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sfv_id          UUID NOT NULL REFERENCES stream_feature_views(sfv_id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    value_type      VARCHAR(50) NOT NULL,
    description     TEXT,
    UNIQUE (sfv_id, name)
);
```

---

## Materialisation & Online Store Metadata

```sql
CREATE TABLE materialisation_jobs (
    job_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    feature_view_id UUID NOT NULL REFERENCES feature_views(feature_view_id),
    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING',  -- PENDING, RUNNING, SUCCEEDED, FAILED
    start_time      TIMESTAMPTZ,
    end_time        TIMESTAMPTZ,
    start_date      TIMESTAMPTZ NOT NULL,   -- materialisation window start
    end_date        TIMESTAMPTZ NOT NULL,   -- materialisation window end
    records_written BIGINT,
    error_message   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_mat_jobs_fv ON materialisation_jobs (feature_view_id);
CREATE INDEX idx_mat_jobs_status ON materialisation_jobs (status);

-- Tracks the last materialised timestamp per feature view
CREATE TABLE materialisation_state (
    feature_view_id UUID PRIMARY KEY REFERENCES feature_views(feature_view_id),
    last_materialised_at TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

## Access Control (RBAC)

```sql
CREATE TABLE users (
    user_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email           VARCHAR(255) NOT NULL UNIQUE,
    display_name    VARCHAR(255),
    auth_provider   VARCHAR(50),           -- local, oidc, ldap
    auth_subject    VARCHAR(255),          -- external identity subject
    is_active       BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE roles (
    role_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(100) NOT NULL UNIQUE,  -- admin, editor, viewer, feature_owner
    description     TEXT
);

CREATE TABLE permissions (
    permission_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    resource_type   VARCHAR(50) NOT NULL,  -- project, feature_view, feature_service, data_source, entity
    action          VARCHAR(50) NOT NULL,  -- create, read, update, delete, materialize, serve
    UNIQUE (resource_type, action)
);

CREATE TABLE role_permissions (
    role_id         UUID NOT NULL REFERENCES roles(role_id) ON DELETE CASCADE,
    permission_id   UUID NOT NULL REFERENCES permissions(permission_id) ON DELETE CASCADE,
    PRIMARY KEY (role_id, permission_id)
);

-- Project-scoped role assignments
CREATE TABLE user_project_roles (
    user_id         UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    project_id      UUID NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    role_id         UUID NOT NULL REFERENCES roles(role_id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, project_id, role_id)
);

-- API keys for service-to-service auth
CREATE TABLE api_keys (
    api_key_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    project_id      UUID REFERENCES projects(project_id),  -- NULL = global key
    key_hash        VARCHAR(255) NOT NULL UNIQUE,           -- bcrypt/argon2 hash of the key
    name            VARCHAR(255) NOT NULL,
    expires_at      TIMESTAMPTZ,
    is_active       BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_api_keys_user ON api_keys (user_id);
```

---

## Monitoring & Data Quality

```sql
CREATE TABLE feature_statistics (
    stat_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    feature_view_id UUID NOT NULL REFERENCES feature_views(feature_view_id),
    feature_name    VARCHAR(255) NOT NULL,
    computed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    row_count       BIGINT,
    null_count      BIGINT,
    null_fraction   DOUBLE PRECISION,
    mean            DOUBLE PRECISION,
    stddev          DOUBLE PRECISION,
    min_value       TEXT,
    max_value       TEXT,
    unique_count    BIGINT,
    -- Distribution drift
    reference_distribution_id UUID,        -- FK to a baseline stat for drift comparison
    drift_score     DOUBLE PRECISION       -- e.g. PSI, KL divergence
);

CREATE INDEX idx_feature_stats_fv ON feature_statistics (feature_view_id);
CREATE INDEX idx_feature_stats_time ON feature_statistics (computed_at);

CREATE TABLE drift_alerts (
    alert_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    feature_view_id UUID NOT NULL REFERENCES feature_views(feature_view_id),
    feature_name    VARCHAR(255) NOT NULL,
    alert_type      VARCHAR(50) NOT NULL,  -- distribution_drift, null_rate_spike, schema_change
    severity        VARCHAR(20) NOT NULL,  -- info, warning, critical
    threshold       DOUBLE PRECISION,
    observed_value  DOUBLE PRECISION,
    message         TEXT,
    acknowledged    BOOLEAN NOT NULL DEFAULT false,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_drift_alerts_fv ON drift_alerts (feature_view_id);
CREATE INDEX idx_drift_alerts_severity ON drift_alerts (severity) WHERE NOT acknowledged;
```

---

## Lineage Tracking

```sql
CREATE TABLE lineage_edges (
    edge_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_type     VARCHAR(50) NOT NULL,  -- data_source, feature_view, on_demand_feature_view, stream_feature_view
    source_id       UUID NOT NULL,
    target_type     VARCHAR(50) NOT NULL,
    target_id       UUID NOT NULL,
    relationship    VARCHAR(50) NOT NULL,  -- feeds_into, derived_from, consumed_by
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_type, source_id, target_type, target_id, relationship)
);

CREATE INDEX idx_lineage_source ON lineage_edges (source_type, source_id);
CREATE INDEX idx_lineage_target ON lineage_edges (target_type, target_id);
```

---

## GDPR Compliance

```sql
CREATE TABLE entity_erasure_requests (
    request_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_name     VARCHAR(255) NOT NULL,
    entity_value    TEXT NOT NULL,          -- the join key value to erase
    project_id      UUID NOT NULL REFERENCES projects(project_id),
    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING',  -- PENDING, IN_PROGRESS, COMPLETED, FAILED
    requested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ,
    requested_by    UUID REFERENCES users(user_id)
);
```

---

## Saved Datasets

```sql
CREATE TABLE saved_datasets (
    dataset_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    name            VARCHAR(255) NOT NULL,
    feature_service_id UUID REFERENCES feature_services(feature_service_id),
    storage_path    TEXT NOT NULL,          -- S3/GCS/ADLS path to the materialised dataset
    format          VARCHAR(50) NOT NULL DEFAULT 'PARQUET',
    row_count       BIGINT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (project_id, name)
);
```

---

## Table Count Summary

| Category | Tables | Notes |
|----------|--------|-------|
| Core Registry | 5 | projects, entities, data_sources, feature_views, features |
| Feature View Relations | 2 | feature_view_entities, feature_service_views |
| Feature Services | 1 | feature_services |
| On-Demand Features | 3 | on_demand_feature_views, odfv_sources, odfv_features |
| Stream Features | 3 | stream_feature_views, stream_feature_view_entities, stream_features |
| Materialisation | 2 | materialisation_jobs, materialisation_state |
| Access Control | 6 | users, roles, permissions, role_permissions, user_project_roles, api_keys |
| Monitoring | 2 | feature_statistics, drift_alerts |
| Lineage | 1 | lineage_edges |
| Compliance | 1 | entity_erasure_requests |
| Datasets | 1 | saved_datasets |
| **Total** | **27** | |

---

## Key Design Decisions

1. **Separate tables for each feature view type** (batch, on-demand, stream) rather than a single polymorphic table — avoids nullable column sprawl and makes each type's constraints explicit.

2. **Features as child rows of feature views** rather than embedded arrays — enables direct SQL queries like "find all features of type FLOAT64 across the registry" without deserialisation.

3. **Junction tables for many-to-many relationships** (feature_view_entities, feature_service_views) — standard relational pattern that prevents data duplication.

4. **Project-scoped RBAC** with a role-permission-assignment model — users are assigned roles per project, and roles map to fine-grained permissions on resource types and actions.

5. **Lineage as an edge table** with generic source/target type+ID columns — models the DAG without requiring a separate table per edge type, while still allowing recursive CTE traversal.

6. **Tags stored as PostgreSQL TEXT arrays** rather than a separate tags table — simpler for the common case of flat tag lists; supports GIN indexing for containment queries (`WHERE tags @> ARRAY['production']`).

7. **Materialisation state tracked separately from jobs** — the `materialisation_state` table provides O(1) lookup of the last materialised timestamp per feature view, while `materialisation_jobs` retains the full history.

8. **Feature statistics stored per computation** rather than updated in place — enables time-series analysis of feature quality and drift detection by comparing current stats against historical baselines.

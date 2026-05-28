# Data Model Suggestion 2: Event-Sourced / Audit-First

> Project: Feature Store · Created: 2026-05-20

## Philosophy

Every change to the feature store registry — creating a feature view, modifying a data source, updating an entity's schema, running a materialisation job — is recorded as an immutable event in an append-only event store. The current state of the registry is a materialised view derived by replaying events. This is the CQRS (Command Query Responsibility Segregation) pattern: writes go to the event store, reads come from precomputed projections.

This approach is used in financial trading systems, healthcare record systems, and any domain where "what was true at time T?" is a critical question. For a feature store, this is directly relevant: point-in-time correct feature retrieval requires knowing the exact schema and configuration of a feature view at the time training data was generated. Event sourcing makes temporal queries natural rather than bolted on.

The Feast data model concepts (Entity, FeatureView, DataSource, FeatureService) are preserved, but they exist as materialised read models rebuilt from the event stream rather than as mutable relational rows. The event store is the single source of truth.

**Best for:** Teams that require full audit trails of all registry changes, need temporal queries ("what was the schema of feature view X when model Y was trained?"), or plan to build AI-powered analytics on registry change patterns.

**Trade-offs:**
- (+) Complete, tamper-evident audit trail of every registry change
- (+) Temporal queries are trivial: replay events up to any point in time
- (+) Natural fit for feature store domain where point-in-time correctness is already a core concept
- (+) Event stream can power real-time notifications, webhooks, and AI analytics
- (+) Schema evolution is safe: old events retain their original structure
- (-) Higher write amplification: every change writes an event + updates projections
- (-) More complex infrastructure: requires event store + projection rebuilder
- (-) Queries against current state require well-maintained materialised views
- (-) Debugging requires understanding the event replay model
- (-) Storage grows monotonically (events are never deleted, only compacted)

---

## Standards Alignment

| Standard | How It's Used |
|----------|---------------|
| Feast Data Model | Entity, FeatureView, DataSource, FeatureService remain the core domain objects, now modeled as event-sourced aggregates |
| Feast Type System (Value.proto) | Feature value types follow the Feast enum in event payloads |
| CQRS / Event Sourcing (Azure Architecture Patterns) | Architectural pattern for separating writes (event store) from reads (projections) |
| OpenTelemetry | Events carry trace context for distributed tracing across the feature pipeline |
| GDPR | Right-to-erasure implemented via crypto-shredding: entity-keyed encryption keys are destroyed, rendering PII-containing events unreadable |
| NIST AI RMF | Full provenance trail satisfies data lineage and bias documentation requirements |

---

## Event Store

### Core Event Table

```sql
-- The single source of truth: append-only event log
CREATE TABLE registry_events (
    event_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Aggregate identification
    aggregate_type  VARCHAR(50) NOT NULL,   -- project, entity, data_source, feature_view, feature_service, stream_feature_view, on_demand_feature_view
    aggregate_id    UUID NOT NULL,          -- the ID of the object being modified
    project_id      UUID NOT NULL,          -- partition key for multi-tenancy
    -- Event metadata
    event_type      VARCHAR(100) NOT NULL,  -- e.g. FeatureViewCreated, FeatureAdded, SchemaChanged, MaterialisationCompleted
    event_version   INTEGER NOT NULL DEFAULT 1,  -- schema version of this event type
    sequence_number BIGINT NOT NULL,        -- per-aggregate ordering (optimistic concurrency)
    -- Payload
    payload         JSONB NOT NULL,         -- event-specific data (see examples below)
    -- Causation & correlation
    correlation_id  UUID,                   -- groups related events (e.g. a single apply operation)
    causation_id    UUID,                   -- the event that caused this event
    -- Actor
    actor_type      VARCHAR(20) NOT NULL,   -- user, api_key, system
    actor_id        UUID,                   -- references users or api_keys
    -- Timing
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now(),  -- when the event happened in the real world
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),  -- when it was persisted (transaction time)

    -- Optimistic concurrency: no two events for the same aggregate can have the same sequence
    UNIQUE (aggregate_id, sequence_number)
);

-- Primary query pattern: replay events for a single aggregate
CREATE INDEX idx_events_aggregate ON registry_events (aggregate_id, sequence_number);

-- Query pattern: all events in a project, ordered by time
CREATE INDEX idx_events_project_time ON registry_events (project_id, occurred_at);

-- Query pattern: all events of a given type (for projection rebuilds)
CREATE INDEX idx_events_type ON registry_events (event_type, occurred_at);

-- Query pattern: correlation tracking
CREATE INDEX idx_events_correlation ON registry_events (correlation_id) WHERE correlation_id IS NOT NULL;

-- Partition by month for storage management (optional, production recommendation)
-- CREATE TABLE registry_events ... PARTITION BY RANGE (occurred_at);
```

### Event Payload Examples

```sql
-- Example: FeatureViewCreated event payload
-- {
--   "name": "driver_hourly_stats",
--   "description": "Hourly aggregated driver statistics",
--   "data_source_id": "a1b2c3d4-...",
--   "entities": ["driver_id"],
--   "features": [
--     {"name": "conv_rate", "value_type": "FLOAT64", "description": "Conversion rate"},
--     {"name": "acc_rate", "value_type": "FLOAT64", "description": "Acceptance rate"},
--     {"name": "avg_daily_trips", "value_type": "INT32", "description": "Average trips per day"}
--   ],
--   "ttl_seconds": 86400,
--   "online_enabled": true,
--   "owner": "ml-platform@example.com",
--   "tags": ["production", "driver"]
-- }

-- Example: FeatureAdded event payload
-- {
--   "feature_name": "surge_multiplier",
--   "value_type": "FLOAT64",
--   "description": "Current surge pricing multiplier"
-- }

-- Example: SchemaChanged event payload
-- {
--   "feature_name": "conv_rate",
--   "old_value_type": "FLOAT32",
--   "new_value_type": "FLOAT64",
--   "reason": "Precision upgrade for fraud model v3"
-- }

-- Example: MaterialisationCompleted event payload
-- {
--   "job_id": "e5f6g7h8-...",
--   "start_date": "2026-05-19T00:00:00Z",
--   "end_date": "2026-05-20T00:00:00Z",
--   "records_written": 1247832,
--   "duration_seconds": 142
-- }
```

---

## Materialised Read Models (Projections)

These tables are rebuilt from the event store. They can be dropped and reconstructed at any time.

### Current State Projections

```sql
-- Current state of all feature views (rebuilt from FeatureView* events)
CREATE TABLE proj_feature_views (
    feature_view_id UUID PRIMARY KEY,
    project_id      UUID NOT NULL,
    name            VARCHAR(255) NOT NULL,
    description     TEXT,
    data_source_id  UUID,
    ttl_seconds     BIGINT,
    online_enabled  BOOLEAN NOT NULL DEFAULT true,
    version         INTEGER NOT NULL DEFAULT 1,
    owner           VARCHAR(255),
    tags            TEXT[],
    is_deleted      BOOLEAN NOT NULL DEFAULT false,
    last_event_id   UUID NOT NULL,          -- the event that last updated this projection
    last_event_at   TIMESTAMPTZ NOT NULL,
    UNIQUE (project_id, name) -- only among non-deleted
);

CREATE INDEX idx_proj_fv_project ON proj_feature_views (project_id) WHERE NOT is_deleted;

-- Current features within feature views
CREATE TABLE proj_features (
    feature_view_id UUID NOT NULL,
    name            VARCHAR(255) NOT NULL,
    value_type      VARCHAR(50) NOT NULL,
    description     TEXT,
    is_deleted      BOOLEAN NOT NULL DEFAULT false,
    last_event_id   UUID NOT NULL,
    PRIMARY KEY (feature_view_id, name)
);

-- Current state of entities
CREATE TABLE proj_entities (
    entity_id       UUID PRIMARY KEY,
    project_id      UUID NOT NULL,
    name            VARCHAR(255) NOT NULL,
    join_key        VARCHAR(255) NOT NULL,
    value_type      VARCHAR(50) NOT NULL,
    description     TEXT,
    owner           VARCHAR(255),
    tags            TEXT[],
    is_deleted      BOOLEAN NOT NULL DEFAULT false,
    last_event_id   UUID NOT NULL,
    last_event_at   TIMESTAMPTZ NOT NULL,
    UNIQUE (project_id, name)
);

-- Current state of data sources
CREATE TABLE proj_data_sources (
    data_source_id  UUID PRIMARY KEY,
    project_id      UUID NOT NULL,
    name            VARCHAR(255) NOT NULL,
    source_type     VARCHAR(50) NOT NULL,
    config          JSONB NOT NULL,         -- source-specific configuration
    timestamp_column VARCHAR(255),
    description     TEXT,
    owner           VARCHAR(255),
    tags            TEXT[],
    is_deleted      BOOLEAN NOT NULL DEFAULT false,
    last_event_id   UUID NOT NULL,
    last_event_at   TIMESTAMPTZ NOT NULL,
    UNIQUE (project_id, name)
);

-- Current state of feature services
CREATE TABLE proj_feature_services (
    feature_service_id UUID PRIMARY KEY,
    project_id      UUID NOT NULL,
    name            VARCHAR(255) NOT NULL,
    description     TEXT,
    feature_view_ids UUID[] NOT NULL,       -- array of included feature view IDs
    owner           VARCHAR(255),
    tags            TEXT[],
    is_deleted      BOOLEAN NOT NULL DEFAULT false,
    last_event_id   UUID NOT NULL,
    last_event_at   TIMESTAMPTZ NOT NULL,
    UNIQUE (project_id, name)
);

-- Current state of projects
CREATE TABLE proj_projects (
    project_id      UUID PRIMARY KEY,
    name            VARCHAR(255) NOT NULL UNIQUE,
    description     TEXT,
    owner_email     VARCHAR(255),
    is_deleted      BOOLEAN NOT NULL DEFAULT false,
    last_event_id   UUID NOT NULL,
    last_event_at   TIMESTAMPTZ NOT NULL
);
```

### Materialisation Tracking Projection

```sql
CREATE TABLE proj_materialisation_state (
    feature_view_id UUID PRIMARY KEY,
    last_materialised_at TIMESTAMPTZ,
    total_jobs       BIGINT NOT NULL DEFAULT 0,
    successful_jobs  BIGINT NOT NULL DEFAULT 0,
    failed_jobs      BIGINT NOT NULL DEFAULT 0,
    total_records_written BIGINT NOT NULL DEFAULT 0,
    last_event_id   UUID NOT NULL
);
```

---

## Snapshots (Periodic State Capture)

```sql
-- Periodic snapshots to avoid replaying the full event history
CREATE TABLE aggregate_snapshots (
    snapshot_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type  VARCHAR(50) NOT NULL,
    aggregate_id    UUID NOT NULL,
    sequence_number BIGINT NOT NULL,        -- the event sequence this snapshot reflects
    state           JSONB NOT NULL,         -- serialised aggregate state
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (aggregate_id, sequence_number)
);

CREATE INDEX idx_snapshots_aggregate ON aggregate_snapshots (aggregate_id, sequence_number DESC);
```

---

## Access Control

```sql
-- Same RBAC structure, but changes are also event-sourced
CREATE TABLE proj_users (
    user_id         UUID PRIMARY KEY,
    email           VARCHAR(255) NOT NULL UNIQUE,
    display_name    VARCHAR(255),
    is_active       BOOLEAN NOT NULL DEFAULT true,
    last_event_id   UUID NOT NULL
);

CREATE TABLE proj_user_project_roles (
    user_id         UUID NOT NULL,
    project_id      UUID NOT NULL,
    role_name       VARCHAR(100) NOT NULL,  -- denormalised for read performance
    last_event_id   UUID NOT NULL,
    PRIMARY KEY (user_id, project_id, role_name)
);

CREATE TABLE proj_api_keys (
    api_key_id      UUID PRIMARY KEY,
    user_id         UUID NOT NULL,
    project_id      UUID,
    key_hash        VARCHAR(255) NOT NULL UNIQUE,
    name            VARCHAR(255) NOT NULL,
    expires_at      TIMESTAMPTZ,
    is_active       BOOLEAN NOT NULL DEFAULT true,
    last_event_id   UUID NOT NULL
);
```

---

## Monitoring (Event-Derived)

```sql
-- Feature statistics are events too (StatisticsComputed event type)
-- This projection provides the latest stats for dashboards
CREATE TABLE proj_latest_statistics (
    feature_view_id UUID NOT NULL,
    feature_name    VARCHAR(255) NOT NULL,
    computed_at     TIMESTAMPTZ NOT NULL,
    row_count       BIGINT,
    null_fraction   DOUBLE PRECISION,
    mean            DOUBLE PRECISION,
    stddev          DOUBLE PRECISION,
    drift_score     DOUBLE PRECISION,
    last_event_id   UUID NOT NULL,
    PRIMARY KEY (feature_view_id, feature_name)
);

-- Full statistics history is queryable from registry_events directly:
-- SELECT payload FROM registry_events
-- WHERE aggregate_type = 'feature_view'
--   AND aggregate_id = $1
--   AND event_type = 'StatisticsComputed'
-- ORDER BY occurred_at;
```

---

## Projection Rebuild Infrastructure

```sql
-- Tracks the last event processed by each projection
CREATE TABLE projection_checkpoints (
    projection_name VARCHAR(100) PRIMARY KEY,  -- e.g. 'proj_feature_views', 'proj_entities'
    last_event_id   UUID NOT NULL,
    last_event_at   TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

## Temporal Query Examples

```sql
-- "What was the schema of feature view X when model Y was trained on 2026-03-15?"
SELECT payload
FROM registry_events
WHERE aggregate_id = '...'  -- feature view ID
  AND aggregate_type = 'feature_view'
  AND event_type IN ('FeatureViewCreated', 'FeatureAdded', 'FeatureRemoved', 'SchemaChanged')
  AND occurred_at <= '2026-03-15T00:00:00Z'
ORDER BY sequence_number;

-- "Who changed this feature view in the last 7 days?"
SELECT e.event_type, e.payload, e.occurred_at, u.email
FROM registry_events e
LEFT JOIN proj_users u ON e.actor_id = u.user_id
WHERE e.aggregate_id = '...'
  AND e.occurred_at >= now() - INTERVAL '7 days'
ORDER BY e.occurred_at DESC;

-- "What was the registry state at a specific point in time?"
-- (Replay all events up to that timestamp)
SELECT aggregate_type, aggregate_id, event_type, payload
FROM registry_events
WHERE project_id = '...'
  AND occurred_at <= '2026-04-01T00:00:00Z'
ORDER BY aggregate_id, sequence_number;
```

---

## Table Count Summary

| Category | Tables | Notes |
|----------|--------|-------|
| Event Store | 1 | registry_events (single append-only table) |
| Snapshots | 1 | aggregate_snapshots (periodic state capture) |
| Current State Projections | 8 | proj_projects, proj_entities, proj_data_sources, proj_feature_views, proj_features, proj_feature_services, proj_materialisation_state, proj_latest_statistics |
| Access Control Projections | 3 | proj_users, proj_user_project_roles, proj_api_keys |
| Infrastructure | 1 | projection_checkpoints |
| **Total** | **14** | 1 source-of-truth table + 13 derived tables |

---

## Key Design Decisions

1. **Single event table as source of truth** — all registry state is derived from `registry_events`. Projections can be dropped and rebuilt at any time without data loss.

2. **JSONB payloads instead of typed event tables** — each event type has a different shape. Using JSONB avoids creating dozens of event-specific tables while remaining queryable via PostgreSQL's JSONB operators.

3. **Aggregate-scoped sequence numbers for optimistic concurrency** — prevents conflicting concurrent writes to the same registry object. A write that detects a sequence gap can retry or fail fast.

4. **Correlation and causation IDs** — a single `feast apply` operation that creates an entity, a data source, and a feature view generates three events sharing the same `correlation_id`, enabling reconstruction of compound operations.

5. **Projections are clearly marked as derived** — the `proj_` prefix signals that these tables are rebuild-safe. Each row tracks its `last_event_id` for incremental updates and debugging.

6. **Snapshots for replay performance** — rather than replaying thousands of events for long-lived aggregates, periodic snapshots allow replay to start from a recent checkpoint.

7. **Soft deletes via `is_deleted` flag in projections** — objects are never physically removed from projections; a `Deleted` event sets the flag. The event store retains the full lifecycle.

8. **GDPR via crypto-shredding** — rather than deleting events (which would break the append-only invariant), entity-keyed encryption keys are destroyed, rendering PII in event payloads unreadable while preserving the event stream structure.

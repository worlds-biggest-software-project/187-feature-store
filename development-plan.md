# Feature Store — Phased Development Plan

> Project: 187-feature-store · Created: 2026-05-25
> Purpose: Provide sufficient detail for Claude Code (Opus) to implement each phase end-to-end.

---

## Technology Decisions

| Concern | Choice | Rationale |
|---------|--------|-----------|
| Language | Python 3.12+ | Feature stores are consumed by ML engineers via Python; Feast's SDK is Python; the entire ecosystem (pandas, Arrow, DuckDB, scikit-learn) is Python. Aligns with every competitor. |
| API Framework | FastAPI | Async-first, automatic OpenAPI 3.1 spec generation (required by standards.md), Pydantic v2 for request/response validation. Feast Feature Server uses a similar pattern. |
| gRPC Framework | grpcio + protobuf | Low-latency online serving requires gRPC (Feast protocol compatibility). Dual HTTP+gRPC serving matches Tecton and Hopsworks patterns. |
| Registry Database | PostgreSQL 16 | Hybrid relational + JSONB model (Data Model Suggestion 3) provides the best MVP trade-off: fewer tables, schema flexibility for provider-specific configs, GIN indexes for metadata queries. SQLite for local dev mode. |
| Offline Store | DuckDB (embedded) + Parquet/S3 | DuckDB provides zero-infrastructure point-in-time joins on Parquet files. Pluggable interface for BigQuery/Snowflake. Avoids mandatory Spark dependency (a key differentiator vs Feast). |
| Online Store | Redis 7 | Sub-millisecond serving; most widely adopted online store across all competitors. Pluggable interface for DynamoDB/Bigtable. |
| Streaming Ingestion | confluent-kafka-python | Kafka consumer for real-time feature ingestion. Pluggable interface for Kinesis. |
| Task Queue | Celery + Redis broker | Materialisation jobs are long-running async workloads. Redis already present as online store. |
| Frontend | React 18 + Vite + TanStack Query | Feature discovery UI, lineage visualization, monitoring dashboards. Separate SPA communicating via REST API. |
| Lineage Visualization | D3.js (dagre-d3) | DAG rendering for feature lineage graphs. |
| Metrics | prometheus-client | Native Prometheus exposition format for feature freshness, serving latency, materialisation health. Matches Feast and industry standard. |
| AI/LLM Integration | anthropic SDK + openai SDK | Feature recommendation, NL-to-code generation, drift analysis. Provider-agnostic with pluggable LLM backend. |
| Containerisation | Docker + docker-compose | Self-hosted deployment target. Multi-stage build for minimal production image. |
| Testing | pytest + pytest-asyncio + httpx | Standard Python test stack. httpx for async FastAPI test client. |
| Code Quality | ruff (lint + format) + mypy (strict) | Ruff replaces flake8+black+isort. mypy in strict mode catches type errors in SDK and API code. |
| Package Manager | uv | Fast dependency resolution; generates lockfile for reproducible builds. |
| Serialisation | Apache Arrow + Parquet | Arrow for in-memory batch feature retrieval; Parquet for offline store persistence. Standard across the ecosystem. |
| Data Validation | Pydantic v2 + Great Expectations (optional) | Pydantic for API/SDK validation; Great Expectations for statistical feature quality validation. |
| Auth | OAuth 2.0 / OIDC via python-jose + passlib | Bearer token auth for API; OIDC for web UI. API key auth for service-to-service. |
| Migration | Alembic | SQLAlchemy-based migration for PostgreSQL schema evolution. |

### Data Model Selection

**Selected: Data Model Suggestion 3 (Hybrid Relational + JSONB)** — 15 tables total.

Rationale: The hybrid model provides the best balance for an MVP that must evolve rapidly. Core registry objects (projects, entities, data_sources, feature_views, feature_services) are relational with referential integrity, while provider-specific configurations and extensible metadata live in JSONB columns. This avoids the 27-table complexity of the fully normalised model (Suggestion 1), the infrastructure overhead of event sourcing (Suggestion 2), and the query complexity of the graph model (Suggestion 4). The graph model's lineage traversal is captured via the `lineage_edges` table with recursive CTEs, which is sufficient for MVP.

### Project Structure

```
feature-store/
├── pyproject.toml
├── uv.lock
├── Dockerfile
├── docker-compose.yml
├── alembic.ini
├── alembic/
│   ├── env.py
│   └── versions/
├── protos/
│   ├── feast_serving.proto
│   └── feature_store.proto
├── src/
│   └── feature_store/
│       ├── __init__.py
│       ├── config.py
│       ├── main.py                      # FastAPI app entry
│       ├── cli.py                       # CLI entry (click)
│       ├── registry/
│       │   ├── __init__.py
│       │   ├── models.py                # SQLAlchemy ORM models
│       │   ├── schemas.py               # Pydantic schemas
│       │   ├── repository.py            # CRUD operations
│       │   └── service.py               # Business logic
│       ├── offline_store/
│       │   ├── __init__.py
│       │   ├── base.py                  # Abstract offline store interface
│       │   ├── duckdb_store.py          # DuckDB + Parquet implementation
│       │   ├── bigquery_store.py        # BigQuery implementation
│       │   └── point_in_time.py         # PIT join logic
│       ├── online_store/
│       │   ├── __init__.py
│       │   ├── base.py                  # Abstract online store interface
│       │   └── redis_store.py           # Redis implementation
│       ├── materialisation/
│       │   ├── __init__.py
│       │   ├── engine.py                # Materialisation job runner
│       │   ├── scheduler.py             # Celery task definitions
│       │   └── state.py                 # Materialisation state tracking
│       ├── serving/
│       │   ├── __init__.py
│       │   ├── rest_server.py           # FastAPI online serving routes
│       │   └── grpc_server.py           # gRPC serving implementation
│       ├── streaming/
│       │   ├── __init__.py
│       │   ├── base.py                  # Abstract stream source interface
│       │   └── kafka_source.py          # Kafka consumer
│       ├── monitoring/
│       │   ├── __init__.py
│       │   ├── metrics.py               # Prometheus metrics
│       │   ├── statistics.py            # Feature statistics computation
│       │   └── drift.py                 # Drift detection algorithms
│       ├── auth/
│       │   ├── __init__.py
│       │   ├── middleware.py            # FastAPI auth middleware
│       │   ├── rbac.py                  # RBAC enforcement
│       │   └── api_keys.py             # API key management
│       ├── lineage/
│       │   ├── __init__.py
│       │   └── tracker.py              # Lineage edge management
│       ├── ai/
│       │   ├── __init__.py
│       │   ├── feature_recommender.py   # AI-powered feature suggestion
│       │   ├── nl_authoring.py          # Natural-language to transformation code
│       │   └── drift_analyzer.py        # Intelligent drift alerting
│       └── sdk/
│           ├── __init__.py
│           ├── client.py               # Python SDK client
│           ├── feature_view.py         # FeatureView definition class
│           ├── entity.py               # Entity definition class
│           ├── data_source.py          # DataSource definition class
│           └── feature_service.py      # FeatureService definition class
├── frontend/
│   ├── package.json
│   ├── vite.config.ts
│   ├── src/
│   │   ├── App.tsx
│   │   ├── api/                        # API client layer
│   │   ├── components/
│   │   │   ├── registry/               # Feature registry browser
│   │   │   ├── lineage/                # DAG visualization
│   │   │   └── monitoring/             # Metrics dashboards
│   │   └── pages/
│   └── tsconfig.json
├── tests/
│   ├── conftest.py
│   ├── unit/
│   │   ├── test_registry.py
│   │   ├── test_offline_store.py
│   │   ├── test_online_store.py
│   │   ├── test_materialisation.py
│   │   ├── test_serving.py
│   │   ├── test_monitoring.py
│   │   └── test_auth.py
│   ├── integration/
│   │   ├── test_registry_db.py
│   │   ├── test_redis_online.py
│   │   ├── test_materialise_pipeline.py
│   │   └── test_serving_e2e.py
│   └── fixtures/
│       ├── sample_features.parquet
│       ├── sample_entities.csv
│       └── feature_definitions/
└── docs/
    └── openapi.yaml                    # Auto-generated
```

---

## Phase 1: Foundation & Project Scaffolding

### Purpose
Establish the project skeleton: package configuration, database connection, configuration management, and the core data model. After this phase, the registry database exists, migrations run, and the test harness is operational. Everything built in subsequent phases depends on this foundation.

### Tasks

#### 1.1 — Project Setup & Configuration

**What**: Create the Python package with uv, configure linting/formatting/typing, and implement hierarchical configuration management.

**Design**:

```python
# src/feature_store/config.py
from pydantic_settings import BaseSettings
from pydantic import Field
from enum import Enum

class OfflineStoreType(str, Enum):
    DUCKDB = "duckdb"
    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"

class OnlineStoreType(str, Enum):
    REDIS = "redis"
    SQLITE = "sqlite"  # local dev only

class FeatureStoreConfig(BaseSettings):
    """Hierarchical config: env vars > .env file > defaults."""
    model_config = {"env_prefix": "FS_"}

    # Registry database
    registry_dsn: str = "postgresql+asyncpg://localhost:5432/feature_store"
    registry_pool_size: int = 10

    # Offline store
    offline_store_type: OfflineStoreType = OfflineStoreType.DUCKDB
    offline_store_path: str = "./data/offline"

    # Online store
    online_store_type: OnlineStoreType = OnlineStoreType.REDIS
    redis_url: str = "redis://localhost:6379/0"

    # Serving
    http_host: str = "0.0.0.0"
    http_port: int = 8666
    grpc_port: int = 50051

    # Auth
    auth_enabled: bool = False
    jwt_secret: str = Field(default="change-me-in-production", min_length=16)
    api_key_salt: str = Field(default="change-me-in-production", min_length=16)

    # Monitoring
    metrics_enabled: bool = True
    metrics_path: str = "/metrics"

    # LLM (for AI features)
    llm_provider: str = "anthropic"  # anthropic, openai
    llm_api_key: str = ""
    llm_model: str = "claude-sonnet-4-20250514"
```

```toml
# pyproject.toml (key sections)
[project]
name = "feature-store"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "fastapi>=0.115",
    "uvicorn[standard]>=0.30",
    "sqlalchemy[asyncio]>=2.0",
    "asyncpg>=0.30",
    "alembic>=1.14",
    "pydantic>=2.9",
    "pydantic-settings>=2.5",
    "click>=8.1",
    "rich>=13.0",
    "httpx>=0.27",
]

[project.optional-dependencies]
redis = ["redis>=5.0"]
kafka = ["confluent-kafka>=2.5"]
grpc = ["grpcio>=1.60", "grpcio-tools>=1.60", "protobuf>=5.0"]
ai = ["anthropic>=0.40", "openai>=1.50"]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.24",
    "ruff>=0.8",
    "mypy>=1.13",
    "httpx>=0.27",
]

[project.scripts]
feature-store = "feature_store.cli:main"

[tool.ruff]
target-version = "py312"
line-length = 100

[tool.mypy]
strict = true
python_version = "3.12"
```

**Testing**:
- `Unit: test_config_defaults` — instantiate `FeatureStoreConfig()` with no env vars, verify all defaults are set correctly
- `Unit: test_config_env_override` — set `FS_HTTP_PORT=9000` env var, verify config reads 9000
- `Unit: test_config_invalid_dsn` — pass empty string for `registry_dsn`, verify `ValidationError`
- `Unit: test_config_jwt_secret_min_length` — pass 5-char jwt_secret, verify `ValidationError` with field name

#### 1.2 — Database Models & Initial Migration

**What**: Implement SQLAlchemy ORM models matching Data Model Suggestion 3 (Hybrid Relational + JSONB) and create the initial Alembic migration.

**Design**:

```python
# src/feature_store/registry/models.py
from __future__ import annotations
import uuid
from datetime import datetime
from sqlalchemy import (
    String, Boolean, BigInteger, Text, DateTime, ForeignKey, UniqueConstraint, Index
)
from sqlalchemy.dialects.postgresql import UUID, JSONB, ARRAY
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
    pass

class Project(Base):
    __tablename__ = "projects"

    project_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    owner_email: Mapped[str | None] = mapped_column(String(255))
    settings: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

class Entity(Base):
    __tablename__ = "entities"
    __table_args__ = (UniqueConstraint("project_id", "name"),)

    entity_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("projects.project_id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    join_key: Mapped[str] = mapped_column(String(255), nullable=False)
    value_type: Mapped[str] = mapped_column(String(50), nullable=False)
    metadata_: Mapped[dict] = mapped_column("metadata", JSONB, nullable=False, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

class DataSource(Base):
    __tablename__ = "data_sources"
    __table_args__ = (UniqueConstraint("project_id", "name"),)

    data_source_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("projects.project_id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_type: Mapped[str] = mapped_column(String(50), nullable=False)  # FILE, BIGQUERY, KAFKA, etc.
    config: Mapped[dict] = mapped_column(JSONB, nullable=False)
    metadata_: Mapped[dict] = mapped_column("metadata", JSONB, nullable=False, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

class FeatureView(Base):
    __tablename__ = "feature_views"
    __table_args__ = (UniqueConstraint("project_id", "name", "version"),)

    feature_view_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("projects.project_id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    view_type: Mapped[str] = mapped_column(String(30), nullable=False, server_default="batch")
    data_source_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("data_sources.data_source_id")
    )
    entity_ids: Mapped[list] = mapped_column(ARRAY(UUID(as_uuid=True)), nullable=False, server_default="{}")
    features: Mapped[list] = mapped_column(JSONB, nullable=False, server_default="[]")
    ttl_seconds: Mapped[int | None] = mapped_column(BigInteger)
    online_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="true")
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="1")
    stream_config: Mapped[dict | None] = mapped_column(JSONB)
    on_demand_config: Mapped[dict | None] = mapped_column(JSONB)
    metadata_: Mapped[dict] = mapped_column("metadata", JSONB, nullable=False, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

class FeatureService(Base):
    __tablename__ = "feature_services"
    __table_args__ = (UniqueConstraint("project_id", "name"),)

    feature_service_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("projects.project_id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    feature_view_ids: Mapped[list] = mapped_column(ARRAY(UUID(as_uuid=True)), nullable=False, server_default="{}")
    metadata_: Mapped[dict] = mapped_column("metadata", JSONB, nullable=False, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

class MaterialisationJob(Base):
    __tablename__ = "materialisation_jobs"

    job_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    feature_view_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("feature_views.feature_view_id"), nullable=False
    )
    status: Mapped[str] = mapped_column(String(20), nullable=False, server_default="PENDING")
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    result: Mapped[dict | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

class User(Base):
    __tablename__ = "users"

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    display_name: Mapped[str | None] = mapped_column(String(255))
    auth_config: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="true")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

class UserProjectRole(Base):
    __tablename__ = "user_project_roles"
    __table_args__ = (
        {"comment": "Project-scoped RBAC"},
    )

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.user_id", ondelete="CASCADE"), primary_key=True
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("projects.project_id", ondelete="CASCADE"), primary_key=True
    )
    role: Mapped[str] = mapped_column(String(100), primary_key=True)
    permissions: Mapped[list] = mapped_column(JSONB, nullable=False, server_default="[]")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

class ApiKey(Base):
    __tablename__ = "api_keys"

    api_key_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False
    )
    project_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("projects.project_id")
    )
    key_hash: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    scopes: Mapped[list] = mapped_column(JSONB, nullable=False, server_default='["read"]')
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="true")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

class FeatureStatistics(Base):
    __tablename__ = "feature_statistics"

    stat_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    feature_view_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("feature_views.feature_view_id"), nullable=False
    )
    computed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    stats: Mapped[dict] = mapped_column(JSONB, nullable=False)
    baseline_stat_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("feature_statistics.stat_id")
    )

class Alert(Base):
    __tablename__ = "alerts"

    alert_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("projects.project_id"), nullable=False
    )
    source_type: Mapped[str] = mapped_column(String(50), nullable=False)
    source_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    alert_data: Mapped[dict] = mapped_column(JSONB, nullable=False)
    acknowledged: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

class LineageEdge(Base):
    __tablename__ = "lineage_edges"
    __table_args__ = (
        UniqueConstraint("source_type", "source_id", "target_type", "target_id", "relationship"),
    )

    edge_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    source_type: Mapped[str] = mapped_column(String(50), nullable=False)
    source_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    target_type: Mapped[str] = mapped_column(String(50), nullable=False)
    target_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    relationship: Mapped[str] = mapped_column(String(50), nullable=False)
    edge_metadata: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

class SavedDataset(Base):
    __tablename__ = "saved_datasets"
    __table_args__ = (UniqueConstraint("project_id", "name"),)

    dataset_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("projects.project_id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    feature_service_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("feature_services.feature_service_id")
    )
    config: Mapped[dict] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

class AuditLog(Base):
    __tablename__ = "audit_log"

    log_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    project_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    actor_type: Mapped[str] = mapped_column(String(20), nullable=False)
    actor_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    action: Mapped[str] = mapped_column(String(100), nullable=False)
    resource_type: Mapped[str] = mapped_column(String(50), nullable=False)
    resource_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True))
    details: Mapped[dict | None] = mapped_column(JSONB)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
```

**Testing**:
- `Unit: test_all_models_inherit_base` — verify every model class inherits from `Base` and has a `__tablename__`
- `Unit: test_project_defaults` — create a `Project` instance, verify `settings` defaults to `{}`
- `Unit: test_feature_view_unique_constraint` — verify `(project_id, name, version)` is unique
- `Integration: test_create_all_tables` — run `Base.metadata.create_all()` against a test PostgreSQL instance, verify all 15 tables exist
- `Integration: test_alembic_upgrade_head` — run `alembic upgrade head` against a clean database, verify migration succeeds
- `Integration: test_alembic_downgrade_base` — run `alembic downgrade base`, verify all tables are removed

#### 1.3 — Pydantic Schemas & Value Types

**What**: Define Pydantic v2 schemas for all API request/response models and the Feast-compatible type system.

**Design**:

```python
# src/feature_store/registry/schemas.py
from __future__ import annotations
import uuid
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field, ConfigDict

class ValueType(str, Enum):
    """Feature value types aligned with Feast Value.proto."""
    INT32 = "INT32"
    INT64 = "INT64"
    FLOAT32 = "FLOAT32"
    FLOAT64 = "FLOAT64"
    STRING = "STRING"
    BYTES = "BYTES"
    BOOL = "BOOL"
    UNIX_TIMESTAMP = "UNIX_TIMESTAMP"
    ARRAY_INT32 = "ARRAY_INT32"
    ARRAY_INT64 = "ARRAY_INT64"
    ARRAY_FLOAT32 = "ARRAY_FLOAT32"
    ARRAY_FLOAT64 = "ARRAY_FLOAT64"
    ARRAY_STRING = "ARRAY_STRING"

class FeatureViewType(str, Enum):
    BATCH = "batch"
    STREAM = "stream"
    ON_DEMAND = "on_demand"

class SourceType(str, Enum):
    FILE = "FILE"
    BIGQUERY = "BIGQUERY"
    SNOWFLAKE = "SNOWFLAKE"
    REDSHIFT = "REDSHIFT"
    KAFKA = "KAFKA"
    KINESIS = "KINESIS"
    PUSH = "PUSH"
    POSTGRESQL = "POSTGRESQL"

# --- Project ---
class ProjectCreate(BaseModel):
    name: str = Field(max_length=255, pattern=r"^[a-z0-9_-]+$")
    description: str | None = None
    owner_email: str | None = None
    settings: dict = Field(default_factory=dict)

class ProjectResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    project_id: uuid.UUID
    name: str
    description: str | None
    owner_email: str | None
    settings: dict
    created_at: datetime
    updated_at: datetime

# --- Entity ---
class EntityCreate(BaseModel):
    name: str = Field(max_length=255)
    join_key: str = Field(max_length=255)
    value_type: ValueType
    metadata: dict = Field(default_factory=dict)

class EntityResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    entity_id: uuid.UUID
    project_id: uuid.UUID
    name: str
    join_key: str
    value_type: ValueType
    metadata: dict
    created_at: datetime
    updated_at: datetime

# --- Feature Definition (embedded in FeatureView) ---
class FeatureDefinition(BaseModel):
    name: str = Field(max_length=255)
    value_type: ValueType
    description: str | None = None
    tags: list[str] = Field(default_factory=list)

# --- FeatureView ---
class FeatureViewCreate(BaseModel):
    name: str = Field(max_length=255)
    view_type: FeatureViewType = FeatureViewType.BATCH
    data_source_id: uuid.UUID | None = None
    entity_ids: list[uuid.UUID] = Field(default_factory=list)
    features: list[FeatureDefinition]
    ttl_seconds: int | None = None
    online_enabled: bool = True
    stream_config: dict | None = None
    on_demand_config: dict | None = None
    metadata: dict = Field(default_factory=dict)

class FeatureViewResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    feature_view_id: uuid.UUID
    project_id: uuid.UUID
    name: str
    view_type: FeatureViewType
    data_source_id: uuid.UUID | None
    entity_ids: list[uuid.UUID]
    features: list[FeatureDefinition]
    ttl_seconds: int | None
    online_enabled: bool
    version: int
    stream_config: dict | None
    on_demand_config: dict | None
    metadata: dict
    created_at: datetime
    updated_at: datetime

# --- DataSource ---
class DataSourceCreate(BaseModel):
    name: str = Field(max_length=255)
    source_type: SourceType
    config: dict
    metadata: dict = Field(default_factory=dict)

class DataSourceResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    data_source_id: uuid.UUID
    project_id: uuid.UUID
    name: str
    source_type: SourceType
    config: dict
    metadata: dict
    created_at: datetime
    updated_at: datetime

# --- FeatureService ---
class FeatureServiceCreate(BaseModel):
    name: str = Field(max_length=255)
    feature_view_ids: list[uuid.UUID]
    metadata: dict = Field(default_factory=dict)

class FeatureServiceResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    feature_service_id: uuid.UUID
    project_id: uuid.UUID
    name: str
    feature_view_ids: list[uuid.UUID]
    metadata: dict
    created_at: datetime
    updated_at: datetime

# --- Online Feature Request/Response ---
class OnlineFeaturesRequest(BaseModel):
    feature_service: str | None = None
    features: list[str] | None = None  # "feature_view:feature_name" format
    entity_rows: list[dict]

class OnlineFeaturesResponse(BaseModel):
    metadata: dict
    results: list[dict]  # one dict per entity row with feature values

# --- Historical Feature Request ---
class HistoricalFeaturesRequest(BaseModel):
    feature_service: str | None = None
    features: list[str] | None = None
    entity_df: dict  # serialised DataFrame or reference
    full_feature_names: bool = False
```

**Testing**:
- `Unit: test_value_type_enum_completeness` — verify all 13 Feast value types are present
- `Unit: test_project_create_valid` — valid ProjectCreate serialises correctly
- `Unit: test_project_create_invalid_name` — name with spaces raises `ValidationError`
- `Unit: test_feature_definition_defaults` — FeatureDefinition with only name+value_type gets empty tags
- `Unit: test_online_features_request_requires_entity_rows` — missing entity_rows raises error
- `Unit: test_feature_view_create_with_features` — FeatureViewCreate with 3 features serialises the JSONB array correctly
- `Unit: test_schema_from_attributes` — create an ORM Project, pass to ProjectResponse.model_validate, verify all fields map

#### 1.4 — Docker & docker-compose Setup

**What**: Create multi-stage Dockerfile and docker-compose.yml with PostgreSQL, Redis, and the feature store service.

**Design**:

```dockerfile
# Dockerfile
FROM python:3.12-slim AS base
WORKDIR /app
RUN pip install uv

FROM base AS builder
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-editable

FROM base AS runtime
COPY --from=builder /app/.venv /app/.venv
COPY src/ ./src/
COPY alembic/ ./alembic/
COPY alembic.ini .
ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 8666 50051
CMD ["uvicorn", "feature_store.main:app", "--host", "0.0.0.0", "--port", "8666"]
```

```yaml
# docker-compose.yml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: feature_store
      POSTGRES_USER: fs_user
      POSTGRES_PASSWORD: fs_password
    ports: ["5432:5432"]
    volumes: [pgdata:/var/lib/postgresql/data]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U fs_user -d feature_store"]
      interval: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      retries: 5

  feature-store:
    build: .
    depends_on:
      postgres: { condition: service_healthy }
      redis: { condition: service_healthy }
    environment:
      FS_REGISTRY_DSN: postgresql+asyncpg://fs_user:fs_password@postgres:5432/feature_store
      FS_REDIS_URL: redis://redis:6379/0
    ports:
      - "8666:8666"
      - "50051:50051"

volumes:
  pgdata:
```

**Testing**:
- `Integration: test_docker_build` — `docker build .` succeeds without errors
- `Integration: test_docker_compose_up` — `docker-compose up -d` starts all 3 services, health checks pass
- `Integration: test_health_endpoint` — `curl localhost:8666/health` returns `{"status": "ok"}`

---

## Phase 2: Registry CRUD & Python SDK

### Purpose
Implement the feature registry: full CRUD operations for projects, entities, data sources, feature views, and feature services, exposed via REST API and consumable through the Python SDK. After this phase, users can define features in Python, apply them to the registry, and browse the catalogue via API.

### Tasks

#### 2.1 — Registry Repository Layer

**What**: Implement async CRUD operations for all registry objects using SQLAlchemy.

**Design**:

```python
# src/feature_store/registry/repository.py
from __future__ import annotations
import uuid
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete
from feature_store.registry.models import (
    Project, Entity, DataSource, FeatureView, FeatureService
)
from feature_store.registry.schemas import (
    ProjectCreate, EntityCreate, DataSourceCreate,
    FeatureViewCreate, FeatureServiceCreate
)

class RegistryRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    # --- Projects ---
    async def create_project(self, data: ProjectCreate) -> Project: ...
    async def get_project(self, project_id: uuid.UUID) -> Project | None: ...
    async def get_project_by_name(self, name: str) -> Project | None: ...
    async def list_projects(self, offset: int = 0, limit: int = 100) -> list[Project]: ...
    async def update_project(self, project_id: uuid.UUID, data: dict) -> Project: ...
    async def delete_project(self, project_id: uuid.UUID) -> None: ...

    # --- Entities ---
    async def create_entity(self, project_id: uuid.UUID, data: EntityCreate) -> Entity: ...
    async def get_entity(self, entity_id: uuid.UUID) -> Entity | None: ...
    async def list_entities(self, project_id: uuid.UUID) -> list[Entity]: ...
    async def update_entity(self, entity_id: uuid.UUID, data: dict) -> Entity: ...
    async def delete_entity(self, entity_id: uuid.UUID) -> None: ...

    # --- DataSources ---
    async def create_data_source(self, project_id: uuid.UUID, data: DataSourceCreate) -> DataSource: ...
    async def get_data_source(self, data_source_id: uuid.UUID) -> DataSource | None: ...
    async def list_data_sources(self, project_id: uuid.UUID, source_type: str | None = None) -> list[DataSource]: ...
    async def update_data_source(self, data_source_id: uuid.UUID, data: dict) -> DataSource: ...
    async def delete_data_source(self, data_source_id: uuid.UUID) -> None: ...

    # --- FeatureViews ---
    async def create_feature_view(self, project_id: uuid.UUID, data: FeatureViewCreate) -> FeatureView: ...
    async def get_feature_view(self, feature_view_id: uuid.UUID) -> FeatureView | None: ...
    async def get_feature_view_by_name(self, project_id: uuid.UUID, name: str, version: int | None = None) -> FeatureView | None: ...
    async def list_feature_views(self, project_id: uuid.UUID, view_type: str | None = None) -> list[FeatureView]: ...
    async def update_feature_view(self, feature_view_id: uuid.UUID, data: dict) -> FeatureView: ...
    async def delete_feature_view(self, feature_view_id: uuid.UUID) -> None: ...

    # --- FeatureServices ---
    async def create_feature_service(self, project_id: uuid.UUID, data: FeatureServiceCreate) -> FeatureService: ...
    async def get_feature_service(self, feature_service_id: uuid.UUID) -> FeatureService | None: ...
    async def list_feature_services(self, project_id: uuid.UUID) -> list[FeatureService]: ...
    async def delete_feature_service(self, feature_service_id: uuid.UUID) -> None: ...

    # --- Cross-cutting queries ---
    async def search_features_by_name(self, project_id: uuid.UUID, query: str) -> list[dict]:
        """Search features across all feature views using JSONB containment."""
        ...

    async def find_feature_views_by_entity(self, entity_id: uuid.UUID) -> list[FeatureView]:
        """Find all feature views that reference a given entity (UUID array containment)."""
        ...
```

**Testing**:
- `Integration: test_create_project` — create a project, retrieve it, verify all fields match
- `Integration: test_create_project_duplicate_name` — creating two projects with the same name raises `IntegrityError`
- `Integration: test_create_entity_in_project` — create entity with project FK, verify FK constraint holds
- `Integration: test_create_feature_view_with_features` — create feature view with 3-element features JSONB array, retrieve and verify array contents
- `Integration: test_list_feature_views_by_type` — create batch and stream views, filter by type, verify correct filtering
- `Integration: test_delete_project_cascades` — delete project, verify entities and feature views are cascaded
- `Integration: test_search_features_jsonb` — create feature view with feature named "conv_rate", search for "conv_rate", verify it appears in results
- `Integration: test_find_feature_views_by_entity` — create 2 feature views referencing entity A, 1 referencing entity B, query by entity A, verify 2 results

#### 2.2 — REST API Routes

**What**: Expose all registry CRUD operations as REST endpoints following OpenAPI 3.1 conventions.

**Design**:

```python
# src/feature_store/main.py
from fastapi import FastAPI
from feature_store.serving.rest_server import registry_router, serving_router

app = FastAPI(
    title="Feature Store",
    version="0.1.0",
    description="Centralised feature computation, storage, and serving for ML",
)

app.include_router(registry_router, prefix="/api/v1")
# serving_router added in Phase 4
```

API endpoint table:

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/projects` | Create project |
| GET | `/api/v1/projects` | List projects |
| GET | `/api/v1/projects/{project_id}` | Get project |
| PUT | `/api/v1/projects/{project_id}` | Update project |
| DELETE | `/api/v1/projects/{project_id}` | Delete project |
| POST | `/api/v1/projects/{project_id}/entities` | Create entity |
| GET | `/api/v1/projects/{project_id}/entities` | List entities |
| GET | `/api/v1/projects/{project_id}/entities/{entity_id}` | Get entity |
| PUT | `/api/v1/projects/{project_id}/entities/{entity_id}` | Update entity |
| DELETE | `/api/v1/projects/{project_id}/entities/{entity_id}` | Delete entity |
| POST | `/api/v1/projects/{project_id}/data-sources` | Create data source |
| GET | `/api/v1/projects/{project_id}/data-sources` | List data sources |
| GET | `/api/v1/projects/{project_id}/data-sources/{ds_id}` | Get data source |
| PUT | `/api/v1/projects/{project_id}/data-sources/{ds_id}` | Update data source |
| DELETE | `/api/v1/projects/{project_id}/data-sources/{ds_id}` | Delete data source |
| POST | `/api/v1/projects/{project_id}/feature-views` | Create feature view |
| GET | `/api/v1/projects/{project_id}/feature-views` | List feature views |
| GET | `/api/v1/projects/{project_id}/feature-views/{fv_id}` | Get feature view |
| PUT | `/api/v1/projects/{project_id}/feature-views/{fv_id}` | Update feature view |
| DELETE | `/api/v1/projects/{project_id}/feature-views/{fv_id}` | Delete feature view |
| POST | `/api/v1/projects/{project_id}/feature-services` | Create feature service |
| GET | `/api/v1/projects/{project_id}/feature-services` | List feature services |
| GET | `/api/v1/projects/{project_id}/feature-services/{fs_id}` | Get feature service |
| DELETE | `/api/v1/projects/{project_id}/feature-services/{fs_id}` | Delete feature service |
| GET | `/api/v1/projects/{project_id}/search/features?q=` | Search features by name |

**Testing**:
- `Integration: test_create_project_api` — POST to `/api/v1/projects` with valid JSON, verify 201 with correct response body
- `Integration: test_create_project_invalid_name` — POST with name containing spaces, verify 422 with validation details
- `Integration: test_get_nonexistent_project` — GET `/api/v1/projects/{random_uuid}`, verify 404
- `Integration: test_list_feature_views_api` — create 3 feature views, GET list endpoint, verify 3 results with pagination
- `Integration: test_feature_search_api` — create feature views, search for feature name substring, verify matching results
- `Integration: test_openapi_spec_generation` — GET `/openapi.json`, verify it parses as valid OpenAPI 3.1

#### 2.3 — Python SDK Client

**What**: Implement the Python SDK that ML engineers use to define and apply features programmatically.

**Design**:

```python
# src/feature_store/sdk/client.py
from __future__ import annotations
import httpx
from feature_store.sdk.feature_view import FeatureViewDef
from feature_store.sdk.entity import EntityDef
from feature_store.sdk.data_source import DataSourceDef, FileSource, BigQuerySource
from feature_store.sdk.feature_service import FeatureServiceDef

class FeatureStore:
    """Main entry point for the Feature Store Python SDK."""

    def __init__(
        self,
        project: str,
        registry_url: str = "http://localhost:8666",
        api_key: str | None = None,
    ) -> None:
        self._project = project
        self._client = httpx.Client(
            base_url=registry_url,
            headers={"Authorization": f"Bearer {api_key}"} if api_key else {},
        )
        self._project_id: str | None = None

    def apply(self, objects: list[EntityDef | DataSourceDef | FeatureViewDef | FeatureServiceDef]) -> None:
        """Apply a list of feature definitions to the registry (create or update)."""
        ...

    def get_historical_features(
        self,
        entity_df: "pd.DataFrame",
        features: list[str],  # "feature_view:feature_name"
        full_feature_names: bool = False,
    ) -> "pd.DataFrame":
        """Retrieve point-in-time correct historical features for training."""
        ...

    def get_online_features(
        self,
        features: list[str],
        entity_rows: list[dict],
    ) -> dict:
        """Retrieve latest feature values for inference."""
        ...

    def materialize(
        self,
        start_date: "datetime",
        end_date: "datetime",
        feature_views: list[str] | None = None,
    ) -> None:
        """Trigger materialisation from offline to online store."""
        ...

    def list_feature_views(self) -> list[FeatureViewDef]:
        """List all feature views in the project."""
        ...

# src/feature_store/sdk/entity.py
from pydantic import BaseModel
from feature_store.registry.schemas import ValueType

class EntityDef(BaseModel):
    """Entity definition for the SDK."""
    name: str
    join_key: str
    value_type: ValueType
    description: str = ""
    tags: list[str] = []
    owner: str = ""

# src/feature_store/sdk/data_source.py
class FileSource(BaseModel):
    """File-based data source (Parquet, CSV)."""
    name: str
    path: str
    format: str = "PARQUET"
    timestamp_column: str = "event_timestamp"
    created_timestamp_column: str | None = None

class BigQuerySource(BaseModel):
    """BigQuery data source."""
    name: str
    table: str  # project.dataset.table
    timestamp_column: str = "event_timestamp"
    query: str | None = None

# src/feature_store/sdk/feature_view.py
class FeatureViewDef(BaseModel):
    """Feature view definition for the SDK."""
    name: str
    entities: list[str]           # entity names
    features: list[FeatureFieldDef]
    source: str                    # data source name
    ttl_seconds: int | None = None
    online: bool = True
    description: str = ""
    tags: list[str] = []
    owner: str = ""

class FeatureFieldDef(BaseModel):
    name: str
    value_type: ValueType
    description: str = ""
```

**Testing**:
- `Unit: test_entity_def_serialisation` — create EntityDef, verify it serialises to dict matching API schema
- `Unit: test_feature_view_def_with_features` — create FeatureViewDef with 3 features, verify features list
- `Integration (mocked API): test_apply_creates_entities` — mock HTTP POST for entities, call `store.apply([entity])`, verify POST was called with correct body
- `Integration (mocked API): test_apply_creates_feature_views` — mock HTTP, apply feature view, verify correct API call
- `Integration (mocked API): test_apply_idempotent` — apply same entity twice, verify update (PUT) on second call

#### 2.4 — CLI (`apply`, `plan`)

**What**: Implement the Click-based CLI with `apply` and `plan` commands for GitOps-style workflows.

**Design**:

```python
# src/feature_store/cli.py
import click
from rich.console import Console
from rich.table import Table

@click.group()
@click.option("--project", envvar="FS_PROJECT", help="Project name")
@click.option("--registry-url", envvar="FS_REGISTRY_URL", default="http://localhost:8666")
@click.pass_context
def main(ctx: click.Context, project: str, registry_url: str) -> None:
    """Feature Store CLI."""
    ctx.ensure_object(dict)
    ctx.obj["project"] = project
    ctx.obj["registry_url"] = registry_url

@main.command()
@click.argument("definition_path", type=click.Path(exists=True))
@click.pass_context
def apply(ctx: click.Context, definition_path: str) -> None:
    """Apply feature definitions from a Python file to the registry."""
    # 1. Import the Python module at definition_path
    # 2. Find all EntityDef, DataSourceDef, FeatureViewDef, FeatureServiceDef instances
    # 3. Call store.apply(objects)
    # 4. Print summary table
    ...

@main.command()
@click.argument("definition_path", type=click.Path(exists=True))
@click.pass_context
def plan(ctx: click.Context, definition_path: str) -> None:
    """Show what changes apply would make without executing them."""
    # Diff local definitions against registry state
    ...
```

**Testing**:
- `Unit: test_cli_help` — invoke `feature-store --help`, verify output contains "Feature Store CLI"
- `Integration (mocked API): test_cli_apply` — create a temp Python file with feature defs, run `feature-store apply <path>`, verify API calls made
- `Integration (mocked API): test_cli_plan_shows_diff` — run `feature-store plan <path>`, verify output shows "create" for new objects

---

## Phase 3: Offline Store & Point-in-Time Joins

### Purpose
Implement the offline store abstraction and the DuckDB-based reference implementation. After this phase, users can call `get_historical_features()` to generate point-in-time correct training datasets from Parquet files — the core value proposition of a feature store.

### Tasks

#### 3.1 — Offline Store Interface

**What**: Define the abstract offline store interface that all implementations (DuckDB, BigQuery, Snowflake) must satisfy.

**Design**:

```python
# src/feature_store/offline_store/base.py
from __future__ import annotations
from abc import ABC, abstractmethod
import pyarrow as pa

class OfflineStore(ABC):
    """Abstract interface for offline feature stores."""

    @abstractmethod
    async def get_historical_features(
        self,
        entity_df: pa.Table,
        feature_views: list[FeatureViewQuerySpec],
        full_feature_names: bool = False,
    ) -> pa.Table:
        """Execute point-in-time correct feature retrieval.

        Args:
            entity_df: Table with entity columns + event_timestamp column.
            feature_views: List of feature view specs with feature names to retrieve.
            full_feature_names: If True, prefix feature names with view name.

        Returns:
            Arrow Table with entity columns + requested feature columns.
        """
        ...

    @abstractmethod
    async def pull_latest_from_source(
        self,
        data_source: DataSourceConfig,
        feature_view_name: str,
        start_date: "datetime",
        end_date: "datetime",
    ) -> pa.Table:
        """Pull raw data from the source for materialisation."""
        ...

    @abstractmethod
    async def write_to_offline_store(
        self,
        feature_view_name: str,
        table: pa.Table,
        mode: str = "append",  # append, overwrite
    ) -> int:
        """Write data to the offline store. Returns row count written."""
        ...

class FeatureViewQuerySpec:
    """Specifies which features to retrieve from a feature view."""
    def __init__(
        self,
        feature_view_name: str,
        feature_names: list[str],
        entity_columns: list[str],
        timestamp_column: str = "event_timestamp",
        ttl_seconds: int | None = None,
    ) -> None:
        self.feature_view_name = feature_view_name
        self.feature_names = feature_names
        self.entity_columns = entity_columns
        self.timestamp_column = timestamp_column
        self.ttl_seconds = ttl_seconds

class DataSourceConfig:
    """Configuration for a data source pull."""
    def __init__(
        self,
        source_type: str,
        config: dict,
        timestamp_column: str,
    ) -> None:
        self.source_type = source_type
        self.config = config
        self.timestamp_column = timestamp_column
```

**Testing**:
- `Unit: test_offline_store_is_abstract` — attempt to instantiate `OfflineStore`, verify `TypeError`
- `Unit: test_feature_view_query_spec_defaults` — create spec, verify timestamp_column defaults to "event_timestamp"

#### 3.2 — DuckDB Offline Store Implementation

**What**: Implement the DuckDB-based offline store with point-in-time correct joins on Parquet files.

**Design**:

The point-in-time join algorithm:
1. For each feature view, load the Parquet data source into DuckDB.
2. For each entity row in `entity_df`, find the most recent feature row where `feature.event_timestamp <= entity.event_timestamp` and `feature.event_timestamp >= entity.event_timestamp - ttl`.
3. This is implemented as a SQL window function with `ROW_NUMBER() OVER (PARTITION BY entity_key ORDER BY event_timestamp DESC)` followed by a filter for `row_num = 1`.
4. Join the result back to the entity DataFrame.

```python
# src/feature_store/offline_store/duckdb_store.py
import duckdb
import pyarrow as pa
from feature_store.offline_store.base import OfflineStore, FeatureViewQuerySpec, DataSourceConfig

class DuckDBOfflineStore(OfflineStore):
    def __init__(self, data_dir: str = "./data/offline") -> None:
        self._data_dir = data_dir
        self._conn = duckdb.connect()

    async def get_historical_features(
        self,
        entity_df: pa.Table,
        feature_views: list[FeatureViewQuerySpec],
        full_feature_names: bool = False,
    ) -> pa.Table:
        # Register entity_df as a DuckDB table
        self._conn.register("entity_df", entity_df)

        result = entity_df
        for spec in feature_views:
            # Build PIT join SQL
            feature_cols = ", ".join(f"fv.{f}" for f in spec.feature_names)
            entity_join = " AND ".join(
                f"fv.{col} = entity_df.{col}" for col in spec.entity_columns
            )
            ttl_clause = ""
            if spec.ttl_seconds:
                ttl_clause = f"AND fv.{spec.timestamp_column} >= entity_df.event_timestamp - INTERVAL '{spec.ttl_seconds} seconds'"

            pit_sql = f"""
            WITH ranked AS (
                SELECT
                    entity_df.*,
                    {feature_cols},
                    ROW_NUMBER() OVER (
                        PARTITION BY {', '.join(f'entity_df.{c}' for c in spec.entity_columns)}, entity_df.event_timestamp
                        ORDER BY fv.{spec.timestamp_column} DESC
                    ) AS _pit_rank
                FROM entity_df
                LEFT JOIN '{self._data_dir}/{spec.feature_view_name}.parquet' AS fv
                    ON {entity_join}
                    AND fv.{spec.timestamp_column} <= entity_df.event_timestamp
                    {ttl_clause}
            )
            SELECT * EXCLUDE (_pit_rank) FROM ranked WHERE _pit_rank = 1
            """
            result = self._conn.execute(pit_sql).arrow()
            self._conn.register("entity_df", result)

        return result

    async def pull_latest_from_source(
        self,
        data_source: DataSourceConfig,
        feature_view_name: str,
        start_date: "datetime",
        end_date: "datetime",
    ) -> pa.Table:
        if data_source.source_type == "FILE":
            path = data_source.config["path"]
            sql = f"""
            SELECT * FROM '{path}'
            WHERE {data_source.timestamp_column} >= '{start_date.isoformat()}'
              AND {data_source.timestamp_column} < '{end_date.isoformat()}'
            """
            return self._conn.execute(sql).arrow()
        raise NotImplementedError(f"Source type {data_source.source_type} not supported in DuckDB store")

    async def write_to_offline_store(
        self,
        feature_view_name: str,
        table: pa.Table,
        mode: str = "append",
    ) -> int:
        path = f"{self._data_dir}/{feature_view_name}.parquet"
        if mode == "overwrite":
            self._conn.execute(f"COPY (SELECT * FROM table) TO '{path}' (FORMAT PARQUET)")
        else:
            # Append by reading existing + union + rewrite
            self._conn.register("new_data", table)
            existing_sql = f"SELECT * FROM '{path}'" if os.path.exists(path) else "SELECT * FROM new_data WHERE false"
            self._conn.execute(f"""
                COPY (SELECT * FROM ({existing_sql}) UNION ALL SELECT * FROM new_data)
                TO '{path}' (FORMAT PARQUET)
            """)
        return table.num_rows
```

**Testing**:
- `Fixture: sample_driver_features.parquet` — 1000 rows with columns: `driver_id (INT64)`, `event_timestamp (TIMESTAMP)`, `conv_rate (FLOAT64)`, `acc_rate (FLOAT64)`, `avg_daily_trips (INT32)`
- `Unit: test_pit_join_basic` — entity_df has 5 drivers at time T, feature data has entries at T-1h, T-2h, T-3h; verify only T-1h values returned
- `Unit: test_pit_join_with_ttl` — set ttl=3600s, feature data has entries at T-2h (outside TTL); verify NULLs returned for expired features
- `Unit: test_pit_join_no_future_leakage` — feature data has entries at T+1h; verify they are NOT included (prevents data leakage)
- `Unit: test_pit_join_multiple_feature_views` — join 2 feature views on same entity, verify all features present in result
- `Unit: test_pit_join_multiple_entities` — entity_df has 3 different entity columns; verify correct join on all
- `Integration: test_pull_latest_from_parquet` — write Parquet file, pull with date range, verify correct filtering
- `Integration: test_write_to_offline_store_append` — write 100 rows, write 50 more, verify 150 total rows
- `Integration: test_write_to_offline_store_overwrite` — write 100 rows, overwrite with 50, verify 50 total

#### 3.3 — SDK `get_historical_features()` Integration

**What**: Wire the SDK's `get_historical_features()` method through the REST API to the DuckDB offline store.

**Design**:

REST endpoint for historical feature retrieval:

```
POST /api/v1/projects/{project_id}/historical-features
```

Request body:
```json
{
  "features": ["driver_hourly_stats:conv_rate", "driver_hourly_stats:acc_rate"],
  "entity_df_json": [
    {"driver_id": 1001, "event_timestamp": "2026-05-20T10:00:00Z"},
    {"driver_id": 1002, "event_timestamp": "2026-05-20T10:00:00Z"}
  ],
  "full_feature_names": false
}
```

Response: Arrow IPC stream (binary, Content-Type: `application/vnd.apache.arrow.stream`).

The SDK converts pandas DataFrame to Arrow, sends via HTTP, and converts the response back to pandas.

**Testing**:
- `E2E: test_get_historical_features_sdk` — define features via SDK, write sample Parquet, call `get_historical_features()`, verify returned DataFrame has correct shape and values
- `E2E: test_get_historical_features_empty_result` — query for entity IDs not in the data, verify empty DataFrame with correct columns
- `E2E: test_get_historical_features_pit_correctness` — create data with known timestamps, query at specific times, verify correct point-in-time values

---

## Phase 4: Online Store & Feature Serving

### Purpose
Implement the online store (Redis) and feature serving endpoints (REST + gRPC). After this phase, users can materialise features from offline to online store and retrieve them at inference time with sub-millisecond latency — completing the dual-store architecture.

### Tasks

#### 4.1 — Online Store Interface & Redis Implementation

**What**: Define the abstract online store interface and implement the Redis-backed online store.

**Design**:

```python
# src/feature_store/online_store/base.py
from __future__ import annotations
from abc import ABC, abstractmethod

class OnlineStore(ABC):
    """Abstract interface for online feature stores."""

    @abstractmethod
    async def write_batch(
        self,
        project: str,
        feature_view: str,
        entity_keys: list[dict],
        feature_values: list[dict],
        timestamps: list["datetime"],
    ) -> int:
        """Write a batch of feature values to the online store.
        Returns number of records written."""
        ...

    @abstractmethod
    async def read_batch(
        self,
        project: str,
        feature_view: str,
        entity_keys: list[dict],
        feature_names: list[str],
    ) -> list[dict | None]:
        """Read feature values for a batch of entity keys.
        Returns list of dicts (one per entity key), None if not found."""
        ...

    @abstractmethod
    async def delete_entity(
        self,
        project: str,
        feature_view: str,
        entity_key: dict,
    ) -> None:
        """Delete all features for a specific entity key (GDPR erasure)."""
        ...

# src/feature_store/online_store/redis_store.py
import redis.asyncio as redis
import json
from feature_store.online_store.base import OnlineStore

class RedisOnlineStore(OnlineStore):
    """Redis-backed online feature store.

    Key format: fs:{project}:{feature_view}:{entity_key_hash}
    Value: JSON-serialised dict of feature values + metadata.
    """

    def __init__(self, redis_url: str = "redis://localhost:6379/0") -> None:
        self._redis = redis.from_url(redis_url)

    def _make_key(self, project: str, feature_view: str, entity_key: dict) -> str:
        # Deterministic key from sorted entity key dict
        key_str = "|".join(f"{k}={v}" for k, v in sorted(entity_key.items()))
        return f"fs:{project}:{feature_view}:{key_str}"

    async def write_batch(self, project, feature_view, entity_keys, feature_values, timestamps):
        pipe = self._redis.pipeline()
        for ek, fv, ts in zip(entity_keys, feature_values, timestamps):
            key = self._make_key(project, feature_view, ek)
            value = json.dumps({"features": fv, "event_timestamp": ts.isoformat()})
            pipe.set(key, value)
        results = await pipe.execute()
        return len(results)

    async def read_batch(self, project, feature_view, entity_keys, feature_names):
        pipe = self._redis.pipeline()
        for ek in entity_keys:
            key = self._make_key(project, feature_view, ek)
            pipe.get(key)
        results = await pipe.execute()
        output = []
        for raw in results:
            if raw is None:
                output.append(None)
            else:
                data = json.loads(raw)
                filtered = {k: data["features"].get(k) for k in feature_names}
                output.append(filtered)
        return output

    async def delete_entity(self, project, feature_view, entity_key):
        key = self._make_key(project, feature_view, entity_key)
        await self._redis.delete(key)
```

**Testing**:
- `Integration: test_write_and_read_single_entity` — write features for one entity, read back, verify values match
- `Integration: test_write_batch_100_entities` — write 100 entities in a batch, read all back, verify all present
- `Integration: test_read_nonexistent_entity` — read an entity that was never written, verify `None` returned
- `Integration: test_delete_entity` — write entity, delete it, verify read returns `None`
- `Integration: test_read_filters_feature_names` — write 5 features, request only 2, verify only 2 returned
- `Unit: test_make_key_deterministic` — verify same entity_key dict in different order produces same Redis key

#### 4.2 — Feature Serving REST Endpoint

**What**: Implement the `/get-online-features` REST endpoint for inference-time feature retrieval.

**Design**:

```python
# src/feature_store/serving/rest_server.py (serving routes)
from fastapi import APIRouter, HTTPException
from feature_store.registry.schemas import OnlineFeaturesRequest, OnlineFeaturesResponse

serving_router = APIRouter(prefix="/serving", tags=["serving"])

@serving_router.post(
    "/projects/{project_name}/get-online-features",
    response_model=OnlineFeaturesResponse,
)
async def get_online_features(
    project_name: str,
    request: OnlineFeaturesRequest,
) -> OnlineFeaturesResponse:
    """Retrieve online features for inference.

    Feature references use the format "feature_view:feature_name".
    Returns one result dict per entity row.
    """
    # 1. Parse feature references into {feature_view: [feature_names]}
    # 2. For each feature view, call online_store.read_batch()
    # 3. Merge results by entity row index
    # 4. Return merged results
    ...
```

Response format:
```json
{
  "metadata": {
    "feature_names": ["driver_hourly_stats:conv_rate", "driver_hourly_stats:acc_rate"]
  },
  "results": [
    {"driver_hourly_stats:conv_rate": 0.42, "driver_hourly_stats:acc_rate": 0.87},
    {"driver_hourly_stats:conv_rate": 0.38, "driver_hourly_stats:acc_rate": 0.91}
  ]
}
```

**Testing**:
- `Integration: test_get_online_features_single_entity` — write features to Redis, POST request with one entity row, verify correct values returned
- `Integration: test_get_online_features_batch` — write 10 entities, POST with all 10, verify all returned correctly
- `Integration: test_get_online_features_missing_entity` — request entity not in store, verify null values in response
- `Integration: test_get_online_features_invalid_feature_ref` — use "invalid_format" (no colon), verify 400 error
- `Integration: test_get_online_features_latency` — measure round-trip for 100 entity rows, assert < 50ms (excluding network)

#### 4.3 — gRPC Feature Serving

**What**: Implement gRPC serving endpoint compatible with the Feast ServingService protocol.

**Design**:

```protobuf
// protos/feature_store.proto
syntax = "proto3";
package feature_store.serving;

service OnlineServingService {
  rpc GetOnlineFeatures(GetOnlineFeaturesRequest) returns (GetOnlineFeaturesResponse);
}

message GetOnlineFeaturesRequest {
  string project = 1;
  oneof features_spec {
    FeatureList features = 2;
    string feature_service = 3;
  }
  repeated EntityRow entity_rows = 4;
}

message FeatureList {
  repeated string val = 1;
}

message EntityRow {
  map<string, Value> fields = 1;
}

message Value {
  oneof val {
    int32 int32_val = 1;
    int64 int64_val = 2;
    float float_val = 3;
    double double_val = 4;
    string string_val = 5;
    bytes bytes_val = 6;
    bool bool_val = 7;
  }
}

message GetOnlineFeaturesResponse {
  FieldValues metadata = 1;
  repeated FieldValues results = 2;
}

message FieldValues {
  map<string, Value> fields = 1;
}
```

**Testing**:
- `Integration: test_grpc_get_online_features` — connect gRPC client, send request, verify correct response
- `Integration: test_grpc_get_online_features_batch` — send 100 entity rows, verify all returned
- `Integration: test_grpc_invalid_project` — request with nonexistent project, verify appropriate gRPC status code

---

## Phase 5: Materialisation Engine

### Purpose
Implement the materialisation pipeline that syncs features from the offline store (Parquet) to the online store (Redis). After this phase, the `materialize` CLI command and SDK method work end-to-end, and the system tracks materialisation state.

### Tasks

#### 5.1 — Materialisation Job Runner

**What**: Implement the materialisation engine that reads from offline store and writes to online store.

**Design**:

```python
# src/feature_store/materialisation/engine.py
from __future__ import annotations
import uuid
from datetime import datetime
from feature_store.offline_store.base import OfflineStore
from feature_store.online_store.base import OnlineStore
from feature_store.registry.repository import RegistryRepository

class MaterialisationEngine:
    def __init__(
        self,
        registry: RegistryRepository,
        offline_store: OfflineStore,
        online_store: OnlineStore,
    ) -> None:
        self._registry = registry
        self._offline = offline_store
        self._online = online_store

    async def materialise_feature_view(
        self,
        project_id: uuid.UUID,
        feature_view_name: str,
        start_date: datetime,
        end_date: datetime,
    ) -> MaterialisationResult:
        """Materialise a single feature view from offline to online store.

        Steps:
        1. Look up feature view and its data source from registry.
        2. Pull data from offline store for the [start_date, end_date) window.
        3. For each entity key, extract the latest feature values.
        4. Write to online store in batches.
        5. Update materialisation state in registry.
        6. Return result with record counts.
        """
        ...

    async def materialise_all(
        self,
        project_id: uuid.UUID,
        start_date: datetime,
        end_date: datetime,
    ) -> list[MaterialisationResult]:
        """Materialise all online-enabled feature views in a project."""
        ...

class MaterialisationResult:
    def __init__(
        self,
        feature_view_name: str,
        status: str,  # SUCCEEDED, FAILED
        records_written: int,
        duration_seconds: float,
        error_message: str | None = None,
    ) -> None:
        self.feature_view_name = feature_view_name
        self.status = status
        self.records_written = records_written
        self.duration_seconds = duration_seconds
        self.error_message = error_message
```

**Testing**:
- `Integration: test_materialise_basic` — create Parquet with 100 entities, materialise, verify all 100 readable from Redis
- `Integration: test_materialise_latest_only` — Parquet has 3 timestamps per entity, verify only latest value in Redis
- `Integration: test_materialise_window` — Parquet has data from Jan-May, materialise Apr-May only, verify only Apr-May values in online store
- `Integration: test_materialise_records_job` — run materialisation, verify `materialisation_jobs` table has a SUCCEEDED row with correct record count
- `Integration: test_materialise_all` — project has 3 online-enabled views, materialise all, verify all 3 have data in Redis
- `Integration: test_materialise_disabled_view_skipped` — feature view with `online_enabled=false`, verify it is not materialised

#### 5.2 — CLI `materialize` and `serve` Commands

**What**: Add `materialize` and `serve` commands to the CLI.

**Design**:

```python
@main.command()
@click.option("--start", required=True, type=click.DateTime(), help="Materialisation window start")
@click.option("--end", required=True, type=click.DateTime(), help="Materialisation window end")
@click.option("--views", multiple=True, help="Specific feature views to materialise (default: all)")
@click.pass_context
def materialize(ctx, start, end, views):
    """Materialise features from offline to online store."""
    ...

@main.command()
@click.option("--host", default="0.0.0.0")
@click.option("--port", default=8666, type=int)
@click.option("--grpc-port", default=50051, type=int)
@click.pass_context
def serve(ctx, host, port, grpc_port):
    """Start the feature serving server (REST + gRPC)."""
    ...
```

**Testing**:
- `E2E: test_cli_materialize` — run `feature-store materialize --start 2026-01-01 --end 2026-05-01`, verify data appears in Redis
- `E2E: test_cli_materialize_specific_views` — run with `--views driver_stats`, verify only that view is materialised
- `E2E: test_cli_serve_starts_server` — run `feature-store serve`, verify HTTP health endpoint responds

#### 5.3 — Celery-Based Async Materialisation

**What**: Wrap materialisation in Celery tasks for async execution and scheduling.

**Design**:

```python
# src/feature_store/materialisation/scheduler.py
from celery import Celery

celery_app = Celery("feature_store", broker="redis://localhost:6379/1")

@celery_app.task(bind=True, max_retries=3)
def materialise_feature_view_task(
    self,
    project_id: str,
    feature_view_name: str,
    start_date: str,  # ISO format
    end_date: str,
) -> dict:
    """Celery task for async materialisation."""
    ...

@celery_app.task
def materialise_all_task(project_id: str, start_date: str, end_date: str) -> list[dict]:
    """Materialise all views in a project."""
    ...

# Periodic task for scheduled materialisation
celery_app.conf.beat_schedule = {
    "materialise-hourly": {
        "task": "feature_store.materialisation.scheduler.materialise_all_task",
        "schedule": 3600.0,
        "args": [],  # configured at runtime
    },
}
```

**Testing**:
- `Integration: test_celery_task_execution` — submit materialise task, verify it completes and job is recorded
- `Integration: test_celery_task_retry_on_failure` — simulate offline store failure, verify task retries up to 3 times

---

## Phase 6: Monitoring & Prometheus Metrics

### Purpose
Implement feature freshness monitoring, serving latency tracking, and a Prometheus metrics endpoint. After this phase, operators can monitor feature store health via Grafana or any Prometheus-compatible tool.

### Tasks

#### 6.1 — Prometheus Metrics Instrumentation

**What**: Instrument the feature store with Prometheus metrics for serving, materialisation, and registry operations.

**Design**:

```python
# src/feature_store/monitoring/metrics.py
from prometheus_client import Counter, Histogram, Gauge, Info

# Serving metrics
SERVING_REQUEST_COUNT = Counter(
    "fs_serving_requests_total",
    "Total number of online feature serving requests",
    ["project", "method", "status"],
)
SERVING_LATENCY = Histogram(
    "fs_serving_latency_seconds",
    "Online feature serving latency",
    ["project"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)
SERVING_FEATURES_RETURNED = Counter(
    "fs_serving_features_returned_total",
    "Total number of feature values returned",
    ["project", "feature_view"],
)

# Materialisation metrics
MATERIALISATION_DURATION = Histogram(
    "fs_materialisation_duration_seconds",
    "Materialisation job duration",
    ["project", "feature_view", "status"],
)
MATERIALISATION_RECORDS = Counter(
    "fs_materialisation_records_total",
    "Total records materialised to online store",
    ["project", "feature_view"],
)

# Feature freshness
FEATURE_FRESHNESS_SECONDS = Gauge(
    "fs_feature_freshness_seconds",
    "Seconds since the last materialisation for a feature view",
    ["project", "feature_view"],
)

# Registry metrics
REGISTRY_OBJECTS_COUNT = Gauge(
    "fs_registry_objects_total",
    "Number of objects in the registry",
    ["project", "object_type"],
)

# System info
SYSTEM_INFO = Info("fs_system", "Feature Store system information")
```

FastAPI middleware for automatic request instrumentation:

```python
# Integrated into main.py
from prometheus_client import make_asgi_app
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)
```

**Testing**:
- `Unit: test_serving_counter_increments` — call the serving endpoint, verify `SERVING_REQUEST_COUNT` increased by 1
- `Unit: test_serving_latency_observed` — call serving endpoint, verify `SERVING_LATENCY` has an observation
- `Integration: test_metrics_endpoint` — GET `/metrics`, verify Prometheus text exposition format with expected metric names
- `Integration: test_feature_freshness_gauge` — materialise a view, verify `FEATURE_FRESHNESS_SECONDS` is set to a recent value

#### 6.2 — Feature Statistics Computation

**What**: Compute and store statistical summaries of feature values for drift detection baselines.

**Design**:

```python
# src/feature_store/monitoring/statistics.py
import pyarrow as pa
import numpy as np

class FeatureStatisticsComputer:
    """Computes statistics over feature columns in an Arrow table."""

    def compute(self, table: pa.Table, feature_names: list[str]) -> dict:
        """Compute statistics for each feature column.

        Returns:
            {
                "feature_name": {
                    "row_count": int,
                    "null_count": int,
                    "null_fraction": float,
                    "mean": float | None,
                    "stddev": float | None,
                    "min": str,
                    "max": str,
                    "p50": float | None,
                    "p95": float | None,
                    "p99": float | None,
                    "unique_count": int,
                }
            }
        """
        stats = {}
        for col_name in feature_names:
            col = table.column(col_name)
            arr = col.to_numpy(zero_copy_only=False)
            row_count = len(arr)
            null_count = int(col.null_count)
            stats[col_name] = {
                "row_count": row_count,
                "null_count": null_count,
                "null_fraction": null_count / row_count if row_count > 0 else 0.0,
                "mean": float(np.nanmean(arr)) if np.issubdtype(arr.dtype, np.number) else None,
                "stddev": float(np.nanstd(arr)) if np.issubdtype(arr.dtype, np.number) else None,
                "min": str(np.nanmin(arr)) if len(arr) > 0 else None,
                "max": str(np.nanmax(arr)) if len(arr) > 0 else None,
                "p50": float(np.nanpercentile(arr, 50)) if np.issubdtype(arr.dtype, np.number) else None,
                "p95": float(np.nanpercentile(arr, 95)) if np.issubdtype(arr.dtype, np.number) else None,
                "p99": float(np.nanpercentile(arr, 99)) if np.issubdtype(arr.dtype, np.number) else None,
                "unique_count": int(col.dictionary_encode().dictionary.length) if row_count > 0 else 0,
            }
        return stats
```

**Testing**:
- `Unit: test_compute_numeric_stats` — pass Arrow table with known values [1,2,3,4,5], verify mean=3.0, stddev=1.414, p50=3.0
- `Unit: test_compute_with_nulls` — pass table with 20% nulls, verify null_fraction=0.2
- `Unit: test_compute_string_stats` — pass string column, verify mean/stddev/percentiles are None, unique_count is correct
- `Unit: test_compute_empty_table` — pass empty table, verify row_count=0 and no errors

#### 6.3 — Drift Detection

**What**: Implement training-serving distribution drift detection with configurable thresholds.

**Design**:

```python
# src/feature_store/monitoring/drift.py
import numpy as np

class DriftDetector:
    """Detects distribution drift between baseline and current feature statistics."""

    def compute_psi(self, baseline: np.ndarray, current: np.ndarray, bins: int = 10) -> float:
        """Compute Population Stability Index (PSI).

        PSI < 0.1: no significant drift
        0.1 <= PSI < 0.25: moderate drift (warning)
        PSI >= 0.25: significant drift (critical)
        """
        ...

    def compute_ks_statistic(self, baseline: np.ndarray, current: np.ndarray) -> float:
        """Compute Kolmogorov-Smirnov statistic."""
        ...

    def detect_drift(
        self,
        baseline_stats: dict,
        current_stats: dict,
        psi_warning_threshold: float = 0.1,
        psi_critical_threshold: float = 0.25,
        null_rate_change_threshold: float = 0.05,
    ) -> list[DriftAlert]:
        """Compare baseline and current stats, return alerts for drifted features."""
        ...

class DriftAlert:
    def __init__(
        self,
        feature_name: str,
        alert_type: str,  # distribution_drift, null_rate_spike
        severity: str,    # info, warning, critical
        threshold: float,
        observed_value: float,
        message: str,
    ) -> None: ...
```

**Testing**:
- `Unit: test_psi_identical_distributions` — same data, verify PSI < 0.01
- `Unit: test_psi_moderate_drift` — shift mean by 1 stddev, verify 0.1 <= PSI < 0.25
- `Unit: test_psi_significant_drift` — completely different distribution, verify PSI >= 0.25
- `Unit: test_null_rate_spike_detection` — baseline 1% nulls, current 10% nulls, verify alert generated
- `Unit: test_no_alerts_for_stable_features` — identical stats, verify empty alert list

---

## Phase 7: Auth, RBAC & API Keys

### Purpose
Implement authentication (JWT + API keys) and role-based access control at the project and feature group level. After this phase, the feature store is production-ready for multi-team environments with access isolation.

### Tasks

#### 7.1 — JWT Authentication Middleware

**What**: Implement FastAPI middleware for JWT bearer token authentication using OAuth 2.0 / OIDC (RFC 6749).

**Design**:

```python
# src/feature_store/auth/middleware.py
from fastapi import Request, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError

class AuthMiddleware:
    """FastAPI dependency for JWT + API key authentication."""

    def __init__(self, jwt_secret: str, api_key_repo: "ApiKeyRepository") -> None:
        self._jwt_secret = jwt_secret
        self._api_key_repo = api_key_repo
        self._bearer = HTTPBearer(auto_error=False)

    async def __call__(self, request: Request) -> AuthContext:
        # 1. Check for Bearer token (JWT)
        # 2. If no Bearer, check for X-API-Key header
        # 3. If neither, return anonymous context (for unauthenticated endpoints)
        ...

class AuthContext:
    user_id: uuid.UUID | None
    email: str | None
    roles: dict[uuid.UUID, list[str]]  # project_id -> roles
    is_authenticated: bool
    auth_method: str  # jwt, api_key, anonymous
```

**Testing**:
- `Unit: test_valid_jwt_decoded` — create JWT, pass to middleware, verify AuthContext has correct user_id
- `Unit: test_expired_jwt_rejected` — create expired JWT, verify 401
- `Unit: test_invalid_signature_rejected` — create JWT with wrong secret, verify 401
- `Unit: test_api_key_authentication` — pass valid API key hash, verify AuthContext populated
- `Unit: test_missing_auth_anonymous` — no auth headers, verify anonymous context

#### 7.2 — RBAC Enforcement

**What**: Implement role-based permission checking at the project and resource level.

**Design**:

```python
# src/feature_store/auth/rbac.py
from enum import Enum

class Permission(str, Enum):
    PROJECT_READ = "project:read"
    PROJECT_WRITE = "project:write"
    FEATURE_VIEW_CREATE = "feature_view:create"
    FEATURE_VIEW_READ = "feature_view:read"
    FEATURE_VIEW_UPDATE = "feature_view:update"
    FEATURE_VIEW_DELETE = "feature_view:delete"
    FEATURE_VIEW_MATERIALIZE = "feature_view:materialize"
    DATA_SOURCE_CREATE = "data_source:create"
    DATA_SOURCE_READ = "data_source:read"
    FEATURE_SERVICE_SERVE = "feature_service:serve"

DEFAULT_ROLE_PERMISSIONS: dict[str, list[Permission]] = {
    "admin": list(Permission),  # all permissions
    "editor": [
        Permission.PROJECT_READ,
        Permission.FEATURE_VIEW_CREATE, Permission.FEATURE_VIEW_READ,
        Permission.FEATURE_VIEW_UPDATE, Permission.FEATURE_VIEW_MATERIALIZE,
        Permission.DATA_SOURCE_CREATE, Permission.DATA_SOURCE_READ,
        Permission.FEATURE_SERVICE_SERVE,
    ],
    "viewer": [
        Permission.PROJECT_READ, Permission.FEATURE_VIEW_READ,
        Permission.DATA_SOURCE_READ,
    ],
}

class RBACEnforcer:
    async def check_permission(
        self,
        auth_context: AuthContext,
        project_id: uuid.UUID,
        permission: Permission,
    ) -> bool:
        """Check if the authenticated user has the required permission."""
        ...

    async def require_permission(
        self,
        auth_context: AuthContext,
        project_id: uuid.UUID,
        permission: Permission,
    ) -> None:
        """Raise 403 if the user lacks the required permission."""
        ...
```

**Testing**:
- `Unit: test_admin_has_all_permissions` — admin role, check every permission, verify all pass
- `Unit: test_viewer_cannot_write` — viewer role, check FEATURE_VIEW_CREATE, verify denied
- `Unit: test_editor_can_materialize` — editor role, check FEATURE_VIEW_MATERIALIZE, verify allowed
- `Unit: test_unauthenticated_denied` — anonymous context, verify all permissions denied
- `Integration: test_api_endpoint_enforces_rbac` — create viewer user, try POST to create feature view, verify 403

#### 7.3 — API Key Management

**What**: Implement API key generation, hashing, and CRUD endpoints.

**Design**:

API endpoints:
| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/api-keys` | Generate new API key |
| GET | `/api/v1/api-keys` | List user's API keys (metadata only, no secrets) |
| DELETE | `/api/v1/api-keys/{key_id}` | Revoke API key |

Key generation: Generate 32-byte random token, hash with argon2, store hash. Return plaintext key only once at creation time.

**Testing**:
- `Integration: test_create_api_key` — POST to create key, verify plaintext returned once, hash stored in DB
- `Integration: test_authenticate_with_api_key` — create key, use it in X-API-Key header, verify authenticated
- `Integration: test_revoke_api_key` — revoke key, verify subsequent auth attempts fail
- `Integration: test_expired_api_key` — create key with past expiry, verify auth fails

---

## Phase 8: Lineage Tracking & Feature Discovery

### Purpose
Implement lineage tracking (data source to feature view to feature service DAG) and feature discovery endpoints. After this phase, users can trace the provenance of any feature and discover reusable features across the registry.

### Tasks

#### 8.1 — Lineage Edge Management

**What**: Automatically track lineage edges when registry objects are created, and expose lineage query endpoints.

**Design**:

```python
# src/feature_store/lineage/tracker.py
from feature_store.registry.models import LineageEdge

class LineageTracker:
    """Manages lineage edges in the registry."""

    async def record_feature_view_lineage(
        self,
        feature_view_id: uuid.UUID,
        data_source_id: uuid.UUID,
        entity_ids: list[uuid.UUID],
    ) -> None:
        """Record lineage: data_source -> feature_view, entities -> feature_view."""
        ...

    async def record_feature_service_lineage(
        self,
        feature_service_id: uuid.UUID,
        feature_view_ids: list[uuid.UUID],
    ) -> None:
        """Record lineage: feature_views -> feature_service."""
        ...

    async def get_upstream(
        self,
        target_type: str,
        target_id: uuid.UUID,
        max_depth: int = 10,
    ) -> list[LineageNode]:
        """Traverse upstream lineage using recursive CTE."""
        # Uses recursive CTE on lineage_edges table
        ...

    async def get_downstream(
        self,
        source_type: str,
        source_id: uuid.UUID,
        max_depth: int = 10,
    ) -> list[LineageNode]:
        """Traverse downstream lineage (impact analysis)."""
        ...

class LineageNode:
    node_type: str
    node_id: uuid.UUID
    name: str
    depth: int
```

API endpoints:
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/projects/{pid}/lineage/{type}/{id}/upstream` | Get upstream lineage |
| GET | `/api/v1/projects/{pid}/lineage/{type}/{id}/downstream` | Get downstream lineage |
| GET | `/api/v1/projects/{pid}/lineage/{type}/{id}/graph` | Get full lineage DAG |

**Testing**:
- `Integration: test_feature_view_lineage_auto_created` — create data source, then feature view referencing it, verify lineage edge exists
- `Integration: test_upstream_traversal` — create chain data_source -> feature_view -> feature_service, query upstream from feature_service, verify both data_source and feature_view returned
- `Integration: test_downstream_impact` — query downstream from data_source, verify feature_view and feature_service found
- `Integration: test_max_depth_limit` — create deep chain, query with max_depth=2, verify depth is respected

#### 8.2 — Feature Discovery & Search

**What**: Implement full-text and metadata-based feature search across the registry.

**Design**:

```python
# Search API endpoint
# GET /api/v1/projects/{pid}/search/features?q=driver&tags=production&value_type=FLOAT64

class FeatureSearchResult:
    feature_view_name: str
    feature_name: str
    value_type: str
    description: str | None
    tags: list[str]
    owner: str | None
    relevance_score: float
```

Search implementation uses PostgreSQL JSONB operators and optional trigram similarity:
- `q` parameter: searches feature names and descriptions using `ILIKE` and trigram similarity
- `tags` parameter: filters using JSONB containment (`metadata->'tags' ? 'production'`)
- `value_type` parameter: filters features JSONB array by value_type field

**Testing**:
- `Integration: test_search_by_name` — create features "conv_rate", "acc_rate", "avg_trips", search for "rate", verify 2 results
- `Integration: test_search_by_tag` — create tagged and untagged features, search by tag, verify only tagged returned
- `Integration: test_search_by_value_type` — create INT and FLOAT features, filter by FLOAT64, verify correct filtering
- `Integration: test_search_empty_query` — search with no filters, verify all features returned
- `Integration: test_search_cross_feature_views` — features in different views, verify search spans all views

---

## Phase 9: Streaming Ingestion

### Purpose
Implement real-time feature ingestion from Kafka topics into the online store. After this phase, features can be updated in near-real-time from streaming data sources, enabling sub-second feature freshness.

### Tasks

#### 9.1 — Kafka Consumer for Feature Ingestion

**What**: Implement a Kafka consumer that reads messages and writes feature values directly to the online store.

**Design**:

```python
# src/feature_store/streaming/kafka_source.py
from confluent_kafka import Consumer, KafkaError
from feature_store.streaming.base import StreamSource

class KafkaStreamSource(StreamSource):
    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        topics: list[str],
        online_store: OnlineStore,
        config: dict | None = None,
    ) -> None:
        self._consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "latest",
            **(config or {}),
        })
        self._online_store = online_store
        self._topics = topics

    async def start(self) -> None:
        """Subscribe and start consuming messages."""
        self._consumer.subscribe(self._topics)
        ...

    async def process_message(self, message: bytes, topic: str) -> None:
        """Parse message, extract entity key + features, write to online store."""
        # 1. Deserialise message (JSON or Avro)
        # 2. Extract entity key columns
        # 3. Extract feature value columns
        # 4. Write to online store
        ...

    async def stop(self) -> None:
        """Gracefully stop the consumer."""
        self._consumer.close()
```

```python
# src/feature_store/streaming/base.py
from abc import ABC, abstractmethod

class StreamSource(ABC):
    @abstractmethod
    async def start(self) -> None: ...
    @abstractmethod
    async def process_message(self, message: bytes, topic: str) -> None: ...
    @abstractmethod
    async def stop(self) -> None: ...
```

**Testing**:
- `Integration (mocked Kafka): test_consume_and_write` — produce 10 messages to mock Kafka, verify all 10 written to online store
- `Integration (mocked Kafka): test_message_parsing_json` — send JSON message with entity key + features, verify correct parsing
- `Integration (mocked Kafka): test_invalid_message_skipped` — send malformed message, verify it is logged and skipped without crashing
- `Integration (mocked Kafka): test_consumer_graceful_shutdown` — start consumer, call stop(), verify clean shutdown

#### 9.2 — Stream Feature View Support

**What**: Wire stream feature views to the Kafka consumer, with aggregation window support.

**Design**:

Stream feature views are registered in the registry with `view_type='stream'` and `stream_config` containing:
```json
{
  "aggregation_window_seconds": 3600,
  "aggregation_slide_interval_seconds": 300,
  "aggregation_functions": {
    "total_trips": "COUNT",
    "avg_fare": "AVG",
    "max_surge": "MAX"
  }
}
```

The consumer processes messages and updates aggregation windows in Redis using sorted sets for time-windowed aggregation.

**Testing**:
- `Integration: test_stream_feature_view_registration` — register a stream FV, verify `view_type='stream'` and `stream_config` populated
- `Integration: test_aggregation_count` — send 5 messages in a window, verify COUNT=5 in online store
- `Integration: test_aggregation_window_expiry` — send messages, advance time past window, verify old window data expires

---

## Phase 10: Feature Discovery UI

### Purpose
Build a React-based web UI for browsing the feature registry, viewing lineage DAGs, and monitoring feature health. After this phase, non-SDK users (data scientists, managers) can discover and understand features visually.

### Tasks

#### 10.1 — Frontend Scaffolding & API Client

**What**: Set up the React + Vite frontend with TanStack Query for API communication.

**Design**:

```typescript
// frontend/src/api/client.ts
import axios from "axios";

const api = axios.create({
  baseURL: "/api/v1",
});

export interface Project {
  project_id: string;
  name: string;
  description: string | null;
  owner_email: string | null;
  created_at: string;
}

export interface FeatureView {
  feature_view_id: string;
  project_id: string;
  name: string;
  view_type: "batch" | "stream" | "on_demand";
  features: FeatureDefinition[];
  metadata: Record<string, unknown>;
  created_at: string;
}

export interface FeatureDefinition {
  name: string;
  value_type: string;
  description: string | null;
  tags: string[];
}

// API functions
export const listProjects = () => api.get<Project[]>("/projects");
export const listFeatureViews = (projectId: string) =>
  api.get<FeatureView[]>(`/projects/${projectId}/feature-views`);
export const searchFeatures = (projectId: string, query: string) =>
  api.get(`/projects/${projectId}/search/features`, { params: { q: query } });
export const getLineageGraph = (projectId: string, type: string, id: string) =>
  api.get(`/projects/${projectId}/lineage/${type}/${id}/graph`);
```

**Testing**:
- `Unit: test_api_client_builds` — `npm run build` succeeds without TypeScript errors
- `E2E (Playwright): test_projects_page_loads` — navigate to `/`, verify projects list renders

#### 10.2 — Registry Browser Components

**What**: Build components for browsing projects, feature views, entities, and feature details.

**Design**:

Pages:
- `/projects` — list of projects with search
- `/projects/:id` — project dashboard: feature views, entities, data sources counts
- `/projects/:id/feature-views` — feature view list with type badges and feature counts
- `/projects/:id/feature-views/:fvId` — feature view detail: features table, metadata, lineage link

**Testing**:
- `E2E (Playwright): test_project_list_displays` — seed 3 projects, verify all 3 appear in list
- `E2E (Playwright): test_feature_view_detail` — navigate to feature view, verify features table shows correct value types
- `E2E (Playwright): test_feature_search` — type "conv" in search box, verify matching features appear

#### 10.3 — Lineage DAG Visualization

**What**: Render interactive lineage DAG using D3.js dagre-d3 layout.

**Design**:

```typescript
// frontend/src/components/lineage/LineageGraph.tsx
interface LineageGraphProps {
  nodes: LineageNode[];
  edges: LineageEdge[];
  highlightNodeId?: string;
}

interface LineageNode {
  id: string;
  type: "data_source" | "entity" | "feature_view" | "feature_service";
  name: string;
  metadata: Record<string, unknown>;
}

interface LineageEdge {
  source: string;
  target: string;
  relationship: string;
}
```

The component renders a left-to-right DAG with colour-coded node types and click-to-inspect interaction.

**Testing**:
- `E2E (Playwright): test_lineage_graph_renders` — navigate to lineage page, verify SVG element appears with nodes
- `E2E (Playwright): test_lineage_node_click` — click a node, verify detail panel opens with metadata

---

## Phase 11: AI-Augmented Features

### Purpose
Implement the AI-native differentiators: feature recommendation from model objectives, natural-language feature authoring, and intelligent drift alerting. These capabilities set this feature store apart from all incumbents.

### Tasks

#### 11.1 — AI-Powered Feature Recommendation

**What**: Given a model training objective, recommend existing features from the registry that are likely relevant.

**Design**:

```python
# src/feature_store/ai/feature_recommender.py
from anthropic import Anthropic

class FeatureRecommender:
    """Recommends existing features based on a model training objective."""

    SYSTEM_PROMPT = """You are a feature engineering expert for machine learning.
    Given a model training objective and a list of available features in a feature store,
    recommend the most relevant features for training this model.

    For each recommended feature, explain WHY it is relevant to the training objective.
    Score relevance from 0.0 to 1.0.

    Return JSON: [{"feature_view": "...", "feature_name": "...", "relevance": 0.95, "reason": "..."}]"""

    def __init__(self, llm_client: Anthropic, registry: RegistryRepository) -> None:
        self._llm = llm_client
        self._registry = registry

    async def recommend(
        self,
        project_id: uuid.UUID,
        objective: str,  # e.g. "Predict driver ETA for ride-hailing"
        max_results: int = 20,
    ) -> list[FeatureRecommendation]:
        # 1. Fetch all features from registry
        # 2. Build context with feature names, types, descriptions
        # 3. Send to LLM with objective
        # 4. Parse and return recommendations
        ...

class FeatureRecommendation:
    feature_view: str
    feature_name: str
    relevance: float
    reason: str
```

API endpoint: `POST /api/v1/projects/{pid}/ai/recommend-features`

**Testing**:
- `Integration (mocked LLM): test_recommend_returns_features` — mock LLM response with 5 recommendations, verify parsed correctly
- `Integration (mocked LLM): test_recommend_empty_registry` — empty registry, verify graceful response with no recommendations
- `Unit: test_system_prompt_includes_features` — verify the LLM call includes feature catalogue in context

#### 11.2 — Natural-Language Feature Authoring

**What**: Accept a plain-English feature description and generate the SQL or Python transformation code.

**Design**:

```python
# src/feature_store/ai/nl_authoring.py
class NLFeatureAuthor:
    """Generates feature transformation code from natural language descriptions."""

    SYSTEM_PROMPT = """You are a feature engineering code generator.
    Given a natural language description of a feature and the available data sources/schemas,
    generate the transformation code (Python or SQL) to compute this feature.

    Output valid, executable code that can be used in a feature view definition.
    Include type annotations and docstrings."""

    async def generate_transformation(
        self,
        description: str,  # e.g. "Average number of trips per driver in the last 7 days"
        data_sources: list[DataSourceSchema],
        output_format: str = "python",  # python, sql
    ) -> GeneratedTransformation:
        ...

class GeneratedTransformation:
    code: str
    feature_name: str
    value_type: str
    explanation: str
    confidence: float
```

API endpoint: `POST /api/v1/projects/{pid}/ai/generate-feature`

**Testing**:
- `Integration (mocked LLM): test_generate_python_transformation` — describe "average trips last 7 days", verify Python code returned
- `Integration (mocked LLM): test_generate_sql_transformation` — request SQL output, verify SQL returned
- `Unit: test_data_source_schema_context` — verify data source schemas are included in LLM context

#### 11.3 — Intelligent Drift Alerting

**What**: Use LLM to distinguish meaningful drift from benign seasonal variation, reducing alert fatigue.

**Design**:

```python
# src/feature_store/ai/drift_analyzer.py
class IntelligentDriftAnalyzer:
    """Uses LLM to contextualize drift alerts and reduce false positives."""

    async def analyze_drift(
        self,
        feature_name: str,
        baseline_stats: dict,
        current_stats: dict,
        drift_score: float,
        historical_context: list[dict],  # past N stat computations
    ) -> DriftAnalysis:
        # 1. Provide LLM with feature name, stats, and historical pattern
        # 2. Ask whether this is meaningful drift or benign variation
        # 3. Return analysis with actionable recommendation
        ...

class DriftAnalysis:
    is_meaningful: bool
    confidence: float
    explanation: str
    recommendation: str  # "investigate", "ignore", "retrain"
```

**Testing**:
- `Integration (mocked LLM): test_seasonal_drift_classified_benign` — provide cyclical historical pattern, verify classified as not meaningful
- `Integration (mocked LLM): test_sudden_drift_classified_meaningful` — provide sudden distribution change, verify classified as meaningful

---

## Phase 12: Production Hardening & Documentation

### Purpose
Finalize the feature store for production use: comprehensive error handling, OpenAPI documentation, deployment guides, and performance optimization. After this phase, the feature store is ready for production workloads.

### Tasks

#### 12.1 — Error Handling & Validation

**What**: Implement comprehensive error handling across all API endpoints and SDK operations.

**Design**:

```python
# src/feature_store/errors.py
class FeatureStoreError(Exception):
    """Base exception for all feature store errors."""
    def __init__(self, message: str, status_code: int = 500) -> None:
        self.message = message
        self.status_code = status_code

class ProjectNotFoundError(FeatureStoreError):
    def __init__(self, project_id: str) -> None:
        super().__init__(f"Project {project_id} not found", status_code=404)

class FeatureViewNotFoundError(FeatureStoreError):
    def __init__(self, name: str) -> None:
        super().__init__(f"Feature view '{name}' not found", status_code=404)

class DuplicateResourceError(FeatureStoreError):
    def __init__(self, resource_type: str, name: str) -> None:
        super().__init__(f"{resource_type} '{name}' already exists", status_code=409)

class MaterialisationError(FeatureStoreError):
    def __init__(self, feature_view: str, message: str) -> None:
        super().__init__(f"Materialisation failed for '{feature_view}': {message}", status_code=500)

class AuthenticationError(FeatureStoreError):
    def __init__(self, message: str = "Authentication required") -> None:
        super().__init__(message, status_code=401)

class AuthorizationError(FeatureStoreError):
    def __init__(self, permission: str) -> None:
        super().__init__(f"Missing permission: {permission}", status_code=403)
```

FastAPI exception handler:
```python
@app.exception_handler(FeatureStoreError)
async def feature_store_error_handler(request: Request, exc: FeatureStoreError):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.message, "type": type(exc).__name__},
    )
```

**Testing**:
- `Integration: test_404_returns_json` — GET nonexistent resource, verify JSON error body with type field
- `Integration: test_409_on_duplicate` — create same project twice, verify 409 with "already exists"
- `Integration: test_422_on_validation` — POST with invalid body, verify 422 with field details
- `Unit: test_all_errors_have_status_codes` — verify every FeatureStoreError subclass sets a non-500 status code

#### 12.2 — OpenAPI Documentation & Health Endpoints

**What**: Ensure the auto-generated OpenAPI spec is complete, add health and readiness endpoints.

**Design**:

```python
@app.get("/health")
async def health():
    return {"status": "ok", "version": "0.1.0"}

@app.get("/ready")
async def readiness():
    """Check database and Redis connectivity."""
    db_ok = await check_db_connection()
    redis_ok = await check_redis_connection()
    status = "ok" if db_ok and redis_ok else "degraded"
    return {
        "status": status,
        "checks": {
            "database": "ok" if db_ok else "error",
            "redis": "ok" if redis_ok else "error",
        },
    }
```

**Testing**:
- `Integration: test_health_endpoint` — GET `/health`, verify 200 with version
- `Integration: test_readiness_all_healthy` — verify "ok" when DB and Redis are up
- `Integration: test_readiness_degraded` — stop Redis, verify "degraded" status
- `Integration: test_openapi_spec_valid` — fetch `/openapi.json`, validate it parses correctly and contains all expected paths

#### 12.3 — GDPR Erasure Workflow

**What**: Implement entity erasure across offline and online stores for GDPR right-to-erasure compliance.

**Design**:

```python
# Erasure API
# POST /api/v1/projects/{pid}/erasure-requests
# Body: {"entity_name": "user_id", "entity_value": "12345"}

class ErasureService:
    async def request_erasure(
        self,
        project_id: uuid.UUID,
        entity_name: str,
        entity_value: str,
        requested_by: uuid.UUID,
    ) -> uuid.UUID:
        """Create an erasure request and begin async processing."""
        # 1. Record request in audit log
        # 2. Find all feature views referencing this entity
        # 3. Delete from online store (Redis)
        # 4. Mark rows in offline store (Parquet) for redaction
        # 5. Update request status to COMPLETED
        ...
```

**Testing**:
- `Integration: test_erasure_removes_from_online` — write entity features, request erasure, verify Redis key deleted
- `Integration: test_erasure_records_audit_log` — request erasure, verify audit_log entry created
- `Integration: test_erasure_request_status_tracking` — create request, verify status progresses from PENDING to COMPLETED

#### 12.4 — Performance Optimization

**What**: Optimize critical paths: connection pooling, batch Redis operations, query optimization.

**Design**:
- Connection pooling: SQLAlchemy async pool with `pool_size=10`, `max_overflow=20`
- Redis pipeline batching: group online store reads into Redis pipelines (already implemented in Phase 4)
- Query optimization: add GIN indexes on JSONB columns (from data model), verify query plans use indexes
- Response caching: cache registry metadata queries with 60s TTL using in-memory LRU cache

**Testing**:
- `Performance: test_online_serving_p99_latency` — serve 1000 requests, verify p99 < 10ms (single entity)
- `Performance: test_batch_serving_100_entities` — serve 100 entities in one request, verify p99 < 50ms
- `Performance: test_registry_list_with_1000_views` — create 1000 feature views, verify list query < 200ms
- `Integration: test_connection_pool_exhaustion` — spawn 50 concurrent queries, verify no connection pool errors

---

## Phase Summary & Dependencies

```
Phase 1: Foundation                     ─── required by everything
    │
Phase 2: Registry CRUD & SDK           ─── requires Phase 1
    │
    ├── Phase 3: Offline Store & PIT Joins    ─── requires Phase 2
    │       │
    │       └── Phase 5: Materialisation      ─── requires Phase 3 + Phase 4
    │
    ├── Phase 4: Online Store & Serving       ─── requires Phase 2
    │       │
    │       └── Phase 5: Materialisation      ─── requires Phase 3 + Phase 4
    │
    ├── Phase 6: Monitoring & Metrics         ─── requires Phase 4, can parallel with Phase 5
    │
    └── Phase 7: Auth & RBAC                  ─── requires Phase 2, can parallel with Phases 3-6
         │
Phase 8: Lineage & Discovery                 ─── requires Phase 2, can parallel with Phases 3-7
    │
Phase 9: Streaming Ingestion                 ─── requires Phase 4 (online store)
    │
Phase 10: Feature Discovery UI               ─── requires Phases 2, 8 (registry + lineage APIs)
    │
Phase 11: AI-Augmented Features              ─── requires Phases 2, 6, 8 (registry + monitoring + lineage)
    │
Phase 12: Production Hardening               ─── requires all previous phases
```

Parallelism opportunities:
- Phases 3, 4 can be developed concurrently after Phase 2
- Phases 6, 7, 8 can all be developed concurrently after Phase 2
- Phase 9 can start as soon as Phase 4 is complete
- Phase 10 can start as soon as Phases 2 and 8 are complete

---

## Definition of Done (per phase)

1. All tasks in the phase are implemented.
2. All unit tests pass (`pytest tests/unit/`).
3. All integration tests pass (`pytest tests/integration/`).
4. Linting passes (`ruff check src/ tests/`).
5. Formatting passes (`ruff format --check src/ tests/`).
6. Type checking passes (`mypy src/`).
7. Docker build succeeds (`docker build .`).
8. New database migrations created and tested (if applicable).
9. New API endpoints appear in auto-generated OpenAPI spec at `/openapi.json`.
10. New configuration options documented in `FeatureStoreConfig` with defaults.
11. Prometheus metrics registered for new operational paths (if applicable).
12. No regressions in existing tests from previous phases.

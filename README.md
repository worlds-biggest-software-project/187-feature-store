# Feature Store

> Part of the [worlds-biggest-software-project](https://github.com/worlds-biggest-software-project) initiative.
>
> An AI-native, open-source feature store for centralised computation, storage, and serving of machine learning features across training and inference.

A feature store unifies offline (training) and online (inference) feature retrieval so that ML teams can reuse, monitor, and govern features across many models. This project targets ML engineers and MLOps platform teams who need point-in-time correct training datasets and millisecond-latency online serving without the vendor lock-in or operational weight of incumbent platforms.

---

## Why Feature Store?

- The leading commercial real-time feature store, Tecton, was acquired by Databricks in August 2025, consolidating commercial innovation inside one vendor's ecosystem and increasing lock-in risk for non-Databricks teams.
- Google Vertex AI Feature Store enters deprecation in May 2026 (full sunset February 2027), leaving GCP-native teams without a first-party path forward.
- Feast remains the most widely adopted open-source option but lacks built-in drift detection, a modern feature discovery UI, and native streaming compute — users must assemble Spark, Flink, and external monitoring.
- Hopsworks community edition is AGPL-3.0, which complicates embedding in SaaS products; self-hosted Hopsworks also carries heavy Kubernetes operational overhead.
- Cloud-native stores (SageMaker, Vertex, Azure ML, Snowflake) each lock features into a single cloud or warehouse, with limited cross-cloud portability.

---

## Key Features

### Core Feature Management

- Feature registry with versioned feature groups, entity definitions, and schema enforcement
- Offline store connectors for Parquet/S3 and at least one SQL warehouse (BigQuery or Snowflake)
- Pluggable online store interface, with Redis as the primary reference implementation
- Point-in-time correct `get_historical_features()` for training dataset generation
- Low-latency `get_online_features()` for inference-time retrieval

### Developer Experience

- Python SDK with an intuitive feature definition API and no mandatory Spark or Flink dependency for basic use
- CLI for `apply`, `materialize`, and `serve` operations following GitOps conventions
- Basic feature metadata: owner, description, tags, data source, last updated
- REST API for metadata and schema queries so feature discovery does not require the Python SDK

### Monitoring & Quality

- Built-in feature freshness and serving latency monitoring exposed via a Prometheus metrics endpoint
- Training-serving skew detection with configurable alerting thresholds
- Streaming ingestion support (Kafka / Kinesis to online store) via pluggable connectors

### Governance & Collaboration

- Role-based access control (RBAC) at the feature group level
- Lineage tracking across data sources, transformations, and downstream models
- Branch-based feature development environments for safe experimentation before promoting to production

### AI-Augmented Capabilities

- AI-powered feature recommendation: given a model objective, suggest relevant existing features in the registry
- Natural-language feature authoring with code generation to SQL or Python transformation logic
- Automated feature documentation generation from lineage and computed statistics
- Embedding and vector feature support with similarity search inside the registry

---

## AI-Native Advantage

Where incumbents bolt monitoring and discovery on as afterthoughts, this project treats AI assistance as a first-class capability: feature suggestion driven by training objectives, natural-language to transformation code generation, intelligent drift alerting that distinguishes meaningful drift from benign seasonal variation, and semantic deduplication that detects when two registered features compute functionally equivalent things. AI-generated lineage summaries and documentation reduce the discovery and reuse friction that today blocks feature sharing across teams.

---

## Tech Stack & Deployment

The project is designed as a self-hostable feature store with a Python-native SDK, CLI workflow (`apply`, `materialize`, `serve`), and pluggable connector model for both offline and online stores. Reference integrations target Parquet/S3, BigQuery or Snowflake (offline), and Redis (online), with Kafka / Kinesis streaming ingestion via pluggable connectors. Serving is exposed over REST and the Python SDK; metrics follow a Prometheus-compatible model. The Feast specification serves as the de facto community interface reference; Delta Lake / Apache Iceberg are recognised offline-store table formats supporting time-travel queries.

---

## Market Context

The broader MLOps market that contains feature stores is valued at $4–6 billion in 2026 with roughly 40% CAGR, and the adjacent key-value database market is projected to add over $1.2 billion in incremental revenue by 2034 from feature store workloads (research.md). Enterprise feature store contracts have historically ranged from $100k–$500k per year. Primary buyers are ML engineers and MLOps platform teams at companies running multiple production models, particularly in financial services, e-commerce, and ad-tech where millisecond online serving is business-critical.

---

## Project Status

> This project is in the **research and specification phase**.  
> Contributions, feedback, and domain expertise are welcome.

---

## Contributing

We welcome contributions from developers, domain experts, and potential users.
See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

**Important:** All contributions must be your own original work or clearly attributed
open-source material with a compatible licence. Copyright infringement and licence
violations will not be tolerated and will result in immediate removal of the offending
contribution. If you are unsure whether a piece of code, text, or other material is
safe to contribute, open an issue and ask before submitting.

---

## Licence

Licence to be determined. See [discussion](#) for context.

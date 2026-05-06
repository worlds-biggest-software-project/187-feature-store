# Standards & API Reference

> Project: Feature Store · Candidate #187 · Generated: 2026-05-03

## Industry Standards & Specifications

### Data Format & Table Standards

**Apache Parquet**
- URL: https://parquet.apache.org/
- Columnar storage format universally used for offline feature store data. Point-in-time queries partition by event-time columns stored in Parquet files. All major feature stores (Feast, SageMaker, Databricks) use Parquet as their canonical offline storage format.

**Apache Arrow**
- URL: https://arrow.apache.org/
- In-memory columnar data format used for efficient batch feature retrieval. Hugging Face Datasets, DuckDB, and training frameworks use Arrow for zero-copy data exchange between offline store queries and model training code.

**Delta Lake Open Source Protocol**
- URL: https://delta.io/
- ACID table format on top of Parquet, providing time-travel (time-travel queries are essential for point-in-time correct feature retrieval). Databricks Feature Store and many offline feature pipelines are built on Delta tables.

**Apache Iceberg**
- URL: https://iceberg.apache.org/
- Open table format specification for massive analytic tables with time-travel, schema evolution, and partition management. Increasingly used as the offline feature store backing format as an alternative to Delta Lake. Supported by Snowflake, AWS Glue, and Databricks.

**Apache Hudi**
- URL: https://hudi.apache.org/
- Streaming upsert table format used by Hopsworks as its offline store layer. Enables efficient incremental reads and upserts from streaming feature pipelines.

---

### Messaging & Streaming Standards

**Apache Kafka Protocol**
- URL: https://kafka.apache.org/protocol
- De facto streaming protocol for real-time feature ingestion. Feast, Hopsworks, Tecton, and SageMaker Feature Store all support Kafka as a streaming source for near-real-time feature freshness.

**AsyncAPI 2.6 / 3.0**
- URL: https://www.asyncapi.com/
- Specification for event-driven APIs (Kafka, MQTT, WebSockets). Relevant for documenting streaming feature ingestion contracts — the interface between upstream event producers and feature store consumers.

---

### API & Serialisation Standards

**OpenAPI Specification 3.1 / 3.2**
- URL: https://spec.openapis.org/oas/v3.2.0.html
- Standard for describing REST APIs. Feature stores that expose HTTP endpoints for online feature retrieval (Feast Feature Server, Hopsworks Online Store REST API, Tecton HTTP API) should publish OpenAPI specifications for client generation.

**Protocol Buffers (protobuf) / gRPC**
- URL: https://grpc.io/docs/ · https://protobuf.dev/
- Binary serialisation and RPC framework used by Feast for its core serving protocol (`ServingService.proto`, `CoreService.proto`). gRPC provides low-latency, strongly-typed feature retrieval suitable for high-throughput inference services. Feast also supports HTTP/JSON transcoding via gRPC-gateway.

**JSON Schema Draft 2020-12**
- URL: https://json-schema.org/specification
- Used for validating feature record schemas in REST-based feature serving (HTTP/JSON endpoints). Relevant for schema-on-read validation in online stores.

---

### Observability Standards

**OpenTelemetry (OTel)**
- URL: https://opentelemetry.io/
- Vendor-neutral standard for metrics, traces, and logs. Emerging as the standard for feature store observability: instrumenting feature serving latency, feature freshness lag, materialisation job durations, and data quality counters.

**Prometheus Exposition Format**
- URL: https://prometheus.io/docs/instrumenting/exposition_formats/
- De facto metrics format for cloud-native services. Feast Feature Server ships built-in Prometheus metrics; Tecton and Hopsworks expose Prometheus endpoints. Standard for feature freshness, request latency, and cache hit rate dashboards.

---

### Security & Compliance Standards

**OAuth 2.0 (RFC 6749) / OpenID Connect**
- URL: https://datatracker.ietf.org/doc/html/rfc6749 · https://openid.net/connect/
- Standard authentication/authorisation frameworks. Enterprise feature stores require OAuth 2.0 bearer tokens for API authentication; OIDC for user identity in web UIs and notebook environments.

**OWASP LLM Top 10 — LLM08:2025 Vector and Embedding Weaknesses**
- URL: https://genai.owasp.org/llmrisk/llm082025-vector-and-embedding-weaknesses/
- Directly relevant for feature stores that serve vector embeddings: addresses risks of embedding inversion, data extraction via similarity search, and access control failures in vector retrieval pipelines.

**GDPR (Regulation (EU) 2016/679)**
- URL: https://gdpr-info.eu/
- Feature stores that hold PII-derived features must implement: data minimisation, access controls per feature group, right-to-erasure workflows (delete feature records by entity ID), and data lineage to prove provenance of personal data features.

**NIST AI Risk Management Framework (AI RMF 1.0)**
- URL: https://www.nist.gov/system/files/documents/2023/01/26/AI%20RMF%201.0.pdf
- Provides governance framework for AI systems including data provenance, bias documentation, and model monitoring — all capabilities that a feature store's lineage and monitoring subsystems contribute to.

---

### MLOps & AI Platform Standards

**MLflow Model Registry (de facto standard)**
- URL: https://mlflow.org/docs/latest/ml/model-registry
- De facto standard for model lifecycle management and data lineage from feature store to trained model. Databricks Feature Store integrates tightly with MLflow for tracking which feature tables and versions were used to train each model version.

**Kubeflow (CNCF Incubating Project)**
- URL: https://www.kubeflow.org/ · https://www.cncf.io/projects/kubeflow/
- CNCF-hosted MLOps platform for Kubernetes. Feature stores (Feast, Hopsworks) integrate with Kubeflow Pipelines as the orchestration layer for feature materialisation jobs.

**CNCF Cloud Native AI White Paper**
- URL: https://www.cncf.io/wp-content/uploads/2024/03/cloud_native_ai24_031424a-2.pdf
- 2024 CNCF guidance on running ML/AI workloads on Kubernetes. Feature stores are identified as a key infrastructure component in the cloud-native AI stack alongside model serving and experiment tracking.

**Model Context Protocol (MCP)**
- URL: https://modelcontextprotocol.io/
- Anthropic-introduced standard (late 2024) for AI agents to discover and call external tools/APIs. Highly relevant for feature stores: an MCP server wrapping the feature registry would allow LLM agents to autonomously discover, query, and retrieve features to enrich their context.

---

## Similar Products — Developer Documentation & APIs

### Feast
- **Description:** Open-source feature store with offline/online store, Python SDK, and HTTP/gRPC serving. Most widely adopted OSS feature store.
- **API Documentation:** https://docs.feast.dev
- **Python API Reference:** https://rtd.feast.dev
- **Source & Protobuf Specs:** https://github.com/feast-dev/feast (protos at `protos/feast/serving/ServingService.proto`)
- **Online Store Format Spec:** https://github.com/feast-dev/feast/blob/master/docs/specs/online_store_format.md
- **Standards:** gRPC/protobuf for serving; REST/JSON via HTTP gateway; Prometheus metrics
- **Authentication:** Bearer token (configurable); no built-in auth by default

### Hopsworks Feature Store
- **Description:** End-to-end AI Lakehouse with integrated feature store, model registry, and deployment; supports Python, Spark, Flink, and Java/Scala.
- **API Documentation:** https://docs.hopsworks.ai/feature-store-api/latest/
- **Python SDK (`hsfs`):** https://github.com/logicalclocks/feature-store-api
- **REST API (Online Store):** https://docs.hopsworks.ai/latest/user_guides/fs/feature_view/feature-server/
- **Standards:** REST/JSON for online serving; HSFS Python API for offline; Spark DataFrames
- **Authentication:** Hopsworks API keys; LDAP/OAuth2 for enterprise

### Tecton (Databricks Feature Engineering)
- **Description:** Production-grade managed feature platform (now part of Databricks); declarative Python definitions compiled by Rift engine to batch/streaming/real-time compute.
- **API Documentation:** https://docs.tecton.ai/docs/sdk-reference
- **Python HTTP Client:** https://tecton-ai.github.io/tecton-http-client-python/
- **GitHub (HTTP Client):** https://github.com/tecton-ai/tecton-http-client-python
- **Standards:** REST/gRPC for online serving; Python SDK for training datasets
- **Authentication:** API keys; Databricks personal access tokens post-acquisition

### Databricks Feature Engineering (Unity Catalog)
- **Description:** Feature Engineering integrated with Unity Catalog: feature tables as Delta tables with governance, lineage, and cross-workspace sharing.
- **API Documentation:** https://docs.databricks.com/aws/en/machine-learning/feature-store
- **Python API Reference:** https://api-docs.databricks.com/python/feature-engineering/latest/feature_engineering.client.html
- **Python SDK:** `databricks-feature-engineering` (PyPI); pre-installed in Databricks Runtime 13.3 LTS ML+
- **Standards:** Delta Lake for offline store; REST/gRPC for online serving; MLflow for lineage
- **Authentication:** Databricks personal access tokens; OAuth 2.0 (M2M)

### Amazon SageMaker Feature Store
- **Description:** AWS-managed dual-store feature service with S3/Athena offline store and low-latency online store.
- **API Documentation:** https://docs.aws.amazon.com/sagemaker/latest/dg/feature-store.html
- **API Reference:** https://docs.aws.amazon.com/sagemaker/latest/APIReference/Welcome.html
- **Boto3 SDK:** https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sagemaker-featurestore-runtime.html
- **Go SDK:** https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/sagemakerfeaturestoreruntime
- **Python SDK (SageMaker):** https://sagemaker.readthedocs.io/en/stable/amazon_sagemaker_featurestore.html
- **Standards:** AWS REST APIs; JSON over HTTPS; IAM authentication
- **Authentication:** AWS IAM (SigV4 request signing)

### Google Vertex AI Feature Store
- **Description:** GCP-managed feature store backed by BigQuery (offline) and Cloud Bigtable (online); supports vector embeddings and RAG pipeline integration. NOTE: Deprecated from May 2026; full sunset February 2027.
- **API Documentation:** https://docs.cloud.google.com/vertex-ai/docs/featurestore/latest/overview
- **RAG Engine Integration:** https://docs.cloud.google.com/vertex-ai/generative-ai/docs/rag-engine/use-feature-store-with-rag
- **Python SDK:** `google-cloud-aiplatform` (PyPI)
- **Standards:** REST API with gRPC transcoding; BigQuery SQL for offline; Bigtable gRPC for online
- **Authentication:** Google Cloud IAM; Application Default Credentials (ADC); OAuth 2.0

### Azure Machine Learning Managed Feature Store
- **Description:** AzureML-integrated feature store with serverless Spark materialisation, offline store on ADLS Gen2, and online store on Azure Cache for Redis.
- **API Documentation:** https://learn.microsoft.com/azure/machine-learning/concept-what-is-managed-feature-store
- **Python SDK:** `azureml-featurestore` (PyPI): https://pypi.org/project/azureml-featurestore/
- **Standards:** REST API; Python SDK; Azure Resource Manager (ARM) for provisioning
- **Authentication:** Azure Active Directory (Entra ID); Managed Identity; Service Principal

### Snowflake Feature Store
- **Description:** Feature store built entirely within Snowflake using Snowpark; offline store as Snowflake tables, online store as Snowflake Online Feature Store.
- **API Documentation:** https://docs.snowflake.com/en/developer-guide/snowflake-ml/feature-store/overview
- **Snowpark Python Guide:** https://docs.snowflake.com/en/developer-guide/snowpark/python/
- **Standards:** Snowpark Python API; SQL; Snowflake REST API
- **Authentication:** Snowflake username/password, key-pair, OAuth 2.0

### Fennel (now Databricks)
- **Description:** Rust CDC-powered fully managed real-time feature platform with Python/Pandas API; no Spark/Flink required. Acquired by Databricks in 2025.
- **API Documentation:** https://fennel.ai/
- **Standards:** REST API for online serving; Python SDK for feature authoring
- **Authentication:** API keys; SOC 2 Type II compliant

---

## Notes

**No universal feature store interoperability standard exists.** Unlike REST (OpenAPI) or messaging (AsyncAPI/Kafka), there is no industry-wide specification for feature store wire protocols, registry schemas, or offline store formats. Feast's protobuf definitions are the closest to a de facto standard, but they are not an official specification body artifact.

**Delta Lake and Apache Iceberg are converging** as the dual standard for offline feature stores. The choice between them is largely determined by the data lakehouse platform in use (Databricks → Delta; Snowflake/AWS Glue → Iceberg). Both support time-travel queries essential for point-in-time correct training data generation.

**MCP is the most strategically relevant emerging standard** for AI-native feature stores. A feature store that exposes an MCP server would allow AI agents and LLM-based data pipelines to discover and retrieve features autonomously, enabling a new generation of agentic ML workflows.

**Vertex AI Feature Store deprecation** (sunset February 2027) creates near-term migration pressure for GCP users, representing a market opportunity for portable open-source or multi-cloud alternatives.

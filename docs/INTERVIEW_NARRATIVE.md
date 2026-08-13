# Microservice Observability Pipeline — Interview Narrative

## The Pain Point

In microservice architectures, a single user request often traverses multiple downstream services and database queries. When an endpoint returns a `500 Internal Server Error` or times out with `504 Gateway Timeout`, diagnosing the root cause across distributed infrastructure is painful:

* **Siloed Logs**: Each service writes logs to its own stdout/stderr or log file. Correlating events across services requires manually cross-referencing timestamps.
* **Lack of Context**: Standard logs lack request context or distributed trace headers, making it impossible to map a downstream database error back to the originating HTTP request.
* **No Real-Time Anomaly Detection**: Log aggregators often store data passively without alerting on sudden error spikes or cascading timeouts.
* **Troubleshooting Latency**: Without unified distributed tracing, identifying whether latency originates from HTTP overhead, service logic, or database query lock contention requires adding ad-hoc logging and re-deploying.

---

## What I Built

I designed and built an end-to-end, production-grade microservice observability pipeline running on Kubernetes:

1. **Microservice Call Chain**: Three FastAPI services (`Service A` -> `Service B` -> `Service C` -> `PostgreSQL`).
2. **Structured Log Telemetry**: Every service emits JSON-structured logs containing `request_id`, `service`, `event`, `trace_id`, and `span_id`.
3. **Log Ingestion & Messaging Buffer**: A Fluent Bit DaemonSet (`fluent-bit-shipper`) tails container logs from `/var/log/containers/*.log` and streams them into an Apache Kafka topic (`app-logs`).
4. **Decoupled Processing & Storage**:
   * A Fluent Bit consumer reads from Kafka and ingests logs into **Grafana Loki**.
   * An independent Python worker (`anomaly-detector`) consumes from Kafka using a separate consumer group, evaluating error rates over sliding 60-second windows and recording alerts into PostgreSQL.
5. **Distributed Tracing**: OpenTelemetry auto-instrumentation generates distributed traces exported via OTLP/gRPC to **Jaeger**.
6. **Single-Pane Observability**: A pre-configured **Grafana** dashboard correlates logs, metrics, and distributed traces, enabling 1-click drilldown from a Loki log line directly into the matching Jaeger trace.

---

## Why Each Tool Was Chosen

| Technology | Role | Technical Justification |
| :--- | :--- | :--- |
| **Apache Kafka** | Log Buffer & Streaming Platform | Provides high-throughput, persistent buffer decoupling log collection from storage. Supports fan-out pattern so multiple consumers (Loki shipper, real-time anomaly detector) read independently without affecting each other or the applications. |
| **Fluent Bit** | Log Shipper & Consumer | C-based, low CPU/memory footprint (~few MBs vs Logstash's hundreds of MBs). Built-in Kubernetes filter parses pod metadata; native Kafka output and Loki output plugins handle streaming cleanly. |
| **Grafana Loki** | Log Aggregation Engine | Index-free log aggregation (indexes metadata labels only, not full log text). Extremely cost-efficient and integrates seamlessly with Grafana and Jaeger derived fields. |
| **Jaeger & OpenTelemetry** | Distributed Tracing | Vendor-neutral OpenTelemetry standards instrument HTTP headers (`X-Request-ID`, W3C trace context) and database queries (`asyncpg`). Jaeger visualizes full request execution graphs and span durations. |
| **Grafana** | Visualization & Correlation | Provides a unified dashboard combining Loki log streams, PostgreSQL alert tables, log-rate time series graphs, and derived field deep-links into Jaeger traces. |
| **asyncpg with Tiny Connection Pool** | Deterministic Failure Injection | Service C uses a fixed pool (`DB_POOL_SIZE=3`). Under concurrent load, connection pool exhaustion causes deterministic HTTP 503 errors and cascading timeouts, proving telemetry reliability under stress without artificial dice rolls. |

---

## Architecture Diagram

```mermaid
flowchart TD
    subgraph Clients & Ingress
        Client[Client / Locust Load Test]
    end

    subgraph Microservices ["Microservice Cluster (FastAPI + OpenTelemetry)"]
        SA["Service A (:8000)"]
        SB["Service B (:8001)"]
        SC["Service C (:8002)"]
        DB[(PostgreSQL)]

        Client -->|HTTP /api/orders| SA
        SA -->|HTTP /internal/orders| SB
        SB -->|HTTP /internal/validate| SC
        SC -->|asyncpg pool = 3| DB
    end

    subgraph Telemetry ["Telemetry & Telemetry Pipeline"]
        FBS["Fluent Bit Shipper (DaemonSet)"]
        Kafka{{Apache Kafka (Topic: app-logs)}}
        FBC["Fluent Bit Consumer"]
        Loki[(Grafana Loki)]
        Jaeger[(Jaeger Tracing)]
        AD["Anomaly Detector"]

        SA -.->|stdout JSON logs| FBS
        SB -.->|stdout JSON logs| FBS
        SC -.->|stdout JSON logs| FBS

        SA -.->|OTLP gRPC| Jaeger
        SB -.->|OTLP gRPC| Jaeger
        SC -.->|OTLP gRPC| Jaeger

        FBS -->|Stream logs| Kafka
        Kafka -->|Consumer Group: loki| FBC
        Kafka -->|Consumer Group: anomaly-detector| AD
        FBC -->|Ingest logs| Loki
        AD -->|Write alerts| DB
    end

    subgraph Observability ["Unified Visualization"]
        Grafana[Grafana Dashboard]
        Loki -->|Log Streams| Grafana
        Jaeger -->|Trace Graphs| Grafana
        DB -->|Alert Table| Grafana
    end
```

---

## The Interview Story & Walkthrough

When presenting this project in an engineering interview, structure the discussion using the **STAR** method (Situation, Task, Action, Result):

1. **Situation**: "In microservice architectures, debugging multi-service failures or database bottlenecks often means manually digging through disparate container logs across multiple servers."
2. **Task**: "I set out to build a production-grade observability pipeline that unifies structured logging, distributed tracing, and real-time anomaly detection into a single visualization pane with automated failure correlation."
3. **Action**:
   * "I implemented a 3-tier FastAPI service chain where each request propagates correlation headers (`X-Request-ID`) and OpenTelemetry trace contexts."
   * "I built a container log streaming pipeline using Fluent Bit, Apache Kafka, Loki, and Jaeger deployed on Kubernetes."
   * "To prove the pipeline works under stress, I configured a deliberately constrained database connection pool (`DB_POOL_SIZE=3`) in Service C to deterministically trigger connection pool exhaustion under load."
   * "I developed a standalone Kafka consumer service that runs sliding-window error rate checks and logs anomaly alerts directly into PostgreSQL."
   * "I configured Grafana with Loki derived fields, enabling 1-click jumps from error log records directly to exact Jaeger trace spans."
4. **Result**: "During a 100-user load test, when Service C experienced pool exhaustion, the entire incident was surfaced in real time: Service A returned HTTP 504 gateway timeouts, Service C emitted `db_pool_exhausted` error logs, Kafka fanned out the telemetry to Loki and the anomaly detector within seconds, and Grafana allowed identifying the root cause (`asyncpg acquire timeout`) within seconds via the embedded trace link."

---

## Key Technical Tradeoffs & Scalability Considerations

* **Kafka vs Direct Shipping**: Direct shipping from Fluent Bit to Loki reduces infrastructure overhead. However, placing Kafka in between provides backpressure buffering during traffic spikes, ensuring log producers never drop logs or degrade core application performance.
* **Loki Label Cardinality**: Loki indexes labels only. I limited Loki labels to high-level metadata (`job`, `service`, `levelname`) and parsed variable fields like `request_id` at query time using Loki's `| json` filter to avoid high-cardinality index bloat.
* **Auto-Instrumentation vs Manual Spans**: OpenTelemetry FastAPI and asyncpg auto-instrumentation capture standard HTTP and database spans automatically with zero invasive code modification. Manual spans are added selectively for custom business logic events.

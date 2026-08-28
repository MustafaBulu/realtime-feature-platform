# Realtime Feature Platform

Realtime Feature Platform is a Java 21 backend project for computing low-latency, entity-based features from streaming events.

The project is intentionally scoped as a streaming infrastructure portfolio project, not a payment, billing, banking, ledger, or fraud decision system.

## Capabilities

- Maven multi-module project structure
- Java 21 build setup
- Spring Boot application skeletons
- Shared event and feature model modules
- JUnit 5 test setup
- Testcontainers dependency setup
- Docker Compose infrastructure
- Kafka, PostgreSQL, Redis, Prometheus, and Grafana services
- Health endpoints
- Structured console logging
- GitHub Actions CI

## Modules

| Module | Purpose |
| --- | --- |
| `event-model` | Domain-independent event records |
| `feature-model` | Feature definition and aggregation model |
| `feature-api` | Online feature serving API skeleton |
| `stream-worker` | Stream processing worker skeleton |
| `workload-generator` | Synthetic event generator skeleton |

## Requirements

- Java 21
- Docker Desktop or a compatible Docker Engine
- Maven Wrapper is included

## Build And Test

```powershell
.\mvnw.cmd -B verify
```

On Unix-like shells:

```bash
./mvnw -B verify
```

## Run Infrastructure And Apps

```powershell
docker compose up --build
```

This starts:

| Service | URL |
| --- | --- |
| Feature API | http://localhost:8080 |
| Stream Worker | http://localhost:8081 |
| Workload Generator | http://localhost:8082 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |
| Kafka | localhost:9092 |
| PostgreSQL | localhost:5432 |
| Redis | localhost:6379 |

Grafana defaults:

- Username: `admin`
- Password: `admin`

## Health Checks

Application health endpoints:

```text
GET http://localhost:8080/internal/health
GET http://localhost:8081/internal/health
GET http://localhost:8082/internal/health
```

Spring Actuator health endpoints:

```text
GET http://localhost:8080/actuator/health
GET http://localhost:8081/actuator/health
GET http://localhost:8082/actuator/health
```

Prometheus metrics:

```text
GET http://localhost:8080/actuator/prometheus
GET http://localhost:8081/actuator/prometheus
GET http://localhost:8082/actuator/prometheus
```

## Roadmap

The platform will grow toward:

- Kafka-backed event ingestion
- Entity-key partitioning
- Event-time windowing
- Tumbling and sliding windows
- Late and out-of-order event handling
- Bounded deduplication
- RocksDB local state
- Redis feature materialization
- Worker restart and Kafka rebalance recovery
- PostgreSQL on-demand aggregation baseline
- Reliability and benchmark evidence

Performance and reliability claims should be added only when backed by tests or benchmark output.

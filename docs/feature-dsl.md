# Feature DSL

Feature definitions describe what the worker should compute without adding a feature-specific Java processor.

## Fields

| Field | Purpose |
| --- | --- |
| `name` | Stable feature name used in Redis and serving APIs |
| `eventType` | Event stream selector, for example `request.completed` |
| `entityType` | Entity scope, for example `service` |
| `aggregationType` | `COUNT`, `SUM`, `AVG`, `RATIO`, or `DISTINCT_COUNT` |
| `valueField` | Payload field used by numeric aggregations |
| `weightField` | Optional payload field for weighted average |
| `filter` | Optional denominator/input filter |
| `numeratorFilter` | Optional ratio numerator filter |
| `windowType` | `NONE`, `TUMBLING`, or `SLIDING` |
| `windowSize` | ISO-8601 duration for windowed features |
| `slide` | Sliding bucket granularity, or tumbling alignment |
| `version` | Integer definition version |
| `state` | `DRAFT` or `ACTIVE` |

## Validation

- Feature names, event types, entity types, versions, and states are required.
- Numeric aggregations require numeric payload fields when a value field is configured.
- Windowed definitions require a valid positive duration.
- Sliding windows require a valid positive `slide`.
- Ratio definitions use denominator input plus numerator filter state.

## Built-In Definitions

The seed definitions cover:

- `request_count_total`
- `entity_event_count_10m`
- `entity_error_rate_10m`
- `entity_avg_latency_ms_5m`

# Event-Time Semantics

The worker separates processing time from event time.

## Classification

- Processing time is when the worker handles the Kafka record.
- Event time is the timestamp carried by the `PlatformEvent`.
- Ingestion lag metrics record processing time minus event time.

## Windowing

Tumbling windows align event time to fixed boundaries. Sliding windows use the feature definition `slide` as the bucket granularity and update every aligned window containing the event.

Reads without `windowStart` return the latest materialized event-time window observed by the worker for that feature/entity. This latest alias is not a trailing rolling "last N minutes from now" query. Late events can correct their historical window-specific Redis key, but historical corrections do not move the latest alias back to older windows.

## Lateness

- Allowed lateness is evaluated against worker wall-clock processing time.
- Events inside allowed lateness are accepted, even if they arrive out of order.
- Accepted late events correct historical window-specific Redis keys.
- Historical corrections do not move the latest-window alias back to an older window.
- Events outside allowed lateness are rejected and counted by late rejection metrics.
- Long worker downtime or large Kafka backlog can cause old events to be rejected because the cutoff is `processingTime - allowedLateness`, not a watermark derived from the stream.

## Serving Freshness

Feature API `metadata.freshnessMillis` is data age at read time, measured from the materialized value's worker update timestamp to the API read time. It is not update-to-availability latency from Kafka broker acknowledgement to first visible API read.

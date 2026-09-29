# Event-Time Semantics

The worker separates processing time from event time.

## Classification

- Processing time is when the worker handles the Kafka record.
- Event time is the timestamp carried by the `PlatformEvent`.
- Ingestion lag metrics record processing time minus event time.

## Windowing

Tumbling windows align event time to fixed boundaries. Sliding windows use the feature definition `slide` as the bucket granularity and update every aligned window containing the event.

## Lateness

- Events inside allowed lateness are accepted, even if they arrive out of order.
- Accepted late events correct historical window-specific Redis keys.
- Historical corrections do not move the latest-window alias back to an older window.
- Events outside allowed lateness are rejected and counted by late rejection metrics.

package com.mustafabulu.realtimefeatureplatform.eventmodel;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public final class RequestCompletedEventFactory {

    private RequestCompletedEventFactory() {
    }

    public static PlatformEvent create(String entityType, String entityId, long count) {
        return create(UUID.randomUUID().toString(), entityType, entityId, count, Instant.now());
    }

    public static PlatformEvent create(String eventId, String entityType, String entityId, long count, Instant eventTime) {
        return create(eventId, entityType, entityId, count, null, null, eventTime);
    }

    public static PlatformEvent create(
            String entityType,
            String entityId,
            long count,
            int statusCode,
            long latencyMs
    ) {
        return create(UUID.randomUUID().toString(), entityType, entityId, count, statusCode, latencyMs, Instant.now());
    }

    public static PlatformEvent create(
            String eventId,
            String entityType,
            String entityId,
            long count,
            Integer statusCode,
            Long latencyMs,
            Instant eventTime
    ) {
        Map<String, Object> payload = new java.util.LinkedHashMap<>();
        payload.put("count", count);
        if (statusCode != null) {
            payload.put("statusCode", statusCode);
        }
        if (latencyMs != null) {
            payload.put("latencyMs", latencyMs);
        }

        return new PlatformEvent(
                eventId,
                "request.completed",
                eventTime,
                new EntityRef(entityType, entityId),
                payload
        );
    }
}

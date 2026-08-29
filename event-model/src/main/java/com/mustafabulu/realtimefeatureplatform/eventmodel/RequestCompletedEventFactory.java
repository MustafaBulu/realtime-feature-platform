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
        return new PlatformEvent(
                eventId,
                "request.completed",
                eventTime,
                new EntityRef(entityType, entityId),
                Map.of("count", count)
        );
    }
}

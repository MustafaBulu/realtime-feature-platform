package com.mustafabulu.realtimefeatureplatform.eventmodel;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

public record PlatformEvent(
        String eventId,
        String eventType,
        Instant eventTime,
        EntityRef entity,
        Map<String, Object> payload
) {

    public PlatformEvent {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(eventTime, "eventTime must not be null");
        Objects.requireNonNull(entity, "entity must not be null");

        if (eventId.isBlank()) {
            throw new IllegalArgumentException("eventId must not be blank");
        }
        if (eventType.isBlank()) {
            throw new IllegalArgumentException("eventType must not be blank");
        }

        payload = payload == null ? Map.of() : Map.copyOf(payload);
    }
}

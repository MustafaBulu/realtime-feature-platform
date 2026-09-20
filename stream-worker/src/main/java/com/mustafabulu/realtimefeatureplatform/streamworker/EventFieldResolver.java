package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import java.util.Map;
import java.util.Optional;

final class EventFieldResolver {

    private EventFieldResolver() {
    }

    static Optional<Object> resolve(PlatformEvent event, String fieldPath) {
        return switch (fieldPath) {
            case "eventId" -> Optional.of(event.eventId());
            case "eventType" -> Optional.of(event.eventType());
            case "eventTime" -> Optional.of(event.eventTime());
            case "entity.type" -> Optional.of(event.entity().type());
            case "entity.id" -> Optional.of(event.entity().id());
            default -> resolvePayload(event.payload(), normalizePayloadPath(fieldPath));
        };
    }

    private static String normalizePayloadPath(String fieldPath) {
        return fieldPath.startsWith("payload.") ? fieldPath.substring("payload.".length()) : fieldPath;
    }

    @SuppressWarnings("unchecked")
    private static Optional<Object> resolvePayload(Map<String, Object> payload, String fieldPath) {
        Object current = payload;
        for (String part : fieldPath.split("\\.")) {
            if (!(current instanceof Map<?, ?> map) || !map.containsKey(part)) {
                return Optional.empty();
            }
            current = ((Map<String, Object>) map).get(part);
        }
        return Optional.ofNullable(current);
    }
}

package com.mustafabulu.realtimefeatureplatform.eventmodel;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

public final class EventJsonCodec {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private EventJsonCodec() {
    }

    public static String toJson(PlatformEvent event) {
        try {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("eventId", event.eventId());
            body.put("eventType", event.eventType());
            body.put("eventTime", event.eventTime().toString());
            body.put("entity", Map.of(
                    "type", event.entity().type(),
                    "id", event.entity().id()
            ));
            body.put("payload", event.payload());

            return MAPPER.writeValueAsString(body);
        } catch (JacksonException ex) {
            throw new IllegalArgumentException("Could not serialize event", ex);
        }
    }

    public static PlatformEvent fromJson(String json) {
        try {
            Map<String, Object> body = MAPPER.readValue(json, MAP_TYPE);
            Map<String, Object> entity = mapValue(body, "entity");
            Map<String, Object> payload = new LinkedHashMap<>(mapValue(body, "payload"));
            normalizeRequestCompletedPayload(body, payload);

            return new PlatformEvent(
                    stringValue(body, "eventId"),
                    stringValue(body, "eventType"),
                    Instant.parse(stringValue(body, "eventTime")),
                    new EntityRef(stringValue(entity, "type"), stringValue(entity, "id")),
                    payload
            );
        } catch (JacksonException ex) {
            throw new IllegalArgumentException("Could not deserialize event", ex);
        }
    }

    private static void normalizeRequestCompletedPayload(Map<String, Object> body, Map<String, Object> payload) {
        if (!"request.completed".equals(body.get("eventType"))) {
            return;
        }

        normalizeLong(payload, "count");
        normalizeLong(payload, "statusCode");
        normalizeLong(payload, "latencyMs");
    }

    private static void normalizeLong(Map<String, Object> payload, String fieldName) {
        Object value = payload.get(fieldName);
        if (value instanceof Number number) {
            payload.put(fieldName, number.longValue());
        }
    }

    private static String stringValue(Map<String, Object> values, String fieldName) {
        Object value = values.get(fieldName);
        if (!(value instanceof String stringValue) || stringValue.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must be a non-blank string");
        }
        return stringValue;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> mapValue(Map<String, Object> values, String fieldName) {
        Object value = values.get(fieldName);
        if (!(value instanceof Map<?, ?> mapValue)) {
            throw new IllegalArgumentException(fieldName + " must be an object");
        }
        return (Map<String, Object>) mapValue;
    }
}

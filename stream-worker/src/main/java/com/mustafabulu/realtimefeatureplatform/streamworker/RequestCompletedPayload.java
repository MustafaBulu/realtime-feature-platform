package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import java.util.OptionalLong;

record RequestCompletedPayload(long count, OptionalLong statusCode, OptionalLong latencyMs) {

    static RequestCompletedPayload from(PlatformEvent event) {
        return new RequestCompletedPayload(
                ((Number) event.payload().get("count")).longValue(),
                optionalLong(event, "statusCode"),
                optionalLong(event, "latencyMs")
        );
    }

    private static OptionalLong optionalLong(PlatformEvent event, String fieldName) {
        Object value = event.payload().get(fieldName);
        if (value instanceof Number number) {
            return OptionalLong.of(number.longValue());
        }
        return OptionalLong.empty();
    }
}

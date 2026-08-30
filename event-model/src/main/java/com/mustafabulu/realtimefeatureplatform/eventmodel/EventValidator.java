package com.mustafabulu.realtimefeatureplatform.eventmodel;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public class EventValidator {

    private final Clock clock;
    private final Duration futureSkewTolerance;

    public EventValidator() {
        this(Clock.systemUTC(), Duration.ofMinutes(5));
    }

    EventValidator(Clock clock, Duration futureSkewTolerance) {
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.futureSkewTolerance = Objects.requireNonNull(futureSkewTolerance, "futureSkewTolerance must not be null");
    }

    public void validate(PlatformEvent event) {
        if (event == null) {
            throw new EventValidationException("event must not be null");
        }
        if (event.eventTime().isAfter(Instant.now(clock).plus(futureSkewTolerance))) {
            throw new EventValidationException("eventTime must not be too far in the future");
        }
        if ("request.completed".equals(event.eventType())) {
            long count = requiredLong(event, "count");
            if (count < 0) {
                throw new EventValidationException("request.completed count must not be negative");
            }
            optionalLong(event, "statusCode").ifPresent(statusCode -> {
                if (statusCode < 100 || statusCode > 599) {
                    throw new EventValidationException("request.completed statusCode must be between 100 and 599");
                }
            });
            optionalLong(event, "latencyMs").ifPresent(latencyMs -> {
                if (latencyMs < 0) {
                    throw new EventValidationException("request.completed latencyMs must not be negative");
                }
            });
        }
    }

    private static long requiredLong(PlatformEvent event, String fieldName) {
        return optionalLong(event, fieldName)
                .orElseThrow(() -> new EventValidationException(
                        "request.completed payload must contain numeric " + fieldName));
    }

    private static java.util.Optional<Long> optionalLong(PlatformEvent event, String fieldName) {
        Object value = event.payload().get(fieldName);
        if (value == null) {
            return java.util.Optional.empty();
        }
        if (!(value instanceof Number number)) {
            throw new EventValidationException("request.completed " + fieldName + " must be numeric");
        }
        return java.util.Optional.of(number.longValue());
    }
}

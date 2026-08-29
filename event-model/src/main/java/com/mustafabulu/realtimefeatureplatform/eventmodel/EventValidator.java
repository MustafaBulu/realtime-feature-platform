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
            Object count = event.payload().get("count");
            if (!(count instanceof Number number)) {
                throw new EventValidationException("request.completed payload must contain numeric count");
            }
            if (number.longValue() < 0) {
                throw new EventValidationException("request.completed count must not be negative");
            }
        }
    }
}

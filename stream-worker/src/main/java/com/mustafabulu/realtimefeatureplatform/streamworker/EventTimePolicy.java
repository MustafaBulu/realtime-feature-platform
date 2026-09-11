package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
class EventTimePolicy {

    private final Clock clock;
    private final Duration allowedLateness;

    @Autowired
    EventTimePolicy(@Value("${rfp.event-time.allowed-lateness:PT10M}") Duration allowedLateness) {
        this(Clock.systemUTC(), allowedLateness);
    }

    EventTimePolicy(Clock clock, Duration allowedLateness) {
        if (allowedLateness.isNegative()) {
            throw new IllegalArgumentException("allowedLateness must not be negative");
        }
        this.clock = clock;
        this.allowedLateness = allowedLateness;
    }

    boolean isTooLate(PlatformEvent event) {
        Instant cutoff = Instant.now(clock).minus(allowedLateness);
        return event.eventTime().isBefore(cutoff);
    }
}

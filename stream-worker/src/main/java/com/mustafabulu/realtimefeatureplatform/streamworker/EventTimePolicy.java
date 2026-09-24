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
        return assess(event).tooLate();
    }

    EventTimeAssessment assess(PlatformEvent event) {
        Instant processingTime = Instant.now(clock);
        Instant cutoff = processingTime.minus(allowedLateness);
        EventTiming timing;
        if (event.eventTime().isBefore(cutoff)) {
            timing = EventTiming.TOO_LATE;
        } else if (event.eventTime().isBefore(processingTime)) {
            timing = EventTiming.LATE_WITHIN_ALLOWED;
        } else {
            timing = EventTiming.ON_TIME;
        }
        return new EventTimeAssessment(event.eventTime(), processingTime, cutoff, timing);
    }
}

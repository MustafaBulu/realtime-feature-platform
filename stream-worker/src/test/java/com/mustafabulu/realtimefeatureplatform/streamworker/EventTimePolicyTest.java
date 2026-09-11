package com.mustafabulu.realtimefeatureplatform.streamworker;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import com.mustafabulu.realtimefeatureplatform.eventmodel.RequestCompletedEventFactory;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

class EventTimePolicyTest {

    private final EventTimePolicy policy = new EventTimePolicy(
            Clock.fixed(Instant.parse("2026-08-28T12:20:00Z"), ZoneOffset.UTC),
            Duration.ofMinutes(10)
    );

    @Test
    void acceptsOutOfOrderEventWithinAllowedLateness() {
        PlatformEvent event = RequestCompletedEventFactory.create(
                "event-1",
                "service",
                "catalog-api",
                100,
                Instant.parse("2026-08-28T12:10:00Z")
        );

        assertFalse(policy.isTooLate(event));
    }

    @Test
    void rejectsTooLateEvent() {
        PlatformEvent event = RequestCompletedEventFactory.create(
                "event-1",
                "service",
                "catalog-api",
                100,
                Instant.parse("2026-08-28T12:09:59.999Z")
        );

        assertTrue(policy.isTooLate(event));
    }

    @Test
    void rejectsNegativeAllowedLateness() {
        assertThrows(IllegalArgumentException.class,
                () -> new EventTimePolicy(Clock.systemUTC(), Duration.ofMillis(-1)));
    }
}

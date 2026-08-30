package com.mustafabulu.realtimefeatureplatform.streamworker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class TumblingWindowTest {

    @Test
    void returnsStartOfContainingWindow() {
        Instant eventTime = Instant.parse("2026-08-28T12:19:59.999Z");

        assertEquals(
                Instant.parse("2026-08-28T12:10:00Z"),
                TumblingWindow.startFor(eventTime, Duration.ofMinutes(10))
        );
    }

    @Test
    void rejectsNonPositiveWindowSize() {
        assertThrows(IllegalArgumentException.class,
                () -> TumblingWindow.startFor(Instant.parse("2026-08-28T12:10:00Z"), Duration.ZERO));
    }
}

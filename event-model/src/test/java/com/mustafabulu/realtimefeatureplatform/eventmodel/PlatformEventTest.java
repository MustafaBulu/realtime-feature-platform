package com.mustafabulu.realtimefeatureplatform.eventmodel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PlatformEventTest {

    @Test
    void normalizesNullPayloadToEmptyMap() {
        PlatformEvent event = new PlatformEvent(
                "event-1",
                "auth.failed",
                Instant.parse("2026-08-28T12:10:14.200Z"),
                new EntityRef("user", "u-1"),
                null
        );

        assertEquals(Map.of(), event.payload());
    }

    @Test
    void rejectsBlankEventType() {
        assertThrows(IllegalArgumentException.class, () -> new PlatformEvent(
                "event-1",
                " ",
                Instant.parse("2026-08-28T12:10:14.200Z"),
                new EntityRef("user", "u-1"),
                Map.of()
        ));
    }
}

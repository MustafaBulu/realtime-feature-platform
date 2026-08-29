package com.mustafabulu.realtimefeatureplatform.eventmodel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
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

    @Test
    void validatesRequestCompletedShape() {
        EventValidator validator = new EventValidator();

        validator.validate(RequestCompletedEventFactory.create(
                "event-1",
                "service",
                "catalog-api",
                100,
                Instant.parse("2026-08-28T12:10:14.200Z")
        ));
    }

    @Test
    void rejectsRequestCompletedWithoutNumericCount() {
        EventValidator validator = new EventValidator();
        PlatformEvent event = new PlatformEvent(
                "event-1",
                "request.completed",
                Instant.parse("2026-08-28T12:10:14.200Z"),
                new EntityRef("service", "catalog-api"),
                Map.of("count", "100")
        );

        assertThrows(EventValidationException.class, () -> validator.validate(event));
    }

    @Test
    void rejectsNegativeRequestCount() {
        EventValidator validator = new EventValidator();
        PlatformEvent event = RequestCompletedEventFactory.create(
                "event-1",
                "service",
                "catalog-api",
                -1,
                Instant.parse("2026-08-28T12:10:14.200Z")
        );

        assertThrows(EventValidationException.class, () -> validator.validate(event));
    }

    @Test
    void rejectsEventsTooFarInTheFuture() {
        Clock fixedClock = Clock.fixed(Instant.parse("2026-08-28T12:10:14.200Z"), ZoneOffset.UTC);
        EventValidator validator = new EventValidator(fixedClock, Duration.ofMinutes(5));
        PlatformEvent event = RequestCompletedEventFactory.create(
                "event-1",
                "service",
                "catalog-api",
                100,
                Instant.parse("2026-08-28T12:20:14.200Z")
        );

        assertThrows(EventValidationException.class, () -> validator.validate(event));
    }

    @Test
    void serializesAndDeserializesEvents() {
        PlatformEvent event = RequestCompletedEventFactory.create(
                "event-1",
                "service",
                "catalog-api",
                100,
                Instant.parse("2026-08-28T12:10:14.200Z")
        );

        PlatformEvent decoded = EventJsonCodec.fromJson(EventJsonCodec.toJson(event));

        assertEquals(event, decoded);
    }
}

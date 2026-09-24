package com.mustafabulu.realtimefeatureplatform.streamworker;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import com.mustafabulu.realtimefeatureplatform.eventmodel.RequestCompletedEventFactory;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ProcessedEventStoreTest {

    private final RequestCountTotalStateStore stateStore = mock(RequestCountTotalStateStore.class);
    private final Instant now = Instant.parse("2026-08-28T12:20:00Z");
    private final ProcessedEventStore processedEventStore = new ProcessedEventStore(
            stateStore,
            Clock.fixed(now, ZoneOffset.UTC),
            Duration.ofHours(24),
            Duration.ofMinutes(5),
            1000
    );

    @Test
    void marksEventIdOnlyOnce() {
        PlatformEvent event = event();
        Instant expiresAt = Instant.parse("2026-08-29T12:20:00Z");
        when(stateStore.markIfAbsent("processed-event:platform.events-0:event-1", expiresAt, now))
                .thenReturn(true, false);

        assertTrue(processedEventStore.markIfFirst(event, "platform.events-0"));
        assertFalse(processedEventStore.markIfFirst(event, "platform.events-0"));
        verify(stateStore).cleanupExpiredMarkers("processed-event:", now, 1000);
    }

    @Test
    void namespacesEventIdsByPartition() {
        PlatformEvent event = event();
        Instant expiresAt = Instant.parse("2026-08-29T12:20:00Z");
        when(stateStore.markIfAbsent("processed-event:platform.events-0:event-1", expiresAt, now)).thenReturn(true);
        when(stateStore.markIfAbsent("processed-event:platform.events-1:event-1", expiresAt, now)).thenReturn(true);

        assertTrue(processedEventStore.markIfFirst(event, "platform.events-0"));
        assertTrue(processedEventStore.markIfFirst(event, "platform.events-1"));
    }

    @Test
    void rejectsDuplicateBurstAfterFirstEvent() {
        PlatformEvent event = event();
        Instant expiresAt = Instant.parse("2026-08-29T12:20:00Z");
        when(stateStore.markIfAbsent("processed-event:platform.events-0:event-1", expiresAt, now))
                .thenReturn(true, false);

        assertTrue(processedEventStore.markIfFirst(event, "platform.events-0"));
        for (int duplicateIndex = 0; duplicateIndex < 100; duplicateIndex++) {
            assertFalse(processedEventStore.markIfFirst(event, "platform.events-0"));
        }
    }

    @Test
    void retainedMarkerSurvivesStoreRestart(@TempDir Path tempDir) {
        PlatformEvent event = event();
        try (RequestCountTotalStateStore firstStateStore =
                     new RequestCountTotalStateStore(tempDir.resolve("rocksdb"))) {
            ProcessedEventStore firstProcessedEventStore = new ProcessedEventStore(
                    firstStateStore,
                    Clock.fixed(now, ZoneOffset.UTC),
                    Duration.ofHours(24),
                    Duration.ofMinutes(5),
                    1000
            );
            assertTrue(firstProcessedEventStore.markIfFirst(event, "platform.events-0"));
        }

        try (RequestCountTotalStateStore secondStateStore =
                     new RequestCountTotalStateStore(tempDir.resolve("rocksdb"))) {
            ProcessedEventStore secondProcessedEventStore = new ProcessedEventStore(
                    secondStateStore,
                    Clock.fixed(now.plusSeconds(1), ZoneOffset.UTC),
                    Duration.ofHours(24),
                    Duration.ofMinutes(5),
                    1000
            );
            assertFalse(secondProcessedEventStore.markIfFirst(event, "platform.events-0"));
        }
    }

    @Test
    void expiredMarkerCanBeAcceptedAgain(@TempDir Path tempDir) {
        Instant expiresAt = now.plusSeconds(1);

        try (RequestCountTotalStateStore realStateStore =
                     new RequestCountTotalStateStore(tempDir.resolve("rocksdb"))) {
            assertTrue(realStateStore.markIfAbsent("processed-event:platform.events-0:event-1", expiresAt, now));
            assertFalse(realStateStore.markIfAbsent("processed-event:platform.events-0:event-1", expiresAt, now));
            assertTrue(realStateStore.markIfAbsent(
                    "processed-event:platform.events-0:event-1",
                    now.plusSeconds(10),
                    now.plusSeconds(2)
            ));
        }
    }

    @Test
    void cleanupDeletesOnlyExpiredMarkers(@TempDir Path tempDir) {
        Instant expiredAt = now.minusSeconds(1);
        Instant futureAt = now.plusSeconds(60);

        try (RequestCountTotalStateStore realStateStore =
                     new RequestCountTotalStateStore(tempDir.resolve("rocksdb"))) {
            assertTrue(realStateStore.markIfAbsent("processed-event:platform.events-0:expired", expiredAt, now.minusSeconds(2)));
            assertTrue(realStateStore.markIfAbsent("processed-event:platform.events-0:fresh", futureAt, now));
            realStateStore.cleanupExpiredMarkers("processed-event:", now, 1000);

            assertTrue(realStateStore.markIfAbsent("processed-event:platform.events-0:expired", futureAt, now));
            assertFalse(realStateStore.markIfAbsent("processed-event:platform.events-0:fresh", futureAt, now));
        }
    }

    private static PlatformEvent event() {
        return RequestCompletedEventFactory.create(
                "event-1",
                "service",
                "catalog-api",
                100,
                Instant.parse("2026-08-28T12:10:14.200Z")
        );
    }
}

package com.mustafabulu.realtimefeatureplatform.streamworker;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import com.mustafabulu.realtimefeatureplatform.eventmodel.RequestCompletedEventFactory;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class ProcessedEventStoreTest {

    private final RequestCountTotalStateStore stateStore = org.mockito.Mockito.mock(RequestCountTotalStateStore.class);
    private final ProcessedEventStore processedEventStore = new ProcessedEventStore(stateStore);

    @Test
    void marksEventIdOnlyOnce() {
        PlatformEvent event = RequestCompletedEventFactory.create(
                "event-1",
                "service",
                "catalog-api",
                100,
                Instant.parse("2026-08-28T12:10:14.200Z")
        );
        when(stateStore.markIfAbsent("processed-event:event-1")).thenReturn(true, false);

        assertTrue(processedEventStore.markIfFirst(event));
        assertFalse(processedEventStore.markIfFirst(event));
    }
}

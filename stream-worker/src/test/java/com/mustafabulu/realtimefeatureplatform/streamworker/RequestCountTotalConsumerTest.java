package com.mustafabulu.realtimefeatureplatform.streamworker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.mustafabulu.realtimefeatureplatform.eventmodel.EventJsonCodec;
import com.mustafabulu.realtimefeatureplatform.eventmodel.RequestCompletedEventFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class RequestCountTotalConsumerTest {

    private final PlatformEventProcessor processor = org.mockito.Mockito.mock(PlatformEventProcessor.class);
    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final StreamWorkerEventMetrics metrics = new StreamWorkerEventMetrics(meterRegistry);
    private final RequestCountTotalConsumer consumer = new RequestCountTotalConsumer(List.of(processor), metrics);

    @Test
    void delegatesValidEventToProcessor() {
        org.mockito.Mockito.when(processor.supports(org.mockito.Mockito.any())).thenReturn(true);
        String payload = EventJsonCodec.toJson(RequestCompletedEventFactory.create(
                "event-1",
                "service",
                "catalog-api",
                100,
                Instant.parse("2026-08-28T12:10:14.200Z")
        ));

        consumer.consume(payload);

        verify(processor).process(org.mockito.Mockito.any());
        assertEquals(0.0, meterRegistry.counter("rfp.worker.events.invalid").count());
        assertEquals(1.0, meterRegistry.counter("rfp.worker.events.processed").count());
    }

    @Test
    void discardsMalformedEventWithoutCallingProcessor() {
        consumer.consume("{\"eventId\":\"event-1\",\"eventType\":\"request.completed\"}");

        verify(processor, never()).process(org.mockito.Mockito.any());
        assertEquals(1.0, meterRegistry.counter("rfp.worker.events.invalid").count());
    }

    @Test
    void discardsEventsRejectedByProcessorValidation() {
        consumer.consume("""
                {
                  "eventId": "event-1",
                  "eventType": "request.completed",
                  "eventTime": "2026-08-28T12:10:14.200Z",
                  "entity": {"type": "service", "id": "catalog-api"},
                  "payload": {"count": "100"}
                }
                """);

        verify(processor, never()).process(org.mockito.Mockito.any());
        assertEquals(1.0, meterRegistry.counter("rfp.worker.events.invalid").count());
    }

    @Test
    void recordsIgnoredWhenNoProcessorSupportsValidEvent() {
        org.mockito.Mockito.when(processor.supports(org.mockito.Mockito.any())).thenReturn(false);

        consumer.consume("""
                {
                  "eventId": "event-1",
                  "eventType": "auth.failed",
                  "eventTime": "2026-08-28T12:10:14.200Z",
                  "entity": {"type": "user", "id": "u-1"},
                  "payload": {"reason": "bad_password"}
                }
                """);

        verify(processor, never()).process(org.mockito.Mockito.any());
        assertEquals(1.0, meterRegistry.counter("rfp.worker.events.ignored").count());
    }
}

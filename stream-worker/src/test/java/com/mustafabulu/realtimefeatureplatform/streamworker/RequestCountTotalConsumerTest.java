package com.mustafabulu.realtimefeatureplatform.streamworker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mustafabulu.realtimefeatureplatform.eventmodel.EventJsonCodec;
import com.mustafabulu.realtimefeatureplatform.eventmodel.RequestCompletedEventFactory;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RequestCountTotalConsumerTest {

    private final PlatformEventProcessor processor = mock(PlatformEventProcessor.class);
    private final ProcessedEventStore processedEventStore = mock(ProcessedEventStore.class);
    private final EventTimePolicy eventTimePolicy = new EventTimePolicy(
            Clock.fixed(Instant.parse("2026-08-28T12:20:00Z"), ZoneOffset.UTC),
            Duration.ofMinutes(10)
    );
    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final StreamWorkerEventMetrics metrics = new StreamWorkerEventMetrics(meterRegistry);
    private final RequestCountTotalConsumer consumer = new RequestCountTotalConsumer(
            List.of(processor),
            processedEventStore,
            eventTimePolicy,
            metrics
    );

    @BeforeEach
    void setUp() {
        when(processedEventStore.markIfFirst(any(), anyString())).thenReturn(true);
    }

    @Test
    void delegatesValidEventToProcessor() {
        when(processor.supports(any())).thenReturn(true);
        String payload = EventJsonCodec.toJson(RequestCompletedEventFactory.create(
                "event-1",
                "service",
                "catalog-api",
                100,
                Instant.parse("2026-08-28T12:10:14.200Z")
        ));

        consumer.consume(payload);

        verify(processor).process(
                any(),
                any(EventTimeAssessment.class)
        );
        assertEquals(0.0, meterRegistry.counter("rfp.worker.events.invalid").count());
        assertEquals(1.0, meterRegistry.counter("rfp.worker.events.processed").count());
    }

    @Test
    void usesKafkaPartitionNamespaceForDedupKey() {
        when(processor.supports(any())).thenReturn(true);
        String payload = EventJsonCodec.toJson(RequestCompletedEventFactory.create(
                "event-1",
                "service",
                "catalog-api",
                100,
                Instant.parse("2026-08-28T12:10:14.200Z")
        ));

        consumer.consume(new ConsumerRecord<>("platform.events", 3, 42L, "catalog-api", payload));

        verify(processedEventStore).markIfFirst(any(), eq("platform.events-3"));
    }

    @Test
    void discardsMalformedEventWithoutCallingProcessor() {
        consumer.consume("{\"eventId\":\"event-1\",\"eventType\":\"request.completed\"}");

        verify(processor, never()).process(
                any(),
                any(EventTimeAssessment.class)
        );
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

        verify(processor, never()).process(
                any(),
                any(EventTimeAssessment.class)
        );
        assertEquals(1.0, meterRegistry.counter("rfp.worker.events.invalid").count());
    }

    @Test
    void recordsIgnoredWhenNoProcessorSupportsValidEvent() {
        when(processor.supports(any())).thenReturn(false);

        consumer.consume("""
                {
                  "eventId": "event-1",
                  "eventType": "auth.failed",
                  "eventTime": "2026-08-28T12:10:14.200Z",
                  "entity": {"type": "user", "id": "u-1"},
                  "payload": {"reason": "bad_password"}
                }
                """);

        verify(processor, never()).process(
                any(),
                any(EventTimeAssessment.class)
        );
        assertEquals(1.0, meterRegistry.counter("rfp.worker.events.ignored").count());
    }

    @Test
    void discardsDuplicateEventWithoutCallingProcessor() {
        when(processedEventStore.markIfFirst(any(), anyString())).thenReturn(false);
        String payload = EventJsonCodec.toJson(RequestCompletedEventFactory.create(
                "event-1",
                "service",
                "catalog-api",
                100,
                Instant.parse("2026-08-28T12:10:14.200Z")
        ));

        consumer.consume(payload);

        verify(processor, never()).process(
                any(),
                any(EventTimeAssessment.class)
        );
        assertEquals(1.0, meterRegistry.counter("rfp.worker.events.duplicate").count());
    }

    @Test
    void discardsTooLateEventWithoutCallingProcessorOrDedupStore() {
        String payload = EventJsonCodec.toJson(RequestCompletedEventFactory.create(
                "event-1",
                "service",
                "catalog-api",
                100,
                Instant.parse("2026-08-28T12:09:59.999Z")
        ));

        consumer.consume(payload);

        verify(processedEventStore, never()).markIfFirst(
                any(),
                anyString()
        );
        verify(processor, never()).process(
                any(),
                any(EventTimeAssessment.class)
        );
        assertEquals(1.0, meterRegistry.counter("rfp.worker.events.late").count());
    }
}

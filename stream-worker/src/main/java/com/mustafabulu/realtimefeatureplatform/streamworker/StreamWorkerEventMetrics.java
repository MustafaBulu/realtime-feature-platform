package com.mustafabulu.realtimefeatureplatform.streamworker;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
class StreamWorkerEventMetrics {

    private final Counter processedEvents;
    private final Counter invalidEvents;
    private final Counter ignoredEvents;
    private final Counter duplicateEvents;
    private final Counter lateEvents;
    private final Counter acceptedLateEvents;
    private final Counter rejectedLateEvents;

    StreamWorkerEventMetrics(MeterRegistry meterRegistry) {
        this.processedEvents = Counter.builder("rfp.worker.events.processed")
                .description("Events successfully processed by the stream worker")
                .register(meterRegistry);
        this.invalidEvents = Counter.builder("rfp.worker.events.invalid")
                .description("Events discarded because they failed deserialization or validation")
                .register(meterRegistry);
        this.ignoredEvents = Counter.builder("rfp.worker.events.ignored")
                .description("Valid events ignored because no feature processor handles their type")
                .register(meterRegistry);
        this.duplicateEvents = Counter.builder("rfp.worker.events.duplicate")
                .description("Events discarded because their eventId was already processed")
                .register(meterRegistry);
        this.lateEvents = Counter.builder("rfp.worker.events.late")
                .description("Events discarded because their eventTime is outside the allowed lateness")
                .register(meterRegistry);
        this.acceptedLateEvents = Counter.builder("rfp.worker.events.late.accepted")
                .description("Late events accepted inside the allowed lateness correction window")
                .register(meterRegistry);
        this.rejectedLateEvents = Counter.builder("rfp.worker.events.late.rejected")
                .description("Late events rejected because their eventTime is outside the allowed lateness")
                .register(meterRegistry);
    }

    void recordProcessed() {
        processedEvents.increment();
    }

    void recordInvalid() {
        invalidEvents.increment();
    }

    void recordIgnored() {
        ignoredEvents.increment();
    }

    void recordDuplicate() {
        duplicateEvents.increment();
    }

    void recordLate() {
        lateEvents.increment();
        rejectedLateEvents.increment();
    }

    void recordAcceptedLate() {
        acceptedLateEvents.increment();
    }
}

package com.mustafabulu.realtimefeatureplatform.streamworker;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
class StreamWorkerEventMetrics {

    private final Counter processedEvents;
    private final Counter invalidEvents;
    private final Counter ignoredEvents;

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
}

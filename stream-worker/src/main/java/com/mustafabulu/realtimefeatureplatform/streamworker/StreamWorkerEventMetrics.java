package com.mustafabulu.realtimefeatureplatform.streamworker;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import org.springframework.stereotype.Component;

@Component
class StreamWorkerEventMetrics {

    private final Counter receivedEvents;
    private final Counter processedEvents;
    private final Counter invalidEvents;
    private final Counter ignoredEvents;
    private final Counter duplicateEvents;
    private final Counter lateEvents;
    private final Counter acceptedLateEvents;
    private final Counter rejectedLateEvents;
    private final Counter assignedPartitions;
    private final Counter revokedPartitions;
    private final DistributionSummary ingestionLagMillis;
    private final Timer restoreDuration;

    StreamWorkerEventMetrics(MeterRegistry meterRegistry) {
        this.receivedEvents = Counter.builder("rfp.worker.events.received")
                .description("Events received by the stream worker Kafka listener")
                .register(meterRegistry);
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
        this.assignedPartitions = Counter.builder("rfp.worker.kafka.partitions.assigned")
                .description("Kafka partitions assigned to this worker")
                .register(meterRegistry);
        this.revokedPartitions = Counter.builder("rfp.worker.kafka.partitions.revoked")
                .description("Kafka partitions revoked or lost from this worker")
                .register(meterRegistry);
        this.ingestionLagMillis = DistributionSummary.builder("rfp.worker.ingestion.lag.millis")
                .description("Difference between processing time and event time in milliseconds")
                .baseUnit("milliseconds")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(meterRegistry);
        this.restoreDuration = Timer.builder("rfp.worker.restore.duration")
                .description("Duration of worker restore and deterministic Redis republish")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(meterRegistry);
    }

    void recordReceived() {
        receivedEvents.increment();
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

    void recordIngestionLag(Duration lag) {
        ingestionLagMillis.record(Math.max(0.0, lag.toMillis()));
    }

    void recordRestoreDuration(Duration duration) {
        restoreDuration.record(duration);
    }

    void recordPartitionsAssigned(int count) {
        assignedPartitions.increment(count);
    }

    void recordPartitionsRevoked(int count) {
        revokedPartitions.increment(count);
    }
}

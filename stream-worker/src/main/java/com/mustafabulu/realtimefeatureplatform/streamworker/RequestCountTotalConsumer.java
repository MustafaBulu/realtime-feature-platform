package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.eventmodel.EventJsonCodec;
import com.mustafabulu.realtimefeatureplatform.eventmodel.EventValidationException;
import com.mustafabulu.realtimefeatureplatform.eventmodel.EventValidator;
import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
class RequestCountTotalConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RequestCountTotalConsumer.class);

    private final EventValidator eventValidator = new EventValidator();
    private final List<PlatformEventProcessor> processors;
    private final ProcessedEventStore processedEventStore;
    private final EventTimePolicy eventTimePolicy;
    private final StreamWorkerEventMetrics metrics;
    private final CheckpointManager checkpointManager;

    @Autowired
    RequestCountTotalConsumer(
            List<PlatformEventProcessor> processors,
            ProcessedEventStore processedEventStore,
            EventTimePolicy eventTimePolicy,
            StreamWorkerEventMetrics metrics,
            CheckpointManager checkpointManager
    ) {
        this.processors = List.copyOf(processors);
        this.processedEventStore = processedEventStore;
        this.eventTimePolicy = eventTimePolicy;
        this.metrics = metrics;
        this.checkpointManager = checkpointManager;
    }

    RequestCountTotalConsumer(
            List<PlatformEventProcessor> processors,
            ProcessedEventStore processedEventStore,
            EventTimePolicy eventTimePolicy,
            StreamWorkerEventMetrics metrics
    ) {
        this(processors, processedEventStore, eventTimePolicy, metrics, null);
    }

    @KafkaListener(topics = "${rfp.kafka.events-topic}")
    void consume(ConsumerRecord<String, String> consumerRecord, Acknowledgment acknowledgment) {
        boolean terminal = consume(
                consumerRecord.value(),
                consumerRecord.topic() + "-" + consumerRecord.partition()
        );
        if (terminal && checkpointManager != null) {
            checkpointManager.markSafe(consumerRecord);
        }
        if (terminal && acknowledgment != null) {
            acknowledgment.acknowledge();
        }
    }

    void consume(ConsumerRecord<String, String> consumerRecord) {
        consume(consumerRecord, null);
    }

    void consume(String eventPayload) {
        consume(eventPayload, "unknown");
    }

    private boolean consume(String eventPayload, String partitionNamespace) {
        metrics.recordReceived();
        try {
            PlatformEvent event = EventJsonCodec.fromJson(eventPayload);
            eventValidator.validate(event);

            EventTimeAssessment assessment = eventTimePolicy.assess(event);
            metrics.recordIngestionLag(Duration.between(event.eventTime(), assessment.processingTime()));
            if (assessment.tooLate()) {
                metrics.recordLate();
                LOGGER.warn("Discarded late platform event: eventId={}", event.eventId());
                return true;
            }
            if (processedEventStore.hasProcessed(event, partitionNamespace)) {
                metrics.recordDuplicate();
                LOGGER.warn("Discarded duplicate platform event: eventId={}", event.eventId());
                return true;
            }
            if (assessment.lateWithinAllowed()) {
                metrics.recordAcceptedLate();
            }

            boolean handled = false;
            for (PlatformEventProcessor processor : processors) {
                if (processor.supports(event)) {
                    processor.process(event, assessment);
                    handled = true;
                }
            }
            if (handled) {
                metrics.recordProcessed();
            } else {
                metrics.recordIgnored();
            }
            processedEventStore.markProcessed(event, partitionNamespace);
            return true;
        } catch (IllegalArgumentException | EventValidationException ex) {
            metrics.recordInvalid();
            LOGGER.warn("Discarded invalid platform event: {}", ex.getMessage());
            return true;
        }
    }
}

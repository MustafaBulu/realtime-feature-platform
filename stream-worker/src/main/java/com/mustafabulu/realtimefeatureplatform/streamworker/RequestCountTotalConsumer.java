package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.eventmodel.EventJsonCodec;
import com.mustafabulu.realtimefeatureplatform.eventmodel.EventValidationException;
import com.mustafabulu.realtimefeatureplatform.eventmodel.EventValidator;
import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
class RequestCountTotalConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RequestCountTotalConsumer.class);

    private final EventValidator eventValidator = new EventValidator();
    private final List<PlatformEventProcessor> processors;
    private final ProcessedEventStore processedEventStore;
    private final EventTimePolicy eventTimePolicy;
    private final StreamWorkerEventMetrics metrics;

    RequestCountTotalConsumer(
            List<PlatformEventProcessor> processors,
            ProcessedEventStore processedEventStore,
            EventTimePolicy eventTimePolicy,
            StreamWorkerEventMetrics metrics
    ) {
        this.processors = List.copyOf(processors);
        this.processedEventStore = processedEventStore;
        this.eventTimePolicy = eventTimePolicy;
        this.metrics = metrics;
    }

    @KafkaListener(topics = "${rfp.kafka.events-topic}")
    void consume(ConsumerRecord<String, String> record) {
        consume(record.value(), record.topic() + "-" + record.partition());
    }

    void consume(String eventPayload) {
        consume(eventPayload, "unknown");
    }

    private void consume(String eventPayload, String partitionNamespace) {
        try {
            PlatformEvent event = EventJsonCodec.fromJson(eventPayload);
            eventValidator.validate(event);

            EventTimeAssessment assessment = eventTimePolicy.assess(event);
            if (assessment.tooLate()) {
                metrics.recordLate();
                LOGGER.warn("Discarded late platform event: eventId={}", event.eventId());
                return;
            }
            if (!processedEventStore.markIfFirst(event, partitionNamespace)) {
                metrics.recordDuplicate();
                LOGGER.warn("Discarded duplicate platform event: eventId={}", event.eventId());
                return;
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
        } catch (IllegalArgumentException | EventValidationException ex) {
            metrics.recordInvalid();
            LOGGER.warn("Discarded invalid platform event: {}", ex.getMessage());
        }
    }
}

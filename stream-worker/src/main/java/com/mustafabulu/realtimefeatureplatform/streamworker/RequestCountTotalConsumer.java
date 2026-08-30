package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.eventmodel.EventJsonCodec;
import com.mustafabulu.realtimefeatureplatform.eventmodel.EventValidationException;
import com.mustafabulu.realtimefeatureplatform.eventmodel.EventValidator;
import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
class RequestCountTotalConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RequestCountTotalConsumer.class);

    private final EventValidator eventValidator = new EventValidator();
    private final List<PlatformEventProcessor> processors;
    private final StreamWorkerEventMetrics metrics;

    RequestCountTotalConsumer(List<PlatformEventProcessor> processors, StreamWorkerEventMetrics metrics) {
        this.processors = List.copyOf(processors);
        this.metrics = metrics;
    }

    @KafkaListener(topics = "${rfp.kafka.events-topic}")
    void consume(String eventPayload) {
        try {
            PlatformEvent event = EventJsonCodec.fromJson(eventPayload);
            eventValidator.validate(event);

            boolean handled = false;
            for (PlatformEventProcessor processor : processors) {
                if (processor.supports(event)) {
                    processor.process(event);
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

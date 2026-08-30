package com.mustafabulu.realtimefeatureplatform.workloadgenerator;

import com.mustafabulu.realtimefeatureplatform.eventmodel.EventValidator;
import com.mustafabulu.realtimefeatureplatform.eventmodel.EventJsonCodec;
import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import com.mustafabulu.realtimefeatureplatform.eventmodel.RequestCompletedEventFactory;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
class RequestCountWorkloadProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final EventValidator eventValidator;
    private final String eventsTopic;

    RequestCountWorkloadProducer(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${rfp.kafka.events-topic}") String eventsTopic
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.eventValidator = new EventValidator();
        this.eventsTopic = eventsTopic;
    }

    List<PlatformEvent> produce(RequestCountWorkloadController.WorkloadRequest request) {
        List<PlatformEvent> events = request.samples().stream()
                .map(sample -> RequestCompletedEventFactory.create(
                        request.entityType(),
                        request.entityId(),
                        sample.count(),
                        sample.statusCode(),
                        sample.latencyMs()
                ))
                .peek(eventValidator::validate)
                .peek(event -> kafkaTemplate.send(eventsTopic, event.entity().id(), EventJsonCodec.toJson(event)))
                .toList();

        kafkaTemplate.flush();
        return events;
    }
}

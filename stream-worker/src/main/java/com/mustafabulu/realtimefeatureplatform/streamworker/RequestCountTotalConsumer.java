package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.eventmodel.EventJsonCodec;
import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
class RequestCountTotalConsumer {

    private final RequestCountTotalProcessor processor;

    RequestCountTotalConsumer(RequestCountTotalProcessor processor) {
        this.processor = processor;
    }

    @KafkaListener(topics = "${rfp.kafka.events-topic}")
    void consume(String eventPayload) {
        PlatformEvent event = EventJsonCodec.fromJson(eventPayload);

        processor.process(event);
    }
}

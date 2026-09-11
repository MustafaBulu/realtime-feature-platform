package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import org.springframework.stereotype.Component;

@Component
class ProcessedEventStore {

    private static final String PREFIX = "processed-event:";

    private final RequestCountTotalStateStore stateStore;

    ProcessedEventStore(RequestCountTotalStateStore stateStore) {
        this.stateStore = stateStore;
    }

    boolean markIfFirst(PlatformEvent event) {
        return stateStore.markIfAbsent(PREFIX + event.eventId());
    }
}

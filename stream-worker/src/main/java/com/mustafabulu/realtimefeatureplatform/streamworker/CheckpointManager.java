package com.mustafabulu.realtimefeatureplatform.streamworker;

import java.time.Clock;
import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
class CheckpointManager {

    private final CheckpointStore checkpointStore;
    private final RequestCountTotalStateStore stateStore;
    private final Clock clock;

    @Autowired
    CheckpointManager(CheckpointStore checkpointStore, RequestCountTotalStateStore stateStore) {
        this(checkpointStore, stateStore, Clock.systemUTC());
    }

    CheckpointManager(CheckpointStore checkpointStore, RequestCountTotalStateStore stateStore, Clock clock) {
        this.checkpointStore = checkpointStore;
        this.stateStore = stateStore;
        this.clock = clock;
    }

    CheckpointMetadata markSafe(ConsumerRecord<?, ?> record) {
        return markSafe(record.topic(), record.partition(), record.offset());
    }

    CheckpointMetadata markSafe(String topic, int partition, long processedOffset) {
        Instant now = clock.instant();
        CheckpointMetadata metadata = new CheckpointMetadata(
                topic,
                partition,
                processedOffset,
                processedOffset + 1L,
                now
        );
        checkpointStore.put(metadata);
        stateStore.flush();
        return metadata;
    }
}

package com.mustafabulu.realtimefeatureplatform.streamworker;

import java.util.List;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

@Component
class CheckpointStore {

    private static final String PREFIX = "checkpoint:";

    private final RequestCountTotalStateStore stateStore;

    CheckpointStore(RequestCountTotalStateStore stateStore) {
        this.stateStore = stateStore;
    }

    void put(CheckpointMetadata metadata) {
        stateStore.put(key(metadata.topic(), metadata.partition()), metadata.serialize());
    }

    CheckpointMetadata get(TopicPartition partition) {
        String value = stateStore.get(key(partition.topic(), partition.partition()));
        return value == null ? null : CheckpointMetadata.parse(value);
    }

    List<CheckpointMetadata> all() {
        return stateStore.entriesWithPrefix(PREFIX).stream()
                .map(RequestCountTotalStateStore.StateEntry::value)
                .map(CheckpointMetadata::parse)
                .toList();
    }

    private static String key(String topic, int partition) {
        return PREFIX + topic + ":" + partition;
    }
}

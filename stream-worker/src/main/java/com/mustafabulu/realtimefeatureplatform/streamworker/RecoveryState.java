package com.mustafabulu.realtimefeatureplatform.streamworker;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

@Component
class RecoveryState {

    private final Set<TopicPartition> assignedPartitions = ConcurrentHashMap.newKeySet();
    private volatile boolean ready;
    private volatile Instant restoredAt;
    private volatile Duration restoreDuration = Duration.ZERO;
    private volatile int republishedFeatureKeys;

    void markRestored(Instant restoredAt, Duration restoreDuration, int republishedFeatureKeys) {
        this.restoredAt = restoredAt;
        this.restoreDuration = restoreDuration;
        this.republishedFeatureKeys = republishedFeatureKeys;
        this.ready = true;
    }

    void markRecovering() {
        this.ready = false;
    }

    void assign(Collection<TopicPartition> partitions) {
        assignedPartitions.addAll(partitions);
    }

    void revoke(Collection<TopicPartition> partitions) {
        assignedPartitions.removeAll(partitions);
    }

    boolean ready() {
        return ready;
    }

    Instant restoredAt() {
        return restoredAt;
    }

    Duration restoreDuration() {
        return restoreDuration;
    }

    int republishedFeatureKeys() {
        return republishedFeatureKeys;
    }

    Set<TopicPartition> assignedPartitions() {
        return Set.copyOf(assignedPartitions);
    }
}

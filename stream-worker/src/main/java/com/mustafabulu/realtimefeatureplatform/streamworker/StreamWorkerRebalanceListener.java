package com.mustafabulu.realtimefeatureplatform.streamworker;

import java.util.Collection;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.stereotype.Component;

@Component
class StreamWorkerRebalanceListener implements ConsumerAwareRebalanceListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamWorkerRebalanceListener.class);

    private final RequestCountTotalStateStore stateStore;
    private final RedisStateRepublisher republisher;
    private final RecoveryState recoveryState;
    private final StreamWorkerEventMetrics metrics;

    StreamWorkerRebalanceListener(
            RequestCountTotalStateStore stateStore,
            RedisStateRepublisher republisher,
            RecoveryState recoveryState,
            StreamWorkerEventMetrics metrics
    ) {
        this.stateStore = stateStore;
        this.republisher = republisher;
        this.recoveryState = recoveryState;
        this.metrics = metrics;
    }

    @Override
    public void onPartitionsRevokedBeforeCommit(
            Consumer<?, ?> consumer,
            Collection<TopicPartition> partitions
    ) {
        stateStore.flush();
        recoveryState.revoke(partitions);
        metrics.recordPartitionsRevoked(partitions.size());
        LOGGER.info("Kafka partitions revoked: partitions={}", partitions);
    }

    @Override
    public void onPartitionsLost(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        recoveryState.revoke(partitions);
        metrics.recordPartitionsRevoked(partitions.size());
        LOGGER.warn("Kafka partitions lost: partitions={}", partitions);
    }

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        recoveryState.markRecovering();
        int republished = republisher.republishAll();
        stateStore.flush();
        recoveryState.assign(partitions);
        recoveryState.markRestored(java.time.Instant.now(), java.time.Duration.ZERO, republished);
        metrics.recordPartitionsAssigned(partitions.size());
        LOGGER.info("Kafka partitions assigned: partitions={}, republishedFeatureKeys={}", partitions, republished);
    }
}

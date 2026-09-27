package com.mustafabulu.realtimefeatureplatform.streamworker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class StreamWorkerRebalanceListenerTest {

    private final RequestCountTotalStateStore stateStore = mock(RequestCountTotalStateStore.class);
    private final RedisStateRepublisher republisher = mock(RedisStateRepublisher.class);
    private final RecoveryState recoveryState = new RecoveryState();
    private final SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
    private final StreamWorkerEventMetrics metrics = new StreamWorkerEventMetrics(meterRegistry);
    private final StreamWorkerRebalanceListener listener = new StreamWorkerRebalanceListener(
            stateStore,
            republisher,
            recoveryState,
            metrics
    );

    @Test
    void flushesAndTracksRevokedPartitions() {
        TopicPartition partition = new TopicPartition("platform.events", 0);
        recoveryState.assign(List.of(partition));

        listener.onPartitionsRevokedBeforeCommit(null, List.of(partition));

        verify(stateStore).flush();
        assertTrue(recoveryState.assignedPartitions().isEmpty());
        assertEquals(1.0, meterRegistry.counter("rfp.worker.kafka.partitions.revoked").count());
    }

    @Test
    void republishesStateWhenPartitionsAreAssigned() {
        TopicPartition partition = new TopicPartition("platform.events", 1);
        when(republisher.republishAll()).thenReturn(3);

        listener.onPartitionsAssigned(null, List.of(partition));

        verify(republisher).republishAll();
        verify(stateStore).flush();
        assertTrue(recoveryState.ready());
        assertTrue(recoveryState.assignedPartitions().contains(partition));
        assertEquals(3, recoveryState.republishedFeatureKeys());
        assertEquals(1.0, meterRegistry.counter("rfp.worker.kafka.partitions.assigned").count());
    }
}

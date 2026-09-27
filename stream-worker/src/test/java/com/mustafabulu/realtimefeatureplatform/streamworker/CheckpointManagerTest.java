package com.mustafabulu.realtimefeatureplatform.streamworker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CheckpointManagerTest {

    @Test
    void persistsLastProcessedAndLastSafeOffset(@TempDir Path tempDir) {
        Instant now = Instant.parse("2026-08-28T12:20:00Z");
        try (RequestCountTotalStateStore stateStore =
                     new RequestCountTotalStateStore(tempDir.resolve("rocksdb"))) {
            CheckpointStore checkpointStore = new CheckpointStore(stateStore);
            CheckpointManager manager = new CheckpointManager(
                    checkpointStore,
                    stateStore,
                    Clock.fixed(now, ZoneOffset.UTC)
            );

            manager.markSafe(new ConsumerRecord<>("platform.events", 2, 41L, "key", "value"));

            CheckpointMetadata checkpoint = checkpointStore.get(new TopicPartition("platform.events", 2));
            assertEquals("platform.events", checkpoint.topic());
            assertEquals(2, checkpoint.partition());
            assertEquals(41L, checkpoint.lastProcessedOffset());
            assertEquals(42L, checkpoint.lastSafeOffset());
            assertEquals(now, checkpoint.updatedAt());
        }
    }
}

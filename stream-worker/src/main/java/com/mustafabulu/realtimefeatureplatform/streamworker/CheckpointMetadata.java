package com.mustafabulu.realtimefeatureplatform.streamworker;

import java.time.Instant;

record CheckpointMetadata(
        String topic,
        int partition,
        long lastProcessedOffset,
        long lastSafeOffset,
        Instant updatedAt
) {

    String serialize() {
        return topic + "|" + partition + "|" + lastProcessedOffset + "|" + lastSafeOffset + "|" + updatedAt;
    }

    static CheckpointMetadata parse(String value) {
        String[] parts = value.split("\\|", -1);
        if (parts.length != 5) {
            throw new IllegalArgumentException("invalid checkpoint metadata");
        }
        return new CheckpointMetadata(
                parts[0],
                Integer.parseInt(parts[1]),
                Long.parseLong(parts[2]),
                Long.parseLong(parts[3]),
                Instant.parse(parts[4])
        );
    }
}

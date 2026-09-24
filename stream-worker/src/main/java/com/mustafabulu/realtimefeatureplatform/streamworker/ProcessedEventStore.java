package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
class ProcessedEventStore {

    private static final String PREFIX = "processed-event:";
    private static final String DEFAULT_PARTITION_NAMESPACE = "unknown";

    private final RequestCountTotalStateStore stateStore;
    private final Clock clock;
    private final Duration retention;
    private final Duration cleanupInterval;
    private final int cleanupBatchSize;
    private Instant nextCleanupAt;

    @Autowired
    ProcessedEventStore(
            RequestCountTotalStateStore stateStore,
            @Value("${rfp.dedup.retention:PT24H}") Duration retention,
            @Value("${rfp.dedup.cleanup-interval:PT5M}") Duration cleanupInterval,
            @Value("${rfp.dedup.cleanup-batch-size:1000}") int cleanupBatchSize
    ) {
        this(stateStore, Clock.systemUTC(), retention, cleanupInterval, cleanupBatchSize);
    }

    ProcessedEventStore(
            RequestCountTotalStateStore stateStore,
            Clock clock,
            Duration retention,
            Duration cleanupInterval,
            int cleanupBatchSize
    ) {
        if (retention.isZero() || retention.isNegative()) {
            throw new IllegalArgumentException("dedup retention must be positive");
        }
        if (cleanupInterval.isNegative()) {
            throw new IllegalArgumentException("dedup cleanup interval must not be negative");
        }
        if (cleanupBatchSize < 0) {
            throw new IllegalArgumentException("dedup cleanup batch size must not be negative");
        }
        this.stateStore = stateStore;
        this.clock = clock;
        this.retention = retention;
        this.cleanupInterval = cleanupInterval;
        this.cleanupBatchSize = cleanupBatchSize;
        this.nextCleanupAt = Instant.EPOCH;
    }

    boolean markIfFirst(PlatformEvent event, String partitionNamespace) {
        Instant now = clock.instant();
        cleanupIfDue(now);
        return stateStore.markIfAbsent(key(partitionNamespace, event.eventId()), now.plus(retention), now);
    }

    private void cleanupIfDue(Instant now) {
        if (cleanupBatchSize == 0 || now.isBefore(nextCleanupAt)) {
            return;
        }
        stateStore.cleanupExpiredMarkers(PREFIX, now, cleanupBatchSize);
        nextCleanupAt = now.plus(cleanupInterval);
    }

    private static String key(String partitionNamespace, String eventId) {
        String namespace = partitionNamespace == null || partitionNamespace.isBlank()
                ? DEFAULT_PARTITION_NAMESPACE
                : partitionNamespace;
        return PREFIX + namespace + ":" + eventId;
    }
}

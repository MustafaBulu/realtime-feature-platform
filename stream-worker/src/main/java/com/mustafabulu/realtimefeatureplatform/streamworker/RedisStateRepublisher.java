package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.featuremodel.AggregationType;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinition;
import com.mustafabulu.realtimefeatureplatform.featuremodel.WindowType;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
class RedisStateRepublisher {

    private static final String FEATURE_PREFIX = "feature:";
    private static final String LATEST_WINDOW_SUFFIX = ":latest-window-start";

    private final RequestCountTotalStateStore stateStore;
    private final WorkerFeatureDefinitionLoader definitionLoader;
    private final FeatureRedisMaterializer materializer;
    private final Clock clock;

    @Autowired
    RedisStateRepublisher(
            RequestCountTotalStateStore stateStore,
            WorkerFeatureDefinitionLoader definitionLoader,
            FeatureRedisMaterializer materializer
    ) {
        this(stateStore, definitionLoader, materializer, Clock.systemUTC());
    }

    RedisStateRepublisher(
            RequestCountTotalStateStore stateStore,
            WorkerFeatureDefinitionLoader definitionLoader,
            FeatureRedisMaterializer materializer,
            Clock clock
    ) {
        this.stateStore = stateStore;
        this.definitionLoader = definitionLoader;
        this.materializer = materializer;
        this.clock = clock;
    }

    int republishAll() {
        Instant restoredAt = clock.instant();
        int published = 0;
        List<RequestCountTotalStateStore.StateEntry> entries = stateStore.entriesWithPrefix(FEATURE_PREFIX);
        for (RequestCountTotalStateStore.StateEntry entry : entries) {
            if (entry.key().endsWith(LATEST_WINDOW_SUFFIX)) {
                continue;
            }
            published += publishStateEntry(entry.key(), entry.value(), restoredAt);
        }
        for (RequestCountTotalStateStore.StateEntry entry : entries) {
            if (!entry.key().endsWith(LATEST_WINDOW_SUFFIX)) {
                continue;
            }
            published += publishLatestAlias(entry.key(), entry.value(), restoredAt);
        }
        return published;
    }

    private int publishStateEntry(String key, String stateValue, Instant restoredAt) {
        FeatureStateKey stateKey = FeatureStateKey.parse(key);
        FeatureDefinition definition = activeDefinition(stateKey);
        if (definition == null) {
            return 0;
        }
        Duration ttl = ttl(definition, stateKey.windowStart(), restoredAt);
        if (definition.windowType() != WindowType.NONE && ttl == null) {
            return 0;
        }
        materializer.materializeRestored(
                key,
                materializedValue(definition.aggregationType(), stateValue),
                restoredAt,
                definition.version(),
                ttl
        );
        return 1;
    }

    private int publishLatestAlias(String markerKey, String markerValue, Instant restoredAt) {
        String featureKey = markerKey.substring(0, markerKey.length() - LATEST_WINDOW_SUFFIX.length());
        String windowKey = featureKey + ":window:" + markerValue;
        String stateValue = stateStore.get(windowKey);
        if (stateValue == null) {
            return 0;
        }
        FeatureStateKey stateKey = FeatureStateKey.parse(windowKey);
        FeatureDefinition definition = activeDefinition(stateKey);
        if (definition == null || ttl(definition, stateKey.windowStart(), restoredAt) == null) {
            return 0;
        }
        materializer.materializeRestored(
                featureKey,
                materializedValue(definition.aggregationType(), stateValue),
                restoredAt,
                definition.version(),
                null
        );
        return 1;
    }

    private FeatureDefinition activeDefinition(FeatureStateKey key) {
        return definitionLoader.activeDefinitions().stream()
                .filter(definition -> definition.name().equals(key.featureName()))
                .filter(definition -> definition.entityType().equals(key.entityType())
                        || FeatureDefinition.ALL_ENTITY_TYPES.equals(definition.entityType()))
                .findFirst()
                .orElse(null);
    }

    private static Number materializedValue(AggregationType aggregationType, String stateValue) {
        return switch (aggregationType) {
            case COUNT -> NumericStateFormat.readLong(stateValue);
            case SUM -> NumericStateFormat.asNumber(NumericStateFormat.readDouble(stateValue));
            case AVG -> AverageState.parse(stateValue).value();
            case DISTINCT_COUNT -> DistinctCountState.parse(stateValue).count();
            case RATIO -> RatioState.parse(stateValue).value();
        };
    }

    private static Duration ttl(FeatureDefinition definition, Instant windowStart, Instant now) {
        if (definition.windowType() == WindowType.NONE) {
            return null;
        }
        Instant expiresAt = windowStart.plus(definition.windowSize()).plus(definition.slide());
        if (!expiresAt.isAfter(now)) {
            return null;
        }
        return Duration.between(now, expiresAt);
    }

    private record FeatureStateKey(String entityType, String entityId, String featureName, Instant windowStart) {

        static FeatureStateKey parse(String key) {
            String[] parts = key.split(":", -1);
            if (parts.length == 4) {
                return new FeatureStateKey(parts[1], parts[2], parts[3], null);
            }
            if (parts.length == 6 && "window".equals(parts[4])) {
                return new FeatureStateKey(
                        parts[1],
                        parts[2],
                        parts[3],
                        Instant.ofEpochMilli(Long.parseLong(parts[5]))
                );
            }
            throw new IllegalArgumentException("invalid feature state key " + key);
        }
    }
}

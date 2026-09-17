package com.mustafabulu.realtimefeatureplatform.featuremodel;

import java.time.Duration;
import java.util.List;

public final class BuiltInFeatureDefinitions {

    private static final String REQUEST_COMPLETED = "request.completed";
    private static final String SERVICE_ENTITY_TYPE = "service";

    private BuiltInFeatureDefinitions() {
    }

    public static List<FeatureDefinition> all() {
        return List.of(
                requestCountTotal(),
                entityEventCountTenMinute(),
                entityAverageLatencyFiveMinute()
        );
    }

    private static FeatureDefinition requestCountTotal() {
        return new FeatureDefinition(
                FeatureNames.REQUEST_COUNT_TOTAL,
                REQUEST_COMPLETED,
                SERVICE_ENTITY_TYPE,
                AggregationType.SUM,
                "count",
                null,
                WindowType.NONE,
                null,
                null,
                1,
                FeatureDefinitionState.ACTIVE
        );
    }

    private static FeatureDefinition entityEventCountTenMinute() {
        return new FeatureDefinition(
                FeatureNames.ENTITY_EVENT_COUNT_10M,
                REQUEST_COMPLETED,
                SERVICE_ENTITY_TYPE,
                AggregationType.COUNT,
                null,
                null,
                WindowType.TUMBLING,
                Duration.ofMinutes(10),
                null,
                1,
                FeatureDefinitionState.ACTIVE
        );
    }

    private static FeatureDefinition entityAverageLatencyFiveMinute() {
        return new FeatureDefinition(
                FeatureNames.ENTITY_AVG_LATENCY_MS_5M,
                REQUEST_COMPLETED,
                SERVICE_ENTITY_TYPE,
                AggregationType.AVG,
                "latencyMs",
                new FeatureFilter("latencyMs", FeatureFilterOperator.EXISTS, null),
                WindowType.TUMBLING,
                Duration.ofMinutes(5),
                null,
                1,
                FeatureDefinitionState.ACTIVE
        );
    }
}

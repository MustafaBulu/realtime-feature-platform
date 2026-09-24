package com.mustafabulu.realtimefeatureplatform.streamworker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import com.mustafabulu.realtimefeatureplatform.featuremodel.AggregationType;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinition;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinitionState;
import com.mustafabulu.realtimefeatureplatform.featuremodel.WindowType;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AggregatorTest {

    @Test
    void countAggregatorIncrementsByEvent() {
        InMemoryAggregationStateStore stateStore = new InMemoryAggregationStateStore();
        CountAggregator aggregator = new CountAggregator();
        AggregationInput input = new AggregationInput(
                definition("event_count", AggregationType.COUNT, null),
                event(Map.of("count", 100L)),
                "feature:service:catalog-api:event_count",
                stateStore
        );

        assertEquals(1L, aggregator.aggregate(input));
        assertEquals(2L, aggregator.aggregate(input));
        assertEquals("2", stateStore.get("feature:service:catalog-api:event_count"));
    }

    @Test
    void sumAggregatorAddsNumericField() {
        InMemoryAggregationStateStore stateStore = new InMemoryAggregationStateStore();
        SumAggregator aggregator = new SumAggregator();

        assertEquals(100L, aggregator.aggregate(new AggregationInput(
                definition("request_count_total", AggregationType.SUM, "count"),
                event(Map.of("count", 100L)),
                "feature:service:catalog-api:request_count_total",
                stateStore
        )));
        assertEquals(450L, aggregator.aggregate(new AggregationInput(
                definition("request_count_total", AggregationType.SUM, "count"),
                event(Map.of("count", 350L)),
                "feature:service:catalog-api:request_count_total",
                stateStore
        )));
    }

    @Test
    void averageAggregatorTracksSumAndCount() {
        InMemoryAggregationStateStore stateStore = new InMemoryAggregationStateStore();
        AverageAggregator aggregator = new AverageAggregator();
        FeatureDefinition definition = definition("avg_latency", AggregationType.AVG, "latencyMs");

        assertEquals(80.0, aggregator.aggregate(new AggregationInput(
                definition,
                event(Map.of("latencyMs", 80L)),
                "feature:service:catalog-api:avg_latency",
                stateStore
        )));
        assertEquals(100.0, aggregator.aggregate(new AggregationInput(
                definition,
                event(Map.of("latencyMs", 120L)),
                "feature:service:catalog-api:avg_latency",
                stateStore
        )));
        assertEquals("200,2", stateStore.get("feature:service:catalog-api:avg_latency"));
    }

    @Test
    void averageAggregatorSupportsWeightedAverage() {
        InMemoryAggregationStateStore stateStore = new InMemoryAggregationStateStore();
        AverageAggregator aggregator = new AverageAggregator();
        FeatureDefinition definition = weightedAverageDefinition();

        assertEquals(80.0, aggregator.aggregate(new AggregationInput(
                definition,
                event(Map.of("latencyMs", 80L, "count", 100L)),
                "feature:service:catalog-api:avg_latency",
                stateStore
        )));
        assertEquals(110.0, aggregator.aggregate(new AggregationInput(
                definition,
                event(Map.of("latencyMs", 120L, "count", 300L)),
                "feature:service:catalog-api:avg_latency",
                stateStore
        )));
        assertEquals("44000,400", stateStore.get("feature:service:catalog-api:avg_latency"));
    }

    @Test
    void distinctCountAggregatorCountsExactUniqueValues() {
        InMemoryAggregationStateStore stateStore = new InMemoryAggregationStateStore();
        DistinctCountAggregator aggregator = new DistinctCountAggregator();
        FeatureDefinition definition = definition("distinct_users", AggregationType.DISTINCT_COUNT, "userId");

        assertEquals(1L, aggregator.aggregate(new AggregationInput(
                definition,
                event(Map.of("userId", "u-1")),
                "feature:service:catalog-api:distinct_users",
                stateStore
        )));
        assertEquals(1L, aggregator.aggregate(new AggregationInput(
                definition,
                event(Map.of("userId", "u-1")),
                "feature:service:catalog-api:distinct_users",
                stateStore
        )));
        assertEquals(2L, aggregator.aggregate(new AggregationInput(
                definition,
                event(Map.of("userId", "u-2")),
                "feature:service:catalog-api:distinct_users",
                stateStore
        )));
    }

    @Test
    void ratioAggregatorTracksFilteredNumeratorOverDenominator() {
        InMemoryAggregationStateStore stateStore = new InMemoryAggregationStateStore();
        RatioAggregator aggregator = new RatioAggregator();
        FeatureDefinition definition = errorRateDefinition();

        assertEquals(0.0, aggregator.aggregate(new AggregationInput(
                definition,
                event(Map.of("count", 400L, "statusCode", 200)),
                "feature:service:catalog-api:entity_error_rate_10m",
                stateStore
        )));
        assertEquals(50.0 / 450.0, aggregator.aggregate(new AggregationInput(
                definition,
                event(Map.of("count", 50L, "statusCode", 500)),
                "feature:service:catalog-api:entity_error_rate_10m",
                stateStore
        )));
        assertEquals("50,450", stateStore.get("feature:service:catalog-api:entity_error_rate_10m"));
    }

    private static PlatformEvent event(Map<String, Object> payload) {
        return new PlatformEvent(
                "event-1",
                "request.completed",
                Instant.parse("2026-08-28T12:14:59.999Z"),
                new com.mustafabulu.realtimefeatureplatform.eventmodel.EntityRef("service", "catalog-api"),
                payload
        );
    }

    private static FeatureDefinition definition(String name, AggregationType aggregationType, String valueField) {
        return new FeatureDefinition(
                name,
                "request.completed",
                "service",
                aggregationType,
                valueField,
                null,
                WindowType.NONE,
                null,
                null,
                1,
                FeatureDefinitionState.ACTIVE
        );
    }

    private static FeatureDefinition weightedAverageDefinition() {
        return new FeatureDefinition(
                "avg_latency",
                "request.completed",
                "service",
                AggregationType.AVG,
                "latencyMs",
                "count",
                null,
                WindowType.NONE,
                null,
                null,
                1,
                FeatureDefinitionState.ACTIVE
        );
    }

    private static FeatureDefinition errorRateDefinition() {
        return new FeatureDefinition(
                "entity_error_rate_10m",
                "request.completed",
                "service",
                AggregationType.RATIO,
                "count",
                null,
                new com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureFilter(
                        "statusCode",
                        com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureFilterOperator.EXISTS,
                        null
                ),
                new com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureFilter(
                        "statusCode",
                        com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureFilterOperator.GTE,
                        "500"
                ),
                WindowType.NONE,
                null,
                null,
                1,
                FeatureDefinitionState.ACTIVE
        );
    }
}

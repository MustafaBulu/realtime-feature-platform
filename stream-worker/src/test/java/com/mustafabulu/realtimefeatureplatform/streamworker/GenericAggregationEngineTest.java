package com.mustafabulu.realtimefeatureplatform.streamworker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import com.mustafabulu.realtimefeatureplatform.eventmodel.RequestCompletedEventFactory;
import com.mustafabulu.realtimefeatureplatform.featuremodel.AggregationType;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinition;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinitionRepository;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinitionState;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureFilter;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureFilterOperator;
import com.mustafabulu.realtimefeatureplatform.featuremodel.InMemoryFeatureDefinitionRepository;
import com.mustafabulu.realtimefeatureplatform.featuremodel.WindowType;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class GenericAggregationEngineTest {

    @Test
    void updatesMultipleActiveDefinitionsForOneEvent() {
        InMemoryAggregationStateStore stateStore = new InMemoryAggregationStateStore();
        GenericAggregationEngine engine = engine(stateStore, List.of(
                definition("request_count_total", AggregationType.SUM, "count", null, WindowType.NONE, null),
                definition("entity_event_count_10m", AggregationType.COUNT, null, null, WindowType.TUMBLING, Duration.ofMinutes(10)),
                averageLatencyDefinition()
        ));
        PlatformEvent event = RequestCompletedEventFactory.create(
                "event-1",
                "service",
                "catalog-api",
                100,
                200,
                80L,
                Instant.parse("2026-08-28T12:14:59.999Z")
        );

        List<AggregationResult> values = engine.process(event);

        assertEquals(3, values.size());
        assertEquals(100L, values.get(0).value());
        assertEquals(1L, values.get(1).value());
        assertEquals(80.0, values.get(2).value());
        assertEquals("100", stateStore.get("feature:service:catalog-api:request_count_total"));
        assertEquals("1", stateStore.get("feature:service:catalog-api:entity_event_count_10m:window:1787919000000"));
        assertEquals("8000,100", stateStore.get("feature:service:catalog-api:entity_avg_latency_ms_5m:window:1787919000000"));
        assertEquals("feature:service:catalog-api:entity_avg_latency_ms_5m:window:1787919000000",
                values.get(2).materializedKey());
    }

    @Test
    void skipsDefinitionsThatDoNotMatchFilter() {
        InMemoryAggregationStateStore stateStore = new InMemoryAggregationStateStore();
        GenericAggregationEngine engine = engine(stateStore, List.of(
                definition(
                        "server_error_count",
                        AggregationType.COUNT,
                        null,
                        new FeatureFilter("statusCode", FeatureFilterOperator.GTE, "500"),
                        WindowType.NONE,
                        null
                )
        ));
        PlatformEvent event = RequestCompletedEventFactory.create(
                "event-1",
                "service",
                "catalog-api",
                1,
                200,
                80L,
                Instant.parse("2026-08-28T12:14:59.999Z")
        );

        assertEquals(List.of(), engine.process(event));
    }

    private static GenericAggregationEngine engine(
            InMemoryAggregationStateStore stateStore,
            List<FeatureDefinition> definitions
    ) {
        FeatureDefinitionRepository repository = new InMemoryFeatureDefinitionRepository(definitions);
        WorkerFeatureDefinitionLoader loader = new WorkerFeatureDefinitionLoader(repository);
        AggregatorRegistry registry = new AggregatorRegistry(List.of(
                new CountAggregator(),
                new SumAggregator(),
                new AverageAggregator(),
                new DistinctCountAggregator(),
                new RatioAggregator()
        ));
        return new GenericAggregationEngine(loader, registry, stateStore);
    }

    private static FeatureDefinition definition(
            String name,
            AggregationType aggregationType,
            String valueField,
            FeatureFilter filter,
            WindowType windowType,
            Duration windowSize
    ) {
        return new FeatureDefinition(
                name,
                "request.completed",
                "service",
                aggregationType,
                valueField,
                filter,
                windowType,
                windowSize,
                null,
                1,
                FeatureDefinitionState.ACTIVE
        );
    }

    private static FeatureDefinition averageLatencyDefinition() {
        return new FeatureDefinition(
                "entity_avg_latency_ms_5m",
                "request.completed",
                "service",
                AggregationType.AVG,
                "latencyMs",
                "count",
                null,
                WindowType.TUMBLING,
                Duration.ofMinutes(5),
                null,
                1,
                FeatureDefinitionState.ACTIVE
        );
    }
}

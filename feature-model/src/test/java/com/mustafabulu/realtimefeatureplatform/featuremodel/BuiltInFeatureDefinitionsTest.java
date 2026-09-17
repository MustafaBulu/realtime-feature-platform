package com.mustafabulu.realtimefeatureplatform.featuremodel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class BuiltInFeatureDefinitionsTest {

    @Test
    void exposesInitialActiveDefinitions() {
        Map<String, FeatureDefinition> definitions = BuiltInFeatureDefinitions.all().stream()
                .collect(Collectors.toMap(FeatureDefinition::name, Function.identity()));

        assertEquals(3, definitions.size());
        assertTrue(definitions.values().stream()
                .allMatch(definition -> definition.state() == FeatureDefinitionState.ACTIVE));

        FeatureDefinition total = definitions.get(FeatureNames.REQUEST_COUNT_TOTAL);
        assertEquals(AggregationType.SUM, total.aggregationType());
        assertEquals("count", total.valueField());
        assertEquals(WindowType.NONE, total.windowType());

        FeatureDefinition count10m = definitions.get(FeatureNames.ENTITY_EVENT_COUNT_10M);
        assertEquals(AggregationType.COUNT, count10m.aggregationType());
        assertEquals(WindowType.TUMBLING, count10m.windowType());
        assertEquals(Duration.ofMinutes(10), count10m.windowSize());

        FeatureDefinition avg5m = definitions.get(FeatureNames.ENTITY_AVG_LATENCY_MS_5M);
        assertEquals(AggregationType.AVG, avg5m.aggregationType());
        assertEquals("latencyMs", avg5m.valueField());
        assertEquals(Duration.ofMinutes(5), avg5m.windowSize());
    }
}

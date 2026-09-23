package com.mustafabulu.realtimefeatureplatform.streamworker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import com.mustafabulu.realtimefeatureplatform.eventmodel.RequestCompletedEventFactory;
import com.mustafabulu.realtimefeatureplatform.featuremodel.BuiltInFeatureDefinitions;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinitionRepository;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureNames;
import com.mustafabulu.realtimefeatureplatform.featuremodel.InMemoryFeatureDefinitionRepository;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

class GenericFeatureProcessorTest {

    private final InMemoryAggregationStateStore stateStore = new InMemoryAggregationStateStore();
    private final StringRedisTemplate redisTemplate = org.mockito.Mockito.mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    private final ValueOperations<String, String> valueOperations = org.mockito.Mockito.mock(ValueOperations.class);
    private final GenericFeatureProcessor processor = new GenericFeatureProcessor(engine(), redisTemplate);

    @BeforeEach
    void setUp() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    @Test
    void materializesBuiltInDefinitionsThroughGenericEngine() {
        PlatformEvent event = RequestCompletedEventFactory.create(
                "event-1",
                "service",
                "catalog-api",
                100,
                200,
                80L,
                Instant.parse("2026-08-28T12:14:59.999Z")
        );
        String totalKey = "feature:service:catalog-api:" + FeatureNames.REQUEST_COUNT_TOTAL;
        String eventCountKey = "feature:service:catalog-api:" + FeatureNames.ENTITY_EVENT_COUNT_10M;
        String eventCountWindowKey = eventCountKey + ":window:1787919000000";
        String avgLatencyKey = "feature:service:catalog-api:" + FeatureNames.ENTITY_AVG_LATENCY_MS_5M;
        String avgLatencyWindowKey = avgLatencyKey + ":window:1787919000000";
        String errorRateKey = "feature:service:catalog-api:" + FeatureNames.ENTITY_ERROR_RATE_10M;
        String errorRateWindowKey = errorRateKey + ":window:1787919000000";

        assertEquals(100L, processor.process(event).value());

        verify(valueOperations).set(totalKey, "100");
        verify(valueOperations).set(eventCountWindowKey, "1");
        verify(valueOperations).set(eventCountKey, "1");
        verify(valueOperations).set(avgLatencyWindowKey, "80.0");
        verify(valueOperations).set(avgLatencyKey, "80.0");
        verify(valueOperations).set(errorRateWindowKey, "0.0");
        verify(valueOperations).set(errorRateKey, "0.0");
    }

    private GenericAggregationEngine engine() {
        FeatureDefinitionRepository repository =
                new InMemoryFeatureDefinitionRepository(BuiltInFeatureDefinitions.all());
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
}

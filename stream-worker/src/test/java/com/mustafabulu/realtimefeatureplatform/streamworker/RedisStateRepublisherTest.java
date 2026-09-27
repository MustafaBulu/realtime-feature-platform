package com.mustafabulu.realtimefeatureplatform.streamworker;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mustafabulu.realtimefeatureplatform.featuremodel.BuiltInFeatureDefinitions;
import com.mustafabulu.realtimefeatureplatform.featuremodel.InMemoryFeatureDefinitionRepository;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

class RedisStateRepublisherTest {

    private final RequestCountTotalStateStore stateStore = mock(RequestCountTotalStateStore.class);
    private final StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    private final ValueOperations<String, String> valueOperations = mock(ValueOperations.class);
    private final Instant restoredAt = Instant.parse("2026-08-28T12:15:00Z");

    @BeforeEach
    void setUp() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    @Test
    void republishesMaterializedValuesFromAggregationState() {
        String avgWindowKey = "feature:service:catalog-api:entity_avg_latency_ms_5m:window:1787919300000";
        String avgLatestMarker = "feature:service:catalog-api:entity_avg_latency_ms_5m:latest-window-start";
        when(stateStore.entriesWithPrefix("feature:")).thenReturn(List.of(
                new RequestCountTotalStateStore.StateEntry(
                        "feature:service:catalog-api:request_count_total",
                        "250"
                ),
                new RequestCountTotalStateStore.StateEntry(avgWindowKey, "8000,100"),
                new RequestCountTotalStateStore.StateEntry(avgLatestMarker, "1787919300000")
        ));
        when(stateStore.get(avgWindowKey)).thenReturn("8000,100");

        republisher().republishAll();

        verify(valueOperations).set("feature:service:catalog-api:request_count_total", "250");
        verify(valueOperations).set(avgWindowKey, "80.0");
        verify(valueOperations).set("feature:service:catalog-api:entity_avg_latency_ms_5m", "80.0");
        verify(valueOperations).set(avgWindowKey + ":updated-at", restoredAt.toString());
        verify(valueOperations).set(avgWindowKey + ":definition-version", "1");
    }

    private RedisStateRepublisher republisher() {
        return new RedisStateRepublisher(
                stateStore,
                new WorkerFeatureDefinitionLoader(new InMemoryFeatureDefinitionRepository(BuiltInFeatureDefinitions.all())),
                new FeatureRedisMaterializer(redisTemplate),
                Clock.fixed(restoredAt, ZoneOffset.UTC)
        );
    }
}

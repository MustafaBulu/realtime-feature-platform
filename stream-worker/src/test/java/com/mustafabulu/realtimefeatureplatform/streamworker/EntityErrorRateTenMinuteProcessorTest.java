package com.mustafabulu.realtimefeatureplatform.streamworker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import com.mustafabulu.realtimefeatureplatform.eventmodel.RequestCompletedEventFactory;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureNames;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

class EntityErrorRateTenMinuteProcessorTest {

    private final RequestCountTotalStateStore stateStore = org.mockito.Mockito.mock(RequestCountTotalStateStore.class);
    private final StringRedisTemplate redisTemplate = org.mockito.Mockito.mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    private final ValueOperations<String, String> valueOperations = org.mockito.Mockito.mock(ValueOperations.class);
    private final EntityErrorRateTenMinuteProcessor processor =
            new EntityErrorRateTenMinuteProcessor(stateStore, redisTemplate);

    @BeforeEach
    void setUp() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    @Test
    void calculatesTenMinuteErrorRateFromStatusCodeAndCount() {
        PlatformEvent event = RequestCompletedEventFactory.create(
                "event-1",
                "service",
                "catalog-api",
                50,
                500,
                300L,
                Instant.parse("2026-08-28T12:19:59.999Z")
        );
        String featureKey = "feature:service:catalog-api:" + FeatureNames.ENTITY_ERROR_RATE_10M;
        String windowedKey = featureKey + ":window:1787919000000";
        when(stateStore.add(windowedKey + ":requests", 50L)).thenReturn(450L);
        when(stateStore.add(windowedKey + ":errors", 50L)).thenReturn(50L);

        assertEquals(50.0 / 450.0, processor.process(event).value());

        verify(valueOperations).set(windowedKey, Double.toString(50.0 / 450.0));
        verify(valueOperations).set(featureKey, Double.toString(50.0 / 450.0));
    }
}

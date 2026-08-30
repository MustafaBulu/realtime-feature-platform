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

class EntityEventCountTenMinuteProcessorTest {

    private final RequestCountTotalStateStore stateStore = org.mockito.Mockito.mock(RequestCountTotalStateStore.class);
    private final StringRedisTemplate redisTemplate = org.mockito.Mockito.mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    private final ValueOperations<String, String> valueOperations = org.mockito.Mockito.mock(ValueOperations.class);
    private final EntityEventCountTenMinuteProcessor processor =
            new EntityEventCountTenMinuteProcessor(stateStore, redisTemplate);

    @BeforeEach
    void setUp() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    @Test
    void countsEventsInTenMinuteEventTimeWindow() {
        PlatformEvent event = RequestCompletedEventFactory.create(
                "event-1",
                "service",
                "catalog-api",
                100,
                Instant.parse("2026-08-28T12:19:59.999Z")
        );
        String featureKey = "feature:service:catalog-api:" + FeatureNames.ENTITY_EVENT_COUNT_10M;
        String windowedKey = featureKey + ":window:1787919000000";
        when(stateStore.add(windowedKey, 1L)).thenReturn(3L);

        assertEquals(3L, processor.process(event).value());

        verify(stateStore).add(windowedKey, 1L);
        verify(valueOperations).set(windowedKey, "3");
        verify(valueOperations).set(featureKey, "3");
    }
}

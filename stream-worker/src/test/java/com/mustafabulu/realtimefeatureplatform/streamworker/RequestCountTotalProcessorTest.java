package com.mustafabulu.realtimefeatureplatform.streamworker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import com.mustafabulu.realtimefeatureplatform.eventmodel.RequestCompletedEventFactory;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureNames;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

class RequestCountTotalProcessorTest {

    private final RequestCountTotalStateStore stateStore = org.mockito.Mockito.mock(RequestCountTotalStateStore.class);
    private final StringRedisTemplate redisTemplate = org.mockito.Mockito.mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    private final ValueOperations<String, String> valueOperations = org.mockito.Mockito.mock(ValueOperations.class);
    private final RequestCountTotalProcessor processor = new RequestCountTotalProcessor(stateStore, redisTemplate);

    @BeforeEach
    void setUp() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    @Test
    void validRequestCompletedEventIncrementsStateAndMaterializesFeature() {
        PlatformEvent event = RequestCompletedEventFactory.create(
                "event-1",
                "service",
                "catalog-api",
                100,
                Instant.parse("2026-08-28T12:10:14.200Z")
        );
        String redisKey = "feature:service:catalog-api:" + FeatureNames.REQUEST_COUNT_TOTAL;
        when(stateStore.add(redisKey, 100L)).thenReturn(450L);

        assertEquals(450L, processor.process(event).value());

        verify(stateStore).add(redisKey, 100L);
        verify(valueOperations).set(redisKey, "450");
    }

    @Test
    void unsupportedEventDoesNotChangeState() {
        PlatformEvent event = new PlatformEvent(
                "event-1",
                "auth.failed",
                Instant.parse("2026-08-28T12:10:14.200Z"),
                new com.mustafabulu.realtimefeatureplatform.eventmodel.EntityRef("user", "u-1"),
                Map.of("reason", "bad_password")
        );

        assertFalse(processor.supports(event));
        assertNull(processor.process(event));

        verify(stateStore, never()).add(org.mockito.Mockito.anyString(), org.mockito.Mockito.anyLong());
        verify(valueOperations, never()).set(org.mockito.Mockito.anyString(), org.mockito.Mockito.anyString());
    }
}

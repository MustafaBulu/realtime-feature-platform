package com.mustafabulu.realtimefeatureplatform.featuremodel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class FeatureKeyTest {

    @Test
    void rendersRedisKey() {
        FeatureKey key = new FeatureKey("service", "catalog-api", FeatureNames.REQUEST_COUNT_TOTAL);

        assertEquals("feature:service:catalog-api:request_count_total", key.redisKey());
    }

    @Test
    void rejectsAmbiguousParts() {
        assertThrows(IllegalArgumentException.class, () ->
                new FeatureKey("service", "catalog:api", FeatureNames.REQUEST_COUNT_TOTAL));
    }

    @Test
    void rendersWindowedRedisKey() {
        FeatureKey key = new FeatureKey("service", "catalog-api", FeatureNames.ENTITY_EVENT_COUNT_10M);
        WindowedFeatureKey windowedKey = new WindowedFeatureKey(key, Instant.parse("2026-08-28T12:10:00Z"));

        assertEquals("feature:service:catalog-api:entity_event_count_10m:window:1787919000000",
                windowedKey.redisKey());
    }
}

package com.mustafabulu.realtimefeatureplatform.featuremodel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
}

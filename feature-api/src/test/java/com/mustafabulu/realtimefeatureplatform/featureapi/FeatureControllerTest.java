package com.mustafabulu.realtimefeatureplatform.featureapi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureNames;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class FeatureControllerTest {

    @Test
    void returnsMaterializedFeatureValue() {
        FeatureController controller = new FeatureController(redisTemplateReturning("450"));

        ResponseEntity<FeatureController.FeatureResponse> response = controller.getFeature(
                "service",
                "catalog-api",
                FeatureNames.REQUEST_COUNT_TOTAL
        );

        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(450L, response.getBody().value());
    }

    @Test
    void returnsNotFoundWhenFeatureIsMissing() {
        FeatureController controller = new FeatureController(redisTemplateReturning(null));

        ResponseEntity<FeatureController.FeatureResponse> response = controller.getFeature(
                "service",
                "catalog-api",
                FeatureNames.REQUEST_COUNT_TOTAL
        );

        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        assertNull(response.getBody());
    }

    @SuppressWarnings("unchecked")
    private StringRedisTemplate redisTemplateReturning(String value) {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOperations = mock(ValueOperations.class);

        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("feature:service:catalog-api:request_count_total")).thenReturn(value);

        return redisTemplate;
    }
}

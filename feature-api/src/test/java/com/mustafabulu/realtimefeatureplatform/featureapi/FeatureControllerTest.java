package com.mustafabulu.realtimefeatureplatform.featureapi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureNames;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class FeatureControllerTest {

    @Test
    void returnsMaterializedFeatureValue() {
        FeatureController controller = new FeatureController(redisTemplateReturning("450"));

        ResponseEntity<?> response = controller.getFeature(
                "service",
                "catalog-api",
                FeatureNames.REQUEST_COUNT_TOTAL,
                null
        );

        assertEquals(HttpStatus.OK, response.getStatusCode());
        FeatureController.FeatureResponse body = (FeatureController.FeatureResponse) response.getBody();
        assertEquals(450L, body.value());
    }

    @Test
    void returnsNotFoundWhenFeatureIsMissing() {
        FeatureController controller = new FeatureController(redisTemplateReturning(null));

        ResponseEntity<?> response = controller.getFeature(
                "service",
                "catalog-api",
                FeatureNames.REQUEST_COUNT_TOTAL,
                null
        );

        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        assertNull(response.getBody());
    }

    @Test
    void returnsWindowedFeatureValue() {
        FeatureController controller = new FeatureController(redisTemplateReturning("0.25"));
        Instant windowStart = Instant.parse("2026-08-28T12:10:00Z");

        ResponseEntity<?> response = controller.getFeature(
                "service",
                "catalog-api",
                FeatureNames.ENTITY_ERROR_RATE_10M,
                windowStart
        );

        assertEquals(HttpStatus.OK, response.getStatusCode());
        FeatureController.WindowedFeatureResponse body =
                (FeatureController.WindowedFeatureResponse) response.getBody();
        assertEquals(windowStart, body.windowStart());
        assertEquals(0.25, body.value());
    }

    @SuppressWarnings("unchecked")
    private StringRedisTemplate redisTemplateReturning(String value) {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOperations = mock(ValueOperations.class);

        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("feature:service:catalog-api:request_count_total")).thenReturn(value);
        when(valueOperations.get("feature:service:catalog-api:entity_error_rate_10m:window:1787919000000"))
                .thenReturn(value);

        return redisTemplate;
    }
}

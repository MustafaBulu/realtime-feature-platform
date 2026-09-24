package com.mustafabulu.realtimefeatureplatform.featureapi;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.mustafabulu.realtimefeatureplatform.featuremodel.BuiltInFeatureDefinitions;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureNames;
import com.mustafabulu.realtimefeatureplatform.featuremodel.MutableInMemoryFeatureDefinitionRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class FeatureControllerTest {

    @Test
    void returnsMaterializedFeatureValue() {
        FeatureController controller = controller(redisTemplateReturning(Map.of(
                "feature:service:catalog-api:request_count_total", "450",
                "feature:service:catalog-api:request_count_total:updated-at", "2026-08-28T12:19:00Z",
                "feature:service:catalog-api:request_count_total:definition-version", "1"
        )));

        ResponseEntity<FeatureController.FeatureReadResponse> response = controller.getFeature(
                "service",
                "catalog-api",
                FeatureNames.REQUEST_COUNT_TOTAL,
                null
        );

        assertEquals(HttpStatus.OK, response.getStatusCode());
        FeatureController.FeatureReadResponse body = requireNonNull(response.getBody());
        FeatureController.FeatureMetadata metadata = requireNonNull(body.metadata());
        assertEquals(450L, requireNonNull(body.value()));
        assertEquals(FeatureController.FeatureReadStatus.PRESENT, metadata.status());
        assertEquals(60_000L, metadata.freshnessMillis());
        assertEquals(1, metadata.definitionVersion());
    }

    @Test
    void modelsMissingFeatureWithoutConfusingItWithZero() {
        FeatureController controller = controller(redisTemplateReturning(Map.of(
                "feature:service:catalog-api:request_count_total", "0"
        )));

        ResponseEntity<FeatureController.FeatureReadResponse> zeroResponse = controller.getFeature(
                "service",
                "catalog-api",
                FeatureNames.REQUEST_COUNT_TOTAL,
                null
        );
        ResponseEntity<FeatureController.FeatureReadResponse> missingResponse = controller.getFeature(
                "service",
                "catalog-api",
                FeatureNames.ENTITY_AVG_LATENCY_MS_5M,
                null
        );

        FeatureController.FeatureReadResponse zeroBody = requireNonNull(zeroResponse.getBody());
        FeatureController.FeatureReadResponse missingBody = requireNonNull(missingResponse.getBody());
        assertEquals(0L, requireNonNull(zeroBody.value()));
        assertEquals(FeatureController.FeatureReadStatus.PRESENT, requireNonNull(zeroBody.metadata()).status());
        assertEquals(FeatureController.FeatureReadStatus.MISSING, requireNonNull(missingBody.metadata()).status());
    }

    @Test
    void returnsWindowedFeatureValue() {
        FeatureController controller = controller(redisTemplateReturning(Map.of(
                "feature:service:catalog-api:entity_error_rate_10m:window:1787919000000", "0.25"
        )));
        Instant windowStart = Instant.parse("2026-08-28T12:10:00Z");

        ResponseEntity<FeatureController.FeatureReadResponse> response = controller.getFeature(
                "service",
                "catalog-api",
                FeatureNames.ENTITY_ERROR_RATE_10M,
                windowStart
        );

        assertEquals(HttpStatus.OK, response.getStatusCode());
        FeatureController.FeatureReadResponse body = requireNonNull(response.getBody());
        assertEquals(windowStart, requireNonNull(body.windowStart()));
        assertEquals(0.25, requireNonNull(body.value()));
    }

    @Test
    void returnsFeatureSubsetForSingleEntity() {
        FeatureController controller = controller(redisTemplateReturning(Map.of(
                "feature:service:catalog-api:request_count_total", "450",
                "feature:service:catalog-api:entity_event_count_10m", "3"
        )));

        ResponseEntity<FeatureController.FeatureSetResponse> response = controller.getFeatures(
                "service",
                "catalog-api",
                List.of(FeatureNames.REQUEST_COUNT_TOTAL, FeatureNames.ENTITY_EVENT_COUNT_10M)
        );

        List<FeatureController.FeatureReadResponse> features =
                requireNonNull(requireNonNull(response.getBody()).features());
        assertEquals(2, features.size());
        assertEquals(450L, requireNonNull(features.get(0).value()));
        assertEquals(3L, requireNonNull(features.get(1).value()));
    }

    @Test
    void returnsBatchFeatureReads() {
        FeatureController controller = controller(redisTemplateReturning(Map.of(
                "feature:service:catalog-api:request_count_total", "450",
                "feature:service:checkout-api:request_count_total", "25"
        )));

        ResponseEntity<FeatureController.BatchFeatureResponse> response = controller.getFeatureBatch(
                new FeatureController.BatchFeatureRequest(List.of(
                        new FeatureController.FeatureLookup(
                                "service",
                                "catalog-api",
                                FeatureNames.REQUEST_COUNT_TOTAL,
                                null
                        ),
                        new FeatureController.FeatureLookup(
                                "service",
                                "checkout-api",
                                FeatureNames.REQUEST_COUNT_TOTAL,
                                null
                        )
                ))
        );

        List<FeatureController.FeatureReadResponse> features =
                requireNonNull(requireNonNull(response.getBody()).features());
        assertEquals(2, features.size());
        assertEquals(25L, requireNonNull(features.get(1).value()));
    }

    @Test
    void marksStaleValuesAndRecordsRedisReadMetrics() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        FeatureController controller = controller(redisTemplateReturning(Map.of(
                "feature:service:catalog-api:request_count_total", "450",
                "feature:service:catalog-api:request_count_total:updated-at", "2026-08-28T12:00:00Z"
        )), meterRegistry);

        ResponseEntity<FeatureController.FeatureReadResponse> response = controller.getFeature(
                "service",
                "catalog-api",
                FeatureNames.REQUEST_COUNT_TOTAL,
                null
        );

        FeatureController.FeatureReadResponse body = requireNonNull(response.getBody());
        assertEquals(FeatureController.FeatureReadStatus.STALE, requireNonNull(body.metadata()).status());
        assertEquals(1.0, meterRegistry.counter("rfp.feature_api.redis.reads", "status", "stale").count());
    }

    @Test
    void modelsRedisUnavailable() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        when(redisTemplate.opsForValue()).thenThrow(new IllegalStateException("redis unavailable"));
        FeatureController controller = controller(redisTemplate);

        ResponseEntity<FeatureController.FeatureReadResponse> response = controller.getFeature(
                "service",
                "catalog-api",
                FeatureNames.REQUEST_COUNT_TOTAL,
                null
        );

        FeatureController.FeatureReadResponse body = requireNonNull(response.getBody());
        assertEquals(FeatureController.FeatureReadStatus.UNAVAILABLE, requireNonNull(body.metadata()).status());
    }

    @SuppressWarnings("unchecked")
    private StringRedisTemplate redisTemplateReturning(Map<String, String> values) {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOperations = mock(ValueOperations.class);

        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        values.forEach((key, value) -> when(valueOperations.get(key)).thenReturn(value));
        values.keySet().stream()
                .filter(key -> !key.endsWith(":updated-at") && !key.endsWith(":definition-version"))
                .forEach(key -> when(redisTemplate.getExpire(key)).thenReturn(300L));

        return redisTemplate;
    }

    private FeatureController controller(StringRedisTemplate redisTemplate) {
        return controller(redisTemplate, new SimpleMeterRegistry());
    }

    private FeatureController controller(StringRedisTemplate redisTemplate, SimpleMeterRegistry meterRegistry) {
        return new FeatureController(
                redisTemplate,
                new MutableInMemoryFeatureDefinitionRepository(BuiltInFeatureDefinitions.all()),
                meterRegistry,
                Clock.fixed(Instant.parse("2026-08-28T12:20:00Z"), ZoneOffset.UTC),
                Duration.ofMinutes(5)
        );
    }
}

package com.mustafabulu.realtimefeatureplatform.featureapi;

import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureKey;
import com.mustafabulu.realtimefeatureplatform.featuremodel.WindowedFeatureKey;
import java.time.Instant;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
class FeatureController {

    private final StringRedisTemplate redisTemplate;

    FeatureController(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @GetMapping("/features/{entityType}/{entityId}/{featureName}")
    ResponseEntity<?> getFeature(
            @PathVariable String entityType,
            @PathVariable String entityId,
            @PathVariable String featureName,
            @RequestParam(required = false) Instant windowStart
    ) {
        FeatureKey key = new FeatureKey(entityType, entityId, featureName);
        String redisKey = windowStart == null ? key.redisKey() : new WindowedFeatureKey(key, windowStart).redisKey();
        String value = redisTemplate.opsForValue().get(redisKey);

        if (value == null) {
            return ResponseEntity.notFound().build();
        }

        Number parsedValue = parseFeatureValue(value);
        if (windowStart == null) {
            return ResponseEntity.ok(new FeatureResponse(entityType, entityId, featureName, parsedValue));
        }

        return ResponseEntity.ok(new WindowedFeatureResponse(entityType, entityId, featureName, windowStart, parsedValue));
    }

    private static Number parseFeatureValue(String value) {
        if (value.contains(".") || value.contains("e") || value.contains("E")) {
            return Double.parseDouble(value);
        }
        return Long.parseLong(value);
    }

    record FeatureResponse(String entityType, String entityId, String featureName, Number value) {
    }

    record WindowedFeatureResponse(String entityType, String entityId, String featureName, Instant windowStart, Number value) {
    }
}

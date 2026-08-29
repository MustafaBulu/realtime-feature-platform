package com.mustafabulu.realtimefeatureplatform.featureapi;

import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureKey;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
class FeatureController {

    private final StringRedisTemplate redisTemplate;

    FeatureController(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @GetMapping("/features/{entityType}/{entityId}/{featureName}")
    ResponseEntity<FeatureResponse> getFeature(
            @PathVariable String entityType,
            @PathVariable String entityId,
            @PathVariable String featureName
    ) {
        FeatureKey key = new FeatureKey(entityType, entityId, featureName);
        String value = redisTemplate.opsForValue().get(key.redisKey());

        if (value == null) {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok(new FeatureResponse(entityType, entityId, featureName, Long.parseLong(value)));
    }

    record FeatureResponse(String entityType, String entityId, String featureName, long value) {
    }
}

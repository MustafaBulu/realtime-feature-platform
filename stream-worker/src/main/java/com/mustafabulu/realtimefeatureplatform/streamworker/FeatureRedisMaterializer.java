package com.mustafabulu.realtimefeatureplatform.streamworker;

import java.time.Duration;
import java.time.Instant;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
class FeatureRedisMaterializer {

    private final StringRedisTemplate redisTemplate;

    FeatureRedisMaterializer(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    void materialize(AggregationResult result) {
        String value = result.value().toString();
        writeMaterializedValue(
                result.materializedKey(),
                value,
                result.updatedAt(),
                result.definition().version(),
                result.windowTtl()
        );
        String latestKey = result.featureKey().redisKey();
        if (result.updateLatest() && !latestKey.equals(result.materializedKey())) {
            writeMaterializedValue(latestKey, value, result.updatedAt(), result.definition().version(), null);
        }
    }

    void materializeRestored(
            String key,
            Number value,
            Instant restoredAt,
            int definitionVersion,
            Duration ttl
    ) {
        writeMaterializedValue(key, value.toString(), restoredAt, definitionVersion, ttl);
    }

    private void writeMaterializedValue(
            String key,
            String value,
            Instant updatedAt,
            int definitionVersion,
            Duration ttl
    ) {
        redisTemplate.opsForValue().set(key, value);
        redisTemplate.opsForValue().set(updatedAtKey(key), updatedAt.toString());
        redisTemplate.opsForValue().set(definitionVersionKey(key), Integer.toString(definitionVersion));
        if (ttl != null) {
            redisTemplate.expire(key, ttl);
            redisTemplate.expire(updatedAtKey(key), ttl);
            redisTemplate.expire(definitionVersionKey(key), ttl);
        }
    }

    private static String updatedAtKey(String key) {
        return key + ":updated-at";
    }

    private static String definitionVersionKey(String key) {
        return key + ":definition-version";
    }
}

package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureValue;
import java.util.List;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
class GenericFeatureProcessor implements PlatformEventProcessor {

    private final GenericAggregationEngine engine;
    private final StringRedisTemplate redisTemplate;

    GenericFeatureProcessor(GenericAggregationEngine engine, StringRedisTemplate redisTemplate) {
        this.engine = engine;
        this.redisTemplate = redisTemplate;
    }

    @Override
    public boolean supports(PlatformEvent event) {
        return engine.supports(event);
    }

    @Override
    public FeatureValue process(PlatformEvent event) {
        return process(event, EventTimeAssessment.onTimeAt(event.eventTime()));
    }

    @Override
    public FeatureValue process(PlatformEvent event, EventTimeAssessment assessment) {
        List<AggregationResult> results = engine.process(event, assessment);
        for (AggregationResult result : results) {
            materialize(result);
        }
        return results.isEmpty() ? null : results.getFirst().featureValue();
    }

    private void materialize(AggregationResult result) {
        String value = result.value().toString();
        writeMaterializedValue(result.materializedKey(), value, result);
        if (result.windowTtl() != null) {
            redisTemplate.expire(result.materializedKey(), result.windowTtl());
            redisTemplate.expire(updatedAtKey(result.materializedKey()), result.windowTtl());
            redisTemplate.expire(definitionVersionKey(result.materializedKey()), result.windowTtl());
        }
        String latestKey = result.featureKey().redisKey();
        if (result.updateLatest() && !latestKey.equals(result.materializedKey())) {
            writeMaterializedValue(latestKey, value, result);
        }
    }

    private void writeMaterializedValue(String key, String value, AggregationResult result) {
        redisTemplate.opsForValue().set(key, value);
        redisTemplate.opsForValue().set(updatedAtKey(key), result.updatedAt().toString());
        redisTemplate.opsForValue().set(definitionVersionKey(key), Integer.toString(result.definition().version()));
    }

    private static String updatedAtKey(String key) {
        return key + ":updated-at";
    }

    private static String definitionVersionKey(String key) {
        return key + ":definition-version";
    }
}

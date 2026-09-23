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
        List<AggregationResult> results = engine.process(event);
        for (AggregationResult result : results) {
            materialize(result);
        }
        return results.isEmpty() ? null : results.getFirst().featureValue();
    }

    private void materialize(AggregationResult result) {
        String value = result.value().toString();
        redisTemplate.opsForValue().set(result.materializedKey(), value);
        String latestKey = result.featureKey().redisKey();
        if (!latestKey.equals(result.materializedKey())) {
            redisTemplate.opsForValue().set(latestKey, value);
        }
    }
}

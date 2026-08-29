package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.eventmodel.EventValidator;
import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureKey;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureNames;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureValue;
import java.time.Instant;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
class RequestCountTotalProcessor {

    private final EventValidator eventValidator = new EventValidator();
    private final RequestCountTotalStateStore stateStore;
    private final StringRedisTemplate redisTemplate;

    RequestCountTotalProcessor(RequestCountTotalStateStore stateStore, StringRedisTemplate redisTemplate) {
        this.stateStore = stateStore;
        this.redisTemplate = redisTemplate;
    }

    FeatureValue process(PlatformEvent event) {
        eventValidator.validate(event);

        if (!"request.completed".equals(event.eventType())) {
            return null;
        }

        long increment = ((Number) event.payload().get("count")).longValue();
        FeatureKey key = new FeatureKey(
                event.entity().type(),
                event.entity().id(),
                FeatureNames.REQUEST_COUNT_TOTAL
        );
        long total = stateStore.add(key.redisKey(), increment);
        FeatureValue value = new FeatureValue(key, total, Instant.now());
        redisTemplate.opsForValue().set(key.redisKey(), Long.toString(value.value()));

        return value;
    }
}

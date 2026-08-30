package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureKey;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureNames;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureValue;
import java.time.Instant;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
class RequestCountTotalProcessor implements PlatformEventProcessor {

    private final RequestCountTotalStateStore stateStore;
    private final StringRedisTemplate redisTemplate;

    RequestCountTotalProcessor(RequestCountTotalStateStore stateStore, StringRedisTemplate redisTemplate) {
        this.stateStore = stateStore;
        this.redisTemplate = redisTemplate;
    }

    @Override
    public boolean supports(PlatformEvent event) {
        return "request.completed".equals(event.eventType());
    }

    @Override
    public FeatureValue process(PlatformEvent event) {
        if (!supports(event)) {
            return null;
        }

        long increment = RequestCompletedPayload.from(event).count();
        FeatureKey key = new FeatureKey(
                event.entity().type(),
                event.entity().id(),
                FeatureNames.REQUEST_COUNT_TOTAL
        );
        long total = stateStore.add(key.redisKey(), increment);
        FeatureValue value = new FeatureValue(key, total, Instant.now());
        redisTemplate.opsForValue().set(key.redisKey(), Long.toString(total));

        return value;
    }
}

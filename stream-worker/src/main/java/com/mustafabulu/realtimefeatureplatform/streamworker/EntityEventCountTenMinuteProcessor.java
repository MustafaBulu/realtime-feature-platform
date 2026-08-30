package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureKey;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureNames;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureValue;
import com.mustafabulu.realtimefeatureplatform.featuremodel.WindowedFeatureKey;
import java.time.Duration;
import java.time.Instant;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

@Service
class EntityEventCountTenMinuteProcessor implements PlatformEventProcessor {

    static final Duration WINDOW_SIZE = Duration.ofMinutes(10);

    private final RequestCountTotalStateStore stateStore;
    private final StringRedisTemplate redisTemplate;

    EntityEventCountTenMinuteProcessor(RequestCountTotalStateStore stateStore, StringRedisTemplate redisTemplate) {
        this.stateStore = stateStore;
        this.redisTemplate = redisTemplate;
    }

    @Override
    public boolean supports(PlatformEvent event) {
        return true;
    }

    @Override
    public FeatureValue process(PlatformEvent event) {
        FeatureKey featureKey = new FeatureKey(
                event.entity().type(),
                event.entity().id(),
                FeatureNames.ENTITY_EVENT_COUNT_10M
        );
        Instant windowStart = TumblingWindow.startFor(event.eventTime(), WINDOW_SIZE);
        WindowedFeatureKey windowedFeatureKey = new WindowedFeatureKey(featureKey, windowStart);
        long count = stateStore.add(windowedFeatureKey.redisKey(), 1L);

        redisTemplate.opsForValue().set(windowedFeatureKey.redisKey(), Long.toString(count));
        redisTemplate.opsForValue().set(featureKey.redisKey(), Long.toString(count));

        return new FeatureValue(featureKey, count, Instant.now());
    }
}

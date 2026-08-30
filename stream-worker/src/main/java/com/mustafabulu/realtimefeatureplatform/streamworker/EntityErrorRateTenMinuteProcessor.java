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
class EntityErrorRateTenMinuteProcessor implements PlatformEventProcessor {

    static final Duration WINDOW_SIZE = Duration.ofMinutes(10);

    private final RequestCountTotalStateStore stateStore;
    private final StringRedisTemplate redisTemplate;

    EntityErrorRateTenMinuteProcessor(RequestCountTotalStateStore stateStore, StringRedisTemplate redisTemplate) {
        this.stateStore = stateStore;
        this.redisTemplate = redisTemplate;
    }

    @Override
    public boolean supports(PlatformEvent event) {
        return "request.completed".equals(event.eventType()) && event.payload().containsKey("statusCode");
    }

    @Override
    public FeatureValue process(PlatformEvent event) {
        RequestCompletedPayload payload = RequestCompletedPayload.from(event);
        FeatureKey featureKey = new FeatureKey(
                event.entity().type(),
                event.entity().id(),
                FeatureNames.ENTITY_ERROR_RATE_10M
        );
        String windowedKey = new WindowedFeatureKey(
                featureKey,
                TumblingWindow.startFor(event.eventTime(), WINDOW_SIZE)
        ).redisKey();

        long requestCount = stateStore.add(windowedKey + ":requests", payload.count());
        long errorCount = stateStore.add(windowedKey + ":errors", isServerError(payload) ? payload.count() : 0L);
        double errorRate = requestCount == 0 ? 0.0 : (double) errorCount / requestCount;

        redisTemplate.opsForValue().set(windowedKey, Double.toString(errorRate));
        redisTemplate.opsForValue().set(featureKey.redisKey(), Double.toString(errorRate));

        return new FeatureValue(featureKey, errorRate, Instant.now());
    }

    private static boolean isServerError(RequestCompletedPayload payload) {
        return payload.statusCode().isPresent() && payload.statusCode().getAsLong() >= 500;
    }
}

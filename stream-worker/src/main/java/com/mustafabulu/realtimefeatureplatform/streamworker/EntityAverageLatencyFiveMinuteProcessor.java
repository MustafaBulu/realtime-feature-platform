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
class EntityAverageLatencyFiveMinuteProcessor implements PlatformEventProcessor {

    static final Duration WINDOW_SIZE = Duration.ofMinutes(5);

    private final RequestCountTotalStateStore stateStore;
    private final StringRedisTemplate redisTemplate;

    EntityAverageLatencyFiveMinuteProcessor(RequestCountTotalStateStore stateStore, StringRedisTemplate redisTemplate) {
        this.stateStore = stateStore;
        this.redisTemplate = redisTemplate;
    }

    @Override
    public boolean supports(PlatformEvent event) {
        return "request.completed".equals(event.eventType()) && event.payload().containsKey("latencyMs");
    }

    @Override
    public FeatureValue process(PlatformEvent event) {
        RequestCompletedPayload payload = RequestCompletedPayload.from(event);
        FeatureKey featureKey = new FeatureKey(
                event.entity().type(),
                event.entity().id(),
                FeatureNames.ENTITY_AVG_LATENCY_MS_5M
        );
        String windowedKey = new WindowedFeatureKey(
                featureKey,
                TumblingWindow.startFor(event.eventTime(), WINDOW_SIZE)
        ).redisKey();

        long weightedLatencySum = stateStore.add(
                windowedKey + ":latency_sum_ms",
                payload.latencyMs().orElse(0L) * payload.count()
        );
        long requestCount = stateStore.add(windowedKey + ":requests", payload.count());
        double averageLatencyMs = requestCount == 0 ? 0.0 : (double) weightedLatencySum / requestCount;

        redisTemplate.opsForValue().set(windowedKey, Double.toString(averageLatencyMs));
        redisTemplate.opsForValue().set(featureKey.redisKey(), Double.toString(averageLatencyMs));

        return new FeatureValue(featureKey, averageLatencyMs, Instant.now());
    }
}

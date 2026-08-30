package com.mustafabulu.realtimefeatureplatform.featuremodel;

import java.time.Instant;
import java.util.Objects;

public record WindowedFeatureKey(FeatureKey featureKey, Instant windowStart) {

    private static final String SEPARATOR = ":";

    public WindowedFeatureKey {
        Objects.requireNonNull(featureKey, "featureKey must not be null");
        Objects.requireNonNull(windowStart, "windowStart must not be null");
    }

    public String redisKey() {
        return featureKey.redisKey() + SEPARATOR + "window" + SEPARATOR + windowStart.toEpochMilli();
    }
}

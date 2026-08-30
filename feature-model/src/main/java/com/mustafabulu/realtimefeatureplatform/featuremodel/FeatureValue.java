package com.mustafabulu.realtimefeatureplatform.featuremodel;

import java.time.Instant;
import java.util.Objects;

public record FeatureValue(FeatureKey key, Number value, Instant updatedAt) {

    public FeatureValue {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
        Objects.requireNonNull(updatedAt, "updatedAt must not be null");
    }
}

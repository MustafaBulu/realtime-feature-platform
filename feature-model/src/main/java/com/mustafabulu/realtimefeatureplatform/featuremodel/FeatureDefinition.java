package com.mustafabulu.realtimefeatureplatform.featuremodel;

import java.time.Duration;
import java.util.Objects;

public record FeatureDefinition(
        String name,
        String eventType,
        AggregationType aggregationType,
        WindowType windowType,
        Duration windowSize,
        Duration slide
) {

    public FeatureDefinition {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(aggregationType, "aggregationType must not be null");
        Objects.requireNonNull(windowType, "windowType must not be null");
        Objects.requireNonNull(windowSize, "windowSize must not be null");

        if (name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        if (eventType.isBlank()) {
            throw new IllegalArgumentException("eventType must not be blank");
        }
        if (windowSize.isZero() || windowSize.isNegative()) {
            throw new IllegalArgumentException("windowSize must be positive");
        }

        slide = slide == null ? windowSize : slide;
        if (slide.isZero() || slide.isNegative()) {
            throw new IllegalArgumentException("slide must be positive");
        }
    }
}

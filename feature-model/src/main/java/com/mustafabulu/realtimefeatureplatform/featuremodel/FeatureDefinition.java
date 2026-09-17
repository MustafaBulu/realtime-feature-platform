package com.mustafabulu.realtimefeatureplatform.featuremodel;

import java.time.Duration;
import java.util.Objects;

public record FeatureDefinition(
        String name,
        String eventType,
        String entityType,
        AggregationType aggregationType,
        String valueField,
        FeatureFilter filter,
        WindowType windowType,
        Duration windowSize,
        Duration slide,
        int version,
        FeatureDefinitionState state
) {

    public static final String ALL_ENTITY_TYPES = "*";

    public FeatureDefinition(
            String name,
            String eventType,
            AggregationType aggregationType,
            WindowType windowType,
            Duration windowSize,
            Duration slide
    ) {
        this(
                name,
                eventType,
                ALL_ENTITY_TYPES,
                aggregationType,
                null,
                null,
                windowType,
                windowSize,
                slide,
                1,
                FeatureDefinitionState.ACTIVE
        );
    }

    public FeatureDefinition {
        Objects.requireNonNull(name, "name must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(entityType, "entityType must not be null");
        Objects.requireNonNull(aggregationType, "aggregationType must not be null");
        Objects.requireNonNull(windowType, "windowType must not be null");
        Objects.requireNonNull(state, "state must not be null");

        if (name.isBlank()) {
            throw new IllegalArgumentException("name must not be blank");
        }
        if (eventType.isBlank()) {
            throw new IllegalArgumentException("eventType must not be blank");
        }
        if (entityType.isBlank()) {
            throw new IllegalArgumentException("entityType must not be blank");
        }
        if (version < 1) {
            throw new IllegalArgumentException("version must be positive");
        }

        if (requiresValueField(aggregationType)) {
            FeatureDefinitionFields.requireValidFieldPath(valueField, "valueField");
        } else if (valueField != null && !valueField.isBlank()) {
            FeatureDefinitionFields.requireValidFieldPath(valueField, "valueField");
        }

        if (windowType == WindowType.NONE) {
            if (windowSize != null || slide != null) {
                throw new IllegalArgumentException("unwindowed definitions must not define windowSize or slide");
            }
        } else {
            Objects.requireNonNull(windowSize, "windowSize must not be null");
            if (windowSize.isZero() || windowSize.isNegative()) {
                throw new IllegalArgumentException("windowSize must be positive");
            }

            slide = slide == null ? windowSize : slide;
            if (slide.isZero() || slide.isNegative()) {
                throw new IllegalArgumentException("slide must be positive");
            }
            if (windowType == WindowType.SLIDING && slide.compareTo(windowSize) > 0) {
                throw new IllegalArgumentException("sliding window slide must not be larger than windowSize");
            }
        }
    }

    private static boolean requiresValueField(AggregationType aggregationType) {
        return aggregationType == AggregationType.SUM
                || aggregationType == AggregationType.AVG
                || aggregationType == AggregationType.DISTINCT_COUNT;
    }
}

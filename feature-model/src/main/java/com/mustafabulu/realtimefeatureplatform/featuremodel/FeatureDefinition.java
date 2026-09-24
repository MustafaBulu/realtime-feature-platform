package com.mustafabulu.realtimefeatureplatform.featuremodel;

import java.time.Duration;
import java.util.Objects;

public record FeatureDefinition(
        String name,
        String eventType,
        String entityType,
        AggregationType aggregationType,
        String valueField,
        String weightField,
        FeatureFilter filter,
        FeatureFilter numeratorFilter,
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
                null,
                null,
                windowType,
                windowSize,
                slide,
                1,
                FeatureDefinitionState.ACTIVE
        );
    }

    public FeatureDefinition(
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
        this(
                name,
                eventType,
                entityType,
                aggregationType,
                valueField,
                null,
                filter,
                null,
                windowType,
                windowSize,
                slide,
                version,
                state
        );
    }

    public FeatureDefinition(
            String name,
            String eventType,
            String entityType,
            AggregationType aggregationType,
            String valueField,
            String weightField,
            FeatureFilter filter,
            WindowType windowType,
            Duration windowSize,
            Duration slide,
            int version,
            FeatureDefinitionState state
    ) {
        this(
                name,
                eventType,
                entityType,
                aggregationType,
                valueField,
                weightField,
                filter,
                null,
                windowType,
                windowSize,
                slide,
                version,
                state
        );
    }

    public FeatureDefinition {
        requireNonBlank(name, "name");
        requireNonBlank(eventType, "eventType");
        requireNonBlank(entityType, "entityType");
        Objects.requireNonNull(aggregationType, "aggregationType must not be null");
        Objects.requireNonNull(windowType, "windowType must not be null");
        Objects.requireNonNull(state, "state must not be null");

        if (version < 1) {
            throw new IllegalArgumentException("version must be positive");
        }

        requireValidAggregationFields(aggregationType, valueField, weightField, numeratorFilter);
        if (windowType == WindowType.NONE) {
            requireUnwindowed(windowSize, slide);
        } else {
            slide = requireWindowed(windowType, windowSize, slide);
        }
    }

    private static void requireNonBlank(String value, String fieldName) {
        Objects.requireNonNull(value, fieldName + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
    }

    private static void requireValidAggregationFields(
            AggregationType aggregationType,
            String valueField,
            String weightField,
            FeatureFilter numeratorFilter
    ) {
        if (requiresValueField(aggregationType) || hasText(valueField)) {
            FeatureDefinitionFields.requireValidFieldPath(valueField, "valueField");
        }
        if (hasText(weightField)) {
            FeatureDefinitionFields.requireValidFieldPath(weightField, "weightField");
        }
        if (aggregationType == AggregationType.RATIO && numeratorFilter == null) {
            throw new IllegalArgumentException("ratio definitions must define numeratorFilter");
        }
    }

    private static void requireUnwindowed(Duration windowSize, Duration slide) {
        if (windowSize != null || slide != null) {
            throw new IllegalArgumentException("unwindowed definitions must not define windowSize or slide");
        }
    }

    private static Duration requireWindowed(WindowType windowType, Duration windowSize, Duration slide) {
        Objects.requireNonNull(windowSize, "windowSize must not be null");
        requirePositive(windowSize, "windowSize");

        Duration effectiveSlide = slide == null ? windowSize : slide;
        requirePositive(effectiveSlide, "slide");
        if (windowType == WindowType.SLIDING && effectiveSlide.compareTo(windowSize) > 0) {
            throw new IllegalArgumentException("sliding window slide must not be larger than windowSize");
        }
        return effectiveSlide;
    }

    private static void requirePositive(Duration duration, String fieldName) {
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(fieldName + " must be positive");
        }
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private static boolean requiresValueField(AggregationType aggregationType) {
        return aggregationType == AggregationType.SUM
                || aggregationType == AggregationType.AVG
                || aggregationType == AggregationType.DISTINCT_COUNT
                || aggregationType == AggregationType.RATIO;
    }
}

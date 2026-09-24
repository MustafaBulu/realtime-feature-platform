package com.mustafabulu.realtimefeatureplatform.featuremodel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class FeatureDefinitionTest {

    @Test
    void defaultsSlideToWindowSize() {
        FeatureDefinition definition = new FeatureDefinition(
                "failed_logins_5m",
                "auth.failed",
                AggregationType.COUNT,
                WindowType.TUMBLING,
                Duration.ofMinutes(5),
                null
        );

        assertEquals(Duration.ofMinutes(5), definition.slide());
        assertEquals(FeatureDefinition.ALL_ENTITY_TYPES, definition.entityType());
        assertEquals(1, definition.version());
        assertEquals(FeatureDefinitionState.ACTIVE, definition.state());
    }

    @Test
    void rejectsNonPositiveWindowSize() {
        assertThrows(IllegalArgumentException.class, () -> new FeatureDefinition(
                "failed_logins_5m",
                "auth.failed",
                AggregationType.COUNT,
                WindowType.TUMBLING,
                Duration.ZERO,
                null
        ));
    }

    @Test
    void supportsUnwindowedDefinition() {
        FeatureDefinition definition = new FeatureDefinition(
                "request_count_total",
                "request.completed",
                "service",
                AggregationType.COUNT,
                null,
                null,
                WindowType.NONE,
                null,
                null,
                2,
                FeatureDefinitionState.DRAFT
        );

        assertEquals(WindowType.NONE, definition.windowType());
        assertEquals("service", definition.entityType());
        assertEquals(2, definition.version());
        assertEquals(FeatureDefinitionState.DRAFT, definition.state());
    }

    @Test
    void requiresValueFieldForNumericAggregations() {
        assertThrows(IllegalArgumentException.class, () -> new FeatureDefinition(
                "request_count_total",
                "request.completed",
                "service",
                AggregationType.SUM,
                null,
                null,
                WindowType.NONE,
                null,
                null,
                1,
                FeatureDefinitionState.ACTIVE
        ));
    }

    @Test
    void acceptsValueFieldAndFilter() {
        FeatureDefinition definition = new FeatureDefinition(
                "server_error_count_10m",
                "request.completed",
                "service",
                AggregationType.SUM,
                "count",
                new FeatureFilter("statusCode", FeatureFilterOperator.GTE, "500"),
                WindowType.TUMBLING,
                Duration.ofMinutes(10),
                null,
                1,
                FeatureDefinitionState.ACTIVE
        );

        assertEquals("count", definition.valueField());
        assertEquals(Duration.ofMinutes(10), definition.slide());
        assertEquals(FeatureFilterOperator.GTE, definition.filter().operator());
    }

    @Test
    void rejectsInvalidValueFieldPath() {
        assertThrows(IllegalArgumentException.class, () -> new FeatureDefinition(
                "request_count_total",
                "request.completed",
                "service",
                AggregationType.SUM,
                "payload-count",
                null,
                WindowType.NONE,
                null,
                null,
                1,
                FeatureDefinitionState.ACTIVE
        ));
    }

    @Test
    void rejectsSlidingWindowSlideLargerThanWindowSize() {
        Duration windowSize = Duration.ofMinutes(10);
        Duration slide = Duration.ofMinutes(15);

        assertThrows(IllegalArgumentException.class, () -> new FeatureDefinition(
                "request_count_10m",
                "request.completed",
                "service",
                AggregationType.COUNT,
                null,
                null,
                WindowType.SLIDING,
                windowSize,
                slide,
                1,
                FeatureDefinitionState.ACTIVE
        ));
    }

    @Test
    void parsesCompactDurations() {
        assertEquals(Duration.ofMillis(250), FeatureDurationParser.parse("250ms"));
        assertEquals(Duration.ofSeconds(30), FeatureDurationParser.parse("30s"));
        assertEquals(Duration.ofMinutes(10), FeatureDurationParser.parse("10m"));
        assertEquals(Duration.ofHours(2), FeatureDurationParser.parse("2h"));
        assertEquals(Duration.ofDays(1), FeatureDurationParser.parse("1d"));
    }

    @Test
    void parsesIsoDurations() {
        assertEquals(Duration.ofMinutes(5), FeatureDurationParser.parse("PT5M"));
    }

    @Test
    void rejectsInvalidDuration() {
        assertThrows(IllegalArgumentException.class, () -> FeatureDurationParser.parse("0m"));
        assertThrows(IllegalArgumentException.class, () -> FeatureDurationParser.parse("ten-minutes"));
    }

    @Test
    void existsFilterClearsValue() {
        FeatureFilter filter = new FeatureFilter("latencyMs", FeatureFilterOperator.EXISTS, "ignored");

        assertEquals("latencyMs", filter.field());
        assertEquals(FeatureFilterOperator.EXISTS, filter.operator());
        assertNull(filter.value());
    }
}

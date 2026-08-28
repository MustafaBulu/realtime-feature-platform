package com.mustafabulu.realtimefeatureplatform.featuremodel;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
}

package com.mustafabulu.realtimefeatureplatform.featuremodel;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

class MutableInMemoryFeatureDefinitionRepositoryTest {

    @Test
    void activatesOneVersionAndInactivatesOtherVersionsForSameFeature() {
        FeatureDefinition first = definition(1, FeatureDefinitionState.ACTIVE);
        FeatureDefinition second = definition(2, FeatureDefinitionState.DRAFT);
        MutableInMemoryFeatureDefinitionRepository repository =
                new MutableInMemoryFeatureDefinitionRepository(List.of(first, second));

        FeatureDefinition activated = repository.activate("request_count_total", 2);

        assertEquals(FeatureDefinitionState.ACTIVE, activated.state());
        assertEquals(List.of(activated), repository.findActive());
    }

    @Test
    void deactivatesDefinition() {
        FeatureDefinition definition = definition(1, FeatureDefinitionState.ACTIVE);
        MutableInMemoryFeatureDefinitionRepository repository =
                new MutableInMemoryFeatureDefinitionRepository(List.of(definition));

        FeatureDefinition deactivated = repository.deactivate("request_count_total", 1);

        assertEquals(FeatureDefinitionState.INACTIVE, deactivated.state());
        assertEquals(List.of(), repository.findActive());
    }

    private static FeatureDefinition definition(int version, FeatureDefinitionState state) {
        return new FeatureDefinition(
                "request_count_total",
                "request.completed",
                "service",
                AggregationType.SUM,
                "count",
                null,
                null,
                WindowType.NONE,
                null,
                null,
                version,
                state
        );
    }
}

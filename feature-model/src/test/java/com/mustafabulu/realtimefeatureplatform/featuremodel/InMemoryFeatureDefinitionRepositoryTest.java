package com.mustafabulu.realtimefeatureplatform.featuremodel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;

class InMemoryFeatureDefinitionRepositoryTest {

    @Test
    void returnsOnlyActiveDefinitions() {
        FeatureDefinition active = definition("active_feature", FeatureDefinitionState.ACTIVE, 1);
        FeatureDefinition draft = definition("draft_feature", FeatureDefinitionState.DRAFT, 1);
        FeatureDefinition inactive = definition("inactive_feature", FeatureDefinitionState.INACTIVE, 1);
        FeatureDefinitionRepository repository =
                new InMemoryFeatureDefinitionRepository(List.of(active, draft, inactive));

        assertEquals(List.of(active), repository.findActive());
    }

    @Test
    void rejectsDuplicateNameAndVersion() {
        FeatureDefinition first = definition("request_count_total", FeatureDefinitionState.ACTIVE, 1);
        FeatureDefinition duplicate = definition("request_count_total", FeatureDefinitionState.DRAFT, 1);
        List<FeatureDefinition> definitions = List.of(first, duplicate);

        assertThrows(IllegalArgumentException.class, () -> new InMemoryFeatureDefinitionRepository(definitions));
    }

    @Test
    void allowsDifferentVersionsForSameName() {
        FeatureDefinition first = definition("request_count_total", FeatureDefinitionState.ACTIVE, 1);
        FeatureDefinition second = definition("request_count_total", FeatureDefinitionState.DRAFT, 2);
        FeatureDefinitionRepository repository = new InMemoryFeatureDefinitionRepository(List.of(first, second));

        assertEquals(List.of(first, second), repository.findAll());
    }

    private static FeatureDefinition definition(String name, FeatureDefinitionState state, int version) {
        return new FeatureDefinition(
                name,
                "request.completed",
                "service",
                AggregationType.COUNT,
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

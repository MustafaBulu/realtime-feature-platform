package com.mustafabulu.realtimefeatureplatform.streamworker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mustafabulu.realtimefeatureplatform.featuremodel.AggregationType;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinition;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinitionState;
import com.mustafabulu.realtimefeatureplatform.featuremodel.InMemoryFeatureDefinitionRepository;
import com.mustafabulu.realtimefeatureplatform.featuremodel.WindowType;
import java.util.List;
import org.junit.jupiter.api.Test;

class WorkerFeatureDefinitionLoaderTest {

    @Test
    void loadsOnlyActiveDefinitionsForProcessing() {
        FeatureDefinition active = definition("active_feature", FeatureDefinitionState.ACTIVE);
        FeatureDefinition draft = definition("draft_feature", FeatureDefinitionState.DRAFT);
        WorkerFeatureDefinitionLoader loader = new WorkerFeatureDefinitionLoader(
                new InMemoryFeatureDefinitionRepository(List.of(active, draft))
        );

        assertEquals(List.of(active, draft), loader.allDefinitions());
        assertEquals(List.of(active), loader.activeDefinitions());
    }

    private static FeatureDefinition definition(String name, FeatureDefinitionState state) {
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
                1,
                state
        );
    }
}

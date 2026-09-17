package com.mustafabulu.realtimefeatureplatform.streamworker;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mustafabulu.realtimefeatureplatform.featuremodel.BuiltInFeatureDefinitions;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureNames;
import com.mustafabulu.realtimefeatureplatform.featuremodel.InMemoryFeatureDefinitionRepository;
import org.junit.jupiter.api.Test;

class FeatureDefinitionDebugControllerTest {

    @Test
    void returnsActiveDefinitionSummary() {
        WorkerFeatureDefinitionLoader loader = new WorkerFeatureDefinitionLoader(
                new InMemoryFeatureDefinitionRepository(BuiltInFeatureDefinitions.all())
        );
        FeatureDefinitionDebugController controller = new FeatureDefinitionDebugController(loader);

        FeatureDefinitionDebugController.FeatureDefinitionDebugResponse response = controller.definitions();

        assertEquals(3, response.totalDefinitions());
        assertEquals(3, response.activeDefinitions());
        assertEquals(FeatureNames.REQUEST_COUNT_TOTAL, response.definitions().getFirst().name());
    }
}

package com.mustafabulu.realtimefeatureplatform.featureapi;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mustafabulu.realtimefeatureplatform.featuremodel.AggregationType;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinition;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinitionState;
import com.mustafabulu.realtimefeatureplatform.featuremodel.MutableInMemoryFeatureDefinitionRepository;
import com.mustafabulu.realtimefeatureplatform.featuremodel.WindowType;
import java.util.List;
import org.junit.jupiter.api.Test;

class FeatureRegistryControllerTest {

    @Test
    void savesAndActivatesDefinitions() {
        MutableInMemoryFeatureDefinitionRepository repository =
                new MutableInMemoryFeatureDefinitionRepository(List.of());
        FeatureRegistryController controller = new FeatureRegistryController(repository);

        FeatureDefinition saved = controller.save(new FeatureRegistryController.FeatureDefinitionRequest(
                "request_count_total",
                "request.completed",
                "service",
                AggregationType.SUM,
                "count",
                null,
                null,
                null,
                WindowType.NONE,
                null,
                null,
                1,
                null
        ));
        FeatureDefinition activated = controller.activate("request_count_total", 1);

        assertEquals(FeatureDefinitionState.DRAFT, saved.state());
        assertEquals(FeatureDefinitionState.ACTIVE, activated.state());
        assertEquals(1, controller.definitions().size());
    }
}

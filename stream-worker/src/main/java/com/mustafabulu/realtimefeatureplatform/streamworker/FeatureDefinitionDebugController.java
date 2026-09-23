package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.featuremodel.AggregationType;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinition;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinitionState;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureFilter;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureFilterOperator;
import com.mustafabulu.realtimefeatureplatform.featuremodel.WindowType;
import java.time.Duration;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
class FeatureDefinitionDebugController {

    private final WorkerFeatureDefinitionLoader definitionLoader;

    FeatureDefinitionDebugController(WorkerFeatureDefinitionLoader definitionLoader) {
        this.definitionLoader = definitionLoader;
    }

    @GetMapping("/internal/feature-definitions")
    FeatureDefinitionDebugResponse definitions() {
        List<FeatureDefinition> allDefinitions = definitionLoader.allDefinitions();
        List<FeatureDefinitionView> activeDefinitions = definitionLoader.activeDefinitions().stream()
                .map(FeatureDefinitionView::from)
                .toList();

        return new FeatureDefinitionDebugResponse(
                allDefinitions.size(),
                activeDefinitions.size(),
                activeDefinitions
        );
    }

    record FeatureDefinitionDebugResponse(int totalDefinitions, int activeDefinitions, List<FeatureDefinitionView> definitions) {
    }

    record FeatureDefinitionView(
            String name,
            String eventType,
            String entityType,
            AggregationType aggregationType,
            String valueField,
            String weightField,
            FilterView filter,
            FilterView numeratorFilter,
            WindowType windowType,
            Duration windowSize,
            Duration slide,
            int version,
            FeatureDefinitionState state
    ) {

        static FeatureDefinitionView from(FeatureDefinition definition) {
            return new FeatureDefinitionView(
                    definition.name(),
                    definition.eventType(),
                    definition.entityType(),
                    definition.aggregationType(),
                    definition.valueField(),
                    definition.weightField(),
                    FilterView.from(definition.filter()),
                    FilterView.from(definition.numeratorFilter()),
                    definition.windowType(),
                    definition.windowSize(),
                    definition.slide(),
                    definition.version(),
                    definition.state()
            );
        }
    }

    record FilterView(String field, FeatureFilterOperator operator, String value) {

        static FilterView from(FeatureFilter filter) {
            if (filter == null) {
                return null;
            }
            return new FilterView(filter.field(), filter.operator(), filter.value());
        }
    }
}

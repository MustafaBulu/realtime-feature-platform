package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinition;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureKey;
import com.mustafabulu.realtimefeatureplatform.featuremodel.WindowType;
import com.mustafabulu.realtimefeatureplatform.featuremodel.WindowedFeatureKey;
import java.time.Instant;
import java.util.List;
import org.springframework.stereotype.Component;

@Component
class GenericAggregationEngine {

    private final WorkerFeatureDefinitionLoader definitionLoader;
    private final AggregatorRegistry aggregatorRegistry;
    private final AggregationStateStore stateStore;

    GenericAggregationEngine(
            WorkerFeatureDefinitionLoader definitionLoader,
            AggregatorRegistry aggregatorRegistry,
            AggregationStateStore stateStore
    ) {
        this.definitionLoader = definitionLoader;
        this.aggregatorRegistry = aggregatorRegistry;
        this.stateStore = stateStore;
    }

    boolean supports(PlatformEvent event) {
        return definitionLoader.activeDefinitions().stream()
                .anyMatch(definition -> matchesEvent(definition, event)
                        && FeatureFilterEvaluator.matches(event, definition.filter()));
    }

    List<AggregationResult> process(PlatformEvent event) {
        return definitionLoader.activeDefinitions().stream()
                .filter(definition -> matchesEvent(definition, event))
                .filter(definition -> FeatureFilterEvaluator.matches(event, definition.filter()))
                .map(definition -> updateFeature(definition, event))
                .toList();
    }

    private AggregationResult updateFeature(FeatureDefinition definition, PlatformEvent event) {
        FeatureKey featureKey = new FeatureKey(event.entity().type(), event.entity().id(), definition.name());
        String stateKey = stateKey(definition, event, featureKey);
        Aggregator aggregator = aggregatorRegistry.get(definition.aggregationType());
        Number value = aggregator.aggregate(new AggregationInput(definition, event, stateKey, stateStore));

        return new AggregationResult(definition, featureKey, stateKey, value, Instant.now());
    }

    private static boolean matchesEvent(FeatureDefinition definition, PlatformEvent event) {
        return definition.eventType().equals(event.eventType())
                && (FeatureDefinition.ALL_ENTITY_TYPES.equals(definition.entityType())
                || definition.entityType().equals(event.entity().type()));
    }

    private static String stateKey(FeatureDefinition definition, PlatformEvent event, FeatureKey featureKey) {
        return switch (definition.windowType()) {
            case NONE -> featureKey.redisKey();
            case TUMBLING -> new WindowedFeatureKey(
                    featureKey,
                    TumblingWindow.startFor(event.eventTime(), definition.windowSize())
            ).redisKey();
            case SLIDING -> throw new IllegalArgumentException("sliding windows are not implemented yet");
        };
    }
}

package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinition;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureKey;
import com.mustafabulu.realtimefeatureplatform.featuremodel.WindowType;
import com.mustafabulu.realtimefeatureplatform.featuremodel.WindowedFeatureKey;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
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
        return process(event, EventTimeAssessment.onTimeAt(event.eventTime()));
    }

    List<AggregationResult> process(PlatformEvent event, EventTimeAssessment assessment) {
        return definitionLoader.activeDefinitions().stream()
                .filter(definition -> matchesEvent(definition, event))
                .filter(definition -> FeatureFilterEvaluator.matches(event, definition.filter()))
                .flatMap(definition -> updateFeature(definition, event, assessment).stream())
                .toList();
    }

    private List<AggregationResult> updateFeature(
            FeatureDefinition definition,
            PlatformEvent event,
            EventTimeAssessment assessment
    ) {
        FeatureKey featureKey = new FeatureKey(event.entity().type(), event.entity().id(), definition.name());
        return windowStarts(definition, event).stream()
                .map(windowStart -> updateWindow(definition, event, assessment, featureKey, windowStart))
                .toList();
    }

    private AggregationResult updateWindow(
            FeatureDefinition definition,
            PlatformEvent event,
            EventTimeAssessment assessment,
            FeatureKey featureKey,
            Instant windowStart
    ) {
        String stateKey = stateKey(definition, featureKey, windowStart);
        Aggregator aggregator = aggregatorRegistry.get(definition.aggregationType());
        Number value = aggregator.aggregate(new AggregationInput(definition, event, stateKey, stateStore));

        return new AggregationResult(
                definition,
                featureKey,
                stateKey,
                value,
                assessment.processingTime(),
                windowStart,
                windowTtl(definition),
                shouldUpdateLatest(definition, event, featureKey, windowStart)
        );
    }

    private static boolean matchesEvent(FeatureDefinition definition, PlatformEvent event) {
        return definition.eventType().equals(event.eventType())
                && (FeatureDefinition.ALL_ENTITY_TYPES.equals(definition.entityType())
                || definition.entityType().equals(event.entity().type()));
    }

    private static List<Instant> windowStarts(FeatureDefinition definition, PlatformEvent event) {
        return switch (definition.windowType()) {
            case NONE -> Collections.singletonList(null);
            case TUMBLING -> List.of(TumblingWindow.startFor(event.eventTime(), definition.windowSize()));
            case SLIDING -> SlidingWindow.startsContaining(
                    event.eventTime(),
                    definition.windowSize(),
                    definition.slide()
            );
        };
    }

    private static String stateKey(FeatureDefinition definition, FeatureKey featureKey, Instant windowStart) {
        if (definition.windowType() == WindowType.NONE) {
            return featureKey.redisKey();
        }
        return new WindowedFeatureKey(featureKey, windowStart).redisKey();
    }

    private static Duration windowTtl(FeatureDefinition definition) {
        if (definition.windowType() == WindowType.NONE) {
            return null;
        }
        return definition.windowSize().plus(definition.slide());
    }

    private boolean shouldUpdateLatest(
            FeatureDefinition definition,
            PlatformEvent event,
            FeatureKey featureKey,
            Instant windowStart
    ) {
        if (definition.windowType() == WindowType.NONE) {
            return true;
        }
        if (definition.windowType() == WindowType.SLIDING
                && !windowStart.equals(SlidingWindow.latestStartFor(event.eventTime(), definition.slide()))) {
            return false;
        }

        String markerKey = latestWindowMarkerKey(featureKey);
        String previous = stateStore.get(markerKey);
        long nextWindowStart = windowStart.toEpochMilli();
        if (previous != null && nextWindowStart < Long.parseLong(previous)) {
            return false;
        }
        stateStore.put(markerKey, Long.toString(nextWindowStart));
        return true;
    }

    private static String latestWindowMarkerKey(FeatureKey featureKey) {
        return featureKey.redisKey() + ":latest-window-start";
    }
}

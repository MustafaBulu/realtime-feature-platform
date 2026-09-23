package com.mustafabulu.realtimefeatureplatform.streamworker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.mustafabulu.realtimefeatureplatform.featuremodel.AggregationType;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinition;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinitionState;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinitionRepository;
import com.mustafabulu.realtimefeatureplatform.featuremodel.InMemoryFeatureDefinitionRepository;
import com.mustafabulu.realtimefeatureplatform.featuremodel.WindowType;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
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

    @Test
    void reloadsDefinitionsAfterConfiguredInterval() {
        FeatureDefinition first = definition("first_feature", FeatureDefinitionState.ACTIVE);
        FeatureDefinition second = definition("second_feature", FeatureDefinitionState.ACTIVE);
        MutableClock clock = new MutableClock(Instant.parse("2026-09-23T00:00:00Z"));
        MutableRepository repository = new MutableRepository(List.of(first));
        WorkerFeatureDefinitionLoader loader = new WorkerFeatureDefinitionLoader(
                repository,
                Duration.ofSeconds(30),
                clock
        );

        assertEquals(List.of(first), loader.activeDefinitions());

        repository.definitions = List.of(second);
        clock.instant = Instant.parse("2026-09-23T00:00:10Z");
        assertEquals(List.of(first), loader.activeDefinitions());

        clock.instant = Instant.parse("2026-09-23T00:00:30Z");
        assertEquals(List.of(second), loader.activeDefinitions());
    }

    @Test
    void usesLastSuccessfulDefinitionsWhenRegistryReloadFails() {
        FeatureDefinition active = definition("active_feature", FeatureDefinitionState.ACTIVE);
        MutableRepository repository = new MutableRepository(List.of(active));
        WorkerFeatureDefinitionLoader loader = new WorkerFeatureDefinitionLoader(
                repository,
                Duration.ZERO,
                Clock.systemUTC()
        );

        assertEquals(List.of(active), loader.activeDefinitions());

        repository.failure = new IllegalStateException("registry unavailable");

        assertEquals(List.of(active), loader.activeDefinitions());
    }

    @Test
    void failsClosedWhenRegistryIsUnavailableBeforeFirstSuccessfulLoad() {
        MutableRepository repository = new MutableRepository(List.of());
        repository.failure = new IllegalStateException("registry unavailable");
        WorkerFeatureDefinitionLoader loader = new WorkerFeatureDefinitionLoader(
                repository,
                Duration.ZERO,
                Clock.systemUTC()
        );

        assertThrows(IllegalStateException.class, loader::activeDefinitions);
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

    private static final class MutableRepository implements FeatureDefinitionRepository {

        private List<FeatureDefinition> definitions;
        private RuntimeException failure;

        private MutableRepository(List<FeatureDefinition> definitions) {
            this.definitions = new ArrayList<>(definitions);
        }

        @Override
        public List<FeatureDefinition> findAll() {
            if (failure != null) {
                throw failure;
            }
            return definitions;
        }
    }

    private static final class MutableClock extends Clock {

        private Instant instant;

        private MutableClock(Instant instant) {
            this.instant = instant;
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }
}

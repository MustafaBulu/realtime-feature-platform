package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinition;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinitionState;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinitionRepository;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
class WorkerFeatureDefinitionLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerFeatureDefinitionLoader.class);

    private final FeatureDefinitionRepository repository;
    private final Duration reloadInterval;
    private final Clock clock;
    private List<FeatureDefinition> cachedDefinitions = List.of();
    private Instant lastSuccessfulReload = Instant.EPOCH;
    private boolean loaded;

    @Autowired
    WorkerFeatureDefinitionLoader(
            FeatureDefinitionRepository repository,
            @Value("${rfp.feature-registry.reload-interval:PT30S}") Duration reloadInterval
    ) {
        this(repository, reloadInterval, Clock.systemUTC());
    }

    WorkerFeatureDefinitionLoader(FeatureDefinitionRepository repository) {
        this(repository, Duration.ZERO, Clock.systemUTC());
    }

    WorkerFeatureDefinitionLoader(FeatureDefinitionRepository repository, Duration reloadInterval, Clock clock) {
        this.repository = repository;
        this.reloadInterval = reloadInterval;
        this.clock = clock;
    }

    List<FeatureDefinition> allDefinitions() {
        refreshIfNeeded();
        return cachedDefinitions;
    }

    List<FeatureDefinition> activeDefinitions() {
        return allDefinitions().stream()
                .filter(definition -> definition.state() == FeatureDefinitionState.ACTIVE)
                .toList();
    }

    private synchronized void refreshIfNeeded() {
        Instant now = clock.instant();
        if (loaded && now.isBefore(lastSuccessfulReload.plus(reloadInterval))) {
            return;
        }
        try {
            cachedDefinitions = List.copyOf(repository.findAll());
            lastSuccessfulReload = now;
            loaded = true;
        } catch (RuntimeException exception) {
            if (!loaded) {
                throw exception;
            }
            LOGGER.warn("Feature definition registry reload failed; using last successful snapshot: {}", exception.toString());
        }
    }
}

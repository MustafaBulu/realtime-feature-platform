package com.mustafabulu.realtimefeatureplatform.streamworker;

import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinition;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinitionRepository;
import java.util.List;
import org.springframework.stereotype.Component;

@Component
class WorkerFeatureDefinitionLoader {

    private final FeatureDefinitionRepository repository;

    WorkerFeatureDefinitionLoader(FeatureDefinitionRepository repository) {
        this.repository = repository;
    }

    List<FeatureDefinition> allDefinitions() {
        return repository.findAll();
    }

    List<FeatureDefinition> activeDefinitions() {
        return repository.findActive();
    }
}

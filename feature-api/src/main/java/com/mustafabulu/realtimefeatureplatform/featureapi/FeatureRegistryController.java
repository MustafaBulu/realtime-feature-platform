package com.mustafabulu.realtimefeatureplatform.featureapi;

import com.mustafabulu.realtimefeatureplatform.featuremodel.AggregationType;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinition;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinitionState;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureFilter;
import com.mustafabulu.realtimefeatureplatform.featuremodel.MutableFeatureDefinitionRepository;
import com.mustafabulu.realtimefeatureplatform.featuremodel.WindowType;
import java.time.Duration;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
class FeatureRegistryController {

    private final MutableFeatureDefinitionRepository repository;

    FeatureRegistryController(MutableFeatureDefinitionRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/registry/definitions")
    List<FeatureDefinition> definitions() {
        return repository.findAll();
    }

    @PostMapping("/registry/definitions")
    FeatureDefinition save(@RequestBody FeatureDefinitionRequest request) {
        return repository.save(request.toDefinition());
    }

    @PostMapping("/registry/definitions/{name}/versions/{version}/activate")
    FeatureDefinition activate(@PathVariable String name, @PathVariable int version) {
        return repository.activate(name, version);
    }

    @PostMapping("/registry/definitions/{name}/versions/{version}/deactivate")
    FeatureDefinition deactivate(@PathVariable String name, @PathVariable int version) {
        return repository.deactivate(name, version);
    }

    record FeatureDefinitionRequest(
            String name,
            String eventType,
            String entityType,
            AggregationType aggregationType,
            String valueField,
            String weightField,
            FeatureFilter filter,
            FeatureFilter numeratorFilter,
            WindowType windowType,
            Duration windowSize,
            Duration slide,
            int version,
            FeatureDefinitionState state
    ) {

        FeatureDefinition toDefinition() {
            return new FeatureDefinition(
                    name,
                    eventType,
                    entityType,
                    aggregationType,
                    valueField,
                    weightField,
                    filter,
                    numeratorFilter,
                    windowType,
                    windowSize,
                    slide,
                    version,
                    state == null ? FeatureDefinitionState.DRAFT : state
            );
        }
    }
}

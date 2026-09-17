package com.mustafabulu.realtimefeatureplatform.featuremodel;

import java.util.List;

public interface FeatureDefinitionRepository {

    List<FeatureDefinition> findAll();

    default List<FeatureDefinition> findActive() {
        return findAll().stream()
                .filter(definition -> definition.state() == FeatureDefinitionState.ACTIVE)
                .toList();
    }
}

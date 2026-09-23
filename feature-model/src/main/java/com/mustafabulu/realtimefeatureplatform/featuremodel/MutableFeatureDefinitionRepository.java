package com.mustafabulu.realtimefeatureplatform.featuremodel;

public interface MutableFeatureDefinitionRepository extends FeatureDefinitionRepository {

    FeatureDefinition save(FeatureDefinition definition);

    FeatureDefinition activate(String name, int version);

    FeatureDefinition deactivate(String name, int version);
}

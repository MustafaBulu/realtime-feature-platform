package com.mustafabulu.realtimefeatureplatform.featuremodel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class MutableInMemoryFeatureDefinitionRepository implements MutableFeatureDefinitionRepository {

    private final Map<String, FeatureDefinition> definitions = new LinkedHashMap<>();

    public MutableInMemoryFeatureDefinitionRepository(Collection<FeatureDefinition> definitions) {
        definitions.forEach(this::save);
    }

    @Override
    public List<FeatureDefinition> findAll() {
        return List.copyOf(definitions.values());
    }

    @Override
    public FeatureDefinition save(FeatureDefinition definition) {
        definitions.put(key(definition.name(), definition.version()), definition);
        return definition;
    }

    @Override
    public FeatureDefinition activate(String name, int version) {
        FeatureDefinition target = require(name, version);
        new ArrayList<>(definitions.values()).stream()
                .filter(definition -> definition.name().equals(name))
                .forEach(definition -> save(withState(definition, FeatureDefinitionState.INACTIVE)));
        return save(withState(target, FeatureDefinitionState.ACTIVE));
    }

    @Override
    public FeatureDefinition deactivate(String name, int version) {
        return save(withState(require(name, version), FeatureDefinitionState.INACTIVE));
    }

    private FeatureDefinition require(String name, int version) {
        FeatureDefinition definition = definitions.get(key(name, version));
        if (definition == null) {
            throw new IllegalArgumentException("feature definition not found: " + name + ":" + version);
        }
        return definition;
    }

    private static FeatureDefinition withState(FeatureDefinition definition, FeatureDefinitionState state) {
        return new FeatureDefinition(
                definition.name(),
                definition.eventType(),
                definition.entityType(),
                definition.aggregationType(),
                definition.valueField(),
                definition.weightField(),
                definition.filter(),
                definition.numeratorFilter(),
                definition.windowType(),
                definition.windowSize(),
                definition.slide(),
                definition.version(),
                state
        );
    }

    private static String key(String name, int version) {
        return name + ":" + version;
    }
}

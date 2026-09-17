package com.mustafabulu.realtimefeatureplatform.featuremodel;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public final class InMemoryFeatureDefinitionRepository implements FeatureDefinitionRepository {

    private final List<FeatureDefinition> definitions;

    public InMemoryFeatureDefinitionRepository(Collection<FeatureDefinition> definitions) {
        Objects.requireNonNull(definitions, "definitions must not be null");
        this.definitions = List.copyOf(definitions);
        requireUniqueNameVersions(this.definitions);
    }

    @Override
    public List<FeatureDefinition> findAll() {
        return definitions;
    }

    private static void requireUniqueNameVersions(List<FeatureDefinition> definitions) {
        Set<String> seen = new HashSet<>();
        for (FeatureDefinition definition : definitions) {
            String key = definition.name() + ":" + definition.version();
            if (!seen.add(key)) {
                throw new IllegalArgumentException("duplicate feature definition version: " + key);
            }
        }
    }
}

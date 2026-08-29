package com.mustafabulu.realtimefeatureplatform.featuremodel;

import java.util.Objects;

public record FeatureKey(String entityType, String entityId, String featureName) {

    private static final String PREFIX = "feature";
    private static final String SEPARATOR = ":";

    public FeatureKey {
        Objects.requireNonNull(entityType, "entityType must not be null");
        Objects.requireNonNull(entityId, "entityId must not be null");
        Objects.requireNonNull(featureName, "featureName must not be null");

        if (entityType.isBlank()) {
            throw new IllegalArgumentException("entityType must not be blank");
        }
        if (entityId.isBlank()) {
            throw new IllegalArgumentException("entityId must not be blank");
        }
        if (featureName.isBlank()) {
            throw new IllegalArgumentException("featureName must not be blank");
        }
        if (entityType.contains(SEPARATOR) || entityId.contains(SEPARATOR) || featureName.contains(SEPARATOR)) {
            throw new IllegalArgumentException("feature key parts must not contain ':'");
        }
    }

    public String redisKey() {
        return String.join(SEPARATOR, PREFIX, entityType, entityId, featureName);
    }
}

package com.mustafabulu.realtimefeatureplatform.featuremodel;

import java.util.Objects;

public record FeatureFilter(String field, FeatureFilterOperator operator, String value) {

    public FeatureFilter {
        FeatureDefinitionFields.requireValidFieldPath(field, "filter field");
        Objects.requireNonNull(operator, "operator must not be null");

        if (operator == FeatureFilterOperator.EXISTS) {
            value = null;
        } else if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("filter value must not be blank");
        }
    }
}

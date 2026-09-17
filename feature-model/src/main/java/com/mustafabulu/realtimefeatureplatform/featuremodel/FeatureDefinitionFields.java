package com.mustafabulu.realtimefeatureplatform.featuremodel;

import java.util.regex.Pattern;

final class FeatureDefinitionFields {

    private static final Pattern FIELD_PATH =
            Pattern.compile("[A-Za-z][A-Za-z0-9_]*(\\.[A-Za-z][A-Za-z0-9_]*)*");

    private FeatureDefinitionFields() {
    }

    static void requireValidFieldPath(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        if (!FIELD_PATH.matcher(value).matches()) {
            throw new IllegalArgumentException(fieldName + " must be a valid field path");
        }
    }
}

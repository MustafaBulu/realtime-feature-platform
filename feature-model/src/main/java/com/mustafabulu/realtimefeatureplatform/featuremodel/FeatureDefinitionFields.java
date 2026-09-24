package com.mustafabulu.realtimefeatureplatform.featuremodel;

final class FeatureDefinitionFields {

    private FeatureDefinitionFields() {
    }

    static void requireValidFieldPath(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        if (!isValidFieldPath(value)) {
            throw new IllegalArgumentException(fieldName + " must be a valid field path");
        }
    }

    private static boolean isValidFieldPath(String value) {
        boolean expectSegmentStart = true;
        for (int index = 0; index < value.length(); index++) {
            char current = value.charAt(index);
            if (expectSegmentStart) {
                if (isInvalidSegmentStart(current)) {
                    return false;
                }
                expectSegmentStart = false;
            } else if (current == '.') {
                expectSegmentStart = true;
            } else if (isInvalidSegmentCharacter(current)) {
                return false;
            }
        }
        return !expectSegmentStart;
    }

    private static boolean isInvalidSegmentStart(char value) {
        return value < 'A' || value > 'Z' && value < 'a' || value > 'z';
    }

    private static boolean isInvalidSegmentCharacter(char value) {
        return isInvalidSegmentStart(value) && !Character.isDigit(value) && value != '_';
    }
}

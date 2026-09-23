package com.mustafabulu.realtimefeatureplatform.streamworker;

record RatioState(double numerator, double denominator) {

    static RatioState empty() {
        return new RatioState(0.0, 0.0);
    }

    static RatioState parse(String value) {
        if (value == null) {
            return empty();
        }
        String[] parts = value.split(",", -1);
        if (parts.length != 2) {
            throw new IllegalArgumentException("invalid ratio state");
        }
        return new RatioState(Double.parseDouble(parts[0]), Double.parseDouble(parts[1]));
    }

    RatioState add(double denominatorIncrement, boolean numeratorMatches) {
        double nextNumerator = numeratorMatches ? numerator + denominatorIncrement : numerator;
        return new RatioState(nextNumerator, denominator + denominatorIncrement);
    }

    double value() {
        return denominator == 0.0 ? 0.0 : numerator / denominator;
    }

    String serialize() {
        return NumericStateFormat.writeNumber(numerator) + "," + NumericStateFormat.writeNumber(denominator);
    }
}

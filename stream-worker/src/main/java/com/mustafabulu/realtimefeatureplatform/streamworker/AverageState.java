package com.mustafabulu.realtimefeatureplatform.streamworker;

record AverageState(double sum, double weight) {

    static AverageState empty() {
        return new AverageState(0.0, 0.0);
    }

    static AverageState parse(String value) {
        if (value == null) {
            return empty();
        }
        String[] parts = value.split(",", -1);
        if (parts.length != 2) {
            throw new IllegalArgumentException("invalid average state");
        }
        return new AverageState(Double.parseDouble(parts[0]), Double.parseDouble(parts[1]));
    }

    AverageState add(double value, double incrementWeight) {
        return new AverageState(sum + (value * incrementWeight), weight + incrementWeight);
    }

    double value() {
        return weight == 0.0 ? 0.0 : sum / weight;
    }

    String serialize() {
        return NumericStateFormat.writeNumber(sum) + "," + NumericStateFormat.writeNumber(weight);
    }
}

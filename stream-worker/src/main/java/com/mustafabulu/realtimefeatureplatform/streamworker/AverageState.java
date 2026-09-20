package com.mustafabulu.realtimefeatureplatform.streamworker;

record AverageState(double sum, long count) {

    static AverageState empty() {
        return new AverageState(0.0, 0L);
    }

    static AverageState parse(String value) {
        if (value == null) {
            return empty();
        }
        String[] parts = value.split(",", -1);
        if (parts.length != 2) {
            throw new IllegalArgumentException("invalid average state");
        }
        return new AverageState(Double.parseDouble(parts[0]), Long.parseLong(parts[1]));
    }

    AverageState add(double value) {
        return new AverageState(sum + value, count + 1L);
    }

    double value() {
        return count == 0 ? 0.0 : sum / count;
    }

    String serialize() {
        return NumericStateFormat.writeNumber(sum) + "," + count;
    }
}

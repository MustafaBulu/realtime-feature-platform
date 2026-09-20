package com.mustafabulu.realtimefeatureplatform.streamworker;

final class NumericStateFormat {

    private NumericStateFormat() {
    }

    static long readLong(String value) {
        return value == null ? 0L : Long.parseLong(value);
    }

    static double readDouble(String value) {
        return value == null ? 0.0 : Double.parseDouble(value);
    }

    static String writeNumber(double value) {
        if (Double.isFinite(value) && value == Math.rint(value)
                && value <= Long.MAX_VALUE && value >= Long.MIN_VALUE) {
            return Long.toString((long) value);
        }
        return Double.toString(value);
    }

    static Number asNumber(double value) {
        if (Double.isFinite(value) && value == Math.rint(value)
                && value <= Long.MAX_VALUE && value >= Long.MIN_VALUE) {
            return (long) value;
        }
        return value;
    }
}

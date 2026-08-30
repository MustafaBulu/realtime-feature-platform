package com.mustafabulu.realtimefeatureplatform.streamworker;

import java.time.Duration;
import java.time.Instant;

final class TumblingWindow {

    private TumblingWindow() {
    }

    static Instant startFor(Instant eventTime, Duration windowSize) {
        if (windowSize.isZero() || windowSize.isNegative()) {
            throw new IllegalArgumentException("windowSize must be positive");
        }

        long windowMillis = windowSize.toMillis();
        long windowStartMillis = Math.floorDiv(eventTime.toEpochMilli(), windowMillis) * windowMillis;
        return Instant.ofEpochMilli(windowStartMillis);
    }
}

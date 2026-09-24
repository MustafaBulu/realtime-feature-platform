package com.mustafabulu.realtimefeatureplatform.streamworker;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

final class SlidingWindow {

    private SlidingWindow() {
    }

    static Instant latestStartFor(Instant eventTime, Duration slide) {
        requirePositive(slide, "slide");
        long slideMillis = slide.toMillis();
        long startMillis = Math.floorDiv(eventTime.toEpochMilli(), slideMillis) * slideMillis;
        return Instant.ofEpochMilli(startMillis);
    }

    static List<Instant> startsContaining(Instant eventTime, Duration windowSize, Duration slide) {
        requirePositive(windowSize, "windowSize");
        requirePositive(slide, "slide");

        long eventMillis = eventTime.toEpochMilli();
        long windowMillis = windowSize.toMillis();
        long slideMillis = slide.toMillis();
        long startMillis = latestStartFor(eventTime, slide).toEpochMilli();

        List<Instant> starts = new ArrayList<>();
        while (startMillis <= eventMillis && eventMillis < startMillis + windowMillis) {
            starts.add(Instant.ofEpochMilli(startMillis));
            startMillis -= slideMillis;
        }
        Collections.reverse(starts);
        return List.copyOf(starts);
    }

    private static void requirePositive(Duration duration, String name) {
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        if (duration.toMillis() < 1) {
            throw new IllegalArgumentException(name + " must be at least one millisecond");
        }
    }
}

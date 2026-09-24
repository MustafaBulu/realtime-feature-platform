package com.mustafabulu.realtimefeatureplatform.streamworker;

import java.time.Instant;

record EventTimeAssessment(
        Instant eventTime,
        Instant processingTime,
        Instant allowedLatenessCutoff,
        EventTiming timing
) {

    static EventTimeAssessment onTimeAt(Instant eventTime) {
        return new EventTimeAssessment(eventTime, eventTime, eventTime, EventTiming.ON_TIME);
    }

    boolean tooLate() {
        return timing == EventTiming.TOO_LATE;
    }

    boolean lateWithinAllowed() {
        return timing == EventTiming.LATE_WITHIN_ALLOWED;
    }
}

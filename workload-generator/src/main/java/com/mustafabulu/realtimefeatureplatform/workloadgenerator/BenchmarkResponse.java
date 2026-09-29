package com.mustafabulu.realtimefeatureplatform.workloadgenerator;

import java.time.Instant;
import java.util.List;

record BenchmarkResponse(
        String runId,
        Instant startedAt,
        Instant finishedAt,
        BenchmarkController.BenchmarkRequest request,
        List<PhaseSummary> phases,
        SqlBaselineReport sqlBaseline,
        List<RawMeasurement> rawResults
) {

    record PhaseSummary(
            String phase,
            int targetEvents,
            int producedEvents,
            double achievedEventRatePerSecond,
            int targetReads,
            int completedReads,
            double achievedReadRatePerSecond,
            LatencySummary eventPublishLatency,
            LatencySummary featureReadLatency
    ) {
    }

    record LatencySummary(
            long count,
            double avgMillis,
            long minMillis,
            long p50Millis,
            long p95Millis,
            long p99Millis,
            long maxMillis
    ) {

        static LatencySummary empty() {
            return new LatencySummary(0, 0.0, 0, 0, 0, 0, 0);
        }
    }

    record RawMeasurement(
            String phase,
            String type,
            Instant startedAt,
            long latencyMillis,
            String entityId,
            String featureName,
            int statusCode,
            boolean success
    ) {
    }

    record SqlBaselineReport(boolean enabled, List<SqlQueryResult> queries, String message) {
    }

    record SqlQueryResult(String featureName, long latencyMillis, int rowsReturned, List<SqlFeatureRow> rows) {
    }

    record SqlFeatureRow(String entityType, String entityId, Instant windowStart, Double value) {
    }
}

package com.mustafabulu.realtimefeatureplatform.workloadgenerator;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

record BenchmarkResponse(
        String runId,
        Instant startedAt,
        Instant finishedAt,
        BenchmarkController.BenchmarkRequest request,
        EnvironmentManifest environment,
        List<PhaseSummary> phases,
        SqlBaselineReport sqlBaseline,
        CorrectnessReport correctness,
        FreshnessProbeReport freshnessProbe,
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
            LatencySummary featureReadLatency,
            ThroughputSummary eventThroughput,
            ThroughputSummary readThroughput,
            OperationSummary eventPublishSummary,
            OperationSummary featureReadSummary
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
            String featureStatus,
            boolean timeout,
            boolean success
    ) {
    }

    record ThroughputSummary(
            double targetPerSecond,
            double completedPerSecond,
            int targetOperations,
            int completedOperations,
            boolean sustainedUnderSlo
    ) {
    }

    record OperationSummary(
            int total,
            int successes,
            int failures,
            double errorRate,
            int httpNon2xx,
            int timeouts,
            List<FeatureStatusCount> featureStatuses
    ) {
    }

    record FeatureStatusCount(String status, int count) {
    }

    record CorrectnessReport(
            boolean enabled,
            int samples,
            int matches,
            int mismatches,
            int unavailable,
            List<CorrectnessMismatch> examples,
            String message
    ) {
    }

    record CorrectnessMismatch(
            String entityId,
            String featureName,
            Number realtimeValue,
            Number baselineValue,
            String realtimeStatus,
            String baselineStatus
    ) {
    }

    record FreshnessProbeReport(
            boolean enabled,
            Duration pollInterval,
            Duration timeout,
            LatencySummary updateToAvailabilityLatency,
            int attempts,
            int successes,
            int timeouts,
            String message
    ) {
    }

    record EnvironmentManifest(
            String resultDirectory,
            String commitSha,
            String cpu,
            long maxMemoryBytes,
            String measurementSource,
            String containerLimits,
            int datasetEntityCount,
            Duration warmupDuration,
            Duration measurementDuration,
            int eventRatePerSecond,
            int readRatePerSecond,
            Duration freshnessProbePollInterval,
            Duration freshnessProbeTimeout
    ) {
    }

    record SqlBaselineReport(boolean enabled, List<SqlQueryResult> queries, String message) {
    }

    record SqlQueryResult(String featureName, long latencyMillis, int rowsReturned, List<SqlFeatureRow> rows) {
    }

    record SqlFeatureRow(String entityType, String entityId, Instant windowStart, Double value) {
    }
}

package com.mustafabulu.realtimefeatureplatform.workloadgenerator;

import java.time.Duration;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
class BenchmarkController {

    private final RequestCountBenchmarkRunner runner;

    BenchmarkController(RequestCountBenchmarkRunner runner) {
        this.runner = runner;
    }

    @PostMapping("/benchmarks/request-count")
    @ResponseStatus(HttpStatus.ACCEPTED)
    BenchmarkResponse run(@RequestBody(required = false) BenchmarkRequest request) {
        return runner.run(request == null ? BenchmarkRequest.defaults() : request.withDefaults());
    }

    record BenchmarkRequest(
            Duration warmupDuration,
            Duration measurementDuration,
            Integer eventRatePerSecond,
            Integer readRatePerSecond,
            Integer entityCardinality,
            EntityDistribution distribution,
            Double zipfSkew,
            List<String> featureNames,
            Boolean writeHistory,
            Boolean runSqlBaseline,
            Boolean includeRawResults
    ) {

        static BenchmarkRequest defaults() {
            return new BenchmarkRequest(
                    Duration.ofSeconds(5),
                    Duration.ofSeconds(15),
                    10,
                    2,
                    100,
                    EntityDistribution.UNIFORM,
                    1.1,
                    List.of(
                            "request_count_total",
                            "entity_event_count_10m",
                            "entity_error_rate_10m",
                            "entity_avg_latency_ms_5m"
                    ),
                    true,
                    true,
                    false
            );
        }

        BenchmarkRequest withDefaults() {
            BenchmarkRequest defaults = defaults();
            return new BenchmarkRequest(
                    positiveDuration(warmupDuration, defaults.warmupDuration),
                    positiveDuration(measurementDuration, defaults.measurementDuration),
                    positiveInt(eventRatePerSecond, defaults.eventRatePerSecond),
                    nonNegativeInt(readRatePerSecond, defaults.readRatePerSecond),
                    positiveInt(entityCardinality, defaults.entityCardinality),
                    distribution == null ? defaults.distribution : distribution,
                    zipfSkew == null || zipfSkew <= 0.0 ? defaults.zipfSkew : zipfSkew,
                    featureNames == null || featureNames.isEmpty() ? defaults.featureNames : List.copyOf(featureNames),
                    writeHistory == null ? defaults.writeHistory : writeHistory,
                    runSqlBaseline == null ? defaults.runSqlBaseline : runSqlBaseline,
                    includeRawResults == null ? defaults.includeRawResults : includeRawResults
            );
        }

        private static Duration positiveDuration(Duration value, Duration fallback) {
            return value == null || value.isZero() || value.isNegative() ? fallback : value;
        }

        private static int positiveInt(Integer value, int fallback) {
            return value == null || value <= 0 ? fallback : value;
        }

        private static int nonNegativeInt(Integer value, int fallback) {
            return value == null || value < 0 ? fallback : value;
        }
    }

    enum EntityDistribution {
        UNIFORM,
        ZIPF
    }
}

package com.mustafabulu.realtimefeatureplatform.workloadgenerator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

class BenchmarkRunnerCorrectnessTest {

    @Test
    void correctnessSamplingCountsMatchesAndMismatches() throws Exception {
        try (TestFeatureApi api = TestFeatureApi.start(new TestFeatureApi.Script()
                .baseline("request_count_total", "PRESENT", 5)
                .realtime("request_count_total", "PRESENT", 5)
                .baseline("entity_event_count_10m", "PRESENT", 7)
                .realtime("entity_event_count_10m", "PRESENT", 8))) {
            BenchmarkResponse response = runner(api).run(request(List.of(
                    "request_count_total",
                    "entity_event_count_10m"
            ), 2, 0, Duration.ofMillis(5), Duration.ofMillis(20)));

            assertEquals(1, response.correctness().matches());
            assertEquals(1, response.correctness().mismatches());
            assertEquals(0, response.correctness().unavailable());
            assertEquals(1, response.correctness().examples().size());
        }
    }

    @Test
    void correctnessSamplingPollsRealtimeUntilItMatchesBaseline() throws Exception {
        AtomicInteger realtimeReads = new AtomicInteger();
        try (TestFeatureApi api = TestFeatureApi.start(new TestFeatureApi.Script()
                .baseline("request_count_total", "PRESENT", 5)
                .realtime("request_count_total", ignored -> {
                    int read = realtimeReads.incrementAndGet();
                    return TestFeatureApi.featureResponse("PRESENT", read == 1 ? 4 : 5);
                }))) {
            BenchmarkResponse response = runner(api).run(request(
                    List.of("request_count_total"),
                    1,
                    0,
                    Duration.ofMillis(10),
                    Duration.ofMillis(500)
            ));

            assertEquals(1, response.correctness().matches());
            assertEquals(0, response.correctness().mismatches());
            assertEquals(0, response.correctness().unavailable());
            assertTrue(realtimeReads.get() >= 2);
        }
    }

    @Test
    void freshnessProbeCountsSuccessWhenPublishedUpdateBecomesVisible() throws Exception {
        AtomicInteger reads = new AtomicInteger();
        try (TestFeatureApi api = TestFeatureApi.start(new TestFeatureApi.Script()
                .realtime("request_count_total", ignored -> {
                    int read = reads.incrementAndGet();
                    return TestFeatureApi.featureResponse("PRESENT", read == 1 ? 0 : 1);
                }))) {
            BenchmarkResponse response = runner(api).run(request(
                    List.of("request_count_total"),
                    0,
                    1,
                    Duration.ofMillis(5),
                    Duration.ofMillis(100)
            ));

            assertEquals(1, response.freshnessProbe().attempts());
            assertEquals(1, response.freshnessProbe().successes());
            assertEquals(0, response.freshnessProbe().timeouts());
        }
    }

    @Test
    void freshnessProbeCountsTimeoutWhenUpdateNeverBecomesVisible() throws Exception {
        try (TestFeatureApi api = TestFeatureApi.start(new TestFeatureApi.Script()
                .realtime("request_count_total", "PRESENT", 0))) {
            BenchmarkResponse response = runner(api).run(request(
                    List.of("request_count_total"),
                    0,
                    1,
                    Duration.ofMillis(5),
                    Duration.ofMillis(20)
            ));

            assertEquals(1, response.freshnessProbe().attempts());
            assertEquals(0, response.freshnessProbe().successes());
            assertEquals(1, response.freshnessProbe().timeouts());
        }
    }

    @Test
    void rawResultsAreBoundedByConfiguredLimit() throws Exception {
        try (TestFeatureApi api = TestFeatureApi.start(new TestFeatureApi.Script()
                .realtime("request_count_total", "PRESENT", 1))) {
            BenchmarkResponse response = runner(api).run(new BenchmarkController.BenchmarkRequest(
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(1),
                    4,
                    0,
                    1,
                    BenchmarkController.EntityDistribution.UNIFORM,
                    1.1,
                    List.of("request_count_total"),
                    false,
                    false,
                    true,
                    3,
                    500L,
                    0,
                    0,
                    Duration.ofMillis(5),
                    Duration.ofMillis(20)
            ));

            assertEquals(3, response.rawResults().size());
        }
    }

    @Test
    void readSummaryCountsFeatureStatuses() throws Exception {
        AtomicInteger reads = new AtomicInteger();
        try (TestFeatureApi api = TestFeatureApi.start(new TestFeatureApi.Script()
                .realtime("request_count_total", ignored -> {
                    int read = reads.incrementAndGet();
                    return TestFeatureApi.featureResponse(read % 2 == 0 ? "STALE" : "PRESENT", 1);
                }))) {
            BenchmarkResponse response = runner(api).run(new BenchmarkController.BenchmarkRequest(
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(1),
                    0,
                    2,
                    1,
                    BenchmarkController.EntityDistribution.UNIFORM,
                    1.1,
                    List.of("request_count_total"),
                    false,
                    false,
                    false,
                    0,
                    500L,
                    0,
                    0,
                    Duration.ofMillis(5),
                    Duration.ofMillis(20)
            ));

            BenchmarkResponse.OperationSummary summary = response.phases().stream()
                    .filter(phase -> "measurement".equals(phase.phase()))
                    .findFirst()
                    .orElseThrow()
                    .featureReadSummary();
            assertEquals(2, summary.total());
            assertEquals(1, count(summary, "PRESENT"));
            assertEquals(1, count(summary, "STALE"));
        }
    }

    private static int count(BenchmarkResponse.OperationSummary summary, String status) {
        return summary.featureStatuses().stream()
                .filter(entry -> status.equals(entry.status()))
                .mapToInt(BenchmarkResponse.FeatureStatusCount::count)
                .findFirst()
                .orElse(0);
    }

    private static RequestCountBenchmarkRunner runner(TestFeatureApi api) {
        return new RequestCountBenchmarkRunner(
                kafkaTemplate(),
                "platform.events",
                api.baseUrl(),
                new HistoricalEventRepository("", "", ""),
                new SqlBaselineFeatureQueryRepository("", "", "")
        );
    }

    private static BenchmarkController.BenchmarkRequest request(
            List<String> featureNames,
            int correctnessSamples,
            int freshnessProbes,
            Duration pollInterval,
            Duration timeout
    ) {
        return new BenchmarkController.BenchmarkRequest(
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                0,
                0,
                1,
                BenchmarkController.EntityDistribution.UNIFORM,
                1.1,
                featureNames,
                false,
                false,
                false,
                0,
                500L,
                correctnessSamples,
                freshnessProbes,
                pollInterval,
                timeout
        );
    }

    @SuppressWarnings("unchecked")
    private static KafkaTemplate<String, String> kafkaTemplate() {
        KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        when(kafkaTemplate.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        return kafkaTemplate;
    }

    private static final class TestFeatureApi implements AutoCloseable {

        private final HttpServer server;

        private TestFeatureApi(HttpServer server) {
            this.server = server;
        }

        static TestFeatureApi start(Script script) throws IOException {
            HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
            server.createContext("/features", exchange -> respond(exchange, script.realtime(exchange.getRequestURI().getPath())));
            server.createContext("/baseline/features", exchange -> respond(exchange, script.baseline(exchange.getRequestURI().getPath())));
            server.start();
            return new TestFeatureApi(server);
        }

        String baseUrl() {
            return "http://localhost:" + server.getAddress().getPort();
        }

        @Override
        public void close() {
            server.stop(0);
        }

        private static String featureResponse(String status, Number value) {
            return """
                    {
                      "value": %s,
                      "metadata": {"status": "%s"}
                    }
                    """.formatted(value, status);
        }

        private static void respond(com.sun.net.httpserver.HttpExchange exchange, String body) throws IOException {
            byte[] bytes = body.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            try (var response = exchange.getResponseBody()) {
                response.write(bytes);
            }
        }

        private interface ResponseFactory {
            String response(String path);
        }

        private static final class Script {

            private final Map<String, ResponseFactory> realtime = new LinkedHashMap<>();
            private final Map<String, ResponseFactory> baseline = new LinkedHashMap<>();

            private Script realtime(String featureName, String status, Number value) {
                return realtime(featureName, ignored -> featureResponse(status, value));
            }

            private Script realtime(String featureName, ResponseFactory factory) {
                realtime.put(featureName, factory);
                return this;
            }

            private Script baseline(String featureName, String status, Number value) {
                baseline.put(featureName, ignored -> featureResponse(status, value));
                return this;
            }

            private String realtime(String path) {
                return response(realtime, path);
            }

            private String baseline(String path) {
                return response(baseline, path);
            }

            private static String response(Map<String, ResponseFactory> factories, String path) {
                return factories.entrySet().stream()
                        .filter(entry -> path.endsWith("/" + entry.getKey()))
                        .findFirst()
                        .map(entry -> entry.getValue().response(path))
                        .orElseGet(() -> featureResponse("PRESENT", 0));
            }
        }
    }
}

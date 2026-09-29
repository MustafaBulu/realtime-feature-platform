package com.mustafabulu.realtimefeatureplatform.workloadgenerator;

import com.mustafabulu.realtimefeatureplatform.eventmodel.EventJsonCodec;
import com.mustafabulu.realtimefeatureplatform.eventmodel.EventValidator;
import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import com.mustafabulu.realtimefeatureplatform.eventmodel.RequestCompletedEventFactory;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
class RequestCountBenchmarkRunner {

    private static final String ENTITY_TYPE = "service";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String eventsTopic;
    private final String featureApiBaseUrl;
    private final EventValidator eventValidator = new EventValidator();
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final HistoricalEventRepository historicalEventRepository;
    private final SqlBaselineFeatureQueryRepository sqlBaselineRepository;

    RequestCountBenchmarkRunner(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${rfp.kafka.events-topic}") String eventsTopic,
            @Value("${rfp.feature-api.base-url:http://localhost:8080}") String featureApiBaseUrl,
            HistoricalEventRepository historicalEventRepository,
            SqlBaselineFeatureQueryRepository sqlBaselineRepository
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.eventsTopic = eventsTopic;
        this.featureApiBaseUrl = stripTrailingSlash(featureApiBaseUrl);
        this.historicalEventRepository = historicalEventRepository;
        this.sqlBaselineRepository = sqlBaselineRepository;
    }

    BenchmarkResponse run(BenchmarkController.BenchmarkRequest request) {
        String runId = UUID.randomUUID().toString();
        Instant startedAt = Instant.now();
        EntitySelector entitySelector = new EntitySelector(
                request.entityCardinality(),
                request.distribution(),
                request.zipfSkew()
        );
        List<BenchmarkResponse.RawMeasurement> rawMeasurements = Collections.synchronizedList(new ArrayList<>());

        BenchmarkResponse.PhaseSummary warmup = runPhase(runId, "warmup", request, entitySelector, rawMeasurements);
        BenchmarkResponse.PhaseSummary measurement = runPhase(runId, "measurement", request, entitySelector, rawMeasurements);
        BenchmarkResponse.SqlBaselineReport sqlBaseline = request.runSqlBaseline()
                ? sqlBaselineRepository.run(runId)
                : new BenchmarkResponse.SqlBaselineReport(false, List.of(), "disabled by request");

        return new BenchmarkResponse(
                runId,
                startedAt,
                Instant.now(),
                request,
                List.of(warmup, measurement),
                sqlBaseline,
                request.includeRawResults() ? List.copyOf(rawMeasurements) : List.of()
        );
    }

    private BenchmarkResponse.PhaseSummary runPhase(
            String runId,
            String phase,
            BenchmarkController.BenchmarkRequest request,
            EntitySelector entitySelector,
            List<BenchmarkResponse.RawMeasurement> rawMeasurements
    ) {
        Duration duration = "warmup".equals(phase) ? request.warmupDuration() : request.measurementDuration();
        int targetEvents = targetOperations(request.eventRatePerSecond(), duration);
        int targetReads = targetOperations(request.readRatePerSecond(), duration);
        List<PlatformEvent> events = Collections.synchronizedList(new ArrayList<>());
        long startedNanos = System.nanoTime();

        try (var executor = Executors.newFixedThreadPool(2)) {
            var eventTask = executor.submit(runEvents(
                    runId,
                    phase,
                    targetEvents,
                    duration,
                    request.eventRatePerSecond(),
                    entitySelector,
                    rawMeasurements,
                    events
            ));
            var readTask = executor.submit(runReads(
                    phase,
                    targetReads,
                    duration,
                    request.readRatePerSecond(),
                    request.featureNames(),
                    entitySelector,
                    rawMeasurements
            ));
            eventTask.get();
            readTask.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Benchmark phase interrupted", ex);
        } catch (ExecutionException ex) {
            throw new IllegalStateException("Benchmark phase failed", ex.getCause());
        }

        kafkaTemplate.flush();
        if (request.writeHistory()) {
            historicalEventRepository.saveAll(runId, phase, List.copyOf(events));
        }

        double elapsedSeconds = (System.nanoTime() - startedNanos) / 1_000_000_000.0;
        List<BenchmarkResponse.RawMeasurement> phaseMeasurements = rawMeasurements.stream()
                .filter(measurement -> measurement.phase().equals(phase))
                .toList();
        List<Long> eventLatencies = phaseMeasurements.stream()
                .filter(measurement -> "event_publish".equals(measurement.type()))
                .map(BenchmarkResponse.RawMeasurement::latencyMillis)
                .toList();
        List<Long> readLatencies = phaseMeasurements.stream()
                .filter(measurement -> "feature_read".equals(measurement.type()))
                .map(BenchmarkResponse.RawMeasurement::latencyMillis)
                .toList();

        return new BenchmarkResponse.PhaseSummary(
                phase,
                targetEvents,
                events.size(),
                events.size() / elapsedSeconds,
                targetReads,
                readLatencies.size(),
                readLatencies.size() / elapsedSeconds,
                latencySummary(eventLatencies),
                latencySummary(readLatencies)
        );
    }

    private Callable<Void> runEvents(
            String runId,
            String phase,
            int targetEvents,
            Duration duration,
            int ratePerSecond,
            EntitySelector entitySelector,
            List<BenchmarkResponse.RawMeasurement> rawMeasurements,
            List<PlatformEvent> events
    ) {
        return () -> {
            pacedLoop(targetEvents, duration, ratePerSecond, index -> {
                String entityId = entitySelector.nextEntityId();
                PlatformEvent event = nextEvent(runId, phase, entityId);
                eventValidator.validate(event);
                Instant startedAt = Instant.now();
                long startedNanos = System.nanoTime();
                kafkaTemplate.send(eventsTopic, event.entity().id(), EventJsonCodec.toJson(event));
                long latencyMillis = (System.nanoTime() - startedNanos) / 1_000_000;
                events.add(event);
                rawMeasurements.add(new BenchmarkResponse.RawMeasurement(
                        phase,
                        "event_publish",
                        startedAt,
                        latencyMillis,
                        entityId,
                        null,
                        202,
                        true
                ));
            });
            return null;
        };
    }

    private Callable<Void> runReads(
            String phase,
            int targetReads,
            Duration duration,
            int ratePerSecond,
            List<String> featureNames,
            EntitySelector entitySelector,
            List<BenchmarkResponse.RawMeasurement> rawMeasurements
    ) {
        return () -> {
            if (targetReads == 0) {
                return null;
            }
            pacedLoop(targetReads, duration, ratePerSecond, index -> {
                String entityId = entitySelector.nextEntityId();
                String featureName = featureNames.get(ThreadLocalRandom.current().nextInt(featureNames.size()));
                Instant startedAt = Instant.now();
                long startedNanos = System.nanoTime();
                int statusCode = readFeature(entityId, featureName);
                long latencyMillis = (System.nanoTime() - startedNanos) / 1_000_000;
                rawMeasurements.add(new BenchmarkResponse.RawMeasurement(
                        phase,
                        "feature_read",
                        startedAt,
                        latencyMillis,
                        entityId,
                        featureName,
                        statusCode,
                        statusCode >= 200 && statusCode < 300
                ));
            });
            return null;
        };
    }

    private PlatformEvent nextEvent(String runId, String phase, String entityId) {
        long count = ThreadLocalRandom.current().nextLong(1, 6);
        int statusCode = ThreadLocalRandom.current().nextDouble() < 0.05 ? 500 : 200;
        long latencyMs = ThreadLocalRandom.current().nextLong(20, 401);
        return RequestCompletedEventFactory.create(
                runId + "-" + phase + "-" + UUID.randomUUID(),
                ENTITY_TYPE,
                entityId,
                count,
                statusCode,
                latencyMs,
                Instant.now()
        );
    }

    private int readFeature(String entityId, String featureName) {
        try {
            String uri = featureApiBaseUrl
                    + "/features/"
                    + encode(ENTITY_TYPE)
                    + "/"
                    + encode(entityId)
                    + "/"
                    + encode(featureName);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(uri))
                    .timeout(Duration.ofSeconds(5))
                    .GET()
                    .build();
            return httpClient.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
        } catch (IOException ex) {
            return 599;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return 598;
        }
    }

    private static void pacedLoop(int targetOperations, Duration duration, int ratePerSecond, Operation operation) {
        if (targetOperations == 0) {
            return;
        }
        long intervalNanos = Math.max(1L, 1_000_000_000L / Math.max(1, ratePerSecond));
        long deadline = System.nanoTime() + duration.toNanos();
        long nextRun = System.nanoTime();
        for (int index = 0; index < targetOperations && System.nanoTime() < deadline; index++) {
            operation.run(index);
            nextRun += intervalNanos;
            long sleepNanos = nextRun - System.nanoTime();
            if (sleepNanos > 0) {
                try {
                    Thread.sleep(Duration.ofNanos(sleepNanos));
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private static int targetOperations(int ratePerSecond, Duration duration) {
        return Math.toIntExact(Math.max(0L, ratePerSecond * duration.toSeconds()));
    }

    private static BenchmarkResponse.LatencySummary latencySummary(List<Long> latencies) {
        if (latencies.isEmpty()) {
            return BenchmarkResponse.LatencySummary.empty();
        }

        List<Long> sorted = latencies.stream().sorted(Comparator.naturalOrder()).toList();
        long count = sorted.size();
        double average = sorted.stream().mapToLong(Long::longValue).average().orElse(0.0);
        return new BenchmarkResponse.LatencySummary(
                count,
                average,
                sorted.getFirst(),
                percentile(sorted, 0.50),
                percentile(sorted, 0.95),
                percentile(sorted, 0.99),
                sorted.getLast()
        );
    }

    private static long percentile(List<Long> sorted, double percentile) {
        int index = (int) Math.ceil(percentile * sorted.size()) - 1;
        return sorted.get(Math.max(0, Math.min(index, sorted.size() - 1)));
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static String stripTrailingSlash(String value) {
        if (value.endsWith("/")) {
            return value.substring(0, value.length() - 1);
        }
        return value;
    }

    private interface Operation {
        void run(int index);
    }

    private static final class EntitySelector {

        private final int cardinality;
        private final BenchmarkController.EntityDistribution distribution;
        private final double[] cumulativeWeights;

        private EntitySelector(
                int cardinality,
                BenchmarkController.EntityDistribution distribution,
                double zipfSkew
        ) {
            this.cardinality = cardinality;
            this.distribution = distribution;
            this.cumulativeWeights = distribution == BenchmarkController.EntityDistribution.ZIPF
                    ? cumulativeZipfWeights(cardinality, zipfSkew)
                    : new double[0];
        }

        private String nextEntityId() {
            int index = distribution == BenchmarkController.EntityDistribution.ZIPF
                    ? nextZipfIndex()
                    : ThreadLocalRandom.current().nextInt(cardinality);
            return "entity-" + String.format("%06d", index);
        }

        private int nextZipfIndex() {
            double value = ThreadLocalRandom.current().nextDouble();
            int low = 0;
            int high = cumulativeWeights.length - 1;
            while (low < high) {
                int mid = (low + high) >>> 1;
                if (value <= cumulativeWeights[mid]) {
                    high = mid;
                } else {
                    low = mid + 1;
                }
            }
            return low;
        }

        private static double[] cumulativeZipfWeights(int cardinality, double skew) {
            double[] weights = new double[cardinality];
            double sum = 0.0;
            for (int index = 0; index < cardinality; index++) {
                sum += 1.0 / Math.pow(index + 1.0, skew);
                weights[index] = sum;
            }
            for (int index = 0; index < cardinality; index++) {
                weights[index] = weights[index] / sum;
            }
            weights[weights.length - 1] = 1.0;
            return weights;
        }
    }
}

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
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Service
class RequestCountBenchmarkRunner {

    private static final String ENTITY_TYPE = "service";
    private static final String REQUEST_COUNT_TOTAL = "request_count_total";
    private static final String WARMUP_PHASE = "warmup";
    private static final String MEASUREMENT_PHASE = "measurement";
    private static final String DISABLED_BY_REQUEST = "disabled by request";
    private static final String UNKNOWN = "unknown";
    private static final Duration KAFKA_ACK_TIMEOUT = Duration.ofSeconds(10);
    private static final int MAX_MISMATCH_EXAMPLES = 10;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String eventsTopic;
    private final String featureApiBaseUrl;
    private final EventValidator eventValidator = new EventValidator();
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();
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
        BenchmarkResponse.EnvironmentManifest environment = environmentManifest(startedAt, request);
        MeasurementRecorder recorder = new MeasurementRecorder(
                isEnabled(request.includeRawResults()),
                request.rawResultLimit()
        );

        BenchmarkResponse.PhaseSummary warmup = runPhase(runId, WARMUP_PHASE, request, recorder);
        BenchmarkResponse.PhaseSummary measurement = runPhase(runId, MEASUREMENT_PHASE, request, recorder);
        BenchmarkResponse.CorrectnessReport correctness = runCorrectnessCheck(runId, request);
        BenchmarkResponse.FreshnessProbeReport freshnessProbe = runFreshnessProbe(runId, request, recorder);
        BenchmarkResponse.SqlBaselineReport sqlBaseline = isEnabled(request.runSqlBaseline())
                ? sqlBaselineRepository.run(runId)
                : new BenchmarkResponse.SqlBaselineReport(false, List.of(), DISABLED_BY_REQUEST);

        return new BenchmarkResponse(
                runId,
                startedAt,
                Instant.now(),
                request,
                environment,
                List.of(warmup, measurement),
                sqlBaseline,
                correctness,
                freshnessProbe,
                recorder.rawResults()
        );
    }

    private BenchmarkResponse.PhaseSummary runPhase(
            String runId,
            String phase,
            BenchmarkController.BenchmarkRequest request,
            MeasurementRecorder recorder
    ) {
        Duration duration = MEASUREMENT_PHASE.equals(phase) ? request.measurementDuration() : request.warmupDuration();
        int targetEvents = targetOperations(request.eventRatePerSecond(), duration);
        int targetReads = targetOperations(request.readRatePerSecond(), duration);
        PhaseRecorder phaseRecorder = new PhaseRecorder();
        EntitySelector eventEntitySelector = new EntitySelector(
                entityNamespace(runId, phase),
                request.entityCardinality(),
                request.distribution(),
                request.zipfSkew()
        );
        EntitySelector readEntitySelector = new EntitySelector(
                entityNamespace(runId, phase),
                request.entityCardinality(),
                request.distribution(),
                request.zipfSkew()
        );
        List<PlatformEvent> events = new ArrayList<>();
        PhaseExecutionContext phaseContext = new PhaseExecutionContext(
                runId,
                phase,
                duration,
                recorder,
                phaseRecorder
        );
        EventLoad eventLoad = new EventLoad(
                targetEvents,
                request.eventRatePerSecond(),
                eventEntitySelector,
                events
        );
        ReadLoad readLoad = new ReadLoad(
                targetReads,
                request.readRatePerSecond(),
                request.featureNames(),
                readEntitySelector
        );
        long startedNanos = System.nanoTime();

        try (var executor = Executors.newFixedThreadPool(2)) {
            var eventTask = executor.submit(runEvents(phaseContext, eventLoad));
            var readTask = executor.submit(runReads(phaseContext, readLoad));
            eventTask.get();
            readTask.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Benchmark phase interrupted", ex);
        } catch (ExecutionException ex) {
            throw new IllegalStateException("Benchmark phase failed", ex.getCause());
        }

        kafkaTemplate.flush();
        if (isEnabled(request.writeHistory())) {
            historicalEventRepository.saveAll(runId, phase, List.copyOf(events));
        }

        double elapsedSeconds = (System.nanoTime() - startedNanos) / 1_000_000_000.0;
        BenchmarkResponse.LatencySummary eventLatency = latencySummary(phaseRecorder.eventLatencies);
        BenchmarkResponse.LatencySummary readLatency = latencySummary(phaseRecorder.readLatencies);
        double achievedEventRate = phaseRecorder.eventSummary.successes() / elapsedSeconds;
        double achievedReadRate = phaseRecorder.readSummary.total() / elapsedSeconds;

        return new BenchmarkResponse.PhaseSummary(
                phase,
                targetEvents,
                phaseRecorder.eventSummary.successes(),
                achievedEventRate,
                targetReads,
                phaseRecorder.readSummary.total(),
                achievedReadRate,
                eventLatency,
                readLatency,
                throughputSummary(request.eventRatePerSecond(), achievedEventRate, targetEvents,
                        phaseRecorder.eventSummary.successes(), eventLatency, request.readSloMillis()),
                throughputSummary(request.readRatePerSecond(), achievedReadRate, targetReads,
                        phaseRecorder.readSummary.total(), readLatency, request.readSloMillis()),
                phaseRecorder.eventSummary.toResponse(),
                phaseRecorder.readSummary.toResponse()
        );
    }

    private Callable<Void> runEvents(
            PhaseExecutionContext context,
            EventLoad load
    ) {
        return () -> {
            pacedLoop(load.targetEvents(), context.duration(), load.ratePerSecond(), index -> {
                String entityId = load.entitySelector().nextEntityId();
                PlatformEvent event = nextEvent(context.runId(), context.phase(), entityId);
                eventValidator.validate(event);
                Instant startedAt = Instant.now();
                PublishResult publishResult = publishAndWaitForAck(event);
                if (publishResult.success()) {
                    load.events().add(event);
                }
                context.phaseRecorder().recordEvent(publishResult);
                context.measurementRecorder().add(new BenchmarkResponse.RawMeasurement(
                        context.phase(),
                        "event_publish",
                        startedAt,
                        publishResult.latencyMillis(),
                        entityId,
                        null,
                        publishResult.statusCode(),
                        null,
                        publishResult.timeout(),
                        publishResult.success()
                ));
            });
            return null;
        };
    }

    private Callable<Void> runReads(
            PhaseExecutionContext context,
            ReadLoad load
    ) {
        return () -> {
            if (load.targetReads() == 0) {
                return null;
            }
            pacedLoop(load.targetReads(), context.duration(), load.ratePerSecond(), index -> {
                String entityId = load.entitySelector().nextEntityId();
                String featureName = load.featureNames()
                        .get(ThreadLocalRandom.current().nextInt(load.featureNames().size()));
                Instant startedAt = Instant.now();
                ReadResult readResult = readFeature(absoluteRealtimeFeatureUri(entityId, featureName));
                context.phaseRecorder().recordRead(readResult);
                context.measurementRecorder().add(new BenchmarkResponse.RawMeasurement(
                        context.phase(),
                        "feature_read",
                        startedAt,
                        readResult.latencyMillis(),
                        entityId,
                        featureName,
                        readResult.statusCode(),
                        readResult.featureStatus(),
                        readResult.timeout(),
                        readResult.success()
                ));
            });
            return null;
        };
    }

    private BenchmarkResponse.CorrectnessReport runCorrectnessCheck(
            String runId,
            BenchmarkController.BenchmarkRequest request
    ) {
        int requestedSamples = request.correctnessSampleSize();
        if (requestedSamples == 0 || request.featureNames().isEmpty()) {
            return new BenchmarkResponse.CorrectnessReport(false, 0, 0, 0, 0, List.of(), DISABLED_BY_REQUEST);
        }

        EntitySelector selector = new EntitySelector(
                entityNamespace(runId, MEASUREMENT_PHASE),
                request.entityCardinality(),
                request.distribution(),
                request.zipfSkew()
        );
        int matches = 0;
        int mismatches = 0;
        int unavailable = 0;
        List<BenchmarkResponse.CorrectnessMismatch> examples = new ArrayList<>();

        for (int sample = 0; sample < requestedSamples; sample++) {
            String entityId = selector.nextEntityId();
            String featureName = request.featureNames().get(sample % request.featureNames().size());
            ReadResult baseline = readFeature(baselineFeatureUri(runId, entityId, featureName));
            if (!baseline.success()) {
                unavailable++;
                continue;
            }
            ReadResult realtime = waitForRealtimeMatch(
                    entityId,
                    featureName,
                    baseline.value(),
                    request.freshnessProbePollInterval(),
                    request.freshnessProbeTimeout()
            );
            if (!realtime.success()) {
                unavailable++;
                continue;
            }
            if (valuesMatch(realtime.value(), baseline.value())) {
                matches++;
            } else {
                mismatches++;
                if (examples.size() < MAX_MISMATCH_EXAMPLES) {
                    examples.add(new BenchmarkResponse.CorrectnessMismatch(
                            entityId,
                            featureName,
                            realtime.value().orElse(null),
                            baseline.value().orElse(null),
                            realtime.featureStatus(),
                            baseline.featureStatus()
                    ));
                }
            }
        }

        return new BenchmarkResponse.CorrectnessReport(true, requestedSamples, matches, mismatches, unavailable,
                List.copyOf(examples), null);
    }

    private ReadResult waitForRealtimeMatch(
            String entityId,
            String featureName,
            Optional<Number> expectedValue,
            Duration pollInterval,
            Duration timeout
    ) {
        long deadline = System.nanoTime() + timeout.toNanos();
        ReadResult last = readFeature(absoluteRealtimeFeatureUri(entityId, featureName));
        while (!valuesMatch(last.value(), expectedValue) && System.nanoTime() < deadline) {
            sleep(pollInterval);
            last = readFeature(absoluteRealtimeFeatureUri(entityId, featureName));
        }
        return last;
    }

    private BenchmarkResponse.FreshnessProbeReport runFreshnessProbe(
            String runId,
            BenchmarkController.BenchmarkRequest request,
            MeasurementRecorder recorder
    ) {
        if (request.freshnessProbeCount() == 0) {
            return new BenchmarkResponse.FreshnessProbeReport(
                    false,
                    request.freshnessProbePollInterval(),
                    request.freshnessProbeTimeout(),
                    BenchmarkResponse.LatencySummary.empty(),
                    0,
                    0,
                    0,
                    DISABLED_BY_REQUEST
            );
        }

        List<Long> latencies = new ArrayList<>();
        int timeouts = 0;
        for (int index = 0; index < request.freshnessProbeCount(); index++) {
            String entityId = "probe-" + runId + "-" + index;
            ReadResult before = readFeature(absoluteRealtimeFeatureUri(entityId, REQUEST_COUNT_TOTAL));
            double baselineValue = before.value().map(Number::doubleValue).orElse(0.0);
            PlatformEvent event = nextEvent(runId, "probe", entityId, 1L);
            Instant startedAt = Instant.now();
            PublishResult publish = publishAndWaitForAck(event);
            recorder.add(new BenchmarkResponse.RawMeasurement(
                    "probe",
                    "event_publish",
                    startedAt,
                    publish.latencyMillis(),
                    entityId,
                    null,
                    publish.statusCode(),
                    null,
                    publish.timeout(),
                    publish.success()
            ));

            if (!publish.success()) {
                timeouts++;
                continue;
            }
            ProbeResult probe = pollUntilVisible(
                    entityId,
                    baselineValue,
                    request.freshnessProbePollInterval(),
                    request.freshnessProbeTimeout()
            );
            if (probe.visible()) {
                latencies.add(probe.latencyMillis());
            } else {
                timeouts++;
            }
        }

        return new BenchmarkResponse.FreshnessProbeReport(
                true,
                request.freshnessProbePollInterval(),
                request.freshnessProbeTimeout(),
                latencySummary(latencies),
                request.freshnessProbeCount(),
                latencies.size(),
                timeouts,
                null
        );
    }

    private ProbeResult pollUntilVisible(
            String entityId,
            double baselineValue,
            Duration pollInterval,
            Duration timeout
    ) {
        long deadline = System.nanoTime() + timeout.toNanos();
        long startedNanos = System.nanoTime();
        while (System.nanoTime() < deadline) {
            ReadResult read = readFeature(absoluteRealtimeFeatureUri(entityId, REQUEST_COUNT_TOTAL));
            if (read.value().map(Number::doubleValue).orElse(baselineValue) > baselineValue) {
                return new ProbeResult(true, (System.nanoTime() - startedNanos) / 1_000_000);
            }
            if (!sleep(pollInterval)) {
                Thread.currentThread().interrupt();
                return new ProbeResult(false, (System.nanoTime() - startedNanos) / 1_000_000);
            }
        }
        return new ProbeResult(false, timeout.toMillis());
    }

    private PlatformEvent nextEvent(String runId, String phase, String entityId) {
        long count = ThreadLocalRandom.current().nextLong(1, 6);
        return nextEvent(runId, phase, entityId, count);
    }

    private PlatformEvent nextEvent(String runId, String phase, String entityId, long count) {
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

    private PublishResult publishAndWaitForAck(PlatformEvent event) {
        long startedNanos = System.nanoTime();
        try {
            kafkaTemplate.send(eventsTopic, event.entity().id(), EventJsonCodec.toJson(event))
                    .get(KAFKA_ACK_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            return new PublishResult((System.nanoTime() - startedNanos) / 1_000_000, 202, false, true);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return new PublishResult((System.nanoTime() - startedNanos) / 1_000_000, 598, false, false);
        } catch (TimeoutException ex) {
            return new PublishResult(KAFKA_ACK_TIMEOUT.toMillis(), 599, true, false);
        } catch (ExecutionException ex) {
            return new PublishResult((System.nanoTime() - startedNanos) / 1_000_000, 599, false, false);
        }
    }

    private ReadResult readFeature(String uri) {
        long startedNanos = System.nanoTime();
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(uri))
                    .timeout(Duration.ofSeconds(5))
                    .GET()
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            long latencyMillis = (System.nanoTime() - startedNanos) / 1_000_000;
            ParsedFeatureResponse parsed = parseFeatureResponse(response.body());
            boolean success = response.statusCode() >= 200 && response.statusCode() < 300
                    && "PRESENT".equals(parsed.status())
                    && parsed.value().isPresent();
            return new ReadResult(
                    latencyMillis,
                    response.statusCode(),
                    parsed.status(),
                    false,
                    success,
                    parsed.value()
            );
        } catch (java.net.http.HttpTimeoutException ex) {
            return new ReadResult(5_000, 598, null, true, false, Optional.empty());
        } catch (IOException ex) {
            return new ReadResult((System.nanoTime() - startedNanos) / 1_000_000, 599, null, false, false,
                    Optional.empty());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return new ReadResult((System.nanoTime() - startedNanos) / 1_000_000, 598, null, false, false,
                    Optional.empty());
        }
    }

    private ParsedFeatureResponse parseFeatureResponse(String responseBody) {
        try {
            JsonNode root = objectMapper.readTree(responseBody);
            JsonNode valueNode = root.path("value");
            Optional<Number> value = valueNode.isMissingNode() || valueNode.isNull()
                    ? Optional.empty()
                    : Optional.of(valueNode.asDouble());
            JsonNode statusNode = root.path("metadata").path("status");
            String status = statusNode.isMissingNode() || statusNode.isNull() ? null : statusNode.asString();
            return new ParsedFeatureResponse(value, status);
        } catch (JacksonException ex) {
            return new ParsedFeatureResponse(Optional.empty(), null);
        }
    }

    private static BenchmarkResponse.ThroughputSummary throughputSummary(
            double targetPerSecond,
            double completedPerSecond,
            int targetOperations,
            int completedOperations,
            BenchmarkResponse.LatencySummary latency,
            long sloMillis
    ) {
        boolean sustained = completedOperations >= targetOperations
                && (latency.count() == 0 || latency.p95Millis() <= sloMillis);
        return new BenchmarkResponse.ThroughputSummary(
                targetPerSecond,
                completedPerSecond,
                targetOperations,
                completedOperations,
                sustained
        );
    }

    private BenchmarkResponse.EnvironmentManifest environmentManifest(
            Instant startedAt,
            BenchmarkController.BenchmarkRequest request
    ) {
        String commitSha = commitSha();
        return new BenchmarkResponse.EnvironmentManifest(
                resultDirectory(startedAt, commitSha),
                commitSha,
                Runtime.getRuntime().availableProcessors() + " logical processors",
                Runtime.getRuntime().maxMemory(),
                "docker stats or Prometheus during the run",
                "record Docker/container CPU and memory limits in the result manifest",
                request.entityCardinality(),
                request.warmupDuration(),
                request.measurementDuration(),
                request.eventRatePerSecond(),
                request.readRatePerSecond(),
                request.freshnessProbePollInterval(),
                request.freshnessProbeTimeout()
        );
    }

    private static String resultDirectory(Instant startedAt, String commitSha) {
        String timestamp = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")
                .withLocale(Locale.ROOT)
                .withZone(ZoneOffset.UTC)
                .format(startedAt);
        return "benchmarks/results/" + timestamp + "-" + commitSha + "/";
    }

    private static String commitSha() {
        try {
            Process process = new ProcessBuilder("git", "rev-parse", "--short", "HEAD")
                    .redirectErrorStream(true)
                    .start();
            if (process.waitFor(2, TimeUnit.SECONDS) && process.exitValue() == 0) {
                return new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
            }
        } catch (IOException ex) {
            return UNKNOWN;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            return UNKNOWN;
        }
        return UNKNOWN;
    }

    private static boolean valuesMatch(Optional<Number> left, Optional<Number> right) {
        if (left.isEmpty() || right.isEmpty()) {
            return left.isEmpty() && right.isEmpty();
        }
        double a = left.get().doubleValue();
        double b = right.get().doubleValue();
        return Math.abs(a - b) <= 0.000001;
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
                if (!sleep(Duration.ofNanos(sleepNanos))) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    private static int targetOperations(int ratePerSecond, Duration duration) {
        return Math.toIntExact(Math.max(0L, ratePerSecond * duration.toSeconds()));
    }

    private static boolean sleep(Duration duration) {
        try {
            Thread.sleep(duration);
            return true;
        } catch (InterruptedException ex) {
            return false;
        }
    }

    private static boolean isEnabled(Boolean value) {
        return Boolean.TRUE.equals(value);
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
        return sorted.get(Math.clamp(index, 0, sorted.size() - 1));
    }

    private static String realtimeFeaturePath(String entityId, String featureName) {
        return "/features/" + encode(ENTITY_TYPE) + "/" + encode(entityId) + "/" + encode(featureName);
    }

    private String baselineFeatureUri(String runId, String entityId, String featureName) {
        return featureApiBaseUrl
                + "/baseline/features/"
                + encode(ENTITY_TYPE)
                + "/"
                + encode(entityId)
                + "/"
                + encode(featureName)
                + "?benchmarkRunId="
                + encode(runId)
                + "&benchmarkPhase="
                + encode(MEASUREMENT_PHASE);
    }

    private String absoluteRealtimeFeatureUri(String entityId, String featureName) {
        return featureApiBaseUrl + realtimeFeaturePath(entityId, featureName);
    }

    private static String entityNamespace(String runId, String phase) {
        return "bench-" + runId + "-" + phase;
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

    private record PhaseExecutionContext(
            String runId,
            String phase,
            Duration duration,
            MeasurementRecorder measurementRecorder,
            PhaseRecorder phaseRecorder
    ) {
    }

    private record EventLoad(
            int targetEvents,
            int ratePerSecond,
            EntitySelector entitySelector,
            List<PlatformEvent> events
    ) {
    }

    private record ReadLoad(
            int targetReads,
            int ratePerSecond,
            List<String> featureNames,
            EntitySelector entitySelector
    ) {
    }

    private record PublishResult(long latencyMillis, int statusCode, boolean timeout, boolean success) {
    }

    private record ReadResult(
            long latencyMillis,
            int statusCode,
            String featureStatus,
            boolean timeout,
            boolean success,
            Optional<Number> value
    ) {
    }

    private record ParsedFeatureResponse(Optional<Number> value, String status) {
    }

    private record ProbeResult(boolean visible, long latencyMillis) {
    }

    private static final class MeasurementRecorder {

        private final boolean enabled;
        private final int limit;
        private final List<BenchmarkResponse.RawMeasurement> rawResults = new ArrayList<>();

        private MeasurementRecorder(boolean enabled, int limit) {
            this.enabled = enabled;
            this.limit = limit;
        }

        private synchronized void add(BenchmarkResponse.RawMeasurement measurement) {
            if (enabled && rawResults.size() < limit) {
                rawResults.add(measurement);
            }
        }

        private synchronized List<BenchmarkResponse.RawMeasurement> rawResults() {
            return List.copyOf(rawResults);
        }
    }

    private static final class PhaseRecorder {

        private final List<Long> eventLatencies = new ArrayList<>();
        private final List<Long> readLatencies = new ArrayList<>();
        private final OperationCounter eventSummary = new OperationCounter();
        private final OperationCounter readSummary = new OperationCounter();

        private PhaseRecorder() {
        }

        private synchronized void recordEvent(PublishResult result) {
            eventLatencies.add(result.latencyMillis());
            eventSummary.add(result.statusCode(), null, result.timeout(), result.success());
        }

        private synchronized void recordRead(ReadResult result) {
            readLatencies.add(result.latencyMillis());
            readSummary.add(result.statusCode(), result.featureStatus(), result.timeout(), result.success());
        }
    }

    private static final class OperationCounter {

        private int total;
        private int successes;
        private int httpNon2xx;
        private int timeouts;
        private final Map<String, Integer> featureStatuses = new LinkedHashMap<>();

        private void add(int statusCode, String featureStatus, boolean timeout, boolean success) {
            total++;
            if (success) {
                successes++;
            }
            if (statusCode < 200 || statusCode >= 300) {
                httpNon2xx++;
            }
            if (timeout) {
                timeouts++;
            }
            if (featureStatus != null) {
                featureStatuses.merge(featureStatus, 1, Integer::sum);
            }
        }

        private int total() {
            return total;
        }

        private int successes() {
            return successes;
        }

        private BenchmarkResponse.OperationSummary toResponse() {
            int failures = total - successes;
            double errorRate = total == 0 ? 0.0 : failures / (double) total;
            List<BenchmarkResponse.FeatureStatusCount> statuses = featureStatuses.entrySet().stream()
                    .map(entry -> new BenchmarkResponse.FeatureStatusCount(entry.getKey(), entry.getValue()))
                    .toList();
            return new BenchmarkResponse.OperationSummary(
                    total,
                    successes,
                    failures,
                    errorRate,
                    httpNon2xx,
                    timeouts,
                    statuses
            );
        }
    }

    private static final class EntitySelector {

        private final String namespace;
        private final int cardinality;
        private final BenchmarkController.EntityDistribution distribution;
        private final double[] cumulativeWeights;

        private EntitySelector(
                String namespace,
                int cardinality,
                BenchmarkController.EntityDistribution distribution,
                double zipfSkew
        ) {
            this.namespace = namespace;
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
            return namespace + "-entity-" + String.format("%06d", index);
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
            if (sum == 0.0) {
                throw new IllegalArgumentException("Zipf weight sum must be positive");
            }
            for (int index = 0; index < cardinality; index++) {
                weights[index] = weights[index] / sum;
            }
            weights[weights.length - 1] = 1.0;
            return weights;
        }
    }
}

package com.mustafabulu.realtimefeatureplatform.featureapi;

import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinition;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinitionRepository;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@ConditionalOnBean(JdbcTemplate.class)
class BaselineFeatureController {

    private static final String DEFAULT_BENCHMARK_PHASE = "measurement";

    private final BaselineFeatureQueryRepository baselineRepository;
    private final FeatureDefinitionRepository definitionRepository;
    private final Clock clock;
    private final Duration staleAfter;

    @Autowired
    BaselineFeatureController(
            BaselineFeatureQueryRepository baselineRepository,
            FeatureDefinitionRepository definitionRepository,
            @Value("${rfp.feature-api.stale-after:PT5M}") Duration staleAfter
    ) {
        this(baselineRepository, definitionRepository, Clock.systemUTC(), staleAfter);
    }

    BaselineFeatureController(
            BaselineFeatureQueryRepository baselineRepository,
            FeatureDefinitionRepository definitionRepository,
            Clock clock,
            Duration staleAfter
    ) {
        this.baselineRepository = baselineRepository;
        this.definitionRepository = definitionRepository;
        this.clock = clock;
        this.staleAfter = staleAfter;
    }

    @GetMapping("/baseline/features/{entityType}/{entityId}/{featureName}")
    ResponseEntity<FeatureController.FeatureReadResponse> getFeature(
            @PathVariable String entityType,
            @PathVariable String entityId,
            @PathVariable String featureName,
            @RequestParam(required = false) Instant windowStart,
            @RequestParam(required = false) String benchmarkRunId,
            @RequestParam(required = false) String benchmarkPhase
    ) {
        return ResponseEntity.ok(readFeature(entityType, entityId, featureName, windowStart, benchmarkRunId, benchmarkPhase));
    }

    @GetMapping("/baseline/features/{entityType}/{entityId}")
    ResponseEntity<FeatureController.FeatureSetResponse> getFeatures(
            @PathVariable String entityType,
            @PathVariable String entityId,
            @RequestParam List<String> featureNames,
            @RequestParam(required = false) String benchmarkRunId,
            @RequestParam(required = false) String benchmarkPhase
    ) {
        List<FeatureController.FeatureReadResponse> features = featureNames.stream()
                .map(featureName -> readFeature(entityType, entityId, featureName, null, benchmarkRunId, benchmarkPhase))
                .toList();
        return ResponseEntity.ok(new FeatureController.FeatureSetResponse(entityType, entityId, features));
    }

    @PostMapping("/baseline/features/batch")
    ResponseEntity<FeatureController.BatchFeatureResponse> getFeatureBatch(
            @RequestBody FeatureController.BatchFeatureRequest request,
            @RequestParam(required = false) String benchmarkRunId,
            @RequestParam(required = false) String benchmarkPhase
    ) {
        List<FeatureController.FeatureReadResponse> features = request.features().stream()
                .map(feature -> readFeature(
                        feature.entityType(),
                        feature.entityId(),
                        feature.featureName(),
                        feature.windowStart(),
                        benchmarkRunId,
                        benchmarkPhase
                ))
                .toList();
        return ResponseEntity.ok(new FeatureController.BatchFeatureResponse(features));
    }

    private FeatureController.FeatureReadResponse readFeature(
            String entityType,
            String entityId,
            String featureName,
            Instant windowStart,
            String benchmarkRunId,
            String benchmarkPhase
    ) {
        return readFeatureSafely(() -> {
            String effectiveBenchmarkPhase = defaultBenchmarkPhase(benchmarkRunId, benchmarkPhase);
            Optional<BaselineFeatureQueryRepository.BaselineFeatureRow> row = baselineRepository.read(
                    new BaselineFeatureQueryRepository.BaselineFeatureRequest(
                            entityType,
                            entityId,
                            featureName,
                            windowStart,
                            benchmarkRunId,
                            effectiveBenchmarkPhase
                    )
            );

            Integer definitionVersion = activeDefinitionVersion(entityType, featureName);
            if (row.isEmpty()) {
                return response(
                        entityType,
                        entityId,
                        featureName,
                        windowStart,
                        null,
                        FeatureController.FeatureReadStatus.MISSING,
                        null,
                        null,
                        definitionVersion
                );
            }

            BaselineFeatureQueryRepository.BaselineFeatureRow baseline = row.get();
            Instant effectiveWindowStart = windowStart == null ? baseline.windowStart() : windowStart;
            FeatureController.FeatureReadStatus status = statusFor(baseline.updatedAt());
            return response(
                    entityType,
                    entityId,
                    featureName,
                    effectiveWindowStart,
                    baseline.value(),
                    status,
                    baseline.updatedAt(),
                    freshnessMillis(baseline.updatedAt()),
                    definitionVersion
            );
        }, entityType, entityId, featureName, windowStart);
    }

    private FeatureController.FeatureReadResponse readFeatureSafely(
            Supplier<FeatureController.FeatureReadResponse> supplier,
            String entityType,
            String entityId,
            String featureName,
            Instant windowStart
    ) {
        try {
            return supplier.get();
        } catch (RuntimeException ex) {
            return response(
                    entityType,
                    entityId,
                    featureName,
                    windowStart,
                    null,
                    FeatureController.FeatureReadStatus.UNAVAILABLE,
                    null,
                    null,
                    activeDefinitionVersion(entityType, featureName)
            );
        }
    }

    private FeatureController.FeatureReadResponse response(
            String entityType,
            String entityId,
            String featureName,
            Instant windowStart,
            Number value,
            FeatureController.FeatureReadStatus status,
            Instant updatedAt,
            Long freshnessMillis,
            Integer definitionVersion
    ) {
        return new FeatureController.FeatureReadResponse(
                entityType,
                entityId,
                featureName,
                windowStart,
                value,
                new FeatureController.FeatureMetadata(
                        status,
                        updatedAt,
                        freshnessMillis,
                        definitionVersion,
                        null
                )
        );
    }

    private FeatureController.FeatureReadStatus statusFor(Instant updatedAt) {
        if (updatedAt != null && updatedAt.plus(staleAfter).isBefore(clock.instant())) {
            return FeatureController.FeatureReadStatus.STALE;
        }
        return FeatureController.FeatureReadStatus.PRESENT;
    }

    private Long freshnessMillis(Instant updatedAt) {
        if (updatedAt == null) {
            return null;
        }
        return Duration.between(updatedAt, clock.instant()).toMillis();
    }

    private Integer activeDefinitionVersion(String entityType, String featureName) {
        return definitionRepository.findActive().stream()
                .filter(definition -> definition.name().equals(featureName))
                .filter(definition -> definition.entityType().equals(entityType)
                        || FeatureDefinition.ALL_ENTITY_TYPES.equals(definition.entityType()))
                .map(FeatureDefinition::version)
                .findFirst()
                .orElse(null);
    }

    private static String defaultBenchmarkPhase(String benchmarkRunId, String benchmarkPhase) {
        if (hasText(benchmarkPhase)) {
            return benchmarkPhase;
        }
        return hasText(benchmarkRunId) ? DEFAULT_BENCHMARK_PHASE : null;
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}

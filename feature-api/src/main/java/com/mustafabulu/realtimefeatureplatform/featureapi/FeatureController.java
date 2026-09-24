package com.mustafabulu.realtimefeatureplatform.featureapi;

import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinition;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinitionRepository;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureKey;
import com.mustafabulu.realtimefeatureplatform.featuremodel.WindowedFeatureKey;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
class FeatureController {

    private final StringRedisTemplate redisTemplate;
    private final FeatureDefinitionRepository definitionRepository;
    private final MeterRegistry meterRegistry;
    private final Clock clock;
    private final Duration staleAfter;

    @Autowired
    FeatureController(
            StringRedisTemplate redisTemplate,
            FeatureDefinitionRepository definitionRepository,
            MeterRegistry meterRegistry,
            @Value("${rfp.feature-api.stale-after:PT5M}") Duration staleAfter
    ) {
        this(redisTemplate, definitionRepository, meterRegistry, Clock.systemUTC(), staleAfter);
    }

    FeatureController(
            StringRedisTemplate redisTemplate,
            FeatureDefinitionRepository definitionRepository,
            MeterRegistry meterRegistry,
            Clock clock,
            Duration staleAfter
    ) {
        this.redisTemplate = redisTemplate;
        this.definitionRepository = definitionRepository;
        this.meterRegistry = meterRegistry;
        this.clock = clock;
        this.staleAfter = staleAfter;
    }

    @GetMapping("/features/{entityType}/{entityId}/{featureName}")
    ResponseEntity<FeatureReadResponse> getFeature(
            @PathVariable String entityType,
            @PathVariable String entityId,
            @PathVariable String featureName,
            @RequestParam(required = false) Instant windowStart
    ) {
        return ResponseEntity.ok(readFeature(entityType, entityId, featureName, windowStart));
    }

    @GetMapping("/features/{entityType}/{entityId}")
    ResponseEntity<FeatureSetResponse> getFeatures(
            @PathVariable String entityType,
            @PathVariable String entityId,
            @RequestParam List<String> featureNames
    ) {
        List<FeatureReadResponse> features = featureNames.stream()
                .map(featureName -> readFeature(entityType, entityId, featureName, null))
                .toList();
        return ResponseEntity.ok(new FeatureSetResponse(entityType, entityId, features));
    }

    @PostMapping("/features/batch")
    ResponseEntity<BatchFeatureResponse> getFeatureBatch(@RequestBody BatchFeatureRequest request) {
        List<FeatureReadResponse> features = request.features().stream()
                .map(feature -> readFeature(
                        feature.entityType(),
                        feature.entityId(),
                        feature.featureName(),
                        feature.windowStart()
                ))
                .toList();
        return ResponseEntity.ok(new BatchFeatureResponse(features));
    }

    private FeatureReadResponse readFeature(
            String entityType,
            String entityId,
            String featureName,
            Instant windowStart
    ) {
        FeatureKey key = new FeatureKey(entityType, entityId, featureName);
        String redisKey = windowStart == null ? key.redisKey() : new WindowedFeatureKey(key, windowStart).redisKey();
        try {
            String value = redisTemplate.opsForValue().get(redisKey);
            Instant updatedAt = parseInstant(redisTemplate.opsForValue().get(updatedAtKey(redisKey)));
            Integer definitionVersion = parseInteger(redisTemplate.opsForValue().get(definitionVersionKey(redisKey)));
            Long ttlSeconds = normalizeTtl(redisTemplate.getExpire(redisKey));
            if (definitionVersion == null) {
                definitionVersion = activeDefinitionVersion(entityType, featureName);
            }
            FeatureReadStatus status = value == null
                    ? FeatureReadStatus.MISSING
                    : statusFor(updatedAt);
            recordRedisRead(status);
            return new FeatureReadResponse(
                    entityType,
                    entityId,
                    featureName,
                    windowStart,
                    value == null ? null : parseFeatureValue(value),
                    new FeatureMetadata(
                            status,
                            updatedAt,
                            freshnessMillis(updatedAt),
                            definitionVersion,
                            ttlSeconds
                    )
            );
        } catch (RuntimeException ex) {
            recordRedisRead(FeatureReadStatus.UNAVAILABLE);
            return new FeatureReadResponse(
                    entityType,
                    entityId,
                    featureName,
                    windowStart,
                    null,
                    new FeatureMetadata(
                            FeatureReadStatus.UNAVAILABLE,
                            null,
                            null,
                            activeDefinitionVersion(entityType, featureName),
                            null
                    )
            );
        }
    }

    private static Number parseFeatureValue(String value) {
        if (value.contains(".") || value.contains("e") || value.contains("E")) {
            return Double.parseDouble(value);
        }
        return Long.parseLong(value);
    }

    private FeatureReadStatus statusFor(Instant updatedAt) {
        if (updatedAt != null && updatedAt.plus(staleAfter).isBefore(clock.instant())) {
            return FeatureReadStatus.STALE;
        }
        return FeatureReadStatus.PRESENT;
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

    private void recordRedisRead(FeatureReadStatus status) {
        meterRegistry.counter("rfp.feature_api.redis.reads", "status", status.name().toLowerCase()).increment();
    }

    private static Instant parseInstant(String value) {
        return value == null ? null : Instant.parse(value);
    }

    private static Integer parseInteger(String value) {
        return value == null ? null : Integer.parseInt(value);
    }

    private static Long normalizeTtl(Long ttlSeconds) {
        return ttlSeconds == null || ttlSeconds < 0 ? null : ttlSeconds;
    }

    private static String updatedAtKey(String key) {
        return key + ":updated-at";
    }

    private static String definitionVersionKey(String key) {
        return key + ":definition-version";
    }

    enum FeatureReadStatus {
        PRESENT,
        MISSING,
        STALE,
        UNAVAILABLE
    }

    record FeatureReadResponse(
            String entityType,
            String entityId,
            String featureName,
            Instant windowStart,
            Number value,
            FeatureMetadata metadata
    ) {
    }

    record FeatureMetadata(
            FeatureReadStatus status,
            Instant updatedAt,
            Long freshnessMillis,
            Integer definitionVersion,
            Long ttlSeconds
    ) {
    }

    record FeatureSetResponse(String entityType, String entityId, List<FeatureReadResponse> features) {
    }

    record BatchFeatureRequest(List<FeatureLookup> features) {
    }

    record FeatureLookup(String entityType, String entityId, String featureName, Instant windowStart) {
    }

    record BatchFeatureResponse(List<FeatureReadResponse> features) {
    }
}

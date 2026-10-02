package com.mustafabulu.realtimefeatureplatform.featureapi;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mustafabulu.realtimefeatureplatform.featuremodel.BuiltInFeatureDefinitions;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinition;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinitionRepository;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureNames;
import com.mustafabulu.realtimefeatureplatform.featuremodel.MutableInMemoryFeatureDefinitionRepository;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class BaselineFeatureControllerTest {

    private static final Instant NOW = Instant.parse("2026-08-28T12:20:00Z");
    private static final Instant TEN_MINUTE_WINDOW = Instant.parse("2026-08-28T12:10:00Z");
    private static final Instant FIVE_MINUTE_WINDOW = Instant.parse("2026-08-28T12:15:00Z");
    private static final Instant UPDATED_AT = Instant.parse("2026-08-28T12:19:00Z");

    @Test
    void returnsRequestTimeBaselineValuesForGoldenFeatures() {
        BaselineFeatureController controller = controller(new FakeBaselineRepository());

        assertFeatureValue(controller, FeatureNames.REQUEST_COUNT_TOTAL, null, 450L);
        assertFeatureValue(controller, FeatureNames.ENTITY_EVENT_COUNT_10M, TEN_MINUTE_WINDOW, 3L);
        assertFeatureValue(controller, FeatureNames.ENTITY_ERROR_RATE_10M, TEN_MINUTE_WINDOW, 50.0 / 450.0);
        assertFeatureValue(controller, FeatureNames.ENTITY_AVG_LATENCY_MS_5M, FIVE_MINUTE_WINDOW, 59_000.0 / 450.0);
    }

    @Test
    void returnsFeatureSubsetWithComparableRealtimeShape() {
        BaselineFeatureController controller = controller(new FakeBaselineRepository());

        ResponseEntity<FeatureController.FeatureSetResponse> response = controller.getFeatures(
                "service",
                "catalog-api",
                List.of(FeatureNames.REQUEST_COUNT_TOTAL, FeatureNames.ENTITY_EVENT_COUNT_10M),
                "run-1",
                null
        );

        assertEquals(HttpStatus.OK, response.getStatusCode());
        FeatureController.FeatureSetResponse body = requireNonNull(response.getBody());
        assertEquals("service", body.entityType());
        assertEquals("catalog-api", body.entityId());
        assertEquals(2, body.features().size());
        assertEquals(450L, body.features().get(0).value());
        assertEquals(3L, body.features().get(1).value());
    }

    @Test
    void defaultsBenchmarkPhaseToMeasurementWhenRunIdIsProvided() {
        CapturingBaselineRepository repository = new CapturingBaselineRepository();
        BaselineFeatureController controller = controller(repository);

        controller.getFeature("service", "catalog-api", FeatureNames.REQUEST_COUNT_TOTAL, null, "run-1", null);

        BaselineFeatureQueryRepository.BaselineFeatureRequest request = requireNonNull(repository.lastRequest);
        assertEquals("run-1", request.benchmarkRunId());
        assertEquals("measurement", request.benchmarkPhase());
    }

    @Test
    void returnsBatchBaselineReads() {
        BaselineFeatureController controller = controller(new FakeBaselineRepository());

        ResponseEntity<FeatureController.BatchFeatureResponse> response = controller.getFeatureBatch(
                new FeatureController.BatchFeatureRequest(List.of(
                        new FeatureController.FeatureLookup("service", "catalog-api", FeatureNames.REQUEST_COUNT_TOTAL, null),
                        new FeatureController.FeatureLookup("service", "catalog-api", FeatureNames.ENTITY_EVENT_COUNT_10M, null)
                )),
                null,
                null
        );

        List<FeatureController.FeatureReadResponse> features =
                requireNonNull(requireNonNull(response.getBody()).features());
        assertEquals(2, features.size());
        assertEquals(450L, features.get(0).value());
        assertEquals(3L, features.get(1).value());
    }

    @Test
    void modelsBaselineUnavailableEvenWhenDefinitionRepositoryFails() {
        BaselineFeatureController controller = new BaselineFeatureController(
                new FailingBaselineRepository(),
                failingDefinitionRepository(),
                Clock.fixed(NOW, ZoneOffset.UTC),
                Duration.ofMinutes(5)
        );

        ResponseEntity<FeatureController.FeatureReadResponse> response = controller.getFeature(
                "service",
                "catalog-api",
                FeatureNames.REQUEST_COUNT_TOTAL,
                null,
                null,
                null
        );

        FeatureController.FeatureReadResponse body = requireNonNull(response.getBody());
        FeatureController.FeatureMetadata metadata = requireNonNull(body.metadata());
        assertEquals(FeatureController.FeatureReadStatus.UNAVAILABLE, metadata.status());
        assertEquals(null, metadata.definitionVersion());
    }

    private static void assertFeatureValue(
            BaselineFeatureController controller,
            String featureName,
            Instant expectedWindowStart,
            Number expectedValue
    ) {
        ResponseEntity<FeatureController.FeatureReadResponse> response = controller.getFeature(
                "service",
                "catalog-api",
                featureName,
                null,
                null,
                null
        );

        assertEquals(HttpStatus.OK, response.getStatusCode());
        FeatureController.FeatureReadResponse body = requireNonNull(response.getBody());
        FeatureController.FeatureMetadata metadata = requireNonNull(body.metadata());
        assertEquals(expectedWindowStart, body.windowStart());
        assertEquals(expectedValue, body.value());
        assertEquals(FeatureController.FeatureReadStatus.PRESENT, metadata.status());
        assertEquals(60_000L, metadata.freshnessMillis());
        assertEquals(1, metadata.definitionVersion());
        assertEquals(null, metadata.ttlSeconds());
    }

    private static BaselineFeatureController controller(BaselineFeatureQueryRepository repository) {
        return new BaselineFeatureController(
                repository,
                new MutableInMemoryFeatureDefinitionRepository(BuiltInFeatureDefinitions.all()),
                Clock.fixed(NOW, ZoneOffset.UTC),
                Duration.ofMinutes(5)
        );
    }

    private static class FakeBaselineRepository extends BaselineFeatureQueryRepository {

        FakeBaselineRepository() {
            super(null);
        }

        @Override
        Optional<BaselineFeatureRow> read(BaselineFeatureRequest request) {
            return switch (request.featureName()) {
                case FeatureNames.REQUEST_COUNT_TOTAL ->
                        Optional.of(new BaselineFeatureRow(450L, null, UPDATED_AT));
                case FeatureNames.ENTITY_EVENT_COUNT_10M ->
                        Optional.of(new BaselineFeatureRow(3L, TEN_MINUTE_WINDOW, UPDATED_AT));
                case FeatureNames.ENTITY_ERROR_RATE_10M ->
                        Optional.of(new BaselineFeatureRow(50.0 / 450.0, TEN_MINUTE_WINDOW, UPDATED_AT));
                case FeatureNames.ENTITY_AVG_LATENCY_MS_5M ->
                        Optional.of(new BaselineFeatureRow(59_000.0 / 450.0, FIVE_MINUTE_WINDOW, UPDATED_AT));
                default -> Optional.empty();
            };
        }
    }

    private static final class CapturingBaselineRepository extends FakeBaselineRepository {

        private BaselineFeatureRequest lastRequest;

        @Override
        Optional<BaselineFeatureRow> read(BaselineFeatureRequest request) {
            lastRequest = request;
            return super.read(request);
        }
    }

    private static final class FailingBaselineRepository extends BaselineFeatureQueryRepository {

        FailingBaselineRepository() {
            super(null);
        }

        @Override
        Optional<BaselineFeatureRow> read(BaselineFeatureRequest request) {
            throw new IllegalStateException("postgres unavailable");
        }
    }

    private static FeatureDefinitionRepository failingDefinitionRepository() {
        return new FeatureDefinitionRepository() {
            @Override
            public List<FeatureDefinition> findAll() {
                throw new IllegalStateException("registry unavailable");
            }
        };
    }
}

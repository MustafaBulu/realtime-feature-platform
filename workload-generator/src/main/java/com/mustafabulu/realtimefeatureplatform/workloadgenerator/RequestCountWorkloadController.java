package com.mustafabulu.realtimefeatureplatform.workloadgenerator;

import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
class RequestCountWorkloadController {

    private final RequestCountWorkloadProducer producer;

    RequestCountWorkloadController(RequestCountWorkloadProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/workloads/request-count-total")
    @ResponseStatus(HttpStatus.ACCEPTED)
    WorkloadResponse produce(@RequestBody(required = false) WorkloadRequest request) {
        WorkloadRequest effectiveRequest = request == null ? WorkloadRequest.defaultRequest() : request.withDefaults();
        List<PlatformEvent> events = producer.produce(effectiveRequest);

        return new WorkloadResponse(
                effectiveRequest.entityType(),
                effectiveRequest.entityId(),
                events.size(),
                events.stream().mapToLong(event -> ((Number) event.payload().get("count")).longValue()).sum(),
                effectiveRequest.expectedErrorRate(),
                effectiveRequest.expectedAverageLatencyMs()
        );
    }

    record WorkloadRequest(String entityType, String entityId, List<Long> values, List<RequestSample> samples) {

        static WorkloadRequest defaultRequest() {
            return new WorkloadRequest(
                    "service",
                    "catalog-api",
                    null,
                    List.of(
                            new RequestSample(100L, 200, 80L),
                            new RequestSample(300L, 200, 120L),
                            new RequestSample(50L, 500, 300L)
                    )
            );
        }

        WorkloadRequest withDefaults() {
            String resolvedEntityType = entityType == null || entityType.isBlank() ? "service" : entityType;
            String resolvedEntityId = entityId == null || entityId.isBlank() ? "catalog-api" : entityId;
            List<RequestSample> resolvedSamples = resolveSamples();

            return new WorkloadRequest(resolvedEntityType, resolvedEntityId, null, resolvedSamples);
        }

        private List<RequestSample> resolveSamples() {
            if (samples != null && !samples.isEmpty()) {
                return samples.stream()
                        .map(RequestSample::withDefaults)
                        .toList();
            }

            List<Long> resolvedValues = values == null || values.isEmpty() ? List.of(100L, 300L, 50L) : List.copyOf(values);
            return resolvedValues.stream()
                    .map(value -> new RequestSample(value, 200, 100L))
                    .toList();
        }

        double expectedErrorRate() {
            long requestCount = samples.stream().mapToLong(RequestSample::count).sum();
            long errorCount = samples.stream()
                    .filter(RequestSample::serverError)
                    .mapToLong(RequestSample::count)
                    .sum();
            return requestCount == 0 ? 0.0 : (double) errorCount / requestCount;
        }

        double expectedAverageLatencyMs() {
            long requestCount = samples.stream().mapToLong(RequestSample::count).sum();
            long weightedLatencySum = samples.stream()
                    .mapToLong(sample -> sample.count() * sample.latencyMs())
                    .sum();
            return requestCount == 0 ? 0.0 : (double) weightedLatencySum / requestCount;
        }
    }

    record RequestSample(Long count, Integer statusCode, Long latencyMs) {

        RequestSample withDefaults() {
            long resolvedCount = count == null ? 1L : count;
            int resolvedStatusCode = statusCode == null ? 200 : statusCode;
            long resolvedLatencyMs = latencyMs == null ? 100L : latencyMs;

            return new RequestSample(resolvedCount, resolvedStatusCode, resolvedLatencyMs);
        }

        boolean serverError() {
            return statusCode >= 500;
        }
    }

    record WorkloadResponse(
            String entityType,
            String entityId,
            int eventsProduced,
            long expectedTotal,
            double expectedErrorRate,
            double expectedAverageLatencyMs
    ) {
    }
}

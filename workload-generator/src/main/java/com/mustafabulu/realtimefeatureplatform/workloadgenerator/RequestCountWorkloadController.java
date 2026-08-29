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
                events.stream().mapToLong(event -> ((Number) event.payload().get("count")).longValue()).sum()
        );
    }

    record WorkloadRequest(String entityType, String entityId, List<Long> values) {

        static WorkloadRequest defaultRequest() {
            return new WorkloadRequest("service", "catalog-api", List.of(100L, 300L, 50L));
        }

        WorkloadRequest withDefaults() {
            String resolvedEntityType = entityType == null || entityType.isBlank() ? "service" : entityType;
            String resolvedEntityId = entityId == null || entityId.isBlank() ? "catalog-api" : entityId;
            List<Long> resolvedValues = values == null || values.isEmpty() ? List.of(100L, 300L, 50L) : List.copyOf(values);

            return new WorkloadRequest(resolvedEntityType, resolvedEntityId, resolvedValues);
        }
    }

    record WorkloadResponse(String entityType, String entityId, int eventsProduced, long expectedTotal) {
    }
}

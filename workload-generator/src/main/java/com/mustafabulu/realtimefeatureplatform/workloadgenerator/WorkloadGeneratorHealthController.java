package com.mustafabulu.realtimefeatureplatform.workloadgenerator;

import java.time.Instant;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
class WorkloadGeneratorHealthController {

    @GetMapping("/internal/health")
    GeneratorHealth health() {
        return new GeneratorHealth(
                "workload-generator",
                "UP",
                Instant.now(),
                Map.of("role", "synthetic event source skeleton")
        );
    }

    record GeneratorHealth(String service, String status, Instant checkedAt, Map<String, String> details) {
    }
}

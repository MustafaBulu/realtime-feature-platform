package com.mustafabulu.realtimefeatureplatform.streamworker;

import java.time.Instant;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
class StreamWorkerHealthController {

    @GetMapping("/internal/health")
    WorkerHealth health() {
        return new WorkerHealth(
                "stream-worker",
                "UP",
                Instant.now(),
                Map.of("role", "stream processing skeleton")
        );
    }

    record WorkerHealth(String service, String status, Instant checkedAt, Map<String, String> details) {
    }
}

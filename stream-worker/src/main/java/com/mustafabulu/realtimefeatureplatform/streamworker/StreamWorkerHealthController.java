package com.mustafabulu.realtimefeatureplatform.streamworker;

import java.time.Instant;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
class StreamWorkerHealthController {

    private final RecoveryState recoveryState;

    StreamWorkerHealthController(RecoveryState recoveryState) {
        this.recoveryState = recoveryState;
    }

    @GetMapping("/internal/health")
    WorkerHealth health() {
        return new WorkerHealth(
                "stream-worker",
                recoveryState.ready() ? "UP" : "RECOVERING",
                Instant.now(),
                Map.of(
                        "role", "stream processing skeleton",
                        "recoveryReady", Boolean.toString(recoveryState.ready()),
                        "assignedPartitions", recoveryState.assignedPartitions().toString()
                )
        );
    }

    record WorkerHealth(String service, String status, Instant checkedAt, Map<String, String> details) {
    }
}

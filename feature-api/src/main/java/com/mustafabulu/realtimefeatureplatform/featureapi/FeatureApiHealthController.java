package com.mustafabulu.realtimefeatureplatform.featureapi;

import java.time.Instant;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
class FeatureApiHealthController {

    @GetMapping("/internal/health")
    ApiHealth health() {
        return new ApiHealth(
                "feature-api",
                "UP",
                Instant.now(),
                Map.of("role", "online feature serving")
        );
    }

    record ApiHealth(String service, String status, Instant checkedAt, Map<String, String> details) {
    }
}

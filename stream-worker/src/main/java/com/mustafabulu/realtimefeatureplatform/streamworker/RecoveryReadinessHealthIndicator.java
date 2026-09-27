package com.mustafabulu.realtimefeatureplatform.streamworker;

import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

@Component("recoveryReadiness")
class RecoveryReadinessHealthIndicator implements HealthIndicator {

    private final RecoveryState recoveryState;

    RecoveryReadinessHealthIndicator(RecoveryState recoveryState) {
        this.recoveryState = recoveryState;
    }

    @Override
    public Health health() {
        Health.Builder builder = recoveryState.ready() ? Health.up() : Health.outOfService();
        return builder
                .withDetail("restoredAt", recoveryState.restoredAt())
                .withDetail("restoreDurationMillis", recoveryState.restoreDuration().toMillis())
                .withDetail("republishedFeatureKeys", recoveryState.republishedFeatureKeys())
                .withDetail("assignedPartitions", recoveryState.assignedPartitions().toString())
                .build();
    }
}

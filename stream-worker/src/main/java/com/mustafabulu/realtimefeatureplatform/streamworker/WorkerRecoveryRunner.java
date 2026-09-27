package com.mustafabulu.realtimefeatureplatform.streamworker;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
class WorkerRecoveryRunner implements ApplicationRunner {

    private final RedisStateRepublisher republisher;
    private final RecoveryState recoveryState;
    private final StreamWorkerEventMetrics metrics;
    private final Clock clock;

    @Autowired
    WorkerRecoveryRunner(
            RedisStateRepublisher republisher,
            RecoveryState recoveryState,
            StreamWorkerEventMetrics metrics
    ) {
        this(republisher, recoveryState, metrics, Clock.systemUTC());
    }

    WorkerRecoveryRunner(
            RedisStateRepublisher republisher,
            RecoveryState recoveryState,
            StreamWorkerEventMetrics metrics,
            Clock clock
    ) {
        this.republisher = republisher;
        this.recoveryState = recoveryState;
        this.metrics = metrics;
        this.clock = clock;
    }

    @Override
    public void run(ApplicationArguments args) {
        recoveryState.markRecovering();
        Instant startedAt = clock.instant();
        int republished = republisher.republishAll();
        Duration duration = Duration.between(startedAt, clock.instant());
        recoveryState.markRestored(clock.instant(), duration, republished);
        metrics.recordRestoreDuration(duration);
    }
}

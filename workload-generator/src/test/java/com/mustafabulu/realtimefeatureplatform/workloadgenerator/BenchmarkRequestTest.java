package com.mustafabulu.realtimefeatureplatform.workloadgenerator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

class BenchmarkRequestTest {

    @Test
    void fillsPhaseEighteenDefaults() {
        BenchmarkController.BenchmarkRequest request = new BenchmarkController.BenchmarkRequest(
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null
        ).withDefaults();

        assertFalse(request.includeRawResults());
        assertEquals(1000, request.rawResultLimit());
        assertEquals(500L, request.readSloMillis());
        assertEquals(20, request.correctnessSampleSize());
        assertEquals(5, request.freshnessProbeCount());
        assertEquals(Duration.ofMillis(100), request.freshnessProbePollInterval());
        assertEquals(Duration.ofSeconds(10), request.freshnessProbeTimeout());
    }

    @Test
    void preservesExplicitRawLimitAndProbeSettings() {
        BenchmarkController.BenchmarkRequest request = new BenchmarkController.BenchmarkRequest(
                Duration.ofSeconds(1),
                Duration.ofSeconds(2),
                10,
                20,
                30,
                BenchmarkController.EntityDistribution.ZIPF,
                1.2,
                List.of("request_count_total"),
                true,
                false,
                true,
                50,
                250L,
                7,
                3,
                Duration.ofMillis(25),
                Duration.ofSeconds(4)
        ).withDefaults();

        assertTrue(request.includeRawResults());
        assertEquals(50, request.rawResultLimit());
        assertEquals(250L, request.readSloMillis());
        assertEquals(7, request.correctnessSampleSize());
        assertEquals(3, request.freshnessProbeCount());
        assertEquals(Duration.ofMillis(25), request.freshnessProbePollInterval());
        assertEquals(Duration.ofSeconds(4), request.freshnessProbeTimeout());
    }
}

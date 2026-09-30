package com.mustafabulu.realtimefeatureplatform.featureapi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureNames;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

class BaselineFeatureQueryRepositoryTest {

    @Test
    void buildsRequestCountTotalSql() {
        CapturedQuery captured = captureQueryFor(FeatureNames.REQUEST_COUNT_TOTAL, null);

        assertTrue(captured.sql().contains("sum(count_value)::bigint"));
        assertTrue(captured.sql().contains("benchmark_run_id = ?"));
        assertTrue(captured.sql().contains("benchmark_phase = ?"));
        assertEquals(List.of("request.completed", "service", "catalog-api", "run-1", "measurement"), captured.args());
    }

    @Test
    void buildsEntityEventCountTenMinuteSql() {
        CapturedQuery captured = captureQueryFor(FeatureNames.ENTITY_EVENT_COUNT_10M, null);

        assertTrue(captured.sql().contains("count(*)::bigint"));
        assertTrue(captured.sql().contains("extract(epoch from event_time) / 600"));
        assertTrue(captured.sql().contains("order by window_start desc"));
    }

    @Test
    void buildsEntityErrorRateTenMinuteSql() {
        CapturedQuery captured = captureQueryFor(FeatureNames.ENTITY_ERROR_RATE_10M, null);

        assertTrue(captured.sql().contains("status_code >= 500"));
        assertTrue(captured.sql().contains("sum(count_value)::double precision"));
        assertTrue(captured.sql().contains("extract(epoch from event_time) / 600"));
    }

    @Test
    void buildsEntityAverageLatencyFiveMinuteSql() {
        CapturedQuery captured = captureQueryFor(FeatureNames.ENTITY_AVG_LATENCY_MS_5M, null);

        assertTrue(captured.sql().contains("latency_ms * count_value"));
        assertTrue(captured.sql().contains("sum(count_value)::double precision"));
        assertTrue(captured.sql().contains("extract(epoch from event_time) / 300"));
    }

    @Test
    void buildsWindowSpecificQueryWhenWindowStartIsProvided() {
        Instant windowStart = Instant.parse("2026-08-28T12:10:00Z");
        CapturedQuery captured = captureQueryFor(FeatureNames.ENTITY_EVENT_COUNT_10M, windowStart);

        assertTrue(captured.sql().contains("event_time >= ?"));
        assertTrue(captured.sql().contains("event_time < ?"));
        assertEquals(8, captured.args().size());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static CapturedQuery captureQueryFor(String featureName, Instant windowStart) {
        JdbcTemplate jdbcTemplate = org.mockito.Mockito.mock(JdbcTemplate.class);
        when(jdbcTemplate.query(anyString(), any(RowMapper.class), any(Object[].class))).thenReturn(List.of());
        BaselineFeatureQueryRepository repository = new BaselineFeatureQueryRepository(jdbcTemplate);

        Optional<BaselineFeatureQueryRepository.BaselineFeatureRow> row = repository.read(
                new BaselineFeatureQueryRepository.BaselineFeatureRequest(
                        "service",
                        "catalog-api",
                        featureName,
                        windowStart,
                        "run-1",
                        "measurement"
                )
        );

        assertTrue(row.isEmpty());
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Object[]> argsCaptor = ArgumentCaptor.forClass(Object[].class);
        org.mockito.Mockito.verify(jdbcTemplate).query(sqlCaptor.capture(), any(RowMapper.class), argsCaptor.capture());
        return new CapturedQuery(sqlCaptor.getValue(), List.of(argsCaptor.getValue()));
    }

    private record CapturedQuery(String sql, List<Object> args) {
    }
}

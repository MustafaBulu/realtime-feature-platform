package com.mustafabulu.realtimefeatureplatform.featureapi;

import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureNames;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
@ConditionalOnBean(JdbcTemplate.class)
class BaselineFeatureQueryRepository {

    private static final String REQUEST_COMPLETED = "request.completed";
    private static final int TEN_MINUTE_SECONDS = 600;
    private static final int FIVE_MINUTE_SECONDS = 300;

    private final JdbcTemplate jdbcTemplate;

    BaselineFeatureQueryRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    Optional<BaselineFeatureRow> read(BaselineFeatureRequest request) {
        if (FeatureNames.REQUEST_COUNT_TOTAL.equals(request.featureName())) {
            return request.windowStart() == null ? query(requestCountTotalSql(request), request) : Optional.empty();
        }
        if (FeatureNames.ENTITY_EVENT_COUNT_10M.equals(request.featureName())) {
            return query(windowedSql(request, entityEventCountExpression(), TEN_MINUTE_SECONDS), request);
        }
        if (FeatureNames.ENTITY_ERROR_RATE_10M.equals(request.featureName())) {
            return query(windowedSql(request, entityErrorRateExpression(), TEN_MINUTE_SECONDS), request);
        }
        if (FeatureNames.ENTITY_AVG_LATENCY_MS_5M.equals(request.featureName())) {
            return query(windowedSql(request, entityAverageLatencyExpression(), FIVE_MINUTE_SECONDS), request);
        }
        return Optional.empty();
    }

    private Optional<BaselineFeatureRow> query(SqlAndArgs sqlAndArgs, BaselineFeatureRequest request) {
        List<BaselineFeatureRow> rows = jdbcTemplate.query(
                sqlAndArgs.sql(),
                (resultSet, rowNum) -> mapRow(resultSet, request.featureName()),
                sqlAndArgs.args().toArray()
        );
        return rows.stream().findFirst();
    }

    private static BaselineFeatureRow mapRow(ResultSet resultSet, String featureName) throws SQLException {
        Instant windowStart = timestampValue(resultSet, "window_start");
        Instant updatedAt = timestampValue(resultSet, "updated_at");
        Number value = switch (featureName) {
            case FeatureNames.REQUEST_COUNT_TOTAL, FeatureNames.ENTITY_EVENT_COUNT_10M ->
                    resultSet.getLong("feature_value");
            case FeatureNames.ENTITY_ERROR_RATE_10M, FeatureNames.ENTITY_AVG_LATENCY_MS_5M ->
                    resultSet.getDouble("feature_value");
            default -> throw new IllegalArgumentException("unsupported baseline feature " + featureName);
        };
        return new BaselineFeatureRow(value, windowStart, updatedAt);
    }

    private static Instant timestampValue(ResultSet resultSet, String columnName) throws SQLException {
        Timestamp value = resultSet.getTimestamp(columnName);
        return value == null ? null : value.toInstant();
    }

    private static SqlAndArgs requestCountTotalSql(BaselineFeatureRequest request) {
        List<Object> args = baseArgs(request);
        return new SqlAndArgs("""
                select cast(null as timestamptz) as window_start,
                       sum(count_value)::bigint as feature_value,
                       max(event_time) as updated_at
                from historical_events
                %s
                having count(*) > 0
                """.formatted(baseWhere(request)), args);
    }

    private static SqlAndArgs windowedSql(
            BaselineFeatureRequest request,
            String featureExpression,
            int windowSeconds
    ) {
        return request.windowStart() == null
                ? latestWindowSql(request, featureExpression, windowSeconds)
                : specificWindowSql(request, featureExpression, windowSeconds);
    }

    private static SqlAndArgs latestWindowSql(
            BaselineFeatureRequest request,
            String featureExpression,
            int windowSeconds
    ) {
        List<Object> args = baseArgs(request);
        String windowStartExpression = windowStartExpression(windowSeconds);
        return new SqlAndArgs("""
                select window_start,
                       feature_value,
                       updated_at
                from (
                    select %s as window_start,
                           %s as feature_value,
                           max(event_time) as updated_at
                    from historical_events
                    %s
                    group by window_start
                    order by window_start desc
                    limit 1
                ) baseline_feature
                """.formatted(windowStartExpression, featureExpression, baseWhere(request)), args);
    }

    private static SqlAndArgs specificWindowSql(
            BaselineFeatureRequest request,
            String featureExpression,
            int windowSeconds
    ) {
        List<Object> args = new ArrayList<>();
        args.add(Timestamp.from(request.windowStart()));
        args.addAll(baseArgs(request));
        args.add(Timestamp.from(request.windowStart()));
        args.add(Timestamp.from(request.windowStart().plusSeconds(windowSeconds)));

        return new SqlAndArgs("""
                select cast(? as timestamptz) as window_start,
                       %s as feature_value,
                       max(event_time) as updated_at
                from historical_events
                %s
                  and event_time >= ?
                  and event_time < ?
                having count(*) > 0
                """.formatted(featureExpression, baseWhere(request)), args);
    }

    private static String baseWhere(BaselineFeatureRequest request) {
        StringBuilder builder = new StringBuilder("""
                where event_type = ?
                  and entity_type = ?
                  and entity_id = ?
                """);
        if (hasText(request.benchmarkRunId())) {
            builder.append("  and benchmark_run_id = ?\n");
        }
        if (hasText(request.benchmarkPhase())) {
            builder.append("  and benchmark_phase = ?\n");
        }
        return builder.toString();
    }

    private static List<Object> baseArgs(BaselineFeatureRequest request) {
        List<Object> args = new ArrayList<>();
        args.add(REQUEST_COMPLETED);
        args.add(request.entityType());
        args.add(request.entityId());
        if (hasText(request.benchmarkRunId())) {
            args.add(request.benchmarkRunId());
        }
        if (hasText(request.benchmarkPhase())) {
            args.add(request.benchmarkPhase());
        }
        return args;
    }

    private static String windowStartExpression(int windowSeconds) {
        return "to_timestamp(floor(extract(epoch from event_time) / %d) * %d)"
                .formatted(windowSeconds, windowSeconds);
    }

    private static String entityEventCountExpression() {
        return "count(*)::bigint";
    }

    private static String entityErrorRateExpression() {
        return """
                case
                    when sum(count_value) = 0 then 0.0
                    else sum(case when status_code >= 500 then count_value else 0 end)::double precision
                         / sum(count_value)::double precision
                end
                """;
    }

    private static String entityAverageLatencyExpression() {
        return """
                case
                    when sum(count_value) = 0 then 0.0
                    else sum(latency_ms * count_value)::double precision / sum(count_value)::double precision
                end
                """;
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    record BaselineFeatureRequest(
            String entityType,
            String entityId,
            String featureName,
            Instant windowStart,
            String benchmarkRunId,
            String benchmarkPhase
    ) {
    }

    record BaselineFeatureRow(Number value, Instant windowStart, Instant updatedAt) {
    }

    private record SqlAndArgs(String sql, List<Object> args) {
    }
}

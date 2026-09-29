package com.mustafabulu.realtimefeatureplatform.workloadgenerator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

@Repository
class SqlBaselineFeatureQueryRepository {

    private static final int ROW_LIMIT = 10;

    private final String jdbcUrl;
    private final String username;
    private final String password;

    SqlBaselineFeatureQueryRepository(
            @Value("${spring.datasource.url:}") String jdbcUrl,
            @Value("${spring.datasource.username:}") String username,
            @Value("${spring.datasource.password:}") String password
    ) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    boolean enabled() {
        return jdbcUrl != null && !jdbcUrl.isBlank();
    }

    BenchmarkResponse.SqlBaselineReport run(String runId) {
        if (!enabled()) {
            return new BenchmarkResponse.SqlBaselineReport(false, List.of(), "spring.datasource.url is not configured");
        }

        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password)) {
            return new BenchmarkResponse.SqlBaselineReport(true, List.of(
                    query(connection, "request_count_total", requestCountTotalSql(), runId),
                    query(connection, "entity_event_count_10m", entityEventCountTenMinuteSql(), runId),
                    query(connection, "entity_error_rate_10m", entityErrorRateTenMinuteSql(), runId),
                    query(connection, "entity_avg_latency_ms_5m", entityAverageLatencyFiveMinuteSql(), runId)
            ), null);
        } catch (SQLException ex) {
            return new BenchmarkResponse.SqlBaselineReport(false, List.of(), ex.getMessage());
        }
    }

    private static BenchmarkResponse.SqlQueryResult query(
            Connection connection,
            String featureName,
            String sql,
            String runId
    ) throws SQLException {
        long started = System.nanoTime();
        List<BenchmarkResponse.SqlFeatureRow> rows = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, runId);
            statement.setInt(2, ROW_LIMIT);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    rows.add(new BenchmarkResponse.SqlFeatureRow(
                            resultSet.getString("entity_type"),
                            resultSet.getString("entity_id"),
                            timestampValue(resultSet, "window_start"),
                            resultSet.getDouble("feature_value")
                    ));
                }
            }
        }

        long latencyMillis = (System.nanoTime() - started) / 1_000_000;
        return new BenchmarkResponse.SqlQueryResult(featureName, latencyMillis, rows.size(), rows);
    }

    private static Instant timestampValue(ResultSet resultSet, String columnName) throws SQLException {
        java.sql.Timestamp value = resultSet.getTimestamp(columnName);
        return value == null ? null : value.toInstant();
    }

    private static String requestCountTotalSql() {
        return """
                select entity_type,
                       entity_id,
                       cast(null as timestamptz) as window_start,
                       sum(count_value)::double precision as feature_value
                from historical_events
                where benchmark_run_id = ?
                  and benchmark_phase = 'measurement'
                  and event_type = 'request.completed'
                group by entity_type, entity_id
                order by entity_id
                limit ?
                """;
    }

    private static String entityEventCountTenMinuteSql() {
        return """
                select entity_type,
                       entity_id,
                       to_timestamp(floor(extract(epoch from event_time) / 600) * 600) as window_start,
                       count(*)::double precision as feature_value
                from historical_events
                where benchmark_run_id = ?
                  and benchmark_phase = 'measurement'
                  and event_type = 'request.completed'
                group by entity_type, entity_id, window_start
                order by window_start desc, entity_id
                limit ?
                """;
    }

    private static String entityErrorRateTenMinuteSql() {
        return """
                select entity_type,
                       entity_id,
                       to_timestamp(floor(extract(epoch from event_time) / 600) * 600) as window_start,
                       case
                           when sum(count_value) = 0 then 0.0
                           else sum(case when status_code >= 500 then count_value else 0 end)::double precision
                                / sum(count_value)::double precision
                       end as feature_value
                from historical_events
                where benchmark_run_id = ?
                  and benchmark_phase = 'measurement'
                  and event_type = 'request.completed'
                group by entity_type, entity_id, window_start
                order by window_start desc, entity_id
                limit ?
                """;
    }

    private static String entityAverageLatencyFiveMinuteSql() {
        return """
                select entity_type,
                       entity_id,
                       to_timestamp(floor(extract(epoch from event_time) / 300) * 300) as window_start,
                       case
                           when sum(count_value) = 0 then 0.0
                           else sum(latency_ms * count_value)::double precision / sum(count_value)::double precision
                       end as feature_value
                from historical_events
                where benchmark_run_id = ?
                  and benchmark_phase = 'measurement'
                  and event_type = 'request.completed'
                group by entity_type, entity_id, window_start
                order by window_start desc, entity_id
                limit ?
                """;
    }
}

package com.mustafabulu.realtimefeatureplatform.workloadgenerator;

import com.mustafabulu.realtimefeatureplatform.eventmodel.EventJsonCodec;
import com.mustafabulu.realtimefeatureplatform.eventmodel.PlatformEvent;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

@Repository
class HistoricalEventRepository {

    private final String jdbcUrl;
    private final String username;
    private final String password;

    HistoricalEventRepository(
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

    void saveAll(String runId, String phase, List<PlatformEvent> events) {
        if (!enabled() || events.isEmpty()) {
            return;
        }

        String sql = """
                insert into historical_events
                (event_id, benchmark_run_id, benchmark_phase, event_type, event_time, entity_type, entity_id,
                 count_value, status_code, latency_ms, event_json)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, cast(? as jsonb))
                on conflict (event_id) do nothing
                """;

        try (Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement statement = connection.prepareStatement(sql)) {
            connection.setAutoCommit(false);
            for (PlatformEvent event : events) {
                bindEvent(statement, runId, phase, event);
                statement.addBatch();
            }
            statement.executeBatch();
            connection.commit();
        } catch (SQLException ex) {
            throw new IllegalStateException("Could not write benchmark historical events", ex);
        }
    }

    private static void bindEvent(
            PreparedStatement statement,
            String runId,
            String phase,
            PlatformEvent event
    ) throws SQLException {
        statement.setString(1, event.eventId());
        statement.setString(2, runId);
        statement.setString(3, phase);
        statement.setString(4, event.eventType());
        statement.setTimestamp(5, Timestamp.from(event.eventTime()));
        statement.setString(6, event.entity().type());
        statement.setString(7, event.entity().id());
        statement.setLong(8, longPayload(event, "count"));
        statement.setInt(9, (int) longPayload(event, "statusCode"));
        statement.setLong(10, longPayload(event, "latencyMs"));
        statement.setString(11, EventJsonCodec.toJson(event));
    }

    private static long longPayload(PlatformEvent event, String fieldName) {
        Object value = event.payload().get(fieldName);
        if (value instanceof Number number) {
            return number.longValue();
        }
        return 0L;
    }
}

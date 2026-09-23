package com.mustafabulu.realtimefeatureplatform.featureregistry;

import com.mustafabulu.realtimefeatureplatform.featuremodel.AggregationType;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinition;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureDefinitionState;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureFilter;
import com.mustafabulu.realtimefeatureplatform.featuremodel.FeatureFilterOperator;
import com.mustafabulu.realtimefeatureplatform.featuremodel.MutableFeatureDefinitionRepository;
import com.mustafabulu.realtimefeatureplatform.featuremodel.WindowType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

public final class JdbcFeatureDefinitionRepository implements MutableFeatureDefinitionRepository {

    private final JdbcTemplate jdbcTemplate;
    private final TransactionTemplate transactionTemplate;

    public JdbcFeatureDefinitionRepository(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.transactionTemplate = transactionTemplate;
    }

    @Override
    public List<FeatureDefinition> findAll() {
        return jdbcTemplate.query("""
                select * from feature_definitions order by name, version
                """, JdbcFeatureDefinitionRepository::map);
    }

    @Override
    public FeatureDefinition save(FeatureDefinition definition) {
        jdbcTemplate.update("""
                insert into feature_definitions
                (name, version, event_type, entity_type, aggregation_type, value_field, weight_field,
                 filter_field, filter_operator, filter_value, numerator_filter_field, numerator_filter_operator,
                 numerator_filter_value, window_type, window_size, slide, state)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                on conflict (name, version) do update set
                  event_type = excluded.event_type,
                  entity_type = excluded.entity_type,
                  aggregation_type = excluded.aggregation_type,
                  value_field = excluded.value_field,
                  weight_field = excluded.weight_field,
                  filter_field = excluded.filter_field,
                  filter_operator = excluded.filter_operator,
                  filter_value = excluded.filter_value,
                  numerator_filter_field = excluded.numerator_filter_field,
                  numerator_filter_operator = excluded.numerator_filter_operator,
                  numerator_filter_value = excluded.numerator_filter_value,
                  window_type = excluded.window_type,
                  window_size = excluded.window_size,
                  slide = excluded.slide,
                  state = excluded.state,
                  updated_at = now()
                """, params(definition));
        return definition;
    }

    @Override
    public FeatureDefinition activate(String name, int version) {
        return transactionTemplate.execute(status -> {
            jdbcTemplate.update("""
                    update feature_definitions set state = 'INACTIVE', updated_at = now() where name = ?
                    """, name);
            jdbcTemplate.update("""
                    update feature_definitions set state = 'ACTIVE', updated_at = now() where name = ? and version = ?
                    """, name, version);
            return require(name, version);
        });
    }

    @Override
    public FeatureDefinition deactivate(String name, int version) {
        jdbcTemplate.update("""
                update feature_definitions set state = 'INACTIVE', updated_at = now() where name = ? and version = ?
                """, name, version);
        return require(name, version);
    }

    private FeatureDefinition require(String name, int version) {
        return jdbcTemplate.query("""
                select * from feature_definitions where name = ? and version = ?
                """, JdbcFeatureDefinitionRepository::map, name, version).stream()
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("feature definition not found: " + name + ":" + version));
    }

    private static Object[] params(FeatureDefinition definition) {
        FeatureFilter filter = definition.filter();
        FeatureFilter numerator = definition.numeratorFilter();
        return new Object[] {
                definition.name(), definition.version(), definition.eventType(), definition.entityType(),
                definition.aggregationType().name(), definition.valueField(), definition.weightField(),
                field(filter), operator(filter), value(filter), field(numerator), operator(numerator), value(numerator),
                definition.windowType().name(), duration(definition.windowSize()), duration(definition.slide()),
                definition.state().name()
        };
    }

    private static FeatureDefinition map(ResultSet rs, int rowNum) throws SQLException {
        return new FeatureDefinition(
                rs.getString("name"),
                rs.getString("event_type"),
                rs.getString("entity_type"),
                AggregationType.valueOf(rs.getString("aggregation_type")),
                rs.getString("value_field"),
                rs.getString("weight_field"),
                filter(rs, "filter"),
                filter(rs, "numerator_filter"),
                WindowType.valueOf(rs.getString("window_type")),
                duration(rs.getString("window_size")),
                duration(rs.getString("slide")),
                rs.getInt("version"),
                FeatureDefinitionState.valueOf(rs.getString("state"))
        );
    }

    private static FeatureFilter filter(ResultSet rs, String prefix) throws SQLException {
        String field = rs.getString(prefix + "_field");
        String operator = rs.getString(prefix + "_operator");
        if (field == null || operator == null) {
            return null;
        }
        return new FeatureFilter(field, FeatureFilterOperator.valueOf(operator), rs.getString(prefix + "_value"));
    }

    private static String field(FeatureFilter filter) {
        return filter == null ? null : filter.field();
    }

    private static String operator(FeatureFilter filter) {
        return filter == null ? null : filter.operator().name();
    }

    private static String value(FeatureFilter filter) {
        return filter == null ? null : filter.value();
    }

    private static String duration(Duration duration) {
        return duration == null ? null : duration.toString();
    }

    private static Duration duration(String value) {
        return value == null ? null : Duration.parse(value);
    }
}

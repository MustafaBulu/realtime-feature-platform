create table if not exists feature_definitions (
    name varchar(128) not null,
    version integer not null,
    event_type varchar(128) not null,
    entity_type varchar(128) not null,
    aggregation_type varchar(32) not null,
    value_field varchar(128),
    weight_field varchar(128),
    filter_field varchar(128),
    filter_operator varchar(32),
    filter_value varchar(256),
    numerator_filter_field varchar(128),
    numerator_filter_operator varchar(32),
    numerator_filter_value varchar(256),
    window_type varchar(32) not null,
    window_size varchar(64),
    slide varchar(64),
    state varchar(32) not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (name, version)
);

with seed_values as (
    select
        'request.completed' as request_completed_event,
        'service' as service_entity_type,
        'count' as count_field,
        'statusCode' as status_code_field,
        'latencyMs' as latency_ms_field,
        'TUMBLING' as tumbling_window,
        'PT10M' as ten_minute_window,
        'PT5M' as five_minute_window,
        'ACTIVE' as active_state
)
insert into feature_definitions
(name, version, event_type, entity_type, aggregation_type, value_field, weight_field,
 filter_field, filter_operator, filter_value, numerator_filter_field, numerator_filter_operator,
 numerator_filter_value, window_type, window_size, slide, state)
select 'request_count_total', 1, request_completed_event, service_entity_type, 'SUM', count_field, null,
       null, null, null, null, null, null, 'NONE', null, null, active_state
from seed_values
union all
select 'entity_event_count_10m', 1, request_completed_event, service_entity_type, 'COUNT', null, null,
       null, null, null, null, null, null, tumbling_window, ten_minute_window, ten_minute_window, active_state
from seed_values
union all
select 'entity_error_rate_10m', 1, request_completed_event, service_entity_type, 'RATIO', count_field, null,
       status_code_field, 'EXISTS', null, status_code_field, 'GTE', '500',
       tumbling_window, ten_minute_window, ten_minute_window, active_state
from seed_values
union all
select 'entity_avg_latency_ms_5m', 1, request_completed_event, service_entity_type, 'AVG', latency_ms_field, count_field,
       latency_ms_field, 'EXISTS', null, null, null, null, tumbling_window, five_minute_window, five_minute_window,
       active_state
from seed_values
on conflict (name, version) do nothing;

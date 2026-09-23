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

insert into feature_definitions
(name, version, event_type, entity_type, aggregation_type, value_field, weight_field,
 filter_field, filter_operator, filter_value, numerator_filter_field, numerator_filter_operator,
 numerator_filter_value, window_type, window_size, slide, state)
values
('request_count_total', 1, 'request.completed', 'service', 'SUM', 'count', null,
 null, null, null, null, null, null, 'NONE', null, null, 'ACTIVE'),
('entity_event_count_10m', 1, 'request.completed', 'service', 'COUNT', null, null,
 null, null, null, null, null, null, 'TUMBLING', 'PT10M', 'PT10M', 'ACTIVE'),
('entity_error_rate_10m', 1, 'request.completed', 'service', 'RATIO', 'count', null,
 'statusCode', 'EXISTS', null, 'statusCode', 'GTE', '500', 'TUMBLING', 'PT10M', 'PT10M', 'ACTIVE'),
('entity_avg_latency_ms_5m', 1, 'request.completed', 'service', 'AVG', 'latencyMs', 'count',
 'latencyMs', 'EXISTS', null, null, null, null, 'TUMBLING', 'PT5M', 'PT5M', 'ACTIVE')
on conflict (name, version) do nothing;

create table if not exists historical_events (
    event_id varchar(64) not null primary key,
    benchmark_run_id varchar(64),
    benchmark_phase varchar(32),
    event_type varchar(128) not null,
    event_time timestamptz not null,
    entity_type varchar(128) not null,
    entity_id varchar(256) not null,
    count_value bigint,
    status_code integer,
    latency_ms bigint,
    event_json jsonb not null,
    created_at timestamptz not null default now()
);

create index if not exists idx_historical_events_run_phase
    on historical_events (benchmark_run_id, benchmark_phase);

create index if not exists idx_historical_events_entity_time
    on historical_events (event_type, entity_type, entity_id, event_time);

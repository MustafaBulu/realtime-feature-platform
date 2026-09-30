create index if not exists idx_historical_events_baseline_run_entity_time
    on historical_events (benchmark_run_id, benchmark_phase, event_type, entity_type, entity_id, event_time);

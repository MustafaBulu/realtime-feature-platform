package com.mustafabulu.realtimefeatureplatform.streamworker;

import java.util.LinkedHashMap;
import java.util.Map;

class InMemoryAggregationStateStore implements AggregationStateStore {

    private final Map<String, String> values = new LinkedHashMap<>();

    @Override
    public String get(String key) {
        return values.get(key);
    }

    @Override
    public void put(String key, String value) {
        values.put(key, value);
    }
}

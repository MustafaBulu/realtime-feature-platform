package com.mustafabulu.realtimefeatureplatform.streamworker;

import org.springframework.stereotype.Component;

@Component
class RocksDbAggregationStateStore implements AggregationStateStore {

    private final RequestCountTotalStateStore stateStore;

    RocksDbAggregationStateStore(RequestCountTotalStateStore stateStore) {
        this.stateStore = stateStore;
    }

    @Override
    public String get(String key) {
        return stateStore.get(key);
    }

    @Override
    public void put(String key, String value) {
        stateStore.put(key, value);
    }
}

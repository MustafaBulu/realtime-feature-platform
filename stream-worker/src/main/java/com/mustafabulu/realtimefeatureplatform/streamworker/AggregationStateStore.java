package com.mustafabulu.realtimefeatureplatform.streamworker;

interface AggregationStateStore {

    String get(String key);

    void put(String key, String value);
}

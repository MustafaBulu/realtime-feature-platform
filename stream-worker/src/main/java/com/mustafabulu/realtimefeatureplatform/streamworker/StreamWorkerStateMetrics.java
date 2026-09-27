package com.mustafabulu.realtimefeatureplatform.streamworker;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
class StreamWorkerStateMetrics {

    StreamWorkerStateMetrics(MeterRegistry meterRegistry, RequestCountTotalStateStore stateStore) {
        Gauge.builder("rfp.worker.rocksdb.state.size.bytes", stateStore, RequestCountTotalStateStore::estimatedStateSizeBytes)
                .description("Approximate RocksDB logical state size in bytes")
                .baseUnit("bytes")
                .register(meterRegistry);
    }
}

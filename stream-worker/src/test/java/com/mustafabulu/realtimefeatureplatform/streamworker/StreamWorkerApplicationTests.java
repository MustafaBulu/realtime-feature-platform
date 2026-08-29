package com.mustafabulu.realtimefeatureplatform.streamworker;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
        "spring.kafka.listener.auto-startup=false",
        "rfp.rocksdb.path=target/test-rocksdb/context-load"
})
class StreamWorkerApplicationTests {

    @Test
    void contextLoads() {
    }
}

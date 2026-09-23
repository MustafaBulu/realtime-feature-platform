package com.mustafabulu.realtimefeatureplatform.streamworker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication(excludeName = {
        "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
        "org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration"
})
public class StreamWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(StreamWorkerApplication.class, args);
    }
}

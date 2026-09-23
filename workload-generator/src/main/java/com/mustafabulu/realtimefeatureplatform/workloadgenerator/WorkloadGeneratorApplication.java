package com.mustafabulu.realtimefeatureplatform.workloadgenerator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(excludeName = {
        "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
        "org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration"
})
public class WorkloadGeneratorApplication {

    public static void main(String[] args) {
        SpringApplication.run(WorkloadGeneratorApplication.class, args);
    }
}

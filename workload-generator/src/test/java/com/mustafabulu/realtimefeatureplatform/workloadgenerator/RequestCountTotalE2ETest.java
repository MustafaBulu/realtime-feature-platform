package com.mustafabulu.realtimefeatureplatform.workloadgenerator;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.mustafabulu.realtimefeatureplatform.featureapi.FeatureApiApplication;
import com.mustafabulu.realtimefeatureplatform.streamworker.StreamWorkerApplication;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.ObjectMapper;

@DisabledIfEnvironmentVariable(named = "CI", matches = "true")
@Testcontainers(disabledWithoutDocker = true)
class RequestCountTotalE2ETest {

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:3.9.0")
    );

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void materializesRequestCountTotalFromKafkaToRedisToApi(@TempDir Path tempDir) throws Exception {
        String consumerGroup = "stream-worker-e2e-" + UUID.randomUUID();
        try (
                ConfigurableApplicationContext worker = startWorker(tempDir, consumerGroup);
                ConfigurableApplicationContext api = startApi();
                ConfigurableApplicationContext generator = startGenerator()
        ) {
            int apiPort = port(api);
            int generatorPort = port(generator);

            postWorkload(generatorPort);

            await()
                    .atMost(Duration.ofSeconds(30))
                    .pollInterval(Duration.ofMillis(250))
                    .untilAsserted(() -> assertEquals(450L, readFeatureValue(apiPort)));
        }
    }

    private ConfigurableApplicationContext startWorker(Path tempDir, String consumerGroup) {
        return new SpringApplicationBuilder(StreamWorkerApplication.class)
                .properties(commonProperties())
                .properties(
                        "server.port=0",
                        "spring.kafka.consumer.group-id=" + consumerGroup,
                        "rfp.rocksdb.path=" + tempDir.resolve("rocksdb")
                )
                .run();
    }

    private ConfigurableApplicationContext startApi() {
        return new SpringApplicationBuilder(FeatureApiApplication.class)
                .properties(commonProperties())
                .properties("server.port=0")
                .run();
    }

    private ConfigurableApplicationContext startGenerator() {
        return new SpringApplicationBuilder(WorkloadGeneratorApplication.class)
                .properties(commonProperties())
                .properties("server.port=0")
                .run();
    }

    private String[] commonProperties() {
        return new String[]{
                "spring.main.banner-mode=off",
                "spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                "spring.data.redis.host=" + REDIS.getHost(),
                "spring.data.redis.port=" + REDIS.getMappedPort(6379),
                "rfp.kafka.events-topic=platform.events"
        };
    }

    private int port(ConfigurableApplicationContext context) {
        return context.getEnvironment().getRequiredProperty("local.server.port", Integer.class);
    }

    private void postWorkload(int port) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/workloads/request-count-total"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("""
                        {"entityType":"service","entityId":"catalog-api","values":[100,300,50]}
                        """))
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        assertEquals(202, response.statusCode());
    }

    private long readFeatureValue(int port) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + port + "/features/service/catalog-api/request_count_total"))
                .GET()
                .build();
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            return -1;
        }

        return objectMapper.readTree(response.body()).path("value").asLong(-1);
    }
}

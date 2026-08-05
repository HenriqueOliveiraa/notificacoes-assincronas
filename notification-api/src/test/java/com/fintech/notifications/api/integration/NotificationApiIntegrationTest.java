package com.fintech.notifications.api.integration;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class NotificationApiIntegrationTest {

    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("channels_db");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    private TestRestTemplate rest;

    @Test
    void createChannelAndDispatchAlertPublishesToKafka() throws Exception {
        Map<String, Object> channel = Map.of(
                "name", "Fatura Disponível",
                "type", "EMAIL",
                "template", "Olá {{clientName}}, sua fatura de {{billingMonth}} está disponível.",
                "config", Map.of("maxRetries", 3, "priority", "HIGH"),
                "active", true);

        ResponseEntity<JsonNode> created = rest.postForEntity("/channels", channel, JsonNode.class);
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String channelId = created.getBody().get("id").asText();
        Map<String, Object> dispatch = Map.of(
                "clientId", 12345,
                "params", Map.of("clientName", "João Silva", "billingMonth", "Novembro/2025"));

        ResponseEntity<JsonNode> accepted =
                rest.postForEntity("/channels/" + channelId + "/alerts", dispatch, JsonNode.class);

        assertThat(accepted.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        String correlationId = accepted.getBody().get("correlationId").asText();
        assertThat(correlationId).isNotBlank();
        ConsumerRecord<String, String> record = pollSingleAlert();
        assertThat(record.key()).isEqualTo(correlationId);
        assertThat(record.value())
                .contains("João Silva")
                .contains("Novembro/2025")
                .contains(correlationId);
    }

    @Test
    void dispatchToInactiveChannelReturnsUnprocessable() {
        Map<String, Object> channel = Map.of(
                "name", "Canal Inativo",
                "type", "SMS",
                "template", "Oi {{name}}",
                "config", Map.of(),
                "active", false);
        ResponseEntity<JsonNode> created = rest.postForEntity("/channels", channel, JsonNode.class);
        String channelId = created.getBody().get("id").asText();

        ResponseEntity<JsonNode> resp = rest.postForEntity(
                "/channels/" + channelId + "/alerts",
                Map.of("clientId", 1, "params", Map.of("name", "Ana")), JsonNode.class);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(resp.getBody().get("error").asText()).isEqualTo("CHANNEL_INACTIVE");
    }

    private ConsumerRecord<String, String> pollSingleAlert() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-verifier");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("notifications.alerts"));
            long deadline = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (!records.isEmpty()) {
                    return records.iterator().next();
                }
            }
            throw new AssertionError("Nenhuma mensagem recebida no tópico notifications.alerts");
        }
    }
}

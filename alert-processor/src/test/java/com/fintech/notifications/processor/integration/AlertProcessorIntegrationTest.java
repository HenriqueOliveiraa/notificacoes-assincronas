package com.fintech.notifications.processor.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fintech.notifications.processor.domain.EventStatus;
import com.fintech.notifications.processor.domain.NotificationEvent;
import com.fintech.notifications.processor.repository.NotificationEventRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
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
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class AlertProcessorIntegrationTest {

    @Container
    static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.4")
            .withDatabaseName("events_db");

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", MYSQL::getJdbcUrl);
        registry.add("spring.datasource.username", MYSQL::getUsername);
        registry.add("spring.datasource.password", MYSQL::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("app.kafka.retry.max-attempts", () -> "1");
        registry.add("app.kafka.retry.initial-interval-ms", () -> "200");
    }

    @Autowired
    private NotificationEventRepository repository;
    @Autowired
    private TestRestTemplate rest;

    private static final String TOPIC = "notifications.alerts";
    private static final String DLT = "notifications.alerts.DLT";

    private String alertJson(UUID correlationId, String channelType) {
        return """
                {
                  "correlationId": "%s",
                  "channelId": "%s",
                  "channelType": "%s",
                  "clientId": 12345,
                  "message": "Olá João Silva, sua fatura está disponível.",
                  "config": { "maxRetries": 3, "priority": "HIGH" },
                  "createdAt": "2025-11-10T14:30:00Z"
                }
                """.formatted(correlationId, UUID.randomUUID(), channelType);
    }

    private void send(String key, String value) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            producer.send(new ProducerRecord<>(TOPIC, key, value));
            producer.flush();
        }
    }

    @Test
    void processesAlertAndPersistsFullLifecycle() {
        UUID correlationId = UUID.randomUUID();
        send(correlationId.toString(), alertJson(correlationId, "EMAIL"));

        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
            List<NotificationEvent> events =
                    repository.findByCorrelationIdOrderBySeqAsc(correlationId.toString());
            assertThat(events).extracting(NotificationEvent::getStatus)
                    .containsExactly(EventStatus.RECEBIDO, EventStatus.PROCESSANDO, EventStatus.PROCESSADO);
        });

        ResponseEntity<JsonNode> status =
                rest.getForEntity("/alerts/" + correlationId + "/status", JsonNode.class);
        assertThat(status.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(status.getBody().get("currentStatus").asText()).isEqualTo("PROCESSADO");
        assertThat(status.getBody().get("events")).hasSize(3);
    }

    @Test
    void consumeIsIdempotentOnRedelivery() {
        UUID correlationId = UUID.randomUUID();
        String json = alertJson(correlationId, "SMS");

        send(correlationId.toString(), json);
        await().atMost(30, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(repository.findByCorrelationIdOrderBySeqAsc(correlationId.toString()))
                        .hasSize(3));
        send(correlationId.toString(), json);
        await().during(5, TimeUnit.SECONDS).atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(repository.findByCorrelationIdOrderBySeqAsc(correlationId.toString()))
                        .hasSize(3));
    }

    @Test
    void malformedMessageIsRoutedToDeadLetterTopic() {
        String key = UUID.randomUUID().toString();
        send(key, "{ isso não é um AlertMessage válido }");

        try (KafkaConsumer<String, String> dltConsumer = dltConsumer()) {
            dltConsumer.subscribe(List.of(DLT));
            await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> {
                ConsumerRecords<String, String> records = dltConsumer.poll(Duration.ofMillis(500));
                assertThat(records.count()).isGreaterThanOrEqualTo(1);
            });
        }
    }

    private KafkaConsumer<String, String> dltConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-verifier-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }
}

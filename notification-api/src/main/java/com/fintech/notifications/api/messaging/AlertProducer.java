package com.fintech.notifications.api.messaging;

import com.fintech.notifications.api.exception.AlertPublishException;
import com.fintech.notifications.contract.AlertMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class AlertProducer {

    private final KafkaTemplate<String, AlertMessage> kafkaTemplate;
    private final String topic;
    private final long publishTimeoutMs;

    public AlertProducer(KafkaTemplate<String, AlertMessage> kafkaTemplate,
                         @Value("${app.kafka.topics.alerts}") String topic,
                         @Value("${app.kafka.publish-timeout-ms:5000}") long publishTimeoutMs) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.publishTimeoutMs = publishTimeoutMs;
    }

    public void publish(AlertMessage alert) {
        String correlationId = alert.correlationId().toString();

        Message<AlertMessage> message = MessageBuilder
                .withPayload(alert)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader(KafkaHeaders.KEY, correlationId)
                .setHeader(AlertMessage.CORRELATION_ID_HEADER,
                        correlationId.getBytes(StandardCharsets.UTF_8))
                .build();

        try {
            var result = kafkaTemplate.send(message).get(publishTimeoutMs, TimeUnit.MILLISECONDS);
            log.info("Alerta publicado no tópico {} partition={} offset={} channelType={} clientId={}",
                    topic,
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset(),
                    alert.channelType(),
                    alert.clientId());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AlertPublishException("Publicação interrompida", e);
        } catch (Exception e) {
            log.error("Falha ao publicar alerta no tópico {}: {}", topic, e.getMessage());
            throw new AlertPublishException("Não foi possível confirmar a publicação do alerta", e);
        }
    }
}

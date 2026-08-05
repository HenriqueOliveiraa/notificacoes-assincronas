package com.fintech.notifications.processor.messaging;

import com.fintech.notifications.contract.AlertMessage;
import com.fintech.notifications.processor.service.NotificationProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class AlertConsumer {

    private final NotificationProcessingService processingService;

    @KafkaListener(
            topics = "${app.kafka.topics.alerts}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory")
    public void consume(AlertMessage message) {
        MDC.put(AlertMessage.CORRELATION_ID_HEADER, message.correlationId().toString());
        try {
            log.info("Mensagem recebida channelType={} clientId={}",
                    message.channelType(), message.clientId());
            processingService.process(message);
        } finally {
            MDC.remove(AlertMessage.CORRELATION_ID_HEADER);
        }
    }
}

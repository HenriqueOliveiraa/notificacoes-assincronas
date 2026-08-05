package com.fintech.notifications.processor.messaging;

import com.fintech.notifications.contract.AlertMessage;
import com.fintech.notifications.processor.service.NotificationProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class FailureRecordingRecoverer implements ConsumerRecordRecoverer {

    private final NotificationProcessingService processingService;
    private final DeadLetterPublishingRecoverer deadLetterRecoverer;

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception exception) {
        Object value = record.value();
        if (value instanceof AlertMessage alert) {
            MDC.put(AlertMessage.CORRELATION_ID_HEADER, alert.correlationId().toString());
            try {
                log.error("Tentativas esgotadas — registrando FALHA e enviando para a DLQ. Motivo: {}",
                        rootMessage(exception));
                processingService.recordFailure(alert, rootMessage(exception));
            } finally {
                MDC.remove(AlertMessage.CORRELATION_ID_HEADER);
            }
        } else {
            log.error("Mensagem não desserializável enviada para a DLQ (offset={}, partition={})",
                    record.offset(), record.partition());
        }
        deadLetterRecoverer.accept(record, exception);
    }

    private String rootMessage(Throwable ex) {
        Throwable cause = ex;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause.getClass().getSimpleName() + ": " + cause.getMessage();
    }
}

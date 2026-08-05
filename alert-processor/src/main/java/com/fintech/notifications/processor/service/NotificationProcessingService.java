package com.fintech.notifications.processor.service;

import com.fintech.notifications.contract.AlertMessage;
import com.fintech.notifications.processor.domain.EventStatus;
import com.fintech.notifications.processor.strategy.AlertStrategy;
import com.fintech.notifications.processor.strategy.AlertStrategyResolver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationProcessingService {

    private final EventStore eventStore;
    private final AlertStrategyResolver strategyResolver;

    public void process(AlertMessage message) {
        if (eventStore.isAlreadyProcessed(message.correlationId())) {
            log.info("Alerta já processado anteriormente — ignorando reentrega (idempotência)");
            return;
        }

        eventStore.append(message.correlationId(), message.channelType(), message.clientId(),
                EventStatus.RECEBIDO, "Mensagem consumida do tópico");

        eventStore.append(message.correlationId(), message.channelType(), message.clientId(),
                EventStatus.PROCESSANDO,
                "Iniciando processamento via estratégia " + message.channelType());

        AlertStrategy strategy = strategyResolver.resolve(message.channelType());
        strategy.process(message);

        eventStore.append(message.correlationId(), message.channelType(), message.clientId(),
                EventStatus.PROCESSADO, "Processamento concluído com sucesso");

        log.info("Alerta processado com sucesso channelType={} clientId={}",
                message.channelType(), message.clientId());
    }

    public void recordFailure(AlertMessage message, String reason) {
        eventStore.append(message.correlationId(), message.channelType(), message.clientId(),
                EventStatus.FALHA, "Processamento falhou após esgotar as tentativas: " + reason);
    }
}

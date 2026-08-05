package com.fintech.notifications.processor.strategy;

import com.fintech.notifications.contract.AlertMessage;
import com.fintech.notifications.contract.ChannelType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class PushAlertStrategy implements AlertStrategy {

    @Override
    public ChannelType type() {
        return ChannelType.PUSH;
    }

    @Override
    public void process(AlertMessage message) {
        log.info("[PUSH] Enviando push para clientId={} conteudo=\"{}\"",
                message.clientId(), message.message());
    }
}

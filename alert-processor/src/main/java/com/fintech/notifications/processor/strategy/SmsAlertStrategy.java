package com.fintech.notifications.processor.strategy;

import com.fintech.notifications.contract.AlertMessage;
import com.fintech.notifications.contract.ChannelType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class SmsAlertStrategy implements AlertStrategy {

    @Override
    public ChannelType type() {
        return ChannelType.SMS;
    }

    @Override
    public void process(AlertMessage message) {
        log.info("[SMS] Enviando SMS para clientId={} conteudo=\"{}\"",
                message.clientId(), message.message());
    }
}

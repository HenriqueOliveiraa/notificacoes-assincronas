package com.fintech.notifications.api.service;

import com.fintech.notifications.api.domain.Channel;
import com.fintech.notifications.api.domain.ChannelConfig;
import com.fintech.notifications.api.exception.ChannelInactiveException;
import com.fintech.notifications.api.messaging.AlertProducer;
import com.fintech.notifications.api.web.dto.DispatchAlertRequest;
import com.fintech.notifications.contract.AlertConfig;
import com.fintech.notifications.contract.AlertMessage;
import lombok.RequiredArgsConstructor;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class AlertDispatchService {

    private final ChannelService channelService;
    private final TemplateResolver templateResolver;
    private final AlertProducer alertProducer;
    private final Clock clock;

    public UUID dispatch(String channelId, DispatchAlertRequest request) {
        Channel channel = channelService.findById(channelId);
        if (!channel.isActive()) {
            throw new ChannelInactiveException(channelId);
        }

        String resolvedMessage = templateResolver.resolve(channel.getTemplate(), request.paramsOrEmpty());

        UUID correlationId = UUID.randomUUID();
        String previous = MDC.get(AlertMessage.CORRELATION_ID_HEADER);
        MDC.put(AlertMessage.CORRELATION_ID_HEADER, correlationId.toString());
        try {
            AlertMessage alert = new AlertMessage(
                    correlationId,
                    channel.getUuid(),
                    channel.getType(),
                    request.clientId(),
                    resolvedMessage,
                    toAlertConfig(channel.getConfig()),
                    Instant.now(clock)
            );
            alertProducer.publish(alert);
            return correlationId;
        } finally {
            if (previous != null) {
                MDC.put(AlertMessage.CORRELATION_ID_HEADER, previous);
            } else {
                MDC.remove(AlertMessage.CORRELATION_ID_HEADER);
            }
        }
    }

    private AlertConfig toAlertConfig(ChannelConfig config) {
        Integer maxRetries = config != null ? config.getMaxRetries() : null;
        return new AlertConfig(
                maxRetries,
                config != null ? config.getPriority() : null
        );
    }
}

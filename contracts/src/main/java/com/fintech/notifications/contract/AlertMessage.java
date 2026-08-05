package com.fintech.notifications.contract;

import java.time.Instant;
import java.util.UUID;

public record AlertMessage(
        UUID correlationId,
        UUID channelId,
        ChannelType channelType,
        long clientId,
        String message,
        AlertConfig config,
        Instant createdAt
) {
    public static final String CORRELATION_ID_HEADER = "correlationId";
}

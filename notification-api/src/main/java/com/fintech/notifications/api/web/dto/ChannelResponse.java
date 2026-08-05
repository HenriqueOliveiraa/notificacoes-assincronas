package com.fintech.notifications.api.web.dto;

import com.fintech.notifications.api.domain.Channel;
import com.fintech.notifications.api.domain.ChannelConfig;
import com.fintech.notifications.contract.ChannelType;

import java.time.Instant;

public record ChannelResponse(
        String id,
        String name,
        ChannelType type,
        String template,
        ChannelConfig config,
        boolean active,
        Instant createdAt,
        Instant updatedAt
) {
    public static ChannelResponse from(Channel c) {
        return new ChannelResponse(
                c.getId(),
                c.getName(),
                c.getType(),
                c.getTemplate(),
                c.getConfig(),
                c.isActive(),
                c.getCreatedAt(),
                c.getUpdatedAt()
        );
    }
}

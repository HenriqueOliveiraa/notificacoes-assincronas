package com.fintech.notifications.processor.web.dto;

import com.fintech.notifications.contract.ChannelType;
import com.fintech.notifications.processor.domain.EventStatus;
import com.fintech.notifications.processor.domain.NotificationEvent;

import java.time.Instant;
import java.util.List;

public record AlertStatusResponse(
        String correlationId,
        ChannelType channelType,
        long clientId,
        EventStatus currentStatus,
        List<EventView> events
) {
    public record EventView(EventStatus status, String detail, Instant occurredAt) {
        static EventView from(NotificationEvent e) {
            return new EventView(e.getStatus(), e.getDetail(), e.getOccurredAt());
        }
    }

    public static AlertStatusResponse from(List<NotificationEvent> ordered) {
        NotificationEvent first = ordered.get(0);
        NotificationEvent last = ordered.get(ordered.size() - 1);
        return new AlertStatusResponse(
                first.getCorrelationId(),
                first.getChannelType(),
                first.getClientId(),
                last.getStatus(),
                ordered.stream().map(EventView::from).toList()
        );
    }
}

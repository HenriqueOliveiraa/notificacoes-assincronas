package com.fintech.notifications.processor.domain;

import com.fintech.notifications.contract.ChannelType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.data.domain.Persistable;

import java.time.Instant;
import java.util.UUID;


@Entity
@Table(name = "notification_events")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class NotificationEvent implements Persistable<String> {

    @Id
    @Column(length = 36, nullable = false, updatable = false)
    private String id;

    @Column(insertable = false, updatable = false)
    private Long seq;

    @Column(name = "correlation_id", length = 36, nullable = false, updatable = false)
    private String correlationId;

    @Enumerated(EnumType.STRING)
    @Column(name = "channel_type", length = 20, nullable = false, updatable = false)
    private ChannelType channelType;

    @Column(name = "client_id", nullable = false, updatable = false)
    private long clientId;

    @Enumerated(EnumType.STRING)
    @Column(length = 20, nullable = false, updatable = false)
    private EventStatus status;

    @Column(columnDefinition = "TEXT", updatable = false)
    private String detail;

    @Column(name = "occurred_at", nullable = false, updatable = false)
    private Instant occurredAt;

    @Transient
    private boolean isNew = false;

    public static NotificationEvent of(UUID correlationId, ChannelType channelType, long clientId,
                                       EventStatus status, String detail, Instant occurredAt) {
        NotificationEvent e = new NotificationEvent();
        e.id = UUID.randomUUID().toString();
        e.correlationId = correlationId.toString();
        e.channelType = channelType;
        e.clientId = clientId;
        e.status = status;
        e.detail = detail;
        e.occurredAt = occurredAt;
        e.isNew = true;
        return e;
    }

    @Override
    public boolean isNew() {
        return isNew;
    }
}

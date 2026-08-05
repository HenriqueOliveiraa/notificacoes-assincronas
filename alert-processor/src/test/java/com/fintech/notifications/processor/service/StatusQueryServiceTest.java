package com.fintech.notifications.processor.service;

import com.fintech.notifications.contract.ChannelType;
import com.fintech.notifications.processor.domain.EventStatus;
import com.fintech.notifications.processor.domain.NotificationEvent;
import com.fintech.notifications.processor.exception.AlertNotFoundException;
import com.fintech.notifications.processor.repository.NotificationEventRepository;
import com.fintech.notifications.processor.web.dto.AlertStatusResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StatusQueryServiceTest {

    @Mock
    private NotificationEventRepository repository;

    private StatusQueryService service;

    @BeforeEach
    void setUp() {
        service = new StatusQueryService(repository);
    }

    @Test
    void returnsCurrentStatusFromLastEvent() {
        UUID correlationId = UUID.randomUUID();
        Instant t0 = Instant.parse("2025-11-10T14:30:01Z");
        List<NotificationEvent> events = List.of(
                NotificationEvent.of(correlationId, ChannelType.EMAIL, 12345L, EventStatus.RECEBIDO, "a", t0),
                NotificationEvent.of(correlationId, ChannelType.EMAIL, 12345L, EventStatus.PROCESSANDO, "b", t0.plusSeconds(1)),
                NotificationEvent.of(correlationId, ChannelType.EMAIL, 12345L, EventStatus.PROCESSADO, "c", t0.plusSeconds(2)));
        when(repository.findByCorrelationIdOrderBySeqAsc(correlationId.toString())).thenReturn(events);

        AlertStatusResponse response = service.getStatus(correlationId.toString());

        assertThat(response.currentStatus()).isEqualTo(EventStatus.PROCESSADO);
        assertThat(response.channelType()).isEqualTo(ChannelType.EMAIL);
        assertThat(response.clientId()).isEqualTo(12345L);
        assertThat(response.events()).hasSize(3);
    }

    @Test
    void throwsWhenNoEvents() {
        when(repository.findByCorrelationIdOrderBySeqAsc("x")).thenReturn(List.of());

        assertThatThrownBy(() -> service.getStatus("x"))
                .isInstanceOf(AlertNotFoundException.class);
    }
}

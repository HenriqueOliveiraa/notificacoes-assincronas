package com.fintech.notifications.api.service;

import com.fintech.notifications.api.domain.Channel;
import com.fintech.notifications.api.domain.ChannelConfig;
import com.fintech.notifications.api.exception.ChannelInactiveException;
import com.fintech.notifications.api.messaging.AlertProducer;
import com.fintech.notifications.api.web.dto.DispatchAlertRequest;
import com.fintech.notifications.contract.AlertMessage;
import com.fintech.notifications.contract.ChannelType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AlertDispatchServiceTest {

    @Mock
    private ChannelService channelService;
    @Mock
    private AlertProducer alertProducer;

    private final TemplateResolver templateResolver = new TemplateResolver();
    private final Clock clock = Clock.fixed(Instant.parse("2025-11-10T14:30:00Z"), ZoneOffset.UTC);

    private AlertDispatchService service;

    @BeforeEach
    void setUp() {
        service = new AlertDispatchService(channelService, templateResolver, alertProducer, clock);
    }

    private Channel activeEmailChannel() {
        ChannelConfig config = new ChannelConfig();
        config.setMaxRetries(3);
        return Channel.create("Fatura", ChannelType.EMAIL,
                "Olá {{clientName}}, fatura de {{billingMonth}}.", config, true,
                Instant.parse("2025-11-10T14:30:00Z"));
    }

    @Test
    void dispatchResolvesTemplateAndPublishes() {
        Channel channel = activeEmailChannel();
        when(channelService.findById(channel.getId())).thenReturn(channel);

        DispatchAlertRequest request = new DispatchAlertRequest(
                12345L, Map.of("clientName", "João Silva", "billingMonth", "Novembro/2025"));

        UUID correlationId = service.dispatch(channel.getId(), request);

        assertThat(correlationId).isNotNull();

        ArgumentCaptor<AlertMessage> captor = ArgumentCaptor.forClass(AlertMessage.class);
        verify(alertProducer).publish(captor.capture());
        AlertMessage published = captor.getValue();

        assertThat(published.correlationId()).isEqualTo(correlationId);
        assertThat(published.channelType()).isEqualTo(ChannelType.EMAIL);
        assertThat(published.clientId()).isEqualTo(12345L);
        assertThat(published.message())
                .isEqualTo("Olá João Silva, fatura de Novembro/2025.");
        assertThat(published.config().maxRetriesOrDefault()).isEqualTo(3);
    }

    @Test
    void dispatchRejectsInactiveChannel() {
        Channel channel = Channel.create("Fatura", ChannelType.EMAIL, "Olá", new ChannelConfig(),
                false, Instant.parse("2025-11-10T14:30:00Z"));
        when(channelService.findById(channel.getId())).thenReturn(channel);

        assertThatThrownBy(() -> service.dispatch(channel.getId(),
                new DispatchAlertRequest(1L, Map.of())))
                .isInstanceOf(ChannelInactiveException.class);

        verify(alertProducer, never()).publish(org.mockito.ArgumentMatchers.any());
    }
}

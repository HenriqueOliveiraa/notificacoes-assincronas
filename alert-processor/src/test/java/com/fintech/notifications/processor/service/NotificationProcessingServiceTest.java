package com.fintech.notifications.processor.service;

import com.fintech.notifications.contract.AlertConfig;
import com.fintech.notifications.contract.AlertMessage;
import com.fintech.notifications.contract.ChannelType;
import com.fintech.notifications.contract.Priority;
import com.fintech.notifications.processor.domain.EventStatus;
import com.fintech.notifications.processor.strategy.AlertStrategy;
import com.fintech.notifications.processor.strategy.AlertStrategyResolver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NotificationProcessingServiceTest {

    @Mock
    private EventStore eventStore;
    @Mock
    private AlertStrategyResolver strategyResolver;
    @Mock
    private AlertStrategy strategy;

    private NotificationProcessingService service;

    @BeforeEach
    void setUp() {
        service = new NotificationProcessingService(eventStore, strategyResolver);
    }

    private AlertMessage message() {
        return new AlertMessage(UUID.randomUUID(), UUID.randomUUID(), ChannelType.EMAIL, 12345L,
                "Olá João", new AlertConfig(3, Priority.HIGH), Instant.parse("2025-11-10T14:30:00Z"));
    }

    @Test
    void happyPathAppendsLifecycleAndInvokesStrategy() {
        AlertMessage msg = message();
        when(eventStore.isAlreadyProcessed(msg.correlationId())).thenReturn(false);
        when(strategyResolver.resolve(ChannelType.EMAIL)).thenReturn(strategy);

        service.process(msg);

        verify(eventStore).append(eq(msg.correlationId()), eq(ChannelType.EMAIL), eq(12345L),
                eq(EventStatus.RECEBIDO), any());
        verify(eventStore).append(eq(msg.correlationId()), eq(ChannelType.EMAIL), eq(12345L),
                eq(EventStatus.PROCESSANDO), any());
        verify(strategy).process(msg);
        verify(eventStore).append(eq(msg.correlationId()), eq(ChannelType.EMAIL), eq(12345L),
                eq(EventStatus.PROCESSADO), any());
    }

    @Test
    void skipsWhenAlreadyProcessed() {
        AlertMessage msg = message();
        when(eventStore.isAlreadyProcessed(msg.correlationId())).thenReturn(true);

        service.process(msg);

        verify(eventStore, never()).append(any(), any(), org.mockito.ArgumentMatchers.anyLong(), any(), any());
        verifyNoInteractions(strategyResolver, strategy);
    }

    @Test
    void propagatesStrategyFailureWithoutAppendingProcessado() {
        AlertMessage msg = message();
        when(eventStore.isAlreadyProcessed(msg.correlationId())).thenReturn(false);
        when(strategyResolver.resolve(ChannelType.EMAIL)).thenReturn(strategy);
        org.mockito.Mockito.doThrow(new RuntimeException("gateway down")).when(strategy).process(msg);

        try {
            service.process(msg);
            org.junit.jupiter.api.Assertions.fail("esperava exceção");
        } catch (RuntimeException expected) {
        }

        verify(eventStore, never()).append(any(), any(), org.mockito.ArgumentMatchers.anyLong(),
                eq(EventStatus.PROCESSADO), any());
    }

    @Test
    void recordFailureAppendsFalhaEvent() {
        AlertMessage msg = message();

        service.recordFailure(msg, "gateway down");

        verify(eventStore).append(eq(msg.correlationId()), eq(ChannelType.EMAIL), eq(12345L),
                eq(EventStatus.FALHA), any());
    }
}

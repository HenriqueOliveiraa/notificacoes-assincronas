package com.fintech.notifications.processor.service;

import com.fintech.notifications.contract.ChannelType;
import com.fintech.notifications.processor.domain.EventStatus;
import com.fintech.notifications.processor.domain.NotificationEvent;
import com.fintech.notifications.processor.repository.NotificationEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class EventStore {

    private final NotificationEventRepository repository;
    private final Clock clock;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void append(UUID correlationId, ChannelType channelType, long clientId,
                       EventStatus status, String detail) {
        if (repository.existsByCorrelationIdAndStatus(correlationId.toString(), status)) {
            log.debug("Evento {} já registrado para correlationId={} — ignorando (idempotência)",
                    status, correlationId);
            return;
        }
        try {
            NotificationEvent event = NotificationEvent.of(
                    correlationId, channelType, clientId, status, detail, Instant.now(clock));
            repository.saveAndFlush(event);
        } catch (DataIntegrityViolationException e) {
            log.debug("Evento {} duplicado para correlationId={} — ignorando", status, correlationId);
        }
    }

    @Transactional(readOnly = true)
    public boolean isAlreadyProcessed(UUID correlationId) {
        return repository.existsByCorrelationIdAndStatus(correlationId.toString(), EventStatus.PROCESSADO);
    }
}

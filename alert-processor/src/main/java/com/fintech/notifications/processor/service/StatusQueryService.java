package com.fintech.notifications.processor.service;

import com.fintech.notifications.processor.domain.NotificationEvent;
import com.fintech.notifications.processor.exception.AlertNotFoundException;
import com.fintech.notifications.processor.repository.NotificationEventRepository;
import com.fintech.notifications.processor.web.dto.AlertStatusResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class StatusQueryService {

    private final NotificationEventRepository repository;

    @Transactional(readOnly = true)
    public AlertStatusResponse getStatus(String correlationId) {
        List<NotificationEvent> events =
                repository.findByCorrelationIdOrderBySeqAsc(correlationId);
        if (events.isEmpty()) {
            throw new AlertNotFoundException(correlationId);
        }
        return AlertStatusResponse.from(events);
    }
}

package com.fintech.notifications.processor.repository;

import com.fintech.notifications.processor.domain.EventStatus;
import com.fintech.notifications.processor.domain.NotificationEvent;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface NotificationEventRepository extends JpaRepository<NotificationEvent, String> {

    List<NotificationEvent> findByCorrelationIdOrderBySeqAsc(String correlationId);

    boolean existsByCorrelationIdAndStatus(String correlationId, EventStatus status);
}

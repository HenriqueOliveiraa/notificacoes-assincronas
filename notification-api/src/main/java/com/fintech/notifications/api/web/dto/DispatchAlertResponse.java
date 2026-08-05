package com.fintech.notifications.api.web.dto;

import java.util.UUID;

public record DispatchAlertResponse(
        UUID correlationId,
        String status
) {
    public static DispatchAlertResponse accepted(UUID correlationId) {
        return new DispatchAlertResponse(correlationId, "ACCEPTED");
    }
}

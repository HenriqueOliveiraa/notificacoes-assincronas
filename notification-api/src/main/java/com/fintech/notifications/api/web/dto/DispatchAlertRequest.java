package com.fintech.notifications.api.web.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.util.Map;

public record DispatchAlertRequest(

        @NotNull(message = "clientId é obrigatório")
        @Positive(message = "clientId deve ser positivo")
        Long clientId,

        Map<String, Object> params
) {
    public Map<String, Object> paramsOrEmpty() {
        return params != null ? params : Map.of();
    }
}

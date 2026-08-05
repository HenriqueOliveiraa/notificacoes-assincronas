package com.fintech.notifications.processor.exception;

public class AlertNotFoundException extends RuntimeException {
    public AlertNotFoundException(String correlationId) {
        super("Nenhum alerta encontrado para o correlationId: " + correlationId);
    }
}

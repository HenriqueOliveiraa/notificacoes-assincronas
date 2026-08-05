package com.fintech.notifications.api.exception;

import java.util.List;

public class TemplateResolutionException extends RuntimeException {

    private final List<String> missingParams;

    public TemplateResolutionException(List<String> missingParams) {
        super("Parâmetros obrigatórios ausentes para resolver o template: " + missingParams);
        this.missingParams = missingParams;
    }

    public List<String> getMissingParams() {
        return missingParams;
    }
}

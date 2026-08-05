package com.fintech.notifications.api.exception;

public class ChannelNotFoundException extends RuntimeException {
    public ChannelNotFoundException(String id) {
        super("Canal não encontrado: " + id);
    }
}

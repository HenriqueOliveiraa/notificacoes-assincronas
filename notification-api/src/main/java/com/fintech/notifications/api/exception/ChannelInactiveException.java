package com.fintech.notifications.api.exception;

public class ChannelInactiveException extends RuntimeException {
    public ChannelInactiveException(String id) {
        super("Canal está inativo e não aceita disparos: " + id);
    }
}

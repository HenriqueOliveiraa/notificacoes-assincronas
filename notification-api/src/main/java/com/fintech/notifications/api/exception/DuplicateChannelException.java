package com.fintech.notifications.api.exception;

import com.fintech.notifications.contract.ChannelType;

public class DuplicateChannelException extends RuntimeException {
    public DuplicateChannelException(String name, ChannelType type) {
        super("Já existe um canal ativo com name='" + name + "' e type='" + type + "'");
    }
}

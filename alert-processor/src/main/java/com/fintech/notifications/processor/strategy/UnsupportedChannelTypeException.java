package com.fintech.notifications.processor.strategy;

import com.fintech.notifications.contract.ChannelType;

public class UnsupportedChannelTypeException extends RuntimeException {
    public UnsupportedChannelTypeException(ChannelType type) {
        super("Nenhuma estratégia de processamento registrada para o tipo: " + type);
    }
}

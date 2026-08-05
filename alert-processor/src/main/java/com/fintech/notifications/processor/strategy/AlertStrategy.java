package com.fintech.notifications.processor.strategy;

import com.fintech.notifications.contract.AlertMessage;
import com.fintech.notifications.contract.ChannelType;

public interface AlertStrategy {

    ChannelType type();

    void process(AlertMessage message);
}

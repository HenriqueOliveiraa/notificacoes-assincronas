package com.fintech.notifications.processor.strategy;

import com.fintech.notifications.contract.ChannelType;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Component
public class AlertStrategyResolver {

    private final Map<ChannelType, AlertStrategy> strategies;

    public AlertStrategyResolver(List<AlertStrategy> strategyList) {
        this.strategies = strategyList.stream()
                .collect(Collectors.toUnmodifiableMap(AlertStrategy::type, Function.identity()));
    }

    public AlertStrategy resolve(ChannelType type) {
        AlertStrategy strategy = strategies.get(type);
        if (strategy == null) {
            throw new UnsupportedChannelTypeException(type);
        }
        return strategy;
    }
}

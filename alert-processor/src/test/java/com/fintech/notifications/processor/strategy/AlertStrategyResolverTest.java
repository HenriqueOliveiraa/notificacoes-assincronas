package com.fintech.notifications.processor.strategy;

import com.fintech.notifications.contract.AlertMessage;
import com.fintech.notifications.contract.ChannelType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AlertStrategyResolverTest {

    private static class FakeStrategy implements AlertStrategy {
        private final ChannelType type;

        FakeStrategy(ChannelType type) {
            this.type = type;
        }

        @Override
        public ChannelType type() {
            return type;
        }

        @Override
        public void process(AlertMessage message) {
        }
    }

    @Test
    void resolvesRegisteredStrategyByType() {
        AlertStrategy email = new FakeStrategy(ChannelType.EMAIL);
        AlertStrategy sms = new FakeStrategy(ChannelType.SMS);
        AlertStrategyResolver resolver = new AlertStrategyResolver(List.of(email, sms));

        assertThat(resolver.resolve(ChannelType.EMAIL)).isSameAs(email);
        assertThat(resolver.resolve(ChannelType.SMS)).isSameAs(sms);
    }

    @Test
    void throwsForUnregisteredType() {
        AlertStrategyResolver resolver = new AlertStrategyResolver(List.of(new FakeStrategy(ChannelType.EMAIL)));

        assertThatThrownBy(() -> resolver.resolve(ChannelType.PUSH))
                .isInstanceOf(UnsupportedChannelTypeException.class);
    }
}

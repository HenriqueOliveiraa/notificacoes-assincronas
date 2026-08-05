package com.fintech.notifications.api.web.dto;

import com.fintech.notifications.api.domain.ChannelConfig;
import com.fintech.notifications.contract.ChannelType;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

public record ChannelRequest(

        @NotBlank(message = "name é obrigatório")
        @Size(max = 150, message = "name deve ter no máximo 150 caracteres")
        String name,

        @NotNull(message = "type é obrigatório (EMAIL, SMS ou PUSH)")
        ChannelType type,

        @NotBlank(message = "template é obrigatório")
        String template,

        @Valid
        ChannelConfig config,

        Boolean active
) {
    public boolean activeOrDefault() {
        return active == null || active;
    }

    public ChannelConfig configOrDefault() {
        return config != null ? config : new ChannelConfig();
    }
}

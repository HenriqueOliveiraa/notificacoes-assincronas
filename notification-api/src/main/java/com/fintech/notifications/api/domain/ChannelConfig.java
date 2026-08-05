package com.fintech.notifications.api.domain;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fintech.notifications.contract.Priority;
import jakarta.validation.constraints.Positive;
import lombok.Getter;
import lombok.Setter;

import java.util.LinkedHashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Getter
@Setter
public class ChannelConfig {

    @Positive
    private Integer maxRetries;

    private Priority priority;
    private final Map<String, Object> additional = new LinkedHashMap<>();

    @JsonAnyGetter
    public Map<String, Object> getAdditional() {
        return additional;
    }

    @JsonAnySetter
    public void addAdditional(String key, Object value) {
        this.additional.put(key, value);
    }
}

package com.fintech.notifications.contract;

public record AlertConfig(
        Integer maxRetries,
        Priority priority
) {
    public static final int DEFAULT_MAX_RETRIES = 3;

    public int maxRetriesOrDefault() {
        return maxRetries != null ? maxRetries : DEFAULT_MAX_RETRIES;
    }
}

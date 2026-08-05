CREATE TABLE notification_events (
    id             CHAR(36)     NOT NULL,
    seq            BIGINT       NOT NULL AUTO_INCREMENT,
    correlation_id CHAR(36)     NOT NULL,
    channel_type   VARCHAR(20)  NOT NULL,
    client_id      BIGINT       NOT NULL,
    status         VARCHAR(20)  NOT NULL,
    detail         TEXT,
    occurred_at    DATETIME(3)  NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_events_seq (seq),
    UNIQUE KEY uk_correlation_status (correlation_id, status),
    INDEX idx_correlation (correlation_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;

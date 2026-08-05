CREATE TABLE channels (
    id           CHAR(36)     NOT NULL,
    name         VARCHAR(150) NOT NULL,
    type         VARCHAR(20)  NOT NULL,
    template     TEXT         NOT NULL,
    config       JSON         NOT NULL,
    active       BOOLEAN      NOT NULL DEFAULT TRUE,
    deleted      BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at   DATETIME(3)  NOT NULL,
    updated_at   DATETIME(3)  NOT NULL,
    active_scope CHAR(36) GENERATED ALWAYS AS (IF(deleted, id, 'ACTIVE')) STORED,
    PRIMARY KEY (id),
    UNIQUE KEY uk_channels_name_type_active (name, type, active_scope),
    INDEX idx_channels_deleted (deleted)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4;

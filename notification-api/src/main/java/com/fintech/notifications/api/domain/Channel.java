package com.fintech.notifications.api.domain;

import com.fintech.notifications.contract.ChannelType;
import jakarta.persistence.Column;
import jakarta.persistence.Convert;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.data.domain.Persistable;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "channels")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED) // exigido pelo JPA
public class Channel implements Persistable<String> {

    @Id
    @Column(length = 36, nullable = false, updatable = false)
    private String id;

    @Column(nullable = false, length = 150)
    private String name;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private ChannelType type;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String template;

    @Convert(converter = ChannelConfigConverter.class)
    @Column(nullable = false, columnDefinition = "JSON")
    private ChannelConfig config = new ChannelConfig();

    @Column(nullable = false)
    private boolean active = true;

    @Column(nullable = false)
    private boolean deleted = false;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Transient
    private boolean isNew = false;

    public static Channel create(String name, ChannelType type, String template,
                                 ChannelConfig config, boolean active, Instant now) {
        Channel c = new Channel();
        c.id = UUID.randomUUID().toString();
        c.name = name;
        c.type = type;
        c.template = template;
        c.config = config != null ? config : new ChannelConfig();
        c.active = active;
        c.deleted = false;
        c.createdAt = now;
        c.updatedAt = now;
        c.isNew = true;
        return c;
    }

    public void update(String name, ChannelType type, String template,
                       ChannelConfig config, boolean active, Instant now) {
        this.name = name;
        this.type = type;
        this.template = template;
        this.config = config != null ? config : new ChannelConfig();
        this.active = active;
        this.updatedAt = now;
    }

    public void markDeleted(Instant now) {
        this.deleted = true;
        this.active = false;
        this.updatedAt = now;
    }

    public UUID getUuid() {
        return UUID.fromString(id);
    }

    @Override
    public boolean isNew() {
        return isNew;
    }
}

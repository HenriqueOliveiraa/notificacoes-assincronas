package com.fintech.notifications.api.service;

import com.fintech.notifications.api.domain.Channel;
import com.fintech.notifications.api.domain.ChannelConfig;
import com.fintech.notifications.api.exception.ChannelNotFoundException;
import com.fintech.notifications.api.exception.DuplicateChannelException;
import com.fintech.notifications.api.repository.ChannelRepository;
import com.fintech.notifications.api.web.dto.ChannelRequest;
import com.fintech.notifications.contract.ChannelType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ChannelServiceTest {

    @Mock
    private ChannelRepository repository;

    private final Clock clock = Clock.fixed(Instant.parse("2025-11-10T14:30:00Z"), ZoneOffset.UTC);

    private ChannelService service;

    @BeforeEach
    void setUp() {
        service = new ChannelService(repository, clock);
    }

    private ChannelRequest request() {
        return new ChannelRequest("Fatura", ChannelType.EMAIL, "Olá {{name}}", new ChannelConfig(), true);
    }

    @Test
    void createPersistsWhenUnique() {
        when(repository.existsByNameAndTypeAndDeletedFalse("Fatura", ChannelType.EMAIL)).thenReturn(false);
        when(repository.saveAndFlush(any(Channel.class))).thenAnswer(inv -> inv.getArgument(0));

        Channel saved = service.create(request());

        assertThat(saved.getName()).isEqualTo("Fatura");
        assertThat(saved.getType()).isEqualTo(ChannelType.EMAIL);
        assertThat(saved.isActive()).isTrue();
        assertThat(saved.isDeleted()).isFalse();
    }

    @Test
    void createRejectsDuplicateNameAndType() {
        when(repository.existsByNameAndTypeAndDeletedFalse("Fatura", ChannelType.EMAIL)).thenReturn(true);

        assertThatThrownBy(() -> service.create(request()))
                .isInstanceOf(DuplicateChannelException.class);

        verify(repository, never()).saveAndFlush(any());
    }

    @Test
    void createTranslatesConstraintViolationToDuplicate() {
        when(repository.existsByNameAndTypeAndDeletedFalse("Fatura", ChannelType.EMAIL)).thenReturn(false);
        when(repository.saveAndFlush(any(Channel.class)))
                .thenThrow(new org.springframework.dao.DataIntegrityViolationException("uk_channels_name_type_active"));

        assertThatThrownBy(() -> service.create(request()))
                .isInstanceOf(DuplicateChannelException.class);
    }

    @Test
    void findByIdThrowsWhenMissing() {
        when(repository.findByIdAndDeletedFalse("x")).thenReturn(Optional.empty());

        assertThatThrownBy(() -> service.findById("x"))
                .isInstanceOf(ChannelNotFoundException.class);
    }

    @Test
    void deleteMarksAsLogicallyDeleted() {
        Channel channel = Channel.create("Fatura", ChannelType.EMAIL, "t", new ChannelConfig(), true,
                Instant.parse("2025-11-10T14:30:00Z"));
        when(repository.findByIdAndDeletedFalse(channel.getId())).thenReturn(Optional.of(channel));
        when(repository.save(any(Channel.class))).thenAnswer(inv -> inv.getArgument(0));

        service.delete(channel.getId());

        ArgumentCaptor<Channel> captor = ArgumentCaptor.forClass(Channel.class);
        verify(repository).save(captor.capture());
        assertThat(captor.getValue().isDeleted()).isTrue();
        assertThat(captor.getValue().isActive()).isFalse();
    }
}

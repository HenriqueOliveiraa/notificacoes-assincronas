package com.fintech.notifications.api.service;

import com.fintech.notifications.api.domain.Channel;
import com.fintech.notifications.api.exception.ChannelNotFoundException;
import com.fintech.notifications.api.exception.DuplicateChannelException;
import com.fintech.notifications.api.repository.ChannelRepository;
import com.fintech.notifications.api.web.dto.ChannelRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.List;

@Service
@RequiredArgsConstructor
public class ChannelService {

    private final ChannelRepository repository;
    private final Clock clock;

    @Transactional
    public Channel create(ChannelRequest request) {
        if (repository.existsByNameAndTypeAndDeletedFalse(request.name(), request.type())) {
            throw new DuplicateChannelException(request.name(), request.type());
        }
        Instant now = Instant.now(clock);
        Channel channel = Channel.create(
                request.name(),
                request.type(),
                request.template(),
                request.configOrDefault(),
                request.activeOrDefault(),
                now
        );
        try {
            return repository.saveAndFlush(channel);
        } catch (DataIntegrityViolationException e) {
            throw new DuplicateChannelException(request.name(), request.type());
        }
    }

    @Transactional(readOnly = true)
    public List<Channel> findAll() {
        return repository.findAllByDeletedFalse();
    }

    @Transactional(readOnly = true)
    public Channel findById(String id) {
        return repository.findByIdAndDeletedFalse(id)
                .orElseThrow(() -> new ChannelNotFoundException(id));
    }

    @Transactional
    public Channel update(String id, ChannelRequest request) {
        Channel channel = findById(id);
        if (repository.existsByNameAndTypeAndDeletedFalseAndIdNot(request.name(), request.type(), id)) {
            throw new DuplicateChannelException(request.name(), request.type());
        }
        channel.update(
                request.name(),
                request.type(),
                request.template(),
                request.configOrDefault(),
                request.activeOrDefault(),
                Instant.now(clock)
        );
        try {
            return repository.saveAndFlush(channel);
        } catch (DataIntegrityViolationException e) {
            throw new DuplicateChannelException(request.name(), request.type());
        }
    }

    @Transactional
    public void delete(String id) {
        Channel channel = findById(id);
        channel.markDeleted(Instant.now(clock));
        repository.save(channel);
    }
}

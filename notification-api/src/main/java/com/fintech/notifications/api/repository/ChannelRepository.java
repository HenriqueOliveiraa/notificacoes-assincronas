package com.fintech.notifications.api.repository;

import com.fintech.notifications.api.domain.Channel;
import com.fintech.notifications.contract.ChannelType;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface ChannelRepository extends JpaRepository<Channel, String> {

    List<Channel> findAllByDeletedFalse();

    Optional<Channel> findByIdAndDeletedFalse(String id);

    boolean existsByNameAndTypeAndDeletedFalse(String name, ChannelType type);

    boolean existsByNameAndTypeAndDeletedFalseAndIdNot(String name, ChannelType type, String id);
}

package com.fintech.notifications.processor.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.apache.kafka.clients.admin.NewTopic;

@Configuration
public class KafkaTopicsConfig {

    @Bean
    public NewTopic alertsTopic(@Value("${app.kafka.topics.alerts}") String name,
                                @Value("${app.kafka.topics.partitions:3}") int partitions) {
        return TopicBuilder.name(name).partitions(partitions).replicas(1).build();
    }

    @Bean
    public NewTopic alertsDltTopic(@Value("${app.kafka.topics.dlt}") String name,
                                   @Value("${app.kafka.topics.partitions:3}") int partitions) {
        return TopicBuilder.name(name).partitions(partitions).replicas(1).build();
    }
}

package com.fintech.notifications.processor.config;

import com.fintech.notifications.contract.AlertMessage;
import com.fintech.notifications.processor.messaging.FailureRecordingRecoverer;
import com.fintech.notifications.processor.strategy.UnsupportedChannelTypeException;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class KafkaConsumerConfig {

    private final KafkaProperties kafkaProperties;

    @Bean
    public ConsumerFactory<String, AlertMessage> consumerFactory() {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties(null));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, AlertMessage.class.getName());
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.fintech.notifications.contract");
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, AlertMessage> kafkaListenerContainerFactory(
            ConsumerFactory<String, AlertMessage> consumerFactory,
            DefaultErrorHandler errorHandler,
            @Value("${app.kafka.listener.concurrency:3}") int concurrency) {
        ConcurrentKafkaListenerContainerFactory<String, AlertMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        factory.setConcurrency(concurrency);
        return factory;
    }

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
            @Value("${app.kafka.topics.dlt}") String dltTopic) {
        Map<String, Object> base = new HashMap<>(kafkaProperties.buildProducerProperties(null));
        base.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        Map<String, Object> jsonProps = new HashMap<>(base);
        jsonProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        jsonProps.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        KafkaTemplate<String, Object> jsonTemplate =
                new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(jsonProps));

        Map<String, Object> bytesProps = new HashMap<>(base);
        bytesProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        KafkaTemplate<String, Object> bytesTemplate =
                new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(bytesProps));

        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, bytesTemplate);
        templates.put(Object.class, jsonTemplate);

        return new DeadLetterPublishingRecoverer(templates,
                (record, ex) -> new TopicPartition(dltTopic, record.partition()));
    }

    @Bean
    public DefaultErrorHandler errorHandler(FailureRecordingRecoverer recoverer,
                                            @Value("${app.kafka.retry.max-attempts:3}") int defaultMaxRetries,
                                            @Value("${app.kafka.retry.initial-interval-ms:1000}") long initialInterval,
                                            @Value("${app.kafka.retry.multiplier:2.0}") double multiplier,
                                            @Value("${app.kafka.retry.max-interval-ms:10000}") long maxInterval) {
        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer,
                backOff(defaultMaxRetries, initialInterval, multiplier, maxInterval));
        handler.setBackOffFunction((record, ex) -> {
            if (record.value() instanceof AlertMessage alert && alert.config() != null) {
                return backOff(alert.config().maxRetriesOrDefault(), initialInterval, multiplier, maxInterval);
            }
            return null;
        });
        handler.addNotRetryableExceptions(UnsupportedChannelTypeException.class);
        return handler;
    }

    private ExponentialBackOff backOff(int maxRetries, long initialInterval, double multiplier, long maxInterval) {
        ExponentialBackOff backOff = new ExponentialBackOff(initialInterval, multiplier);
        backOff.setMaxInterval(maxInterval);
        backOff.setMaxAttempts(maxRetries);
        return backOff;
    }
}

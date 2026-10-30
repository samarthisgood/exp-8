package com.example.backend.config;

import com.example.backend.entity.ScheduledPost;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Configuration
public class KafkaConfig {

    private ObjectMapper kafkaMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }

    @Bean
    public ProducerFactory<String, ScheduledPost> producerFactory(KafkaProperties properties) {
        JsonSerializer<ScheduledPost> valueSerializer = new JsonSerializer<>(kafkaMapper());
        valueSerializer.setAddTypeInfo(false);
        return new DefaultKafkaProducerFactory<>(
                properties.buildProducerProperties(),
                new StringSerializer(),
                valueSerializer);
    }

    @Bean
    public KafkaTemplate<String, ScheduledPost> kafkaTemplate(ProducerFactory<String, ScheduledPost> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    public ConsumerFactory<String, ScheduledPost> consumerFactory(KafkaProperties properties) {
        JsonDeserializer<ScheduledPost> valueDeserializer =
                new JsonDeserializer<>(ScheduledPost.class, kafkaMapper(), false);
        return new DefaultKafkaConsumerFactory<>(
                properties.buildConsumerProperties(),
                new StringDeserializer(),
                valueDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ScheduledPost> kafkaListenerContainerFactory(
            ConsumerFactory<String, ScheduledPost> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, ScheduledPost> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }

    @Bean
    public NewTopic postTopic() {
        return TopicBuilder.name("post-topic")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic dlqTopic() {
        return TopicBuilder.name("post-topic-dlq")
                .partitions(1)
                .replicas(1)
                .build();
    }
}

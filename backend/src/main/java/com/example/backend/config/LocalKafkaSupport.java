package com.example.backend.config;

import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

/**
 * Starts an in-process KRaft Kafka broker when Docker Kafka is not available.
 * Disable with -Dembedded.kafka=false if you are using docker-compose instead.
 */
public final class LocalKafkaSupport {

    private static EmbeddedKafkaBroker broker;

    private LocalKafkaSupport() {
    }

    public static void startIfNeeded() {
        if (!Boolean.parseBoolean(System.getProperty("embedded.kafka", "true"))) {
            return;
        }

        broker = new EmbeddedKafkaKraftBroker(1, 3, "post-topic", "post-topic-dlq");
        broker.afterPropertiesSet();
        String bootstrap = broker.getBrokersAsString();
        System.setProperty("spring.kafka.bootstrap-servers", bootstrap);
        System.out.println("Embedded Kafka started at " + bootstrap);
    }
}

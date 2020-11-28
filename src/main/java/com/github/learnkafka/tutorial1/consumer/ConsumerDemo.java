package com.github.learnkafka.tutorial1.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class ConsumerDemo {

    private static final String bootstrapServers = "127.0.0.1:9092";
    private static final String groupId = "my-fourth-application";
    private static final String topic = "first_topic";

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(createProperties());
        consumer.subscribe(Collections.singletonList(topic));

        while (true) {
            consumer.poll(Duration.ofMillis(100))
                    .forEach(record -> {
                                        logger.info("Key: " + record.key() + "\n" +
                                                    "Value: " + record.value() + "\n" +
                                                    "Partition: " + record.partition() + "\n" +
                                                    "Offset: " + record.offset());
            });
        }
    }

    private static Properties createProperties() {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(GROUP_ID_CONFIG, groupId);
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }
}

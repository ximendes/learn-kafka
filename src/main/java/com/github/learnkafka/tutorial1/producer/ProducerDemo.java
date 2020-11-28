package com.github.learnkafka.tutorial1.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class ProducerDemo {

    private static final String bootstrapServers = "127.0.0.1:9092";
    private static final String topic = "first_topic";
    private static final String value = "hello world";

    public static void main(String[] args) {

        KafkaProducer<String, String> producer = new KafkaProducer<>(createProperties());
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, value);

        producer.send(record);
        producer.flush();
        producer.close();
    }

    private static Properties createProperties() {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}

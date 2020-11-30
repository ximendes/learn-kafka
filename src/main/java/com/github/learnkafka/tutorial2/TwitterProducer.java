package com.github.learnkafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class TwitterProducer {

    final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    private static final String consumerKey = "XxgrmKwrpXT9pY2WnA5rew51s";
    private static final String consumerSecret = "7cyRUE9F69gIt9h9QQkuzRzx6IXSLUz0JfVIj6jkicpft9ZKK3";
    private static final String token = "271717719-VhGpbkggZmpk18qRB6uhktmOHwEBzoIhmXH2Mm0a";
    private static final String secret = "shN23FTP8GMeoeTPGy7MnHWzuCYlRsU6sIa6CFT0EpicX";
    private static final String bootstrapServers = "127.0.0.1:9092";
    private final List<String> terms = Lists.newArrayList("kafka");

    public TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        logger.info("Setup");

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
        Client client = createTwitterClient(msgQueue);
        client.connect();


        KafkaProducer<String, String> producer = createKafkaProducer();
        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            logger.info("stopping Application....");
            logger.info("shutting down cient from twitter....");
            client.stop();
            logger.info("closing producer...");
            producer.close();
            logger.info("Done!");
        }));

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), (recordMetadata, exception) -> {
                    if(exception != null){
                        logger.error("Somenthing bad happend", exception);
                    }
                });
            }
        }
        logger.info("End of Application");
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        return new KafkaProducer<>(createProperties());
    }


    private static Properties createProperties() {
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
                                                   .hosts(hosebirdHosts)
                                                   .authentication(hosebirdAuth)
                                                   .endpoint(hosebirdEndpoint)
                                                   .processor(new StringDelimitedProcessor(msgQueue)); // optional: use this if you want to process client events

        return builder.build();

    }
}

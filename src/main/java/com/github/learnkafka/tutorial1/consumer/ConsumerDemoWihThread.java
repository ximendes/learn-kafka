package com.github.learnkafka.tutorial1.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWihThread {

    private static final String bootstrapServers = "127.0.0.1:9092";
    private static final String groupId = "my-sixth-application";
    private static final String topic = "first_topic";

    public ConsumerDemoWihThread() {
    }

    public static void main(String[] args) {
        new ConsumerDemoWihThread().run();
    }

    public void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWihThread.class.getName());

        CountDownLatch latch = new CountDownLatch(1);

        logger.info("creating  the consumer thread");
        Runnable consumerRunnable = new ConsumerRunnable(bootstrapServers, groupId, topic, latch);

        Thread myThread = new Thread(consumerRunnable);
        myThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caugth shutdown hook ");
            ((ConsumerRunnable) consumerRunnable).shutdow();

            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException exception) {
            logger.error("Application got error", exception);
        } finally {
            logger.info("Application is closing");
        }
    }
}

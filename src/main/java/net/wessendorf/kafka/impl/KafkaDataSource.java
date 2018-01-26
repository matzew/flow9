package net.wessendorf.kafka.impl;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.concurrent.Flow;
import java.util.concurrent.SubmissionPublisher;
import java.util.logging.Logger;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

public class KafkaDataSource implements Runnable {


    final Properties properties = new Properties();
    final KafkaConsumer<String, String> consumer;
    private final Logger logger = Logger.getLogger(KafkaDataSource.class.getName());
    private final String topic;


    SubmissionPublisher<String> publisher = new SubmissionPublisher<>();

    public KafkaDataSource(final String topic) {
        this.topic = topic;
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "172.30.87.100:9092");
        properties.put(GROUP_ID_CONFIG, "foo_bar1");
        properties.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        consumer = new KafkaConsumer(properties);
    }

    public void subscribe(Flow.Subscriber<String> subscriber) {
        publisher.subscribe(subscriber);
    }

    @Override
    public void run() {

        consumer.subscribe(Arrays.asList("fooo"));

        logger.warning("Done w/ subscribing");

        boolean running = true;
        while (running) {
            final ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {

                publisher.submit(record.value());
                logger.warning("got: " + record.value() + " from " + new Date(record.timestamp()) + ", partition: " + record.partition()  );
            }
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }

}

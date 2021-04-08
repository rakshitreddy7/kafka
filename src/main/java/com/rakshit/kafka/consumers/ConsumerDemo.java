package com.rakshit.kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    private static final String KAFKA_SERVER = "0.0.0.0:9092";
    private static final String CONSUMER_GROUP = "test_consumer_group";
    private static final String TOPIC = "java_topic";
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    /*
    * 1. Configure KafkaConsumer properties
    * 2. Create Consumer
    * 3. Subscribe Consumer to a topic/s
    * 4. Poll for new data
    * */
    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList(TOPIC));

        while(true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : consumerRecords) {
                logger.info("Key : " + record.key() + " " + "Value : " + record.value());
                logger.info("Partition : " + record.partition() + "," + "Offset : " + record.offset());
            }
        }
    }
}

/*
* The poll method is a blocking method waiting for specified time in seconds.
* If no records are available after the time period specified, the poll method returns an empty ConsumerRecords.
* When new records become available, the poll method returns straight away.
* */
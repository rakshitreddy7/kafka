package com.rakshit.kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class AssignAndSeek {
    private static final String KAFKA_SERVER = "0.0.0.0:9092";
    private static final String CONSUMER_GROUP = "test_consumer_group_1";
    private static final String TOPIC_1 = "java_topic_1";
    private static final String TOPIC_2 = "java_topic_2";
    private static final Logger logger = LoggerFactory.getLogger(AssignAndSeek.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        TopicPartition topicPartitionToReadFrom_1 = new TopicPartition(TOPIC_1, 2);
        TopicPartition topicPartitionToReadFrom_2 = new TopicPartition(TOPIC_2, 1);
        long offsetToReadFrom_1 = 0L;
        long offsetToReadFrom_2 = 5L;

        consumer.assign(Arrays.asList(topicPartitionToReadFrom_1, topicPartitionToReadFrom_2));

        consumer.seek(topicPartitionToReadFrom_1, offsetToReadFrom_1);
        consumer.seek(topicPartitionToReadFrom_2, offsetToReadFrom_2);

        int count = 0;
        boolean messagesSoFar = true;

        while(messagesSoFar) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : consumerRecords) {
                logger.info("Topic : " + record.topic());
                logger.info("Key : " + record.key() + " " + "Value : " + record.value());
                logger.info("Partition : " + record.partition() + "," + "Offset : " + record.offset());
                count++;
                if (count >= 2) {
                    messagesSoFar = false;
                    break;
                }
            }
        }
    }
}

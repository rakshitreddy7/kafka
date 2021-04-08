package com.rakshit.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemo {
    private static final String BOOTSTRAP_SERVER = "0.0.0.0:9092";
    private static final String TOPIC = "java_topic";
    private static final String MESSAGE = "hello world";

    /*
    * 1. Create KafkaProducer properties
    * 2. Create a KafkaProducer
    * 3. Create a ProducerRecord
    * 4. Then, send this record via Producer
    * */
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ProducerDemo.class);
        Properties properties = new Properties();

        logger.info("Configuring Producer properties..");

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        logger.info("Producer is sending messages now...");

        for (int i=0; i<10; i++) {
            String KEY = "id_" + i;
            logger.info("Key: " + KEY);
            String VALUE = MESSAGE + "_" + i;

            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC, KEY, VALUE);

            producer.send(producerRecord, (recordMetadata, e) -> {
                if (null == e) {
                    logger.info("Topic name: " + recordMetadata.topic() + "\n"
                            + "Partition : " + recordMetadata.partition() + "\n"
                            + "Offset: " + recordMetadata.offset() +"\n"
                    );
                } else {
                    logger.info("Exception has occurred : " + e.getMessage());
                }
            }).get(); //to make it async
        }

        producer.flush();
        producer.close();
    }
}

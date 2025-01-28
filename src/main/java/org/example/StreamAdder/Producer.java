package org.example.StreamAdder;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Producer {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(Producer.class);

        // Kafka producer properties
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(prop);
        Random random = new Random();

        try {
            while (true) {
                // Generate random single-digit number
                int randomDigit = random.nextInt(10);

                // Create key as "key<randomDigit>"
                String key = "key" + randomDigit;

                // Create the ProducerRecord
                ProducerRecord<String, String> record = new ProducerRecord<>(
                        "random-numbers-topic", // Topic
                        key,                   // Key
                        Integer.toString(randomDigit) // Value
                );

                // Send the record
                producer.send(record, (RecordMetadata metadata, Exception e) -> {
                    if (e == null) {
                        logger.info("Sent successfully: Key: {} Value: {} to Partition: {} Offset: {}",
                                key, randomDigit, metadata.partition(), metadata.offset());
                    } else {
                        logger.error("Error while sending", e);
                    }
                });

                // Wait for 5 seconds before sending the next message
                TimeUnit.SECONDS.sleep(5);
            }
        } catch (InterruptedException e) {
            logger.error("Producer interrupted", e);
        } finally {
            producer.close();
        }
    }
}

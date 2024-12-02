package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.JSONObject;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {

        // Create logger for class
        final Logger logger = LoggerFactory.getLogger(Consumer.class);

        // Create variables for strings
        final String bootstrapServers = "127.0.0.1:9092";
        final String consumerGroupID = "java-group-consumer";

        // Create and populate properties object
        Properties p = new Properties();
        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupID);
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create Consumer
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);

        // Subscribe to topic(s)
        consumer.subscribe(Arrays.asList("logs-topic-1"));

        // Poll and Consume records
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                // Deserialize the value (JSON string)
                String jsonString = record.value();
                JSONObject logEntry = new JSONObject(jsonString);

                // Log the data in a structured format
                logger.info("Received new log: \n" +
                        "Key: " + record.key() + ", " +
                        "Message: " + logEntry.getString("Message") + ", " +
                        "MachineName: " + logEntry.getString("MachineName") + ", " +
                        "Category: " + logEntry.getString("Category") + ", " +
                        "Topic: " + record.topic() + ", " +
                        "Partition: " + record.partition() + ", " +
                        "Offset: " + record.offset() + "\n");
            }
        }
    }
}

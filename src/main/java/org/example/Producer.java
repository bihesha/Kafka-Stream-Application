package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(Producer.class);

        // Kafka producer properties
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Kafka Producer
        final KafkaProducer<String, String> producer = new KafkaProducer<>(prop);

        String csvFilePath = "D:\\Kafka-Stream-Application\\Kafka-Stream-Application\\src\\main\\dataset\\eventlog.csv"; // Replace with the actual path to the CSV file

        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
            String headerLine = br.readLine(); // Skip the header
            String line;
            int logId = 1;

            // Read and process each line in the CSV file
            while ((line = br.readLine()) != null) {
                // Handle potential empty or malformed lines gracefully
                if (line.trim().isEmpty()) {
                    continue; // Skip empty lines
                }

                String[] fields = line.split(",", -1); // Using -1 to handle trailing commas

                // Check if the number of fields matches the expected column count (for validation)
                if (fields.length < 13) { // Adjust this check based on your CSV format
                    logger.warn("Skipping malformed line: {}", line);
                    continue;
                }

                // Create a JSON object for structured logging
                JSONObject logEntry = new JSONObject();
                logEntry.put("MachineName", fields[1]);
                logEntry.put("Category", fields[2]);
                logEntry.put("EntryType", fields[3]);
                logEntry.put("Message", fields[4]);
                logEntry.put("Source", fields[5]);
                logEntry.put("TimeGenerated", fields[6]);
                logEntry.put("country", fields[7]);
                logEntry.put("regionName", fields[8]);
                logEntry.put("city", fields[9]);
                logEntry.put("zip", fields[10]);
                logEntry.put("timezone", fields[11]);
                logEntry.put("isp", fields[12]);

                // Create the ProducerRecord
                ProducerRecord<String, String> record = new ProducerRecord<>(
                        "logs-topic-2", // Kafka topic name
                        "log_" + logId, // Key
                        logEntry.toString() // Value as JSON string
                );

                // Send log data
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null) {
                            logger.info("Log sent successfully. Metadata:\n" +
                                    "Topic: " + recordMetadata.topic() + ", Partition: " + recordMetadata.partition() +
                                    ", Offset: " + recordMetadata.offset() + ", Timestamp: " + recordMetadata.timestamp());
                        } else {
                            logger.error("Error sending log data", e);
                        }
                    }
                });

                logId++;

                // Introduce a delay of 5 seconds
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    logger.error("Interrupted while sleeping", e);
                    Thread.currentThread().interrupt();
                }
            }

        } catch (IOException e) {
            logger.error("Error reading CSV file", e);
        } finally {
            // Flush and close producer
            producer.flush();
            producer.close();
        }
    }
}

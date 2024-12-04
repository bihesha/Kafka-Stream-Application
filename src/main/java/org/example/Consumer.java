package org.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Consumer {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(Consumer.class);

        // Kafka Streams configuration
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "log-processor");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Create StreamsBuilder
        StreamsBuilder builder = new StreamsBuilder();

        // Read data from the Kafka topic
        KStream<String, String> logs = builder.stream("logs-topic-2");

        // Use a transformer to access metadata
        logs.transform(() -> new Transformer<String, String, KeyValue<String, String>>() {
            private ProcessorContext context;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
            }

            @Override
            public KeyValue<String, String> transform(String key, String value) {
                JSONObject logEntry = new JSONObject(value);
                logger.info("Processed log: \n" +
                        "Key: " + key + ", " +
                        "Message: " + logEntry.getString("Message") + ", " +
                        "MachineName: " + logEntry.getString("MachineName") + ", " +
                        "Category: " + logEntry.getString("Category") + ", " +
                        "Topic: " + context.topic() + ", " +
                        "Partition: " + context.partition() + ", " +
                        "Offset: " + context.offset() + "\n");
                return KeyValue.pair(key, value); // Pass the record along unchanged
            }

            @Override
            public void close() {
                // No-op
            }
        });

        // Start the stream processing application
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        // Add shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
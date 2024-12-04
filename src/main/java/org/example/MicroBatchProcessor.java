package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
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

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class MicroBatchProcessor {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(MicroBatchProcessor.class);

        // Kafka Streams configuration
        Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "log-processor");
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Kafka producer properties
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        // Create StreamsBuilder
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> logs = builder.stream("logs-topic-2");

        // Shared buffer for micro-batch processing
        BlockingQueue<JSONObject> buffer = new LinkedBlockingQueue<>();
        AtomicInteger batchSize = new AtomicInteger(10); // Initial batch size

        // Background thread for processing batches
        Thread batchProcessor = new Thread(() -> {
            while (true) {
                try {
                    // Collect batch
                    List<JSONObject> batch = new ArrayList<>();
                    buffer.drainTo(batch, batchSize.get());

                    if (!batch.isEmpty()) {
                        long startTime = System.currentTimeMillis();

                        // Process batch (e.g., aggregate, transform, etc.)
                        for (JSONObject log : batch) {
                            logger.info("Processing log in batch: {}", log);

                            // Send processed log to another topic
                            ProducerRecord<String, String> record = new ProducerRecord<>(
                                    "processed-logs-topic-1",
                                    log.getString("MachineName"),
                                    log.toString()
                            );
                            producer.send(record);
                        }

                        // Adjust batch size dynamically based on processing time
                        long processingTime = System.currentTimeMillis() - startTime;
                        if (processingTime > 1000) {
                            batchSize.decrementAndGet();
                        } else {
                            batchSize.incrementAndGet();
                        }
                        logger.info("Batch processed. New batch size: {}", batchSize.get());
                        logger.info("Processing time for this batch: {} ms", processingTime);

                    }

                    // Sleep to prevent tight looping
                    Thread.sleep(1000);

                } catch (Exception e) {
                    logger.error("Error in batch processing", e);
                }
            }
        });
        batchProcessor.setDaemon(true);
        batchProcessor.start();

        // Stream transformation with buffer
        logs.transform(() -> new Transformer<String, String, KeyValue<String, String>>() {
            private ProcessorContext context;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
            }

            @Override
            public KeyValue<String, String> transform(String key, String value) {
                try {
                    JSONObject logEntry = new JSONObject(value);
                    buffer.put(logEntry); // Add to buffer
                } catch (Exception e) {
                    logger.error("Failed to parse log: {}", value, e);
                }
                return null; // Drop record after buffering
            }

            @Override
            public void close() {
                // No-op
            }
        });

        // Start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfig);
        streams.start();

        // Add shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            producer.close();
        }));
    }
}

package org.example.StreamAdder;

import com.thulawa.kafka.ThulawaKafkaStreams;
import com.thulawa.kafka.internals.configs.ThulawaConfigs;
import com.thulawa.kafka.internals.suppliers.ThulawaProcessorSupplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamsWithMetricsApp {

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(StreamsWithMetricsApp.class);

        class ProcessEach implements Processor {

            @Override
            public void process(Record record) {
                String currentTime = java.time.LocalDateTime.now().toString(); // Get the current timestamp
//                System.out.println("Hello!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                System.out.println("Processed by thread: " + Thread.currentThread().getName() + "    value : " + record.value());
//                System.out.println("Processed at time: " + currentTime); // Print the time
            }
        }

        // Kafka Streams configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-with-metrics-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//        props.put(StreamsConfig.InternalConfig.PROCESSING_THREADS_ENABLED, true);

        props.put(ThulawaConfigs.HIGH_PRIORITY_KEY_LIST, "key1, key2, key3");

        // Create the StreamsBuilder
        StreamsBuilder builder = new StreamsBuilder();

        // Process the stream
        KStream<String, String> numberStream = builder.stream("random-numbers-topic");
//        numberStream.foreach((key, value) -> logger.info("Processed Value: {}", value));
        numberStream.process(
                ThulawaProcessorSupplier.createThulawaProcessorSupplier(new ProcessEach())
        );

        // Build and start the Kafka Streams application
//        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        ThulawaKafkaStreams streams = new ThulawaKafkaStreams(builder.build(), props);
        streams.start();

//        KafkaStreams streams1 = new KafkaStreams(builder.build(), props);


        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}

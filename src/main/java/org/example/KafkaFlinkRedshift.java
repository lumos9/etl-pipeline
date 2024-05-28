package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class KafkaFlinkRedshift {
    private static final Logger logger = LogManager.getLogger(KafkaFlinkRedshift.class);
    private static final int BATCH_SIZE = 1000;

    public static void main(String[] args) throws Exception {
        logger.info("Starting Kafka Consumer...");
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("sample-stream")
                .setGroupId("flink-consumer-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Add the consumer to the environment
        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Process data and check for shutdown signal
        DataStream<String> processedStream = stream
            .keyBy(value -> {
                if (value.split(",").length >= 2) {
                    return value.split(",")[1];
                }
                return value;
            })
            .process(new KeyedProcessFunction<>() {

                @Override
                public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                    out.collect(value);
                    //logger.info("received: '{}'", value);
                }
            });

        // Sink the processed stream to a Redshift sink (replace with your actual sink)
        processedStream.addSink(new RedshiftSinkBatchAsync());

        // Execute the Flink job
        env.execute("Kafka to Flink to Redshift");
    }
}
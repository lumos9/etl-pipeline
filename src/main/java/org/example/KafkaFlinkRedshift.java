package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.sink.RedshiftSinkBatchAsync;

import java.util.List;

public class KafkaFlinkRedshift {
    private static final Logger logger = LogManager.getLogger(KafkaFlinkRedshift.class);

    public static void main(String[] args) throws Exception {
        logger.info("Starting Kafka Consumer...");
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        int parallelism = env.getParallelism();
        logger.info("Available parallelism: {}", parallelism);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("sample-stream")
                .setGroupId("flink-consumer-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Add the consumer to the environment
        DataStream<String> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<String> transformedStream = stream.map(value -> {
            if (value != null) {
                String[] fields = value.split(",");
                if (fields.length >= 4) {
                    String index = fields[0];
                    String id = fields[1];
                    String name = fields[2];
                    String value_1 = fields[3];

                    return String.join(",",
                            List.of(index,
                                    id,
                                    "transformed_name_" + name.split("_")[1],
                                    "transformed_value_" + value_1.split("_")[1]));
                }
                return value;
            }

            return null;
        }).setParallelism(parallelism); // Ensure transformation is also parallel

        // Sink the processed stream to a Redshift sink (replace with your actual sink)
        transformedStream.addSink(new RedshiftSinkBatchAsync()).name("Amazon Redshift").setParallelism(parallelism); // Ensure sink is parallel

        // Execute the Flink job
        env.execute("Kafka to Flink to Redshift");
    }
}
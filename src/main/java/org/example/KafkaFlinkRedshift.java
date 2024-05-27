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

public class KafkaFlinkRedshift {
    private static final Logger logger = LogManager.getLogger(KafkaFlinkRedshift.class);

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

        //env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Add the consumer to the environment
        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Transformation: Split CSV and apply transformation
//        DataStream<String> transformedStream = stream.map((MapFunction<String, String>) value -> {
//            String[] fields = value.split(",");
//            int id = Integer.parseInt(fields[0]);
//            String name = fields[1].toUpperCase();
//            String newValue = fields[2] + "_transformed";
//            return id + "," + name + "," + newValue;
//        });

        // Process data and check for shutdown signal
        DataStream<String> processedStream = stream
                .keyBy(value -> value.split(",")[0]) // Assuming id is the key // Assuming keyBy a field, modify as per your key
                .process(new KeyedProcessFunction<>() {

                    private transient ValueState<Boolean> shutdownState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("shutdownState",
                                Boolean.class);
                        shutdownState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        if ("SHUTDOWN".equals(value)) {
                            shutdownState.update(true);
                        } else {
                            String[] fields = value.split(",");
                            int id = Integer.parseInt(fields[0]);
                            String name = fields[1].toUpperCase();
                            String newValue = fields[2] + "_transformed";
                            out.collect(id + "," + name + "," + newValue);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        // No timer required for this example
                    }

                    @Override
                    public void close() throws Exception {
                        if (shutdownState.value() != null && shutdownState.value()) {
                            // Trigger job cancellation or other shutdown logic
                            // For example, stopping the execution environment
                            logger.info("Shutdown signal received. Shutting down...");
                            // ctx.getExecutionEnvironment().cancel(); // Uncomment if you have a reference to the execution environment
                        }
                        super.close();
                    }
                });

        // Here, you can sink the transformed stream to Amazon Redshift
        // For simplicity, this example will just print the transformed records
        //transformedStream.print();
        //transformedStream.addSink(new LoggingSink<>());
        processedStream.addSink(new RedshiftSinkBatchAsync());
        ///Add another sink
        //transformedStream.addSink(new RedshiftSink());

        // Execute the Flink job
        env.execute("Kafka to Flink to Redshift");
    }
}

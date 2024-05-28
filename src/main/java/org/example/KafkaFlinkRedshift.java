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
                .keyBy(value -> {
                   if(value.split(",").length >= 2) {
                       return value.split(",")[1];
                   }
                   return value;
                })
                .process(new KeyedProcessFunction<>() {

                    private transient ValueState<Boolean> shutdownState;
                    private transient ValueState<Long> messageCountState;
                    private static final long CHECK_INTERVAL = 60000; // 60 seconds

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("shutdownState",
                                Boolean.class);
                        shutdownState = getRuntimeContext().getState(descriptor);

                        ValueStateDescriptor<Long> messageCountDescriptor = new ValueStateDescriptor<>("messageCountState", Long.class);
                        messageCountState = getRuntimeContext().getState(messageCountDescriptor);

                        // Set a timer to periodically check the shutdown state and log the message count
                        //long currentTime = getRuntimeContext()..getCurrentProcessingTime();
                        //getRuntimeContext().getProcessingTimeService().registerProcessingTimeTimer(currentTime + CHECK_INTERVAL);
                    }

                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        Long currentCount = messageCountState.value();
                        if (currentCount == null) {
                            currentCount = 0L;
                        }
                        messageCountState.update(currentCount + 1);

                        if ("SHUTDOWN".equals(value)) {
                            shutdownState.update(true);
                        } else {
                            if(value.split(",").length >= 4) {
                                String[] fields = value.split(",");
                                int index = Integer.parseInt(fields[0]);
                                long id = Long.parseLong(fields[1]);
                                String name = fields[2].toUpperCase();
                                String newValue = fields[3] + "_transformed";
                                out.collect(index + "," + id + "," + name + "," + newValue);
                            } else {
                                out.collect(value);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        // Log the message count periodically
                        Long currentCount = messageCountState.value();
                        if (currentCount != null) {
                            logger.info("Current message count: {}", currentCount);
                        }

                        // Check the shutdown state
                        if (shutdownState.value() != null && shutdownState.value()) {
                            // Trigger job cancellation or other shutdown logic
                            logger.info("Shutdown signal received. Shutting down...");
                            // ctx.getExecutionEnvironment().cancel(); // Uncomment if you have a reference to the execution environment
                        } else {
                            // Register the next timer
                            long currentTime = ctx.timerService().currentProcessingTime();
                            ctx.timerService().registerProcessingTimeTimer(currentTime + CHECK_INTERVAL);
                        }
                    }

                    @Override
                    public void close() throws Exception {
                        if (shutdownState.value() != null && shutdownState.value()) {
                            // Trigger job cancellation or other shutdown logic
                            // For example, stopping the execution environment
                            logger.info("Final shutdown check. Total messages processed: {}",
                                    messageCountState.value());
                            // ctx.getExecutionEnvironment().cancel(); // Uncomment if you have a reference to the execution environment
                        }
                        super.close();
                    }
                });

        // Here, you can sink the transformed stream to Amazon Redshift
        // For simplicity, this example will just print the transformed records
        //transformedStream.print();
        //processedStream.addSink(new LoggingSink<>()).setParallelism(1);
        processedStream.addSink(new RedshiftSinkBatchAsync()).setParallelism(1);
        ///Add another sink
        //transformedStream.addSink(new RedshiftSink());

        // Execute the Flink job
        env.execute("Kafka to Flink to Redshift");
    }
}

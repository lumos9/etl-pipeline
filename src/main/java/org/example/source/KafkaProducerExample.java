package org.example.source;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.sink.JDBCSink;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class KafkaProducerExample {
    private static final Logger logger = LogManager.getLogger(KafkaProducerExample.class);

    public static void main(String[] args) {
        // Kafka configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "csv-producer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, "0");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 100000);  // Batch size in bytes
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);  // Buffering time in milliseconds
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);  // Buffer size in bytes
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 1000);  // Maximum time to block
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");  // Compression type
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            // Send the shutdown signal
            ProducerRecord<String, String> shutdownRecord = new ProducerRecord<>("sample-stream",
                    "INIT", "INIT");

            producer.send(shutdownRecord, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Failed to deliver init message: {}", exception.getMessage());
                } else {
                    logger.info("init message produced: {}", metadata.toString());
                }
            });
        } catch (Exception exception) {
            logger.error("Failed to send init record. Details: {}",
                    ExceptionUtils.getStackTrace(exception));
        }

        // Number of records to produce
        int numRecords = 1_000_000;
        int print_limit = 1000;
        final int[] limit = {0};
        AtomicInteger total = new AtomicInteger();

        long start = System.nanoTime();
        final long[] batchStart = {System.nanoTime()};  // Time measurement for the current batch
        final double[] maxRecordsPerSec = {0};
        final double[] minRecordsPerSec = {0};

        // Produce records
        for (int i = 1; i <= numRecords; i++) {
            String record = i + "," + System.currentTimeMillis() + ",name_" + i + ",value_" + i;
            try {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("sample-stream",
                        String.valueOf(i), record);

                // Send record asynchronously with a callback
                producer.send(producerRecord, (metadata, exception) -> {
                    if (exception != null) {
                        logger.error("Failed to deliver message: {}", exception.getMessage());
                    } else {
                        limit[0]++;
                        //logger.info("Message produced: {}", metadata.toString());
                        if(limit[0] == print_limit) {
                            int currentCount = total.addAndGet(limit[0]);
                            long batchEnd = System.nanoTime();
                            long batchTime = batchEnd - batchStart[0];
                            double seconds = batchTime / 1_000_000_000.0;
                            double messagesPerSecond = limit[0] / seconds;
                            if(messagesPerSecond > maxRecordsPerSec[0]) {
                                maxRecordsPerSec[0] = messagesPerSecond;
                            }
                            if(minRecordsPerSec[0] == 0 || messagesPerSecond < minRecordsPerSec[0]) {
                                minRecordsPerSec[0] = messagesPerSecond;
                            }
                            batchStart[0] = System.nanoTime();
                            logger.info("{} messages produced in {} seconds. Rate: {} messages/sec. Total: {}",
                                    limit[0], String.format("%.3f", seconds), String.format("%.3f", messagesPerSecond), currentCount);

                            limit[0] = 0;
                        }
                    }
                });

                // Poll to serve delivery reports and free up space
                producer.flush();

                // Optional: Sleep to throttle message production
                // Thread.sleep(1);
            } catch (Exception exception) {
                logger.error("Failed to send record for iteration {}. Details: {}",
                        i, ExceptionUtils.getStackTrace(exception));
            }
        }

        logger.info("data sent in {}", JDBCSink.getHumanReadableTimeDifference(start, System.nanoTime()));

        if (limit[0] > 0) {
            int currentCount = total.addAndGet(limit[0]);
            long batchEnd = System.nanoTime();
            long batchTime = batchEnd - batchStart[0];
            double seconds = batchTime / 1_000_000_000.0;
            double messagesPerSecond = limit[0] / seconds;
            if(messagesPerSecond > maxRecordsPerSec[0]) {
                maxRecordsPerSec[0] = messagesPerSecond;
            }
            if(minRecordsPerSec[0] == 0 || messagesPerSecond < minRecordsPerSec[0]) {
                minRecordsPerSec[0] = messagesPerSecond;
            }
            batchStart[0] = System.nanoTime();
            logger.info("last batch: {} messages produced in {} seconds. Rate: {} messages/sec. Total: {}",
                    limit[0], String.format("%.3f", seconds), String.format("%.3f", messagesPerSecond), currentCount);
        }

        logger.info("Max Rate: {} messages/sec and Min Rate: {} messages/sec", maxRecordsPerSec[0], minRecordsPerSec[0]);

        try {
            // Send the shutdown signal
            ProducerRecord<String, String> shutdownRecord = new ProducerRecord<>("sample-stream",
                    "SHUTDOWN", "SHUTDOWN");

            producer.send(shutdownRecord, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Failed to deliver shutdown message: {}", exception.getMessage());
                } else {
                    logger.info("Shutdown message produced: {}", metadata.toString());
                }
            });
        } catch (Exception exception) {
            logger.error("Failed to send SHUTDOWN record. Details: {}",
                    ExceptionUtils.getStackTrace(exception));
        } finally {
            // Ensure all messages are delivered before exiting
            producer.close();
        }
    }
}
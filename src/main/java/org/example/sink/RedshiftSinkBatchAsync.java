package org.example.sink;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;

public class RedshiftSinkBatchAsync extends RichSinkFunction<String> {
    private static final Logger logger = LogManager.getLogger(RedshiftSinkBatchAsync.class);
    private static final int BATCH_SIZE = 1000;

    private transient ComboPooledDataSource dataSource;
    private transient LinkedBlockingQueue<String> buffer;
    private transient ExecutorService executorService;
    private transient CountDownLatch shutdownLatch;

    private String id;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dataSource = new ComboPooledDataSource();
        dataSource.setDriverClass("com.amazon.redshift.jdbc42.Driver");
        dataSource.setJdbcUrl("jdbc:redshift://your-redshift-cluster:5439/your-database");
        dataSource.setUser("your-username");
        dataSource.setPassword("your-password");
        dataSource.setMaxPoolSize(10);
        dataSource.setMinPoolSize(1);
        dataSource.setAcquireIncrement(1);
        dataSource.setMaxStatements(100);

        buffer = new LinkedBlockingQueue<>();
        executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()); // Use a thread pool for parallel writes
        shutdownLatch = new CountDownLatch(1);
        id = UUID.randomUUID().toString();

        // Start a background task to flush the buffer
        executorService.submit(this::flushBuffer);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        if ("SHUTDOWN".equalsIgnoreCase(value)) {
            logger.info("{} - Shutdown signal received. Flushing remaining records...", id);
            shutdownLatch.countDown();
        } else {
            buffer.put(value);
        }
    }

    @Override
    public void close() throws Exception {
        if (!buffer.isEmpty()) {
            logger.info("{} = Loading last batch...", id);
            flush();
        }
        logger.info("{} = Waiting for shutdown latch / signal...", id);
        shutdownLatch.await();
        executorService.shutdown();
        if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
            executorService.shutdownNow();
        }
        if (dataSource != null) {
            dataSource.close();
        }
        super.close();
    }

    private void flushBuffer() {
        try {
            while (!Thread.currentThread().isInterrupted() || !buffer.isEmpty()) {
                //logger.info("{} - Current buffer size: {}", id, buffer.size());
                flush();
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("Error in flushing buffer: ", e);
        }
    }

    private void flush() throws Exception {
        if (!buffer.isEmpty()) {
            List<String> batch = new ArrayList<>();
            buffer.drainTo(batch, BATCH_SIZE);
            if (!batch.isEmpty()) {
                logger.info("{} - Attempting to load batch with size: {}", id, batch.size());
                executorService.submit(() -> {
                    try {
                        pretendLoad(batch);
                    } catch (Exception e) {
                        logger.error("{} - Error in loading to Redshift: ", id, e);
                    }
                });
            } else {
                logger.info("{} - Nothing to load. Batch is empty", id);
            }
        }
    }

    private void pretendLoad(List<String> batch) throws Exception {
        //Thread.sleep(500);
        logger.info("{} - {} Loaded", id, batch.size());
    }

    private void loadToRedShift(List<String> batch) throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            String sql = "INSERT INTO your_table (id, timestamp, name, value) VALUES (?, ?, ?, ?)";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                for (String record : batch) {
                    String[] fields = record.split(",");
                    int id = Integer.parseInt(fields[0]);
                    long timestamp = Long.parseLong(fields[1]);
                    String name = fields[2];
                    String value = fields[3];
                    statement.setInt(1, id);
                    statement.setLong(2, timestamp);
                    statement.setString(3, name);
                    statement.setString(4, value);
                    statement.addBatch();
                }
                statement.executeBatch();
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            }
        }
    }
}
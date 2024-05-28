package org.example;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RedshiftSinkBatchAsync extends RichSinkFunction<String> {
    private static final Logger logger = LogManager.getLogger(RedshiftSinkBatchAsync.class);
    private transient ComboPooledDataSource dataSource;
    private transient ExecutorService executorService;
    private final List<String> buffer = new ArrayList<>();
    private static final int BATCH_SIZE = 1000;
    private final AtomicInteger batchNum = new AtomicInteger(0);

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dataSource = new ComboPooledDataSource();
        dataSource.setDriverClass("com.amazon.redshift.jdbc.Driver");
        dataSource.setJdbcUrl("jdbc:redshift://your-redshift-cluster:5439/your-database");
        dataSource.setUser("your-username");
        dataSource.setPassword("your-password");
        dataSource.setMaxPoolSize(10);
        dataSource.setMinPoolSize(1);
        dataSource.setAcquireIncrement(1);
        dataSource.setMaxStatements(100);

        int numThreads = Runtime.getRuntime().availableProcessors() * 2;
        executorService = Executors.newFixedThreadPool(numThreads);
    }

    private void submitBatch(List<String> batchData, int num) {
        Batch batch = new Batch(num, batchData.size(), batchData);
        executorService.execute(() -> {
            try {
                processBatch(batch);
                logger.info("Processed batch #{} with size: {}", batch.getNum(), batch.getSize());
            } catch (Exception e) {
                logger.error("Error processing batch #{}: {}", batch.getNum(), ExceptionUtils.getStackTrace(e));
            }
        });
        logger.info("Submitted batch #{} with size: {}", batch.getNum(), batch.getSize());
    }

    private void processBatch(Batch batch) throws SQLException, InterruptedException {
        // Simulate processing time
        Thread.sleep(2000);

        // Implement your actual Redshift batch processing logic here
//        try (Connection connection = dataSource.getConnection()) {
//            connection.setAutoCommit(false);
//            String sql = "INSERT INTO test (id, name, value) VALUES (?, ?, ?)";
//            try (PreparedStatement statement = connection.prepareStatement(sql)) {
//                for (String record : batch.getData()) {
//                    String[] fields = record.split(",");
//                    int id = Integer.parseInt(fields[0]);
//                    String name = fields[1];
//                    String newValue = fields[2];
//                    statement.setInt(1, id);
//                    statement.setString(2, name);
//                    statement.setString(3, newValue);
//                    statement.addBatch();
//                }
//                statement.executeBatch();
//                connection.commit();
//            }
//        } catch (SQLException e) {
//            logger.error("SQLException during batch processing: {}", ExceptionUtils.getStackTrace(e));
//            throw e;
//        }
    }

    @Override
    public synchronized void invoke(String value, Context context) throws Exception {
        buffer.add(value);
        if (buffer.size() >= BATCH_SIZE) {
            submitBatch(new ArrayList<>(buffer), batchNum.incrementAndGet());
            buffer.clear();
        }
    }

    @Override
    public void close() throws Exception {
        if (!buffer.isEmpty()) {
            logger.info("Processing last batch...");
            submitBatch(new ArrayList<>(buffer), batchNum.incrementAndGet());
            buffer.clear();
        }
        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.MINUTES)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
            logger.info("Executor service shut down.");
        }
        if (dataSource != null) {
            dataSource.close();
        }
        super.close();
    }

    private static class Batch {
        private final int num;
        private final int size;
        private final List<String> data;

        public Batch(int num, int size, List<String> data) {
            this.num = num;
            this.size = size;
            this.data = data;
        }

        public int getNum() {
            return num;
        }

        public int getSize() {
            return size;
        }

        public List<String> getData() {
            return data;
        }
    }
}
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
    private List<String> buffer;
    private static final int BATCH_SIZE = 1000;
    private final AtomicInteger batchNum = new AtomicInteger(0);

    static class Batch {
        int num;
        int size;
        List<String> data;

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
        buffer = new ArrayList<>();
    }

    private synchronized void submitBatch(List<String> list, int num) {
        List<String> batchData = new ArrayList<>(list);
        Batch b = new Batch(num, batchData.size(), batchData);
        executorService.execute(() -> {
            for (String record : b.getData()) {
                String[] fields = record.split(",");
                int id = Integer.parseInt(fields[0]);
                String name = fields[1];
                String newValue = fields[2];
                //logger.info("id: {}, name: {}, value: {}", id, name, newValue);
            }

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
//            try (Connection connection = dataSource.getConnection()) {
//                String sql = "INSERT INTO test (id, name, value) VALUES (?, ?, ?)";
//                try (PreparedStatement statement = connection.prepareStatement(sql)) {
//                    for (String record : batch) {
//                        String[] fields = record.split(",");
//                        int id = Integer.parseInt(fields[0]);
//                        String name = fields[1];
//                        String newValue = fields[2];
//                        statement.setInt(1, id);
//                        statement.setString(2, name);
//                        statement.setString(3, newValue);
//                        statement.addBatch();
//                    }
//                    statement.executeBatch();
//                }
//            } catch (SQLException e) {
//                logger.error("Loading batch failed! Details: {}", ExceptionUtils.getStackTrace(e));
//            }
            logger.info("processed batch #{} with size: {}", b.getNum(), b.getSize());
        });
        logger.info("Submitted batch #{} with size: {}", b.getNum(), b.getSize());
    }

    @Override
    public synchronized void invoke(String value, Context context) throws Exception {
        buffer.add(value);
        if (buffer.size() >= BATCH_SIZE) {
            submitBatch(buffer, batchNum.incrementAndGet());
            buffer.clear();
        }
    }

    @Override
    public void close() throws Exception {
        if (!buffer.isEmpty()) {
            logger.info("Processing last batch...");
            submitBatch(buffer, batchNum.incrementAndGet()); // Process remaining records
        }
        if (executorService != null) {
            // Initiate shutdown
            executorService.shutdown();
            try {
                // Wait for existing tasks to complete
                boolean terminated = executorService.awaitTermination(5, TimeUnit.MINUTES);
                if (!terminated) {
                    logger.error("Some tasks were not terminated within the timeout.");
                    // Optionally force shutdown if needed
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // Force shutdown in case of interruption
                executorService.shutdownNow();
            }
            logger.info("Executor service shut down.");
        }
        if (dataSource != null) {
            dataSource.close();
        }
        super.close();
    }
}
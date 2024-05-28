package org.example;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class RedshiftSinkBatchAsync extends RichSinkFunction<String> {
    private static final Logger logger = LogManager.getLogger(RedshiftSinkBatchAsync.class);

    private transient ComboPooledDataSource dataSource;
    private transient List<String> buffer;
    private static final int BATCH_SIZE = 1000;

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

        buffer = new ArrayList<>();
    }

    @Override
    public void invoke(String value, SinkFunction.Context context) throws Exception {
        if ("SHUTDOWN".equalsIgnoreCase(value)) {
            logger.info("Shutdown signal received. Flushing remaining records...");
            flush();
        } else {
            buffer.add(value);
            if (buffer.size() >= BATCH_SIZE) {
                flush();
            }
        }
    }

    @Override
    public void close() throws Exception {
        logger.info("about to close sink...");
        if (!buffer.isEmpty()) {
            logger.info("Loading last batch...");
            flush();
        }
        if (dataSource != null) {
            dataSource.close();
        }
        super.close();
    }

    private void flush() throws Exception {
        logger.info("Attempting to load batch with size: {}", buffer.size());
        if (!buffer.isEmpty()) {
            pretendLoad();
            buffer.clear();
            logger.info("loaded");
        } else {
            logger.info("Nothing to load. Batch is empty");
        }
    }

    private void pretendLoad() throws Exception {
        Thread.sleep(2000);
    }

    private void loadToRedShift() throws SQLException {
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            String sql = "INSERT INTO your_table (id, timestamp, name, value) VALUES (?, ?, ?, ?)";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                for (String record : buffer) {
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
package org.example.sink;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serial;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class JDBCSink extends RichSinkFunction<String> {
    private static final Logger logger = LogManager.getLogger(JDBCSink.class);

    @Serial
    private static final long serialVersionUID = 1L;
    private transient ComboPooledDataSource dataSource;
    private List<String> buffer;
    private static final int BATCH_SIZE = 1000;
    private String loadid;

    public static String toShortString(UUID uuid) {
        // Convert UUID to byte array
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
        byteBuffer.putLong(uuid.getMostSignificantBits());
        byteBuffer.putLong(uuid.getLeastSignificantBits());

        // Encode byte array to Base64
        return Base64.getUrlEncoder().withoutPadding().encodeToString(byteBuffer.array());
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    private void init() throws Exception {
        dataSource = new ComboPooledDataSource();
        //dataSource.setDriverClass("com.amazon.redshift.jdbc42.Driver");
        //dataSource.setJdbcUrl("jdbc:redshift://your-redshift-cluster:5439/your-database");
        dataSource.setDriverClass("org.postgresql.Driver");
        dataSource.setJdbcUrl("jdbc:postgresql://localhost:5432/postgres");
        dataSource.setUser("postgres");
        dataSource.setPassword("password");
        dataSource.setMaxPoolSize(20);
        dataSource.setMinPoolSize(1);
        dataSource.setAcquireIncrement(1);
        dataSource.setMaxStatements(100);

        buffer = new ArrayList<>();
        loadid = toShortString(UUID.randomUUID());
        logger.info("{} - Initialized", loadid);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        if(value.equalsIgnoreCase("INIT")) {
            init();
        } else if(value.equalsIgnoreCase("SHUTDOWN")) {
            List<String> batch = new ArrayList<>(buffer);
            loadToDB(batch);
            buffer.clear();
            cleanup();
        } else {
            buffer.add(value);
            if(buffer.size() >= BATCH_SIZE) {
                logger.info("{} - Attempting to Load {} records", loadid,  buffer.size());
                List<String> batch = new ArrayList<>(buffer);
                loadToDB(batch);
                buffer.clear();
            }
        }
    }

    private void loadToDB(List<String> batch) throws SQLException {
        if(!batch.isEmpty()) {
            try (Connection connection = dataSource.getConnection()) {
                connection.setAutoCommit(false);
                String sql = "INSERT INTO test (id, timestamp, name, value) VALUES (?, ?, ?, ?)";
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    for (String record : batch) {
                        String[] fields = record.split(",");
                        int id = Integer.parseInt(fields[0]);
                        long timestamp = Long.parseLong(fields[1]);
                        String name = fields[2];
                        String value = fields[3];
                        statement.setInt(1, id);
                        statement.setTimestamp(2, new Timestamp(timestamp));
                        statement.setString(3, name);
                        statement.setString(4, value);
                        statement.addBatch();
                    }
                    statement.executeBatch();
                    connection.commit();
                    logger.info("{} - Loaded: {}", loadid, batch.size());
                } catch (SQLException e) {
                    connection.rollback();
                    throw e;
                } finally {
                    connection.close();
                }
            }
        }
    }

    private void cleanup() {
        if (dataSource != null) {
            dataSource.close();
        }
        logger.info("{} - cleanup complete", loadid);
    }

    @Override
    public void close() throws Exception {
        cleanup();
        super.close();
    }
}

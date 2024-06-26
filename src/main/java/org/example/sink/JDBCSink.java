package org.example.sink;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serial;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class JDBCSink extends RichSinkFunction<String> {
    private static final Logger logger = LogManager.getLogger(JDBCSink.class);

    @Serial
    private static final long serialVersionUID = 1L;
    private transient ComboPooledDataSource dataSource;
    private List<String> buffer;
    private final int batchSize;
    private String loadid;
    private transient ExecutorService executorService;
    private transient CountDownLatch shutdownLatch;
    private long start;
    private Connection connection;
    private PreparedStatement statement;
    private AtomicLong total;

    public static String toShortString(UUID uuid) {
        // Convert UUID to byte array
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
        byteBuffer.putLong(uuid.getMostSignificantBits());
        byteBuffer.putLong(uuid.getLeastSignificantBits());

        // Encode byte array to Base64
        return Base64.getUrlEncoder().withoutPadding().encodeToString(byteBuffer.array());
    }

    public static String getHumanReadableTimeDifference(long startTime, long endTime) {
        if (endTime < startTime) {
            throw new IllegalArgumentException("End time must be greater than start time");
        }

        long durationInNanos = endTime - startTime;

        long days = TimeUnit.NANOSECONDS.toDays(durationInNanos);
        durationInNanos -= TimeUnit.DAYS.toNanos(days);

        long hours = TimeUnit.NANOSECONDS.toHours(durationInNanos);
        durationInNanos -= TimeUnit.HOURS.toNanos(hours);

        long minutes = TimeUnit.NANOSECONDS.toMinutes(durationInNanos);
        durationInNanos -= TimeUnit.MINUTES.toNanos(minutes);

        long seconds = TimeUnit.NANOSECONDS.toSeconds(durationInNanos);
        durationInNanos -= TimeUnit.SECONDS.toNanos(seconds);

        long milliseconds = TimeUnit.NANOSECONDS.toMillis(durationInNanos);
        durationInNanos -= TimeUnit.MILLISECONDS.toNanos(milliseconds);

        long microseconds = TimeUnit.NANOSECONDS.toMicros(durationInNanos);
        durationInNanos -= TimeUnit.MICROSECONDS.toNanos(microseconds);

        long nanoseconds = durationInNanos;

        StringBuilder result = new StringBuilder();
        if (days > 0) {
            result.append(days).append(" days ");
        }
        if (hours > 0) {
            result.append(hours).append(" hrs ");
        }
        if (minutes > 0) {
            result.append(minutes).append(" mins ");
        }
        if (seconds > 0) {
            result.append(seconds).append(" secs ");
        }
        if (milliseconds > 0) {
            result.append(milliseconds).append(" ms ");
        }
//        if (microseconds > 0) {
//            result.append(microseconds).append(" µs ");
//        }
//        if (nanoseconds > 0 || result.isEmpty()) {
//            result.append(nanoseconds).append(" ns");
//        }

        return result.toString().trim();
    }

    public JDBCSink(int batchSize) {
        this.batchSize = batchSize;
        this.buffer = new ArrayList<>();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    void init() throws Exception {
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

        buffer = new ArrayList<>(); // Adjust the buffer size as needed
        loadid = toShortString(UUID.randomUUID());
        logger.info("{} - Initialized", loadid);
        total = new AtomicLong(0);

        start = System.nanoTime();
    }


    @Override
    public void invoke(String value, Context context) throws Exception {
        if(value.equalsIgnoreCase("INIT")) {
            init();
        } else if(value.equalsIgnoreCase("SHUTDOWN")) {
            handleEndOfStream();
        } else if(value.split(",").length >= 4) {
            buffer.add(value);
            if (buffer.size() >= batchSize) {
                flush();
            }
        }
    }

    private void flush() throws SQLException {
        if(!buffer.isEmpty()) {
            long start = System.nanoTime();
            connection = dataSource.getConnection();
            statement = connection.prepareStatement("INSERT INTO test (id, timestamp, name, value) VALUES (?, ?, ?, ?)" +
                    "ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value");
            connection.setAutoCommit(false);
            for (String record : buffer) {
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
            logger.info("{} - Loaded {} records in {}",
                    loadid, buffer.size(),  getHumanReadableTimeDifference(start, System.nanoTime()));
            total.addAndGet(buffer.size());
            buffer.clear();
            if (connection != null) {
                connection.close();
            }
        } else {
            logger.info("{} - Buffer is empty. Nothing to load.", loadid);
        }
    }

    void handleEndOfStream() throws Exception {
        logger.info("{} - End of Stream signal received. Flushing last batch...", loadid);
        flush(); // Flush any remaining records
        if (statement != null) {
            statement.close();
        }
        if (dataSource != null) {
            dataSource.close();
        }
        logger.info("{} - cleanup complete. Total time: {}. Total records Loaded: {}",
                loadid, getHumanReadableTimeDifference(start, System.nanoTime()), total.get());
    }

    @Override
    public void close() throws Exception {
        super.close();
        handleEndOfStream();
    }
}

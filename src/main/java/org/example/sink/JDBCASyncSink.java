package org.example.sink;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.units.qual.A;

import java.io.Serial;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class JDBCASyncSink extends RichSinkFunction<String> {
    private static final Logger logger = LogManager.getLogger(JDBCASyncSink.class);

    @Serial
    private static final long serialVersionUID = 1L;
    private transient ComboPooledDataSource dataSource;
    private LinkedBlockingQueue<String> buffer;
    private static final int BATCH_SIZE = 50_000;
    private String loadid;
    private transient ExecutorService executorService;
    private transient CountDownLatch shutdownLatch;
    private long start;
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

        buffer = new LinkedBlockingQueue<>(BATCH_SIZE * 10); // Adjust the buffer size as needed
        executorService = Executors.newFixedThreadPool(Math.max(4, Runtime.getRuntime().availableProcessors()));
        shutdownLatch = new CountDownLatch(1);
        loadid = toShortString(UUID.randomUUID());
        logger.info("{} - Initialized", loadid);

        executorService.submit(this::flushBuffer);
        start = System.nanoTime();
        total = new AtomicLong(0);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        if(value.equalsIgnoreCase("INIT")) {
            init();
        } else if(value.equalsIgnoreCase("SHUTDOWN")) {
            cleanup();
            shutdownLatch.countDown();
        } else {
            buffer.put(value);
        }
    }

    private void flushBuffer() {
        try {
            while (!Thread.currentThread().isInterrupted() || !buffer.isEmpty()) {
                flush();
                Thread.sleep(500); // Reduce the sleep interval to 500ms or lower
            }
            logger.info("Buffer empty. Exiting background flushBuffer...");
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
            submitBatch(batch);
        }
    }

    private  void submitBatch(List<String> batch) throws Exception {
        if (!batch.isEmpty()) {
            executorService.submit(() -> {
                try {
                    loadToDB(batch);
                } catch (Exception e) {
                    logger.error("{} - Error in loading to Redshift: ", loadid, e);
                }
            });
            logger.info("{} - submitted batch with size: {}", loadid, batch.size());
        } else {
            logger.info("{} - Nothing to load. batch is empty", loadid);
        }
    }

    private void loadToDB(List<String> batch) throws SQLException {
        if(!batch.isEmpty()) {
            long start = System.nanoTime();
            logger.debug("{} - Attempting to load batch with size: {}", loadid, batch.size());
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
                    logger.info("{} - Loaded in {}: {}",
                            loadid, getHumanReadableTimeDifference(start, System.nanoTime()), batch.size());
                    total.addAndGet(batch.size());
                } catch (SQLException e) {
                    connection.rollback();
                    throw e;
                } finally {
                    connection.close();
                }
            }
        } else {
            logger.info("{} - Nothing to load. Batch is empty", loadid);
        }
    }

    private void cleanup() throws Exception {
        if (!buffer.isEmpty()) {
            submitBatch(buffer.stream().toList());
        }
        executorService.shutdown();
        // Wait with a timeout and log the progress
        if (!executorService.awaitTermination(2, TimeUnit.SECONDS)) {
            logger.warn("Tasks are taking too long to complete. Forcing shutdown...");
            // Force shutdown if tasks are not completed within the timeout
            executorService.shutdownNow();
            // Await termination again to make sure all threads are terminated
            if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                logger.error("Executor service did not terminate.");
            }
        } else {
            logger.info("All tasks completed within the timeout.");
        }
        if (dataSource != null) {
            dataSource.close();
        }
        logger.info("{} - cleanup complete. Total time: {}. Total records Loaded: {}",
                loadid, getHumanReadableTimeDifference(start, System.nanoTime()), total.get());
    }

    @Override
    public void close() throws Exception {
        cleanup();
        shutdownLatch.await();
        super.close();
    }
}

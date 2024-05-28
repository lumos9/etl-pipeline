package org.example.sink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class LoggingSink<T> implements SinkFunction<T> {
    private static final Logger logger = LogManager.getLogger(LoggingSink.class);

    @Override
    public void invoke(T value, Context context) throws Exception {
        if (value instanceof List) {
            List<?> list = (List<?>) value;
            for (Object record : list) {
                logger.info("Transformed record: {}", record);
            }
        } else {
            logger.info("Transformed record: {}", value);
        }
    }
}
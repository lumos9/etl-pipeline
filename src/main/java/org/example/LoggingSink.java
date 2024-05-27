package org.example;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LoggingSink<T> implements SinkFunction<T> {
    private static final Logger logger = LogManager.getLogger(LoggingSink.class);

    @Override
    public void invoke(T value, Context context) throws Exception {
        logger.info("Transformed record: {}", value);
    }
}

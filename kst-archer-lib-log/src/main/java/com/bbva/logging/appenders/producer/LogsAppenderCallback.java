package com.bbva.logging.appenders.producer;

import com.bbva.common.producers.ProducerCallback;
import org.apache.log4j.helpers.LogLog;

public class LogsAppenderCallback implements ProducerCallback {

    @Override
    public void onCompletion(final Object id, final Exception exception) {
        if (exception != null) {
            LogLog.error("Failed to send log", exception);
        }
    }
}

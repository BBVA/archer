package com.bbva.logging.appenders.producer;

import com.bbva.common.producers.ProducerCallback;
import org.apache.log4j.helpers.LogLog;

/**
 * Callback to manage the log production
 */
public class LogsAppenderCallback implements ProducerCallback {

    /**
     * Trace the errors in the production
     *
     * @param id        object id
     * @param exception exception in the production
     */
    @Override
    public void onCompletion(final Object id, final Exception exception) {
        if (exception != null) {
            LogLog.error("Failed to send log", exception);
        }
    }
}

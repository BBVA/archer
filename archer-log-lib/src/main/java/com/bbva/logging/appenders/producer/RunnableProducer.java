package com.bbva.logging.appenders.producer;

import com.bbva.avro.LogEvent;
import com.bbva.common.producers.CachedProducer;
import com.bbva.common.producers.record.PRecord;
import com.bbva.common.utils.headers.RecordHeaders;

/**
 * Runnable producer
 */
public class RunnableProducer implements Runnable {

    private final LogEvent eventLog;
    private final CachedProducer logsProducer;
    private final LogsAppenderCallback logsAppenderCallback = new LogsAppenderCallback();
    private final String sourceName;

    /**
     * Constructor
     *
     * @param baseName topic name
     * @param logEvent log event
     * @param producer producer
     */
    public RunnableProducer(final String baseName, final LogEvent logEvent, final CachedProducer producer) {
        eventLog = logEvent;
        logsProducer = producer;
        sourceName = baseName;
    }

    /**
     * Save log event
     */
    @Override
    public void run() {
        final PRecord record = new PRecord(
                sourceName, eventLog.getTime().toString(), eventLog, new RecordHeaders());

        logsProducer.add(record, logsAppenderCallback);
    }
}

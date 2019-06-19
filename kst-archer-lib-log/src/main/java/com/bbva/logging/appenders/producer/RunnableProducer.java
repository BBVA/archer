package com.bbva.logging.appenders.producer;

import com.bbva.avro.LogEvent;
import com.bbva.common.producers.CachedProducer;
import com.bbva.common.producers.PRecord;
import com.bbva.common.utils.headers.RecordHeaders;

public class RunnableProducer implements Runnable {

    private final LogEvent eventLog;
    CachedProducer logsProducer;
    private final LogsAppenderCallback logsAppenderCallback = new LogsAppenderCallback();
    String sourceName;

    public RunnableProducer(final String baseName, final LogEvent logEvent, final CachedProducer producer) {
        eventLog = logEvent;
        logsProducer = producer;
        sourceName = baseName;
    }

    @Override
    public void run() {
        final PRecord<String, LogEvent> record = new PRecord<>(
                sourceName, eventLog.getTime().toString(), eventLog, new RecordHeaders());

        logsProducer.add(record, logsAppenderCallback);
    }
}

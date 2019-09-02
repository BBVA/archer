package com.bbva.logging.appenders;

import com.bbva.avro.LogEvent;
import com.bbva.common.config.AppConfiguration;
import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.producers.CachedProducer;
import com.bbva.logging.appenders.producer.RunnableProducer;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.MDC;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.config.plugins.Plugin;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Plugin(name = "LogsAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE)
public class LogsAppender extends AppenderSkeleton {

    public static final String DEFAULT_BASE_NAME = "logs_events";
    private CachedProducer logsProducer;
    private String hostName;
    private String logsSinkName;
    private final ExecutorService executor = Executors.newFixedThreadPool(1);

    @Override
    public void activateOptions() {
        super.activateOptions();

        logsSinkName = System.getenv("LOG_SINK_NAME") != null
                ? System.getenv("LOG_SINK_NAME")
                : DEFAULT_BASE_NAME;
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (final UnknownHostException e) {
            hostName = "UnknownHost";
        }

        final ApplicationConfig applicationConfig = new AppConfiguration().init();
        logsProducer = new CachedProducer(applicationConfig);

    }


    @Override
    public void close() {
        executor.shutdown();
    }


    @Override
    public boolean requiresLayout() {
        return true;
    }

    @Override
    protected void append(final LoggingEvent event) {
        final LogEvent logEvent = LogEvent.newBuilder()
                .setLevel(event.getLevel().toString())
                .setTime(event.getTimeStamp())
                .setHostname(hostName)
                .setThread(event.getThreadName())
                .setName(MDC.get("loggerName").toString())
                .setFunction(MDC.get("msFunction").toString())
                .setMessage(event.getMessage().toString()).build();

        final Runnable produceLog = new RunnableProducer(logsSinkName, logEvent, logsProducer);
        executor.execute(produceLog);

    }

}

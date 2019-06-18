package com.bbva.logging;

import com.bbva.common.config.AppConfiguration;
import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.producers.CachedProducer;
import org.apache.log4j.MDC;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.Properties;

public class LoggerFactory {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LoggerFactory.class);

    private static final String LOG4J_PROPS_PATH = "custom_log4j.properties";
    private final ApplicationConfig applicationConfig;
    CachedProducer logsProducer;

    private static LoggerFactory instance;

    private LoggerFactory() {
        applicationConfig = new AppConfiguration().init();
        logsProducer = new CachedProducer(applicationConfig);
        try {
            final InputStream inputStream =
                    getClass().getClassLoader().getResourceAsStream(LOG4J_PROPS_PATH);
            final Properties logPorperties = new Properties();
            if (inputStream != null) {
                logPorperties.load(inputStream);
            }
            PropertyConfigurator.configure(logPorperties);

            MDC.put("hostName", InetAddress.getLocalHost().getHostName());
            MDC.put("appName",
                    System.getenv("APPLICATION_NAME") != null
                            ? System.getenv("APPLICATION_NAME")
                            : "AppNoConfigured");

        } catch (final IOException e) {
            logger.error("Failed to load log4j properties from file", e);
        }
    }

    public static Logger getLogger(final Class mainClass) {
        if (instance == null) {
            instance = new LoggerFactory();
        }
        return new Logger(org.slf4j.LoggerFactory.getLogger(mainClass));
    }
}

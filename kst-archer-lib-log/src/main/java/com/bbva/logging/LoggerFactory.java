package com.bbva.logging;

import org.apache.log4j.MDC;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.Properties;

public class LoggerFactory {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LoggerFactory.class);

    private static final String LOG4J_PROPS_PATH = "custom_log4j.properties";
    private static final String CUSTOM_LOG_PROPS_PATH = "appender_log4j.properties";

    private static LoggerFactory instance;

    private LoggerFactory() {
        try {
            final Properties logProperties = getConfigByRuntime(LOG4J_PROPS_PATH);

            if (System.getenv("LOG_APPENDER_CONFIG") != null && System.getenv("LOG_APPENDER_CONFIG").indexOf("=") > 0) {
                final String[] appenderConfig = System.getenv("LOG_APPENDER_CONFIG").split("=");
                logProperties.put(String.format("log4j.logger.%s", appenderConfig[0]), appenderConfig[1]);
                logProperties.putAll(getConfigByRuntime(CUSTOM_LOG_PROPS_PATH));
            }

            PropertyConfigurator.configure(logProperties);

            MDC.put("hostName", InetAddress.getLocalHost().getHostName());

        } catch (final IOException e) {
            logger.error("Failed to load log4j properties from file", e);
        }
    }

    private Properties getConfigByRuntime(final String filePath) throws IOException {

        final InputStream inputStream =
                getClass().getClassLoader().getResourceAsStream(filePath);

        final Properties logPorperties = new Properties();

        if (inputStream != null) {
            logPorperties.load(inputStream);
        }

        return logPorperties;
    }

    public static Logger getLogger(final Class mainClass) {
        if (instance == null) {
            instance = new LoggerFactory();
        }
        return new Logger(org.slf4j.LoggerFactory.getLogger(mainClass));
    }
}

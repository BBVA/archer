package com.bbva.logging;

import org.apache.log4j.MDC;
import org.apache.log4j.PropertyConfigurator;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class LoggerFactory {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LoggerFactory.class);

    private static final String LOG4J_PROPS_PATH = "custom_log4j.properties";
    private static final String LOG4J_PROPS_PATH_TEST = "custom_log4j_test.properties";

    private static LoggerFactory instance;

    private LoggerFactory() {
        try {

            PropertyConfigurator.configure(getConfigByRuntime());

            MDC.put("hostName", InetAddress.getLocalHost().getHostName());

        } catch (final IOException e) {
            logger.error("Failed to load log4j properties from file", e);
        }
    }

    private Properties getConfigByRuntime() throws IOException {

        final StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
        final List<StackTraceElement> list = Arrays.asList(stackTrace);

        final String configFile = getConfigFile(list);

        final InputStream inputStream =
                getClass().getClassLoader().getResourceAsStream(configFile);

        final Properties logPorperties = new Properties();
        if (inputStream != null) {
            logPorperties.load(inputStream);
        }

        return logPorperties;
    }

    private String getConfigFile(final List<StackTraceElement> list) {
        for (final StackTraceElement element : list) {
            if (element.getClassName().startsWith("org.junit.")) {
                return LOG4J_PROPS_PATH_TEST;
            }
        }
        return LOG4J_PROPS_PATH;
    }

    public static Logger getLogger(final Class mainClass) {
        if (instance == null) {
            instance = new LoggerFactory();
        }
        return new Logger(org.slf4j.LoggerFactory.getLogger(mainClass));
    }
}

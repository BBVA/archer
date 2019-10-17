package com.bbva.logging;

import com.bbva.common.producers.DefaultProducer;
import com.bbva.common.util.PowermockExtension;
import com.bbva.logging.appenders.LogsAppender;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({System.class, LoggerFactory.class, LogsAppender.class})
public class LoggerFactoryTest {

    @DisplayName("Create logger ok")
    @Test
    public void createLogger() throws Exception {

        PowerMockito.mockStatic(System.class);
        PowerMockito.when(System.getenv("LOG_APPENDER_CONFIG")).thenReturn("com.bbva.logging=ERROR, archerAppender");
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));

        final Logger logger = LoggerFactory.getLogger(LoggerFactoryTest.class);
        logger.error("error msg");
        Assertions.assertNotNull(logger);
    }


    @DisplayName("Create logger with bad appender format ok")
    @Test
    public void createLoggerApopenderWithoutFormat() {

        PowerMockito.mockStatic(System.class);
        PowerMockito.when(System.getenv("LOG_APPENDER_CONFIG")).thenReturn("com.bbva.logging, archerAppender");

        final Logger logger = LoggerFactory.getLogger(LoggerFactoryTest.class);
        logger.error("error msg");
        Assertions.assertNotNull(logger);
    }

    @DisplayName("Create logger without appender ok")
    @Test
    public void createLoggerWithoutAppender() {

        PowerMockito.mockStatic(System.class);
        PowerMockito.when(System.getenv("LOG_APPENDER_CONFIG")).thenReturn(null);

        final Logger logger = LoggerFactory.getLogger(LoggerFactoryTest.class);
        logger.error("error msg");
        Assertions.assertNotNull(logger);
    }
}

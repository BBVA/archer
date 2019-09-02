package com.bbva.gateway;

import com.bbva.logging.Logger;
import org.apache.log4j.MDC;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.slf4j.LoggerFactory;

@RunWith(JUnit5.class)
public class LoggerTest {

    @DisplayName("Create logger and intercept ok")
    @Test
    public void createLoggerAndIntercept() {

        final Logger logger = new Logger(LoggerFactory.getLogger("test"));
        logger.intercept();

        Assertions.assertEquals("test", MDC.get("loggerName"));
        Assertions.assertEquals("invoke0", MDC.get("msFunction"));
    }

    @DisplayName("Create logger and trace ok")
    @Test
    public void createLoggerAndTraceOk() {

        final Logger logger = com.bbva.logging.LoggerFactory.getLogger(getClass());
        logger.info("info msg");

        Assertions.assertNotNull(logger);
    }

}

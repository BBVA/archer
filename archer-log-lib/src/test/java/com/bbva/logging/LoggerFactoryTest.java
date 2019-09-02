package com.bbva.logging;

import com.bbva.common.util.PowermockExtension;
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
@PrepareForTest({System.class, LoggerFactory.class})
public class LoggerFactoryTest {

    @DisplayName("Create logger ok")
    @Test
    public void createLogger() {

        PowerMockito.mockStatic(System.class);
        PowerMockito.when(System.getenv("LOG_APPENDER_CONFIG")).thenReturn("com.bbva.logging=ERROR, archerAppender");

        final Logger logger = LoggerFactory.getLogger(LoggerFactoryTest.class);
        logger.error("error msg");
        Assertions.assertNotNull(logger);
    }

}

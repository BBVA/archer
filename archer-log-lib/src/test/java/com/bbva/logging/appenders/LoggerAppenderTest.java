package com.bbva.logging.appenders;

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
@PrepareForTest({System.class, LogsAppender.class})
public class LoggerAppenderTest {

    @DisplayName("Create appender and close ok")
    @Test
    public void createAndCloseOk() {
        final LogsAppender appender = new LogsAppender();
        appender.close();
        Assertions.assertNotNull(appender);
    }

    @DisplayName("Create appender and activate options ok")
    @Test
    public void appenderActivateOptions() {
        PowerMockito.mockStatic(System.class);
        PowerMockito.when(System.getenv("LOG_SINK_NAME")).thenReturn("test");

        final LogsAppender appender = new LogsAppender();
        appender.activateOptions();

        Assertions.assertNotNull(appender);
    }
}

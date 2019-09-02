package com.bbva.logging.appenders;

import com.bbva.common.exceptions.ApplicationException;
import com.bbva.logging.appenders.producer.LogsAppenderCallback;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;

@RunWith(JUnit5.class)
public class LoggerAppenderCallbackTest {

    @DisplayName("Create callback and complete ok")
    @Test
    public void callbackOk() {
        final LogsAppenderCallback callback = new LogsAppenderCallback();
        callback.onCompletion("id", null);
        Assertions.assertNotNull(callback);
    }

    @DisplayName("Create callback and complete with exception")
    @Test
    public void callbackException() {
        final LogsAppenderCallback callback = new LogsAppenderCallback();
        callback.onCompletion("id", new ApplicationException());
        Assertions.assertNotNull(callback);
    }

}

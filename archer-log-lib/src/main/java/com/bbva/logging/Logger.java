package com.bbva.logging;

import com.bbva.logging.interceptor.LoggerInterceptor;
import org.apache.log4j.MDC;

/**
 * Logger class
 */
public final class Logger extends LoggerInterceptor {

    private static final long serialVersionUID = 8744571658261340864L;

    /**
     * Constructor
     *
     * @param slf4jLogger slf4j logger instance
     */
    public Logger(final org.slf4j.Logger slf4jLogger) {
        super(slf4jLogger);
    }

    /**
     * Intercept the traces and add generic fields
     */
    @Override
    public void intercept() {
        MDC.put("loggerName", getName());
        MDC.put("msFunction", getInvocationMethod());
    }

    private static String getInvocationMethod() {
        final StackTraceElement[] stackTrace = Thread.currentThread()
                .getStackTrace();

        return stackTrace[stackTrace.length > 4 ? 4 : stackTrace.length - 1].getMethodName();
    }
}

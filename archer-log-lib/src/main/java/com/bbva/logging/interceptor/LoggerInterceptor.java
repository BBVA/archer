package com.bbva.logging.interceptor;

import org.slf4j.Logger;
import org.slf4j.Marker;

import java.io.Serializable;

public abstract class LoggerInterceptor implements Logger, Serializable {

    private static final long serialVersionUID = 7799143653351702744L;
    /**
     * Underlying logger
     */
    private final transient Logger logger;


    /**
     * Creates the interceptor
     *
     * @param slf4jLogger the logger to wrap
     */
    public LoggerInterceptor(final Logger slf4jLogger) {
        this.logger = slf4jLogger;
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#getName()
     */
    @Override
    public String getName() {
        return logger.getName();
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#isTraceEnabled()
     */
    @Override
    public boolean isTraceEnabled() {
        return logger.isTraceEnabled();
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#trace(String)
     */
    @Override
    public void trace(final String msg) {
        intercept();
        logger.trace(msg);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#trace(String, Object)
     */
    @Override
    public void trace(final String format, final Object arg) {
        intercept();
        logger.trace(format, arg);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#trace(String, Object,
     * Object)
     */
    @Override
    public void trace(final String format, final Object arg1, final Object arg2) {
        intercept();
        logger.trace(format, arg1, arg2);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#trace(String, Object[])
     */
    @Override
    public void trace(final String format, final Object... arguments) {
        intercept();
        logger.trace(format, arguments);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#trace(String, Throwable)
     */
    @Override
    public void trace(final String msg, final Throwable t) {
        intercept();
        logger.trace(msg, t);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#isTraceEnabled(Marker)
     */
    @Override
    public boolean isTraceEnabled(final Marker marker) {
        return logger.isTraceEnabled(marker);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#trace(Marker, String)
     */
    @Override
    public void trace(final Marker marker, final String msg) {
        intercept();
        logger.trace(marker, msg);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#trace(Marker, String,
     * Object)
     */
    @Override
    public void trace(final Marker marker, final String format, final Object arg) {
        intercept();
        logger.trace(marker, format, arg);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#trace(Marker, String,
     * Object, Object)
     */
    @Override
    public void trace(final Marker marker, final String format, final Object arg1, final Object arg2) {
        intercept();
        logger.trace(marker, format, arg1, arg2);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#trace(Marker, String,
     * Object[])
     */
    @Override
    public void trace(final Marker marker, final String format, final Object... argArray) {
        intercept();
        logger.trace(marker, format, argArray);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#trace(Marker, String,
     * Throwable)
     */
    @Override
    public void trace(final Marker marker, final String msg, final Throwable t) {
        intercept();
        logger.trace(marker, msg, t);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#isDebugEnabled()
     */
    @Override
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#debug(String)
     */
    @Override
    public void debug(final String msg) {
        intercept();
        logger.debug(msg);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#debug(String, Object)
     */
    @Override
    public void debug(final String format, final Object arg) {
        intercept();
        logger.debug(format, arg);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#debug(String, Object,
     * Object)
     */
    @Override
    public void debug(final String format, final Object arg1, final Object arg2) {
        intercept();
        logger.debug(format, arg1, arg2);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#debug(String, Object[])
     */
    @Override
    public void debug(final String format, final Object... arguments) {
        intercept();
        logger.debug(format, arguments);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#debug(String, Throwable)
     */
    @Override
    public void debug(final String msg, final Throwable t) {
        intercept();
        logger.debug(msg, t);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#isDebugEnabled(Marker)
     */
    @Override
    public boolean isDebugEnabled(final Marker marker) {
        return logger.isDebugEnabled(marker);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#debug(Marker, String)
     */
    @Override
    public void debug(final Marker marker, final String msg) {
        intercept();
        logger.debug(marker, msg);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#debug(Marker, String,
     * Object)
     */
    @Override
    public void debug(final Marker marker, final String format, final Object arg) {
        intercept();
        logger.debug(marker, format, arg);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#debug(Marker, String,
     * Object, Object)
     */
    @Override
    public void debug(final Marker marker, final String format, final Object arg1, final Object arg2) {
        intercept();
        logger.debug(marker, format, arg1, arg2);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#debug(Marker, String,
     * Object[])
     */
    @Override
    public void debug(final Marker marker, final String format, final Object... arguments) {
        intercept();
        logger.debug(marker, format, arguments);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#debug(Marker, String,
     * Throwable)
     */
    @Override
    public void debug(final Marker marker, final String msg, final Throwable t) {
        intercept();
        logger.debug(marker, msg, t);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#isInfoEnabled()
     */
    @Override
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#info(String)
     */
    @Override
    public void info(final String msg) {
        intercept();
        logger.info(msg);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#info(String, Object)
     */
    @Override
    public void info(final String format, final Object arg) {
        intercept();
        logger.info(format, arg);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#info(String, Object,
     * Object)
     */
    @Override
    public void info(final String format, final Object arg1, final Object arg2) {
        intercept();
        logger.info(format, arg1, arg2);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#info(String, Object[])
     */
    @Override
    public void info(final String format, final Object... arguments) {
        intercept();
        logger.info(format, arguments);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#info(String, Throwable)
     */
    @Override
    public void info(final String msg, final Throwable t) {
        intercept();
        logger.info(msg, t);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#isInfoEnabled(Marker)
     */
    @Override
    public boolean isInfoEnabled(final Marker marker) {
        return logger.isInfoEnabled(marker);

    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#info(Marker, String)
     */
    @Override
    public void info(final Marker marker, final String msg) {
        intercept();
        logger.info(marker, msg);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#info(Marker, String,
     * Object)
     */
    @Override
    public void info(final Marker marker, final String format, final Object arg) {
        intercept();
        logger.info(marker, format, arg);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#info(Marker, String,
     * Object, Object)
     */
    @Override
    public void info(final Marker marker, final String format, final Object arg1, final Object arg2) {
        intercept();
        logger.info(marker, format, arg1, arg2);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#info(Marker, String,
     * Object[])
     */
    @Override
    public void info(final Marker marker, final String format, final Object... arguments) {
        intercept();
        logger.info(marker, format, arguments);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#info(Marker, String,
     * Throwable)
     */
    @Override
    public void info(final Marker marker, final String msg, final Throwable t) {
        intercept();
        logger.info(marker, msg, t);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#isWarnEnabled()
     */
    @Override
    public boolean isWarnEnabled() {
        return logger.isWarnEnabled();
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#warn(String)
     */
    @Override
    public void warn(final String msg) {
        intercept();
        logger.warn(msg);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#warn(String, Object)
     */
    @Override
    public void warn(final String format, final Object arg) {
        intercept();
        logger.warn(format, arg);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#warn(String, Object[])
     */
    @Override
    public void warn(final String format, final Object... arguments) {
        intercept();
        logger.warn(format, arguments);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#warn(String, Object,
     * Object)
     */
    @Override
    public void warn(final String format, final Object arg1, final Object arg2) {
        intercept();
        logger.warn(format, arg1, arg2);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#warn(String, Throwable)
     */
    @Override
    public void warn(final String msg, final Throwable t) {
        intercept();
        logger.warn(msg, t);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#isWarnEnabled(Marker)
     */
    @Override
    public boolean isWarnEnabled(final Marker marker) {
        return logger.isWarnEnabled(marker);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#warn(Marker, String)
     */
    @Override
    public void warn(final Marker marker, final String msg) {
        intercept();
        logger.warn(marker, msg);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#warn(Marker, String,
     * Object)
     */
    @Override
    public void warn(final Marker marker, final String format, final Object arg) {
        intercept();
        logger.warn(marker, format, arg);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#warn(Marker, String,
     * Object, Object)
     */
    @Override
    public void warn(final Marker marker, final String format, final Object arg1, final Object arg2) {
        intercept();
        logger.warn(marker, format, arg1, arg2);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#warn(Marker, String,
     * Object[])
     */
    @Override
    public void warn(final Marker marker, final String format, final Object... arguments) {
        intercept();
        logger.warn(marker, format, arguments);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#warn(Marker, String,
     * Throwable)
     */
    @Override
    public void warn(final Marker marker, final String msg, final Throwable t) {
        intercept();
        logger.warn(marker, msg, t);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#isErrorEnabled()
     */
    @Override
    public boolean isErrorEnabled() {
        return logger.isErrorEnabled();
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#error(String)
     */
    @Override
    public void error(final String msg) {
        intercept();
        logger.error(msg);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#error(String, Object)
     */
    @Override
    public void error(final String format, final Object arg) {
        intercept();
        logger.error(format, arg);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#error(String, Object,
     * Object)
     */
    @Override
    public void error(final String format, final Object arg1, final Object arg2) {
        intercept();
        logger.error(format, arg1, arg2);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#error(String, Object[])
     */
    @Override
    public void error(final String format, final Object... arguments) {
        intercept();
        logger.error(format, arguments);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#error(String, Throwable)
     */
    @Override
    public void error(final String msg, final Throwable t) {
        intercept();
        logger.error(msg, t);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#isErrorEnabled(Marker)
     */
    @Override
    public boolean isErrorEnabled(final Marker marker) {
        return logger.isErrorEnabled(marker);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#error(Marker, String)
     */
    @Override
    public void error(final Marker marker, final String msg) {
        intercept();
        logger.error(marker, msg);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#error(Marker, String,
     * Object)
     */
    @Override
    public void error(final Marker marker, final String format, final Object arg) {
        intercept();
        logger.error(marker, format, arg);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#error(Marker, String,
     * Object, Object)
     */
    @Override
    public void error(final Marker marker, final String format, final Object arg1, final Object arg2) {
        intercept();
        logger.error(marker, format, arg1, arg2);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#error(Marker, String,
     * Object[])
     */
    @Override
    public void error(final Marker marker, final String format, final Object... arguments) {
        intercept();
        logger.error(marker, format, arguments);
    }


    /**
     * {@inheritDoc}
     *
     * @see Logger#error(Marker, String,
     * Throwable)
     */
    @Override
    public void error(final Marker marker, final String msg, final Throwable t) {
        intercept();
        logger.error(marker, msg, t);
    }

    public abstract void intercept();

}

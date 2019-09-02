package com.bbva.logging.interceptor;

import com.bbva.logging.Logger;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.slf4j.Marker;

import java.util.Iterator;

@RunWith(JUnit5.class)
public class LoggerInterceptorTest {

    private static final Marker marker = createMarker();

    @DisplayName("Create logger and check info traces")
    @Test
    public void infoTraces() {

        final Logger logger = com.bbva.logging.LoggerFactory.getLogger(getClass());
        logger.info("info msg");
        logger.info("info msg", new Throwable());
        logger.info("test formated %s", "test");
        logger.info("test formated %s %s", "test", "test2");
        logger.info("test formated %s %s", new Object[]{"test", "test2"});
        logger.info(marker, "info msg");
        logger.info(marker, "info msg", new Throwable());
        logger.info(marker, "test formated %s", "test");
        logger.info(marker, "test formated %s %s", "test", "test2");
        logger.info(marker, "test formated %s %s", new Object[]{"test", "test2"});

        Assertions.assertTrue(logger.isInfoEnabled());
        Assertions.assertTrue(logger.isInfoEnabled(marker));
    }

    @DisplayName("Create logger and check warm traces")
    @Test
    public void warnTraces() {

        final Logger logger = com.bbva.logging.LoggerFactory.getLogger(getClass());
        logger.warn("info msg");
        logger.warn("info msg", new Throwable());
        logger.warn("test formated %s", "test");
        logger.warn("test formated %s %s", "test", "test2");
        logger.warn("test formated %s %s", new Object[]{"test", "test2"});
        logger.warn(marker, "info msg");
        logger.warn(marker, "info msg", new Throwable());
        logger.warn(marker, "test formated %s", "test");
        logger.warn(marker, "test formated %s %s", "test", "test2");
        logger.warn(marker, "test formated %s %s", new Object[]{"test", "test2"});

        Assertions.assertTrue(logger.isWarnEnabled());
        Assertions.assertTrue(logger.isWarnEnabled(marker));
    }

    @DisplayName("Create logger and check error traces")
    @Test
    public void errorTraces() {

        final Logger logger = com.bbva.logging.LoggerFactory.getLogger(getClass());
        logger.error("info msg");
        logger.error("info msg", new Throwable());
        logger.error("test formated %s", "test");
        logger.error("test formated %s %s", "test", "test2");
        logger.error("test formated %s %s", new Object[]{"test", "test2"});
        logger.error(marker, "info msg");
        logger.error(marker, "info msg", new Throwable());
        logger.error(marker, "test formated %s", "test");
        logger.error(marker, "test formated %s %s", "test", "test2");
        logger.error(marker, "test formated %s %s", new Object[]{"test", "test2"});

        Assertions.assertTrue(logger.isErrorEnabled());
        Assertions.assertTrue(logger.isErrorEnabled(marker));
    }

    @DisplayName("Create logger and check debug traces")
    @Test
    public void debugTraces() {

        final Logger logger = com.bbva.logging.LoggerFactory.getLogger(getClass());
        logger.debug("info msg");
        logger.debug("info msg", new Throwable());
        logger.debug("test formated %s", "test");
        logger.debug("test formated %s %s", "test", "test2");
        logger.debug("test formated %s %s", new Object[]{"test", "test2"});
        logger.debug(marker, "info msg");
        logger.debug(marker, "info msg", new Throwable());
        logger.debug(marker, "test formated %s", "test");
        logger.debug(marker, "test formated %s %s", "test", "test2");
        logger.debug(marker, "test formated %s %s", new Object[]{"test", "test2"});

        Assertions.assertFalse(logger.isDebugEnabled());
        Assertions.assertFalse(logger.isDebugEnabled(marker));
    }

    @DisplayName("Create logger and check debug traces")
    @Test
    public void traceTraces() {

        final Logger logger = com.bbva.logging.LoggerFactory.getLogger(getClass());
        logger.trace("info msg");
        logger.trace("info msg", new Throwable());
        logger.trace("test formated %s", "test");
        logger.trace("test formated %s %s", "test", "test2");
        logger.trace("test formated %s %s", new Object[]{"test", "test2"});
        logger.trace(marker, "info msg");
        logger.trace(marker, "info msg", new Throwable());
        logger.trace(marker, "test formated %s", "test");
        logger.trace(marker, "test formated %s %s", "test", "test2");
        logger.trace(marker, "test formated %s %s", new Object[]{"test", "test2"});

        Assertions.assertFalse(logger.isTraceEnabled());
        Assertions.assertFalse(logger.isTraceEnabled(marker));
    }

    private static Marker createMarker() {
        return new Marker() {
            private static final long serialVersionUID = 3645616979118071633L;

            @Override
            public String getName() {
                return "test-marker";
            }

            @Override
            public void add(final Marker reference) {

            }

            @Override
            public boolean remove(final Marker reference) {
                return false;
            }

            @Override
            public boolean hasChildren() {
                return false;
            }

            @Override
            public boolean hasReferences() {
                return false;
            }

            @Override
            public Iterator<Marker> iterator() {
                return null;
            }

            @Override
            public boolean contains(final Marker other) {
                return false;
            }

            @Override
            public boolean contains(final String name) {
                return false;
            }
        };
    }
}

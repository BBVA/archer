package com.bbva.common.producers.callback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultProducerCallback implements ProducerCallback {

    private static final Logger logger = LoggerFactory.getLogger(DefaultProducerCallback.class);

    @Override
    public void onCompletion(final Object id, final Exception exception) {
        logger.info("Message sent with id {}", id);
    }
}

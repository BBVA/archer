package com.bbva.ddd.util;

import com.bbva.common.exceptions.ApplicationException;
import com.bbva.dataprocessors.ReadableStore;
import com.bbva.dataprocessors.States;
import com.bbva.dataprocessors.exceptions.StoreNotFoundException;
import com.bbva.ddd.domain.HelperDomain;
import kst.logging.Logger;
import kst.logging.LoggerFactory;
import org.apache.kafka.streams.errors.InvalidStateStoreException;

public class StoreUtil {

    private static final Logger logger = LoggerFactory.getLogger(HelperDomain.class);

    public static <K, V> ReadableStore<K, V> getStore(final String store) throws StoreNotFoundException {
        while (true) {
            try {
                return States.get().getStore(store);
            } catch (final InvalidStateStoreException ignored) {
                try {
                    Thread.sleep(500);
                } catch (final InterruptedException e) {
                    logger.error("Problems sleeping the execution", e);
                    throw new ApplicationException("Problems sleeping the execution", e);
                }
            }
        }
    }

}

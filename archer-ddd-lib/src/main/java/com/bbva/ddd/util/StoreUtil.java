package com.bbva.ddd.util;

import com.bbva.common.exceptions.ApplicationException;
import com.bbva.dataprocessors.ReadableStore;
import com.bbva.dataprocessors.States;
import com.bbva.dataprocessors.exceptions.StoreNotFoundException;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;
import org.apache.kafka.streams.errors.InvalidStateStoreException;

/**
 * utility to manage readable stores
 */
public class StoreUtil {

    private static final Logger logger = LoggerFactory.getLogger(StoreUtil.class);

    /**
     * Get store by name in the states
     *
     * @param store store name
     * @param <K>   Key class in the store
     * @param <V>   Value class in the store
     * @return the store
     * @throws StoreNotFoundException store not exists
     */
    public static <K, V> ReadableStore<K, V> getStore(final String store) throws StoreNotFoundException {
        while (true) {
            try {
                return States.get().getStore(store);
            } catch (final InvalidStateStoreException ignored) {
                try {
                    Thread.sleep(500);
                } catch (final InterruptedException e) { //NOSONAR
                    logger.error("Problems sleeping the execution", e);
                    throw new ApplicationException("Problems sleeping the execution", e);
                }
            }
        }
    }

    /**
     * Check if store exists in the states
     *
     * @param store store name
     * @return true/false
     */
    public static boolean checkStoreStatus(final String store) {
        return States.get().getStoreState(store).isRunning();
    }

}

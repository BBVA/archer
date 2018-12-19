package com.bbva.ddd;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.dataprocessors.ReadableStore;
import com.bbva.dataprocessors.States;
import com.bbva.ddd.domain.commands.write.Command;
import com.bbva.ddd.domain.events.write.Event;
import kst.logging.LoggerGen;
import kst.logging.LoggerGenesis;
import org.apache.kafka.streams.errors.InvalidStateStoreException;

import java.util.HashMap;
import java.util.Map;

public class ApplicationServices {

    private static final LoggerGen logger = LoggerGenesis.getLogger(ApplicationServices.class.getName());

    private final ApplicationConfig applicationConfig;
    private static ApplicationServices instance;
    private final Map<String, Command> cacheCommandPersistance;
    private final Map<String, Event> cacheEventLog;
    private boolean replayMode;

    public ApplicationServices(final ApplicationConfig applicationConfig) {
        this.applicationConfig = applicationConfig;
        cacheCommandPersistance = new HashMap<>();
        cacheEventLog = new HashMap<>();
        instance = this;
    }

    public static ApplicationServices get() {
        return instance;
    }

    public ApplicationConfig getConfig() {
        return applicationConfig;
    }

    public boolean isReplayMode() {
        return replayMode;
    }

    public void setReplayMode(final boolean replayMode) {
        this.replayMode = replayMode;
    }

    /**
     * @param store
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K, V> ReadableStore<K, V> getStore(final String store) {
        while (true) {
            try {
                return States.get().getStore(store);
            } catch (final InvalidStateStoreException ignored) {
                // store not yet ready for querying
                try {
                    Thread.sleep(500);
                } catch (final InterruptedException e) {
                    logger.error("Problems sleeping the execution", e);
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * This method is empty
     *
     * @param sql
     */
    public void query(final String sql) {

    }

    public synchronized Command persistsCommandTo(final String baseName) {
        return this.writeCommandTo(baseName, true);
    }

    public synchronized Command sendCommandTo(final String baseName) {
        return this.writeCommandTo(baseName, false);
    }

    public synchronized Command writeCommandTo(final String baseName, final boolean persist) {
        if (cacheCommandPersistance.containsKey(baseName)) {
            return cacheCommandPersistance.get(baseName);
        } else {
            final Command command = new Command(baseName, applicationConfig, persist);
            cacheCommandPersistance.put(baseName, command);
            return command;
        }
    }

    // TODO seems not used
    public synchronized Event sendEventLogTo(final String baseName) {
        if (cacheEventLog.containsKey(baseName)) {
            return cacheEventLog.get(baseName);
        } else {
            final Event eventWriter = new Event(baseName, applicationConfig);
            cacheEventLog.put(baseName, eventWriter);
            return eventWriter;
        }
    }
}

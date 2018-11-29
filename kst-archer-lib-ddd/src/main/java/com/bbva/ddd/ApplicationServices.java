package com.bbva.ddd;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.ddd.domain.commands.write.Command;
import com.bbva.ddd.domain.events.write.Event;
import com.bbva.dataprocessors.ReadableStore;
import com.bbva.dataprocessors.States;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class ApplicationServices {

    private static final Logger logger = Logger.getLogger(ApplicationServices.class);

    private ApplicationConfig applicationConfig;
    private static ApplicationServices instance;
    private Map<String, Command> cacheCommandPersistance;
    private Map<String, Event> cacheEventLog;
    private boolean replayMode;


    public ApplicationServices(ApplicationConfig applicationConfig) {
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

    public void setReplayMode(boolean replayMode) {
        this.replayMode = replayMode;
    }

    /**
     *
     * @param store
     * @param <K>
     * @param <V>
     * @return
     */
    public <K, V> ReadableStore<K, V> getStore(String store) {
        while (true) {
            try {
                return States.get().getStore(store);
            } catch (InvalidStateStoreException ignored) {
                // store not yet ready for querying
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    logger.error("Problems sleeping the execution", e);
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * This method is empty
     * @param sql
     */
    public void query(String sql) {

    }

    public synchronized Command persistsCommandTo(String baseName) {
        return this.writeCommandTo(baseName, true);
    }

    public synchronized Command sendCommandTo(String baseName) {
        return this.writeCommandTo(baseName, false);
    }

    public synchronized Command writeCommandTo(String baseName, boolean persist) {
        if (cacheCommandPersistance.containsKey(baseName)) {
            return cacheCommandPersistance.get(baseName);
        } else {
            Command command = new Command(baseName, applicationConfig, persist);
            cacheCommandPersistance.put(baseName, command);
            return command;
        }
    }

    public synchronized Event sendEventLogTo(String baseName) {
        if (cacheEventLog.containsKey(baseName)) {
            return cacheEventLog.get(baseName);
        } else {
            Event eventWriter = new Event(baseName, applicationConfig);
            cacheEventLog.put(baseName, eventWriter);
            return eventWriter;
        }
    }
}

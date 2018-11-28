package com.bbva.ddd;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.ddd.domain.Domain;
import com.bbva.ddd.domain.commands.write.Command;
import com.bbva.ddd.domain.events.write.Event;
import com.bbva.dataprocessors.ReadableStore;
import com.bbva.dataprocessors.States;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class ApplicationServices {

    private ApplicationConfig applicationConfig;
    private static ApplicationServices instance = null;
    private final Logger logger;
    private Map<String, Command> cacheCommandPersistance;
    private Map<String, Event> cacheEventLog;
    private boolean replayMode;

    public ApplicationServices(ApplicationConfig applicationConfig) {
        this.applicationConfig = applicationConfig;
        logger = Logger.getLogger(Domain.class);
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
    public <K, V> ReadableStore<K, V> getStore(String store) throws NullPointerException, InterruptedException {
        while (true) {
            try {
                return States.get().getStore(store);
            } catch (InvalidStateStoreException ignored) {
                // store not yet ready for querying
                Thread.sleep(500);
            }
        }
    }

    /**
     *
     * @param sql
     * @throws Exception
     */
    public void query(String sql) throws Exception {

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

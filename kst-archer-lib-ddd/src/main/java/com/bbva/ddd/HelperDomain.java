package com.bbva.ddd;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.utils.TopicManager;
import com.bbva.ddd.domain.commands.write.Command;
import com.bbva.ddd.domain.events.write.Event;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HelperDomain {

    private final ApplicationConfig applicationConfig;
    private static HelperDomain instance;
    private final Map<String, Command> cacheCommandPersistance;
    private final Map<String, Event> cacheEventLog;
    private boolean replayMode;

    public HelperDomain(final ApplicationConfig applicationConfig) {
        this.applicationConfig = applicationConfig;
        cacheCommandPersistance = new HashMap<>();
        cacheEventLog = new HashMap<>();
        instance = this;
    }

    public static HelperDomain get() {
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

    public synchronized Event sendEventLogTo(final String baseName) {
        if (cacheEventLog.containsKey(baseName)) {
            return cacheEventLog.get(baseName);
        } else {
            final Event eventWriter = new Event(baseName, applicationConfig);
            cacheEventLog.put(baseName, eventWriter);
            return eventWriter;
        }
    }

    public synchronized void createEvents(final List<String> produceEvents) {
        final Map<String, String> producerEvents =
                Stream.of(produceEvents).flatMap(Collection::stream)
                        .collect(Collectors.toMap(k -> k + ApplicationConfig.EVENTS_RECORD_NAME_SUFFIX, type -> ApplicationConfig.COMMON_RECORD_TYPE,
                                (command1, command2) -> command1));

        TopicManager.createTopics(producerEvents, applicationConfig);
    }
}

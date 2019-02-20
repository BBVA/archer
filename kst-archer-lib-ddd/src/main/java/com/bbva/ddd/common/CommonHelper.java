package com.bbva.ddd.common;

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

public class CommonHelper {

    protected final ApplicationConfig applicationConfig;
    protected static CommonHelper instance;
    protected final Map<String, Command> cacheCommandPersistance;
    protected final Map<String, Event> cacheEvents;

    public CommonHelper(final ApplicationConfig applicationConfig) {
        this.applicationConfig = applicationConfig;
        cacheCommandPersistance = new HashMap<>();
        cacheEvents = new HashMap<>();
        instance = this;
    }


    public ApplicationConfig getConfig() {
        return applicationConfig;
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
            final Map<String, String> commandTopic = new HashMap<>();
            commandTopic.put(baseName + ApplicationConfig.COMMANDS_RECORD_NAME_SUFFIX, ApplicationConfig.COMMANDS_RECORD_TYPE);
            TopicManager.createTopics(commandTopic, applicationConfig);

            final Command command = new Command(baseName, applicationConfig, persist);
            cacheCommandPersistance.put(baseName, command);
            return command;
        }
    }

    public synchronized Event sendEventTo(final String baseName) {
        if (cacheEvents.containsKey(baseName)) {
            return cacheEvents.get(baseName);
        } else {
            final Map<String, String> commandTopic = new HashMap<>();
            commandTopic.put(baseName + ApplicationConfig.EVENTS_RECORD_NAME_SUFFIX, ApplicationConfig.EVENTS_RECORD_TYPE);
            TopicManager.createTopics(commandTopic, applicationConfig);

            final Event eventWriter = new Event(baseName, applicationConfig);
            cacheEvents.put(baseName, eventWriter);
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

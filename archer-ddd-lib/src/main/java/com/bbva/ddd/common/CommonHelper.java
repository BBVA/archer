package com.bbva.ddd.common;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.utils.TopicManager;
import com.bbva.ddd.domain.commands.write.Command;
import com.bbva.ddd.domain.events.write.Event;

import java.util.HashMap;
import java.util.Map;

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

    /**
     * It gets the current configuration
     *
     * @return current configuration
     */
    public ApplicationConfig getConfig() {
        return applicationConfig;
    }

    /**
     * It persists a command in the event store. Persistent commands are usually used to store data that came from out of
     * system.
     *
     * @param name Command name of the stream without suffix "_command"
     * @return An instance of Command producer
     */
    public synchronized Command persistsCommandTo(final String name) {
        return this.writeCommandTo(name, true);
    }

    /**
     * It sends a command to the event store. Non-persistent commands are usually used to send data inside of the system
     *
     * @param name Command name of the stream without suffix "_command"
     * @return An instance of Command producer
     */
    public synchronized Command sendCommandTo(final String name) {
        return this.writeCommandTo(name, false);
    }

    /**
     * It sends a command to the event store.
     *
     * @param name    Command name without suffix "_command"
     * @param persist if the command persists
     * @return An instance of Command producer
     */
    public synchronized Command writeCommandTo(final String name, final boolean persist) {
        if (cacheCommandPersistance.containsKey(name)) {
            return cacheCommandPersistance.get(name);
        } else {
            final Map<String, String> commandTopic = new HashMap<>();
            commandTopic.put(name + ApplicationConfig.COMMANDS_RECORD_NAME_SUFFIX, ApplicationConfig.COMMANDS_RECORD_TYPE);
            TopicManager.createTopics(commandTopic, applicationConfig);

            final Command command = new Command(name, applicationConfig, persist);
            cacheCommandPersistance.put(name, command);
            return command;
        }
    }

    /**
     * It sends an event to the event store.
     *
     * @param name Event name of the stream without suffix "_event"
     * @return An instance of Event producer
     */
    public synchronized Event sendEventTo(final String name) {
        if (cacheEvents.containsKey(name)) {
            return cacheEvents.get(name);
        } else {
            final Map<String, String> eventTopic = new HashMap<>();
            eventTopic.put(name + ApplicationConfig.EVENTS_RECORD_NAME_SUFFIX, ApplicationConfig.EVENTS_RECORD_TYPE);
            TopicManager.createTopics(eventTopic, applicationConfig);

            final Event eventWriter = new Event(name, applicationConfig);
            cacheEvents.put(name, eventWriter);
            return eventWriter;
        }
    }
}

package com.bbva.ddd.common;

import com.bbva.common.config.AppConfig;
import com.bbva.common.utils.TopicManager;
import com.bbva.ddd.domain.commands.producers.Command;
import com.bbva.ddd.domain.events.producers.Event;

import java.util.HashMap;
import java.util.Map;

/**
 * Commons helper methods
 */
public class CommonHelper {

    protected final AppConfig appConfig;
    protected static CommonHelper instance;
    protected final Map<String, Command> cacheCommandPersistence;
    protected final Map<String, Event> cacheEvents;

    /**
     * Constructor
     *
     * @param appConfig configuration
     */
    public CommonHelper(final AppConfig appConfig) {
        this.appConfig = appConfig;
        cacheCommandPersistence = new HashMap<>();
        cacheEvents = new HashMap<>();
        instance = this;
    }

    /**
     * It gets the current configuration
     *
     * @return current configuration
     */
    public AppConfig getConfig() {
        return appConfig;
    }

    /**
     * It persists a command in the event store. Persistent commands are usually used to store data that came from out of
     * system.
     *
     * @param name Command name of the stream without suffix "_command"
     * @return An instance of Command producer
     */
    public synchronized Command persistsCommandTo(final String name) {
        return writeCommandTo(name, true);
    }

    /**
     * It sends a command to the event store. Non-persistent commands are usually used to send data inside of the system
     *
     * @param name Command name of the stream without suffix "_command"
     * @return An instance of Command producer
     */
    public synchronized Command sendCommandTo(final String name) {
        return writeCommandTo(name, false);
    }

    /**
     * It sends a command to the event store.
     *
     * @param name    Command name without suffix "_command"
     * @param persist if the command persists
     * @return An instance of Command producer
     */
    public synchronized Command writeCommandTo(final String name, final boolean persist) {
        if (cacheCommandPersistence.containsKey(name)) {
            return cacheCommandPersistence.get(name);
        } else {
            final Map<String, String> commandTopic = new HashMap<>();
            commandTopic.put(name + AppConfig.COMMANDS_RECORD_NAME_SUFFIX, AppConfig.COMMANDS_RECORD_TYPE);
            TopicManager.createTopics(commandTopic, appConfig);

            //TODO to remove
            final Command command =
                    new Command.Builder(null)
                            .action(Command.CREATE_ACTION)
                            .build();
            cacheCommandPersistence.put(name, command);
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
            eventTopic.put(name + AppConfig.EVENTS_RECORD_NAME_SUFFIX, AppConfig.EVENTS_RECORD_TYPE);
            TopicManager.createTopics(eventTopic, appConfig);

            //TODO to remove
            final Event eventWriter = new Event.Builder(null)
                    .name(name).producerName(name).build();
            cacheEvents.put(name, eventWriter);
            return eventWriter;
        }
    }
}

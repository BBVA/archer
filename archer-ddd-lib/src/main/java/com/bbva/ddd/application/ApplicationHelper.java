package com.bbva.ddd.application;

import com.bbva.common.config.AppConfig;
import com.bbva.common.producers.DefaultProducer;
import com.bbva.common.producers.Producer;
import com.bbva.ddd.domain.commands.producers.Command;
import com.bbva.ddd.domain.events.producers.Event;

/**
 * Helper to manage application layer and send events/commands.
 */
public final class ApplicationHelper {

    private static ApplicationHelper instance;
    private final Producer producer;

    /**
     * Constructor
     *
     * @param appConfig general configuration
     */
    private ApplicationHelper(final AppConfig appConfig) {
        producer = new DefaultProducer(appConfig);
    }

    /**
     * Create a helper instance
     *
     * @param configs configuration
     * @return instance
     */
    public static ApplicationHelper create(final AppConfig configs) {
        instance = new ApplicationHelper(configs);
        return instance;
    }

    /**
     * Get actual instance of the helper
     *
     * @return instance
     */
    public static ApplicationHelper get() {
        return instance;
    }

    /**
     * Get command generator to send
     *
     * @param action action to perform
     * @return command builder
     */
    public Command.Builder command(final String action) {
        return new Command.Builder(null, producer, false).action(action);
    }

    /**
     * Get command generator to send
     *
     * @param action action to perform
     * @return command builder
     */
    public Command.Builder command(final Command.Action action) {
        return new Command.Builder(null, producer, false).action(action.name());
    }

    /**
     * Get event generator to send
     *
     * @param name name of the event
     * @return event builder
     */
    public Event.Builder event(final String name) {
        return new Event.Builder(null, producer, false).name(name);
    }
}

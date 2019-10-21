package com.bbva.ddd.domain.handlers;

import com.bbva.common.consumers.contexts.ConsumerContext;
import com.bbva.ddd.domain.changelogs.repository.Repository;
import com.bbva.ddd.domain.commands.producers.Command;
import com.bbva.ddd.domain.events.producers.Event;

/**
 * Handler common interface to manage new messages
 */
public interface HandlerContext extends ConsumerContext {

    /**
     * Get repository instance to manage it
     *
     * @return instance
     */
    Repository repository();

    /**
     * Get command generator to send
     *
     * @param action action to perform
     * @return command builder
     */
    Command.Builder command(String action);

    /**
     * Get command generator to send
     *
     * @param action action to perform
     * @return command builder
     */
    Command.Builder command(Command.Action action);

    /**
     * Get event generator to send
     *
     * @param action name of the event
     * @return event builder
     */
    Event.Builder event(String name);

}

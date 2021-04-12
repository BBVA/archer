package com.bbva.ddd.domain.handlers.contexts;

import com.bbva.common.consumers.contexts.ConsumerContext;
import com.bbva.dataprocessors.states.ReadableStore;
import com.bbva.ddd.domain.changelogs.repository.Repository;
import com.bbva.ddd.domain.commands.producers.Command;
import com.bbva.ddd.domain.events.producers.Event;

/**
 * Handler common interface to manage new messages
 */
public interface HandlerContext extends ConsumerContext {

    /**
     * Know if application or consumed record is in replay mode
     *
     * @return true in case of application or consumed record is in replay mode
     */
    Boolean isReplay();

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
     * @param name name of the event
     * @return event builder
     */
    Event.Builder event(String name);

    /**
     * Get event generator to send
     *
     * @param name store name
     * @return event builder
     */
    <K, V> ReadableStore<K, V> loadStore(String name);

}

package com.bbva.ddd.domain.handlers;

import com.bbva.common.consumers.contexts.ConsumerContext;
import com.bbva.ddd.domain.changelogs.repository.Repository;
import com.bbva.ddd.domain.commands.producers.Command;
import com.bbva.ddd.domain.events.producers.Event;

public interface HandlerContext extends ConsumerContext {

    Repository repository();

    Command.Builder command(String action);

    Command.Builder command(Command.Action action);

    Event.Builder event(String name);

}

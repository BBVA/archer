package com.bbva.ddd.domain.handlers;

import com.bbva.common.consumers.contexts.ConsumerContext;
import com.bbva.ddd.domain.changelogs.Repository;
import com.bbva.ddd.domain.commands.write.Command;
import com.bbva.ddd.domain.events.write.Event;

public interface HandlerContext extends ConsumerContext {

    Repository repository();

    Command.Builder command(String action);

    Command.Builder createCommand();

    Command.Builder deleteCommand();

    Event.Builder event(String name);

}

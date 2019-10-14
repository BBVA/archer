package com.bbva.ddd.domain.callback;

import com.bbva.ddd.domain.annotations.Changelog;
import com.bbva.ddd.domain.annotations.Command;
import com.bbva.ddd.domain.annotations.Event;
import com.bbva.ddd.domain.commands.consumers.CommandHandlerContext;

public class ActionHandler {

    @Event(baseName = "eventName")
    public void processEvent() {
        //Do nothing
    }

    @Command(baseName = "commandName", commandAction = "create")
    public static void processCommand(final CommandHandlerContext commandRecord) {
        //Do nothing
    }

    @Changelog(baseName = "eventName")
    public void processChangelog() {
        //Do nothing
    }
}

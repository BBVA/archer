package com.bbva.ddd.domain.callback;

import com.bbva.ddd.domain.annotations.Changelog;
import com.bbva.ddd.domain.annotations.Command;
import com.bbva.ddd.domain.annotations.Event;
import com.bbva.ddd.domain.commands.read.CommandRecord;

public class ActionHandler {

    @Event(baseName = "eventName")
    public void processEvent() {

    }

    @Command(baseName = "commandName", commandAction = "create")
    public static void processCommand(final CommandRecord commandRecord) {

    }

    @Changelog(baseName = "eventName")
    public void processChangelog() {

    }
}

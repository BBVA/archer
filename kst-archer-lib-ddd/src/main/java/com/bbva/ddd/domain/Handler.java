package com.bbva.ddd.domain;

import com.bbva.common.consumers.CRecord;
import com.bbva.ddd.domain.changelogs.read.ChangelogRecord;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import com.bbva.ddd.domain.events.read.EventRecord;

import java.util.Arrays;
import java.util.List;

public interface Handler {

    default void processMessage(CRecord record) {
    }

    default void processCommand(CommandRecord commandMessage) {
    }

    default void processEvent(EventRecord eventMessage) {
    }

    default void processDataChangelog(ChangelogRecord changelogMessage) {

    }

    default List<String> commandsSubscribed() {
        return Arrays.asList();
    }

    default List<String> eventsSubscribed() {
        return Arrays.asList();
    }

    default List<String> dataChangelogsSubscribed() {
        return Arrays.asList();
    }

    // Map<String, Class<? extends Aggregate>> aggregates();
    // List<Class<? extends Aggregate>> aggregates();

}

package com.bbva.ddd.domain.handlers;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.ddd.domain.changelogs.repository.Repository;
import com.bbva.ddd.domain.commands.producers.Command;
import com.bbva.ddd.domain.events.producers.Event;

public class HandlerContextImpl implements HandlerContext {

    protected CRecord consumedRecord;

    public HandlerContextImpl(final CRecord consumedRecord) {
        this.consumedRecord = consumedRecord;
    }

    @Override
    public CRecord consumedRecord() {
        return consumedRecord;
    }


    @Override
    public Repository repository() {
        return null;
    }

    @Override
    public Command.Builder command(final String action) {
        return null;
    }

    @Override
    public Command.Builder command(final Command.Action action) {
        return null;
    }

    @Override
    public Event.Builder event(final String name) {
        return null;
    }
}

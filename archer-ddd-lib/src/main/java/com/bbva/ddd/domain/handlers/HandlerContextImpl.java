package com.bbva.ddd.domain.handlers;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.ddd.domain.changelogs.Repository;
import com.bbva.ddd.domain.commands.write.Command;
import com.bbva.ddd.domain.events.write.Event;

public class HandlerContextImpl implements HandlerContext {

    protected CRecord consumedRecord;

    public HandlerContextImpl(final CRecord consumedRecord) {
        this.consumedRecord = consumedRecord;
    }


    @Override
    public void consumedRecord(final CRecord record) {
        consumedRecord = record;
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
    public Command.Builder createCommand() {
        return null;
    }

    @Override
    public Command.Builder deleteCommand() {
        return null;
    }

    @Override
    public Event.Builder event(final String name) {
        return null;
    }
}

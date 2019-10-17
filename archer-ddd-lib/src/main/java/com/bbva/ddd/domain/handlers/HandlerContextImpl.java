package com.bbva.ddd.domain.handlers;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.Producer;
import com.bbva.ddd.domain.changelogs.repository.Repository;
import com.bbva.ddd.domain.changelogs.repository.RepositoryImpl;
import com.bbva.ddd.domain.commands.producers.Command;
import com.bbva.ddd.domain.events.producers.Event;

public class HandlerContextImpl implements HandlerContext {

    protected CRecord consumedRecord;
    protected Repository repository;
    protected final Producer producer;

    public HandlerContextImpl(final Producer producer, final CRecord consumedRecord) {
        this.consumedRecord = consumedRecord;
        this.producer = producer;
        if (producer != null) {
            repository = new RepositoryImpl<>(consumedRecord, producer);
        } else {
            repository = new RepositoryImpl<>(consumedRecord);
        }

    }

    @Override
    public CRecord consumedRecord() {
        return consumedRecord;
    }


    @Override
    public Repository repository() {
        return repository;
    }

    @Override
    public Command.Builder command(final String action) {
        return new Command.Builder(producer, consumedRecord).action(action);
    }

    @Override
    public Command.Builder command(final Command.Action action) {
        return new Command.Builder(producer, consumedRecord).action(action.name());
    }

    @Override
    public Event.Builder event(final String name) {
        return new Event.Builder(producer, consumedRecord).name(name);
    }
}

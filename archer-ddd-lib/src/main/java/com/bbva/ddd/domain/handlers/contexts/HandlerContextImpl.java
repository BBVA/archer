package com.bbva.ddd.domain.handlers.contexts;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.Producer;
import com.bbva.dataprocessors.states.ReadableStore;
import com.bbva.dataprocessors.states.States;
import com.bbva.ddd.domain.changelogs.repository.Repository;
import com.bbva.ddd.domain.changelogs.repository.RepositoryImpl;
import com.bbva.ddd.domain.commands.producers.Command;
import com.bbva.ddd.domain.events.producers.Event;

public class HandlerContextImpl implements HandlerContext {

    protected CRecord consumedRecord;
    protected Repository repository;
    protected final Producer producer;
    protected Boolean isReplay;

    public HandlerContextImpl(final CRecord consumedRecord, final Producer producer, final Boolean isReplay) {
        this.consumedRecord = consumedRecord;
        this.producer = producer;
        this.isReplay = isReplay || consumedRecord.isReplayMode();
        repository = new RepositoryImpl<>(consumedRecord, producer, isReplay);
    }

    @Override
    public Boolean isReplay() {
        return isReplay;
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
        return new Command.Builder(consumedRecord, producer, isReplay).action(action);
    }

    @Override
    public Command.Builder command(final Command.Action action) {
        return new Command.Builder(consumedRecord, producer, isReplay).action(action.name());
    }

    @Override
    public Event.Builder event(final String name) {
        return new Event.Builder(consumedRecord, producer, isReplay).name(name);
    }

    @Override
    public <K, V> ReadableStore<K, V> loadStore(final String name) {
        return States.get().getStore(name);
    }
}

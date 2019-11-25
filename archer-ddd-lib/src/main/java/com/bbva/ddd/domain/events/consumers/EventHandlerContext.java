package com.bbva.ddd.domain.events.consumers;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.Producer;
import com.bbva.ddd.domain.handlers.contexts.HandlerContextImpl;

/**
 * Event consumed context to handle events before consume.
 */
public class EventHandlerContext extends HandlerContextImpl {

    /**
     * Constructor
     *
     * @param record   consumed record
     * @param producer producer instance
     * @param isReplay flag of replay
     */
    public EventHandlerContext(final CRecord record, final Producer producer, final Boolean isReplay) {
        super(new EventRecord(record), producer, isReplay);
    }

    @Override
    public EventRecord consumedRecord() {
        return (EventRecord) consumedRecord;
    }
}

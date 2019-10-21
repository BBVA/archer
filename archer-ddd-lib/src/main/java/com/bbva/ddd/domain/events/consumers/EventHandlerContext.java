package com.bbva.ddd.domain.events.consumers;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.Producer;
import com.bbva.ddd.domain.handlers.HandlerContextImpl;

public class EventHandlerContext extends HandlerContextImpl {

    public EventHandlerContext(final CRecord consumedRecord, final Producer producer, final Boolean isReplay) {
        super(new EventRecord(consumedRecord), producer, isReplay);
    }

    @Override
    public EventRecord consumedRecord() {
        return (EventRecord) consumedRecord;
    }
}

package com.bbva.ddd.domain.events.read;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.ddd.domain.consumers.HandlerContextImpl;

public class EventHandlerContext extends HandlerContextImpl {

    public EventHandlerContext(final CRecord consumedRecord) {
        super(new EventRecord(consumedRecord));
    }

    @Override
    public EventRecord consumedRecord() {
        return (EventRecord) consumedRecord;
    }
}

package com.bbva.ddd.domain.commands.consumers;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.Producer;
import com.bbva.ddd.domain.handlers.contexts.HandlerContextImpl;

public class CommandHandlerContext extends HandlerContextImpl {

    public CommandHandlerContext(final CRecord consumedRecord, final Producer producer, final Boolean isReplay) {
        super(new CommandRecord(consumedRecord), producer, isReplay);
    }

    @Override
    public CommandRecord consumedRecord() {
        return (CommandRecord) consumedRecord;
    }
}

package com.bbva.ddd.domain.commands.consumers;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.Producer;
import com.bbva.ddd.domain.handlers.HandlerContextImpl;

public class CommandHandlerContext extends HandlerContextImpl {

    public CommandHandlerContext(final Producer producer, final CRecord consumedRecord) {
        super(producer, new CommandRecord(consumedRecord));
    }

    @Override
    public CommandRecord consumedRecord() {
        return (CommandRecord) consumedRecord;
    }
}

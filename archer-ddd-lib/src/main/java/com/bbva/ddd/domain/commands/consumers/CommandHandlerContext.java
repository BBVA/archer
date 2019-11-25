package com.bbva.ddd.domain.commands.consumers;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.Producer;
import com.bbva.ddd.domain.handlers.contexts.HandlerContextImpl;

/**
 * Command consumed context to handle events before consume.
 */
public class CommandHandlerContext extends HandlerContextImpl {

    /**
     * Constructor
     *
     * @param record   consumed record
     * @param producer producer instance
     * @param isReplay flag of replay
     */
    public CommandHandlerContext(final CRecord record, final Producer producer, final Boolean isReplay) {
        super(new CommandRecord(record), producer, isReplay);
    }

    @Override
    public CommandRecord consumedRecord() {
        return (CommandRecord) consumedRecord;
    }
}

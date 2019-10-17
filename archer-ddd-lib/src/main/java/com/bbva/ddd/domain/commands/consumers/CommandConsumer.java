package com.bbva.ddd.domain.commands.consumers;

import com.bbva.common.config.AppConfig;
import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.Producer;
import com.bbva.ddd.domain.consumers.RunnableConsumer;

import java.util.List;
import java.util.function.Consumer;

/**
 * Specific consumer for command records
 */
public class CommandConsumer extends RunnableConsumer<CommandHandlerContext> {

    /**
     * Constructor
     *
     * @param id        consumer id
     * @param topics    list of command topics to consume
     * @param callback  callback to manage events produced
     * @param appConfig configuration
     */
    public CommandConsumer(final int id, final List<String> topics, final Consumer<CommandHandlerContext> callback,
                           final AppConfig appConfig) {

        super(id, topics, callback, appConfig);
    }

    @Override
    public CommandHandlerContext context(final Producer producer, final CRecord record) {
        final CommandHandlerContext commandHandlerContext = new CommandHandlerContext(producer, record);
        return commandHandlerContext;
    }

}

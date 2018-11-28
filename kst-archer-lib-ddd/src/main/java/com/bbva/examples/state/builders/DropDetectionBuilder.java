package com.bbva.examples.state.builders;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.utils.TopicManager;
import com.bbva.dataprocessors.builders.dataflows.DataflowBuilder;
import com.bbva.dataprocessors.contexts.dataflow.DataflowProcessorContext;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.log4j.Logger;

import java.util.Arrays;

public class DropDetectionBuilder implements DataflowBuilder {
    private final Logger logger;
    private DataflowProcessorContext context;

    public DropDetectionBuilder() {
        logger = Logger.getLogger(DropDetectionBuilder.class);
    }

    @Override
    public void init(DataflowProcessorContext context) {
        this.context = context;
    }

    @Override
    public void build() {
        String topic = context.name();
        String store = topic + ApplicationConfig.STORE_NAME_SUFFIX;

        TopicManager.createTopics(Arrays.asList(topic), context.configs());

        final KStream<String, Long> dataStream = context.streamsBuilder().stream(topic,
                Consumed.with(Serdes.String(), Serdes.Long()));

        dataStream.groupByKey(Serialized.with(Serdes.String(), Serdes.Long())).aggregate(() -> 0L,
                (aggKey, newValue, aggValue) -> newValue, Materialized.as(store));
    }
}

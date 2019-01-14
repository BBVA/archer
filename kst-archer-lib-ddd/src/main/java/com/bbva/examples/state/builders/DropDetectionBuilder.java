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

import java.util.HashMap;
import java.util.Map;

public class DropDetectionBuilder implements DataflowBuilder {

    private DataflowProcessorContext context;

    public DropDetectionBuilder() {

    }

    @Override
    public void init(final DataflowProcessorContext context) {
        this.context = context;
    }

    @Override
    public void build() {
        final String topic = context.name();
        final String store = topic + ApplicationConfig.STORE_NAME_SUFFIX;

        final Map<String, String> topics = new HashMap();
        topics.put(topic, ApplicationConfig.CHANGELOG_RECORD_TYPE);
        TopicManager.createTopics(topics, context.configs());

        final KStream<String, Long> dataStream = context.streamsBuilder().stream(topic,
                Consumed.with(Serdes.String(), Serdes.Long()));

        dataStream.groupByKey(Serialized.with(Serdes.String(), Serdes.Long())).aggregate(() -> 0L,
                (aggKey, newValue, aggValue) -> newValue, Materialized.as(store));
    }
}

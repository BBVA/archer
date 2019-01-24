package com.bbva.gateway.consumer.builder;


import com.bbva.common.utils.serdes.SpecificAvroSerde;
import com.bbva.dataprocessors.transformers.EntityTransformer;
import com.bbva.gateway.consumer.transformer.ChangelogTransformer;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;


public class ChangelogKeyBuilder<K, V extends SpecificRecordBase> extends ChangelogBuilder {

    public ChangelogKeyBuilder(final String baseName, final String topic) {
        super(baseName, topic);
    }

    @Override
    protected EntityTransformer newTransformer(final String entityName) {
        return new ChangelogTransformer(entityName);
    }

    @Override
    protected void addTable(final StreamsBuilder builder, final String name, final SpecificAvroSerde valueSerde) {
        builder.table(this.snapshotTopicName,
                Consumed.with(Serdes.String(), valueSerde),
                Materialized.as(name));
    }
}

package com.bbva.examples.aggregates;

import com.bbva.avro.Channels;
import com.bbva.ddd.domain.aggregates.SpecificAggregate;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;

@Aggregate(baseName = "test_channels")
public class ChannelsAggregate extends SpecificAggregate<String, Channels> {


    public ChannelsAggregate(final String id, final Channels channels) {
        super(id, channels);
    }

    public static String baseName() {
        return "test_channels";
    }

}

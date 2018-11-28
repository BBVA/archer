package com.bbva.examples.aggregates;

import com.bbva.avro.Channels;
import com.bbva.ddd.domain.aggregates.AbstractAggregateBase;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;
import com.bbva.ddd.domain.commands.read.CommandRecord;

import java.util.logging.Logger;

@Aggregate(baseName = "test_channels")
public class ChannelsAggregate extends AbstractAggregateBase<String, Channels> {

    private static final Logger logger = Logger.getLogger(ChannelsAggregate.class.getName());

    public ChannelsAggregate(String id, Channels channels) {
        super(id, channels);
    }

    public void update(Channels modifiedData, CommandRecord commandMessage) {
        apply("update", modifiedData, commandMessage, (id, e) -> {
            if (e != null) {
                e.printStackTrace();
            }
            logger.info("change data from " + getData() + " to " + modifiedData);
        });
    }

    public static String baseName() {
        return "test_channels";
    }

}

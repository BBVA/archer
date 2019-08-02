package com.bbva.ddd.domain;

import com.bbva.common.util.PowermockExtension;
import com.bbva.ddd.domain.aggregates.SpecificAggregate;
import com.bbva.ddd.domain.callback.DefaultProducerCallback;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.util.Date;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest(AggregateFactory.class)
public class AggregateFactoryTest {

    @DisplayName("Load aggregates ok")
    @Test
    public void loadAggregateOk() {
        Assertions.assertAll("AggregateFactory",
                () -> Assertions.assertNull(AggregateFactory.load(SpecificAggregate.class, "key"))
        );
    }

    @DisplayName("Create aggregate ok")
    @Test
    public void createAggregateOk() {
        PowerMockito.mockStatic(AggregateFactory.class);
        Assertions.assertNull(AggregateFactory.create(
                null, null,
                new CommandRecord(
                        "topic", 1, 1L, new Date().getTime(),
                        TimestampType.CREATE_TIME, "key", null, null),
                new DefaultProducerCallback()));

    }

    @DisplayName("Create aggregate ok")
    @Test
    public void createAggregate2Ok() {
        PowerMockito.mockStatic(AggregateFactory.class);
        Assertions.assertNull(AggregateFactory.create(
                null, null,
                new CommandRecord(
                        "topic", 1, 1L, new Date().getTime(),
                        TimestampType.CREATE_TIME, "key", null, null),
                new DefaultProducerCallback()));

    }

}

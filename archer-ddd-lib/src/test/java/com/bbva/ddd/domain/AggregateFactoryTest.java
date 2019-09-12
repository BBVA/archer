package com.bbva.ddd.domain;

import com.bbva.common.util.PowermockExtension;
import com.bbva.ddd.domain.callback.DefaultProducerCallback;
import com.bbva.ddd.domain.changelogs.aggregate.PersonalDataAggregate;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import org.apache.kafka.common.record.TimestampType;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;

import java.util.Date;
import java.util.HashMap;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
public class AggregateFactoryTest {

    @DisplayName("Load aggregates ok")
    @Test
    public void loadAggregateOk() {
        Repositories.getInstance().setRepositories(new HashMap<>());
        Assertions.assertAll("AggregateFactory",
                () -> Assertions.assertNotNull(new AggregateFactory()),
                () -> Assertions.assertNull(AggregateFactory.load(PersonalDataAggregate.class, "key"))
        );
    }

    @DisplayName("Create aggregate ok")
    @Test
    public void createAggregateOk() {

        Repositories.getInstance().setRepositories(new HashMap<>());
        Assertions.assertNull(AggregateFactory.create(
                PersonalDataAggregate.class, null,
                new CommandRecord(
                        "topic", 1, 1L, new Date().getTime(),
                        TimestampType.CREATE_TIME, "key", null, null),
                new DefaultProducerCallback()));

    }

    @DisplayName("Create aggregate ok")
    @Test
    public void createAggregate2Ok() {

        Repositories.getInstance().setRepositories(new HashMap<>());
        Assertions.assertNull(AggregateFactory.create(
                PersonalDataAggregate.class, null,
                new CommandRecord(
                        "topic", 1, 1L, new Date().getTime(),
                        TimestampType.CREATE_TIME, "key", null, null),
                new DefaultProducerCallback()));

    }

}

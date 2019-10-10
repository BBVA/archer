package com.bbva.ddd.domain.aggregates;

import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.CachedProducer;
import com.bbva.common.util.PowermockExtension;
import com.bbva.ddd.domain.callback.DefaultProducerCallback;
import com.bbva.ddd.domain.changelogs.Repository;
import com.bbva.ddd.domain.changelogs.aggregate.PersonalDataAggregate;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import com.bbva.ddd.domain.commands.write.records.PersonalData;
import org.apache.kafka.common.record.TimestampType;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.util.Date;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({CachedProducer.class, Repository.class})
public class SpecificAggregateTest {

    @DisplayName("Create aggregate ok")
    @Test
    public void createAggregate() {
        final PersonalDataAggregate personalData = new PersonalDataAggregate("id", new PersonalData());
        personalData.onCompleteTest();

        Assertions.assertAll("PersonalDataAggregate",
                () -> Assertions.assertNotNull(personalData.getData()),
                () -> Assertions.assertNotNull(personalData.getId())
        );
    }

    @DisplayName("Apply aggregate ok")
    @Test
    public void applyAggregate() {
        final PersonalDataAggregate personalData = new PersonalDataAggregate("id", new PersonalData());
        personalData.setDeleteRecordCallback((method, recordClass, record, callback) -> {
            //Do nothing
        });
        personalData.apply("method", new CRecord("topic", 1, 1, new Date().getTime(),
                TimestampType.CREATE_TIME, "key", new PersonalData(), null), new DefaultProducerCallback());

        Assertions.assertAll("PersonalDataAggregate",
                () -> Assertions.assertNotNull(personalData.getData()),
                () -> Assertions.assertNotNull(personalData.getId())
        );
    }

    @DisplayName("Apply command aggregate ok")
    @Test
    public void applyCommandAggregate() {
        final PersonalDataAggregate personalData = new PersonalDataAggregate("id", new PersonalData());
        personalData.setApplyRecordCallback((method, recordClass, record, callback) -> {
            //Do nothing
        });
        personalData.apply("method", new PersonalData(), new CommandRecord("topic", 1, 1, new Date().getTime(),
                TimestampType.CREATE_TIME, "key", new PersonalData(), null), new DefaultProducerCallback());

        Assertions.assertAll("PersonalDataAggregate",
                () -> Assertions.assertNotNull(personalData.getData()),
                () -> Assertions.assertNotNull(personalData.getId())
        );
    }

    @DisplayName("Update ok")
    @Test
    public void updateOK() {
        final PersonalDataAggregate personalData = new PersonalDataAggregate("id", new PersonalData());
        personalData.setApplyRecordCallback((method, recordClass, record, callback) -> {
            //Do nothing
        });
        personalData.update(new PersonalData(), new CommandRecord("topic", 1, 1, new Date().getTime(),
                TimestampType.CREATE_TIME, "key", new PersonalData(), null), () -> {

        });

        Assertions.assertAll("PersonalDataAggregate",
                () -> Assertions.assertNotNull(personalData.getData()),
                () -> Assertions.assertNotNull(personalData.getId())
        );
    }

    @DisplayName("Delete ok")
    @Test
    public void deleteOK() {
        final PersonalDataAggregate personalData = new PersonalDataAggregate("id", new PersonalData());
        personalData.setDeleteRecordCallback((method, recordClass, record, callback) -> {
            //Do nothing
        });
        personalData.delete(new CommandRecord("topic", 1, 1, new Date().getTime(),
                TimestampType.CREATE_TIME, "key", new PersonalData(), null), () -> {

        });

        Assertions.assertAll("PersonalDataAggregate",
                () -> Assertions.assertNotNull(personalData.getData()),
                () -> Assertions.assertNotNull(personalData.getId())
        );
    }
}

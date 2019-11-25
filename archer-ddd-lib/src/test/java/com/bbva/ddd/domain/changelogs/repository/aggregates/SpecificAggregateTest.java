package com.bbva.ddd.domain.changelogs.repository.aggregates;

import com.bbva.common.util.PowermockExtension;
import com.bbva.ddd.domain.callback.DefaultProducerCallback;
import com.bbva.ddd.domain.changelogs.aggregate.PersonalDataAggregate;
import com.bbva.ddd.domain.changelogs.repository.Repository;
import com.bbva.ddd.domain.commands.producers.records.PersonalData;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({Repository.class})
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
        personalData.setDeleteRecordCallback((method, callback) -> {
            //Do nothing
        });
        personalData.apply("method", new DefaultProducerCallback());

        Assertions.assertAll("PersonalDataAggregate",
                () -> Assertions.assertNotNull(personalData.getData()),
                () -> Assertions.assertNotNull(personalData.getId())
        );
    }

    @DisplayName("Apply command aggregate ok")
    @Test
    public void applyCommandAggregate() {
        final PersonalDataAggregate personalData = new PersonalDataAggregate("id", new PersonalData());
        personalData.setApplyRecordCallback((method, recordClass, callback) -> {
            //Do nothing
        });
        personalData.apply("method", new PersonalData(), new DefaultProducerCallback());

        Assertions.assertAll("PersonalDataAggregate",
                () -> Assertions.assertNotNull(personalData.getData()),
                () -> Assertions.assertNotNull(personalData.getId())
        );
    }

    @DisplayName("Update ok")
    @Test
    public void updateOK() {
        final PersonalDataAggregate personalData = new PersonalDataAggregate("id", new PersonalData());
        personalData.setApplyRecordCallback((method, recordClass, callback) -> {
            //Do nothing
        });
        personalData.update(new PersonalData(), () -> {

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
        personalData.setDeleteRecordCallback((method, callback) -> {
            //Do nothing
        });
        personalData.delete(() -> {

        });

        Assertions.assertAll("PersonalDataAggregate",
                () -> Assertions.assertNotNull(personalData.getData()),
                () -> Assertions.assertNotNull(personalData.getId())
        );
    }
}

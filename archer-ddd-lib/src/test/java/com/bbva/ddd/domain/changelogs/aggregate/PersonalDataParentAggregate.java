package com.bbva.ddd.domain.changelogs.aggregate;

import com.bbva.ddd.domain.changelogs.repository.aggregates.SpecificAggregate;
import com.bbva.ddd.domain.changelogs.repository.aggregates.annotations.Aggregate;
import com.bbva.ddd.domain.commands.producers.records.PersonalData;

@Aggregate(baseName = "aggregateBaseNameParent")
public class PersonalDataParentAggregate extends SpecificAggregate<String, PersonalData> {
    public PersonalData personalData;

    public PersonalDataParentAggregate(final String id, final PersonalData record) {
        super(id, record);
    }

    public PersonalData getPersonalData() {
        return personalData;
    }

    public void setPersonalData(final PersonalData personalData) {
        this.personalData = personalData;
    }
}

package com.bbva.ddd.domain.changelogs.aggregate;

import com.bbva.ddd.domain.aggregates.SpecificAggregate;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;
import com.bbva.ddd.domain.commands.write.records.PersonalData;

@Aggregate(baseName = "aggregateBaseNameParent")
public class PersonalDataParentAggregate extends SpecificAggregate<String, PersonalData> {
    public PersonalData personalData;

    public PersonalDataParentAggregate(final String id, final PersonalData record) {
        super(id, record);
    }

    public PersonalData getPersonalData() {
        return personalData;
    }

    public void setPersonalData(PersonalData personalData) {
        this.personalData = personalData;
    }
}

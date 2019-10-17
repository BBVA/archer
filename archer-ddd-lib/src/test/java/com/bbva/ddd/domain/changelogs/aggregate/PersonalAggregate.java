package com.bbva.ddd.domain.changelogs.aggregate;

import com.bbva.common.exceptions.ApplicationException;
import com.bbva.common.producers.records.SpecificRecordBaseImpl;
import com.bbva.ddd.domain.callback.DefaultAggregateCallback;
import com.bbva.ddd.domain.changelogs.repository.aggregates.SpecificAggregate;
import com.bbva.ddd.domain.commands.producers.records.PersonalData;

public class PersonalAggregate extends SpecificAggregate<String, PersonalData> {

    public PersonalAggregate(final String id, final SpecificRecordBaseImpl record) {
        super(id, (PersonalData) record.get(0));

    }

    public PersonalAggregate(final String id, final PersonalData record) {
        super(id, record);

    }

    public void onCompleteTest() {
        onComplete(null, new ApplicationException(), null);
        onComplete(new DefaultAggregateCallback(), null, "message");
    }
}

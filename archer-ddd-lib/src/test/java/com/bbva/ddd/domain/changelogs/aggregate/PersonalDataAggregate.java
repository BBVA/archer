package com.bbva.ddd.domain.changelogs.aggregate;

import com.bbva.common.exceptions.ApplicationException;
import com.bbva.ddd.domain.aggregates.SpecificAggregate;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;
import com.bbva.ddd.domain.aggregates.annotations.AggregateParent;
import com.bbva.ddd.domain.callback.DefaultAggregateCallback;
import com.bbva.ddd.domain.commands.write.records.PersonalData;

@Aggregate(baseName = "aggregateBaseName")
@AggregateParent(value = PersonalDataParentAggregate.class)
public class PersonalDataAggregate extends SpecificAggregate<String, PersonalData> {

    public PersonalDataAggregate(final String id, final PersonalData record) {
        super(id, record);

    }

    public void onCompleteTest() {
        onComplete(null, new ApplicationException(), null);
        onComplete(new DefaultAggregateCallback(), null, "message");
    }
}

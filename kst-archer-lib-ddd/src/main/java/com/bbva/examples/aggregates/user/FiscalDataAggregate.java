package com.bbva.examples.aggregates.user;

import com.bbva.avro.users.FiscalData;
import com.bbva.ddd.domain.aggregates.SpecificAggregate;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;
import com.bbva.ddd.domain.aggregates.annotations.AggregateParent;
import com.bbva.examples.aggregates.UserAggregate;

@Aggregate(baseName = "test_users_fiscal_data")
@AggregateParent(UserAggregate.class)
public class FiscalDataAggregate extends SpecificAggregate<String, FiscalData> {

    public FiscalDataAggregate(final String id, final FiscalData data) {
        super(id, data);
    }

    public static String baseName() {
        return "test_users_fiscal_data";
    }

}

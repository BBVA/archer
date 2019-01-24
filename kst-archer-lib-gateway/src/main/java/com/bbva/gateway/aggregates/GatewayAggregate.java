package com.bbva.gateway.aggregates;

import com.bbva.archer.avro.gateway.TransactionChangelog;
import com.bbva.ddd.domain.aggregates.SpecificAggregate;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;
import com.bbva.gateway.constants.Constants;

@Aggregate(baseName = Constants.INTERNAL_SUFFIX)
public class GatewayAggregate extends SpecificAggregate<String, TransactionChangelog> {

    public GatewayAggregate(final String id, final TransactionChangelog transactionChangelog) {
        super(id, transactionChangelog);
    }

}

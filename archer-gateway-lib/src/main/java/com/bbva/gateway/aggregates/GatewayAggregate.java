package com.bbva.gateway.aggregates;

import com.bbva.archer.avro.gateway.TransactionChangelog;
import com.bbva.ddd.domain.aggregates.SpecificAggregate;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;
import com.bbva.gateway.constants.Constants;

/**
 * Internala aggregate for manage gateway with ddd
 */
@Aggregate(baseName = Constants.INTERNAL_SUFFIX)
public class GatewayAggregate extends SpecificAggregate<String, TransactionChangelog> {

    /**
     * Constructor
     *
     * @param id                   aggregate id
     * @param transactionChangelog changelog
     */
    public GatewayAggregate(final String id, final TransactionChangelog transactionChangelog) {
        super(id, transactionChangelog);
    }

}

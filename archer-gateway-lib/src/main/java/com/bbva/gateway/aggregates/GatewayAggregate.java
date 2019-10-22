package com.bbva.gateway.aggregates;

import com.bbva.archer.avro.gateway.TransactionChangelog;
import com.bbva.ddd.domain.changelogs.repository.aggregates.CommonAggregate;
import com.bbva.ddd.domain.changelogs.repository.aggregates.annotations.Aggregate;
import com.bbva.gateway.constants.Constants;

/**
 * Internal aggregate for manage gateway with ddd
 */
@Aggregate(baseName = Constants.INTERNAL_SUFFIX)
public class GatewayAggregate extends CommonAggregate<String, TransactionChangelog> {

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

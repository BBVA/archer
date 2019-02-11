package com.bbva.examples.aggregates.user;

import com.bbva.avro.users.FiscalData;
import com.bbva.ddd.domain.aggregates.AbstractChildAggregateBase;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;
import com.bbva.ddd.domain.aggregates.annotations.AggregateParent;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import com.bbva.examples.aggregates.UserAggregate;
import kst.logging.Logger;
import kst.logging.LoggerFactory;

@Aggregate(baseName = "test_users_fiscal_data")
@AggregateParent(UserAggregate.class)
public class FiscalDataAggregate extends AbstractChildAggregateBase<String, FiscalData> {

    private static final Logger logger = LoggerFactory.getLogger(FiscalDataAggregate.class);

    public FiscalDataAggregate(final String id, final FiscalData data) {
        super(id, data);
    }

    public void update(final FiscalData modifiedData, final CommandRecord commandMessage) {
        apply("update", modifiedData, commandMessage, (id, e) -> {
            if (e != null) {
                logger.error("Error aggregating", e);
            }
            logger.info("change fiscal data from {} to {}", getData(), modifiedData);
        });
    }

    public static String baseName() {
        return "test_users_fiscal_data";
    }

}

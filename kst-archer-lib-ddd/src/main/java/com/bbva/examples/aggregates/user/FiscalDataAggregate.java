package com.bbva.examples.aggregates.user;

import com.bbva.avro.users.FiscalData;
import com.bbva.ddd.domain.aggregates.AbstractChildAggregateBase;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;
import com.bbva.ddd.domain.aggregates.annotations.AggregateParent;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import com.bbva.examples.aggregates.UserAggregate;

import java.util.logging.Logger;

@Aggregate(baseName = "test_users_fiscal_data")
@AggregateParent(UserAggregate.class)
public class FiscalDataAggregate extends AbstractChildAggregateBase<String, FiscalData> {

    private static final Logger logger = Logger.getLogger(FiscalDataAggregate.class.getName());

    public FiscalDataAggregate(String id, FiscalData data) {
        super(id, data);
    }

    public void update(FiscalData modifiedData, CommandRecord commandMessage) {
        apply("update", modifiedData, commandMessage, (id, e) -> {
            if (e != null) {
                e.printStackTrace();
            }
            logger.info("change fiscal data from " + getData() + " to " + modifiedData);
        });
    }

    public static String baseName() {
        return "test_users_fiscal_data";
    }

}

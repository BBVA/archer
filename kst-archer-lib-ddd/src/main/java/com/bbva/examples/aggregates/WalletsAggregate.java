package com.bbva.examples.aggregates;

import com.bbva.avro.Wallets;
import com.bbva.ddd.domain.aggregates.AbstractAggregateBase;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;
import com.bbva.ddd.domain.commands.read.CommandRecord;

@Aggregate(baseName = "test_wallets")
public class WalletsAggregate extends AbstractAggregateBase<String, Wallets> {

    public WalletsAggregate(final String id, final Wallets wallet) {
        super(id, wallet);
    }

    public void update(final Wallets modifiedData, final CommandRecord commandMessage) {
        apply("update", modifiedData, commandMessage, (id, e) -> {
            if (e != null) {
                e.printStackTrace();
            }
            System.out.println("change data from " + getData() + " to " + modifiedData);
        });
    }

    public static String baseName() {
        return "test_wallets";
    }

}

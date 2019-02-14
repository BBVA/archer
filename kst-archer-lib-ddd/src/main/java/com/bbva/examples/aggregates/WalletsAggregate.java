package com.bbva.examples.aggregates;

import com.bbva.avro.Wallets;
import com.bbva.ddd.domain.aggregates.SpecificAggregate;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;

@Aggregate(baseName = "test_wallets")
public class WalletsAggregate extends SpecificAggregate<String, Wallets> {

    public WalletsAggregate(final String id, final Wallets wallet) {
        super(id, wallet);
    }

    public static String baseName() {
        return "test_wallets";
    }

}

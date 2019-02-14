package com.bbva.examples.aggregates.user;

import com.bbva.avro.users.Settings;
import com.bbva.ddd.domain.aggregates.SpecificAggregate;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;
import com.bbva.ddd.domain.aggregates.annotations.AggregateParent;
import com.bbva.examples.aggregates.UserAggregate;

@Aggregate(baseName = "test_users_settings")
@AggregateParent(UserAggregate.class)
public class SettingsAggregate extends SpecificAggregate<String, Settings> {

    public SettingsAggregate(final String id, final Settings settings) {
        super(id, settings);
    }

    public static String baseName() {
        return "test_users_settings";
    }
}

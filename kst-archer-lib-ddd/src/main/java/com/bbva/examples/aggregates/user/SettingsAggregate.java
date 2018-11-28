package com.bbva.examples.aggregates.user;

import com.bbva.avro.users.Settings;
import com.bbva.ddd.domain.aggregates.AbstractChildAggregateBase;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;
import com.bbva.ddd.domain.aggregates.annotations.AggregateParent;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import com.bbva.examples.aggregates.UserAggregate;

import java.util.logging.Logger;

@Aggregate(baseName = "test_users_settings")
@AggregateParent(UserAggregate.class)
public class SettingsAggregate extends AbstractChildAggregateBase<String, Settings> {

    private static final Logger logger = Logger.getLogger(SettingsAggregate.class.getName());

    public SettingsAggregate(String id, Settings settings) {
        super(id, settings);
    }

    public void update(Settings modifiedData, CommandRecord commandMessage) {
        apply("update", modifiedData, commandMessage, (id, e) -> {
            if (e != null) {
                e.printStackTrace();
            }
            logger.info("change notifications from " + getData().getNotifications() + " to "
                    + modifiedData.getNotifications());
        });
    }

    public static String baseName() {
        return "test_users_settings";
    }
}

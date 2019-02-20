package com.bbva.examples.aggregates;

import com.bbva.avro.Users;
import com.bbva.avro.users.FiscalData;
import com.bbva.avro.users.Settings;
import com.bbva.ddd.domain.AggregateFactory;
import com.bbva.ddd.domain.aggregates.SpecificAggregate;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import com.bbva.examples.aggregates.user.FiscalDataAggregate;
import com.bbva.examples.aggregates.user.SettingsAggregate;

import java.util.logging.Logger;

@Aggregate(baseName = "test_users")
public class UserAggregate extends SpecificAggregate<String, Users> {

    private static final Logger logger = Logger.getLogger(UserAggregate.class.getName());

    public UserAggregate(final String id, final Users user) {
        super(id, user);
    }

    public void createFiscalData(final FiscalData fiscalData, final CommandRecord commandMessage) {
        AggregateFactory.create(FiscalDataAggregate.class, this.getId(), fiscalData, commandMessage, (key, e) -> {
            if (e != null) {
                e.printStackTrace();
            } else {
                logger.info("Fiscal data added to user " + this.getId());
            }
        });
    }

    public void updateFiscalData(final FiscalData fiscalData, final CommandRecord commandMessage) {
        try {
            AggregateFactory.load(FiscalDataAggregate.class, this.getId()).update(fiscalData, commandMessage, null);
        } catch (final NullPointerException e) {
            logger.warning("[WARN] Aggregate not found");
        }
    }

    public void addSettings(final Settings settings, final CommandRecord commandMessage) {
        final SettingsAggregate settingsAggregate = AggregateFactory.load(SettingsAggregate.class, this.getId());

        if (settingsAggregate != null) {
            settingsAggregate.update(settings, commandMessage, null);

        } else {
            AggregateFactory.create(SettingsAggregate.class, this.getId(), settings, commandMessage, (key, e) -> {
                if (e != null) {
                    e.printStackTrace();
                } else {
                    logger.info("Settings added to user " + this.getId());
                }
            });
        }
    }


    public static String baseName() {
        return "test_users";
    }

}

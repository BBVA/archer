package com.bbva.examples.aggregates;

import com.bbva.avro.Users;
import com.bbva.avro.users.FiscalData;
import com.bbva.avro.users.Settings;
import com.bbva.ddd.domain.AggregateFactory;
import com.bbva.ddd.domain.aggregates.AbstractAggregateBase;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import com.bbva.examples.aggregates.user.FiscalDataAggregate;
import com.bbva.examples.aggregates.user.SettingsAggregate;

import java.lang.reflect.InvocationTargetException;
import java.util.logging.Logger;

@Aggregate(baseName = "test_users")
public class UserAggregate extends AbstractAggregateBase<String, Users> {

    private static final Logger logger = Logger.getLogger(UserAggregate.class.getName());

    public UserAggregate(String id, Users user) {
        super(id, user);
    }

    public void createFiscalData(FiscalData fiscalData, CommandRecord commandMessage) {
        AggregateFactory.create(FiscalDataAggregate.class, this.getId(), fiscalData, commandMessage, (key, e) -> {
            if (e != null) {
                e.printStackTrace();
            } else {
                logger.info("Fiscal data added to user " + this.getId());
            }
        });
    }

    public void updateFiscalData(FiscalData fiscalData, CommandRecord commandMessage) {
        try {
            AggregateFactory.load(FiscalDataAggregate.class, this.getId()).update(fiscalData, commandMessage);

        } catch (NullPointerException e) {
            logger.warning("[WARN] Aggregate not found");
        }
    }

    public void addSettings(Settings settings, CommandRecord commandMessage) {
        SettingsAggregate settingsAggregate = AggregateFactory.load(SettingsAggregate.class, this.getId());

        if (settingsAggregate != null) {
            settingsAggregate.update(settings, commandMessage);

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

    public void delete(CommandRecord commandMessage) {
        try {
            apply("delete", commandMessage, (id, e) -> {
                if (e != null) {
                    e.printStackTrace();
                }
                System.out.println("remove user " + getId());
            });
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
    }

    public static String baseName() {
        return "test_users";
    }

}

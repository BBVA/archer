package com.bbva.examples.aggregates;

import com.bbva.avro.Users;
import com.bbva.ddd.domain.AggregateFactory;
import com.bbva.ddd.domain.annotations.Command;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import com.bbva.examples.aggregates.user.FiscalDataAggregate;
import com.bbva.examples.aggregates.user.SettingsAggregate;

import java.util.logging.Logger;

import static com.bbva.ddd.domain.commands.write.Command.CREATE_ACTION;
import static com.bbva.ddd.domain.commands.write.Command.DELETE_ACTION;

//@Handler
public class RootAggregate {

    private static final Logger logger = Logger.getLogger(RootAggregate.class.getName());

    public static final String ADD_FISCAL_DATA_ACTION = "addFiscalData";
    public static final String ADD_SETTINGS_ACTION = "addSettings";
    public static final String UPDATE_DEVICE_ACTION = "updateDevice";
    public static final String UPDATE_WALLET_ACTION = "updateWallet";
    public static final String UPDATE_CHANNEL_ACTION = "updateChannel";

    @Command(baseName = "test_users_fiscal_data", commandAction = CREATE_ACTION)
    public static void createUserFiscalData(final CommandRecord command) {
        try {
            AggregateFactory.create(UserAggregate.class, new Users(), command, (userId, e) -> {
                if (e != null) {
                    e.printStackTrace();
                } else {
                    logger.info("User created! id: " + userId);
                }
            }).createFiscalData(command.value(), command);
        } catch (final NullPointerException e) {
            logger.warning("[WARN] Aggregate not found");
        }
    }

    @Command(aggregateClass = FiscalDataAggregate.class, commandAction = ADD_FISCAL_DATA_ACTION)
    public static void updateUserFiscalData(final CommandRecord command) {
        try {
            AggregateFactory.load(UserAggregate.class, command.entityId()).updateFiscalData(command.value(), command);

        } catch (final NullPointerException e) {
            logger.warning("[WARN] Aggregate not found");
        }
    }

    @Command(aggregateClass = SettingsAggregate.class, commandAction = ADD_SETTINGS_ACTION)
    public static void updateUserSettings(final CommandRecord command) {
        try {
            AggregateFactory.load(UserAggregate.class, command.entityId()).addSettings(command.value(), command);

        } catch (final NullPointerException e) {
            logger.warning("[WARN] Aggregate not found");
        }
    }

    @Command(aggregateClass = UserAggregate.class, commandAction = DELETE_ACTION)
    public static void deleteUser(final CommandRecord command) {
        try {
            AggregateFactory.load(UserAggregate.class, command.entityId()).delete(command);

        } catch (final NullPointerException e) {
            logger.warning("[WARN] Aggregate not found");
        }
    }

    @Command(aggregateClass = DeviceAggregate.class, commandAction = CREATE_ACTION)
    public static void createDevice(final CommandRecord command) {
        AggregateFactory.create(DeviceAggregate.class, command.entityId(), command.value(), command, (key, e) -> {
            if (e != null) {
                e.printStackTrace();
            } else {
                logger.info("Device data added to device " + command.entityId());
            }
        });
    }

    @Command(aggregateClass = DeviceAggregate.class, commandAction = UPDATE_DEVICE_ACTION)
    public static void updateDevice(final CommandRecord command) {
        try {
            AggregateFactory.load(DeviceAggregate.class, command.entityId()).update(command.value(), command);

        } catch (final NullPointerException e) {
            logger.warning("[WARN] Aggregate not found");
        }
    }

    @Command(aggregateClass = DeviceAggregate.class, commandAction = DELETE_ACTION)
    public static void deleteDevice(final CommandRecord command) {
        try {
            AggregateFactory.load(DeviceAggregate.class, command.entityId()).delete(command);

        } catch (final NullPointerException e) {
            logger.warning("[WARN] Aggregate not found");
        }
    }

    @Command(aggregateClass = WalletsAggregate.class, commandAction = CREATE_ACTION)
    public static void createWallets(final CommandRecord command) {
        AggregateFactory.create(WalletsAggregate.class, command.entityId(), command.value(), command, (key, e) -> {
            if (e != null) {
                e.printStackTrace();
            } else {
                logger.info("Wallet data added to wallet " + command.entityId());
            }
        });
    }

    @Command(aggregateClass = WalletsAggregate.class, commandAction = UPDATE_WALLET_ACTION)
    public static void updateWallets(final CommandRecord command) {
        try {
            AggregateFactory.load(WalletsAggregate.class, command.entityId()).update(command.value(), command);

        } catch (final NullPointerException e) {
            logger.warning("[WARN] Aggregate not found");
        }
    }

    @Command(aggregateClass = ChannelsAggregate.class, commandAction = CREATE_ACTION)
    public static void createChannels(final CommandRecord command) {
        AggregateFactory.create(ChannelsAggregate.class, command.entityId(), command.value(), command, (key, e) -> {
            if (e != null) {
                e.printStackTrace();
            } else {
                logger.info("Channel data added to channel " + command.entityId());
            }
        });
    }

    @Command(aggregateClass = ChannelsAggregate.class, commandAction = UPDATE_CHANNEL_ACTION)
    public static void updateChannels(final CommandRecord command) {
        try {
            AggregateFactory.load(ChannelsAggregate.class, command.entityId()).update(command.value(), command);

        } catch (final NullPointerException e) {
            logger.warning("[WARN] Aggregate not found");
        }
    }
}

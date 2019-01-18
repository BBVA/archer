package com.bbva.examples.aggregates;

import com.bbva.avro.Channels;
import com.bbva.avro.Devices;
import com.bbva.avro.Users;
import com.bbva.avro.Wallets;
import com.bbva.avro.users.FiscalData;
import com.bbva.avro.users.Settings;
import com.bbva.ddd.domain.AggregateFactory;
import com.bbva.ddd.domain.commands.read.CommandRecord;

import java.util.logging.Logger;

public class RootAggregate {

    private static final Logger logger = Logger.getLogger(RootAggregate.class.getName());

    public static void createUserFiscalData(final FiscalData fiscalData, final CommandRecord command) {
        try {
            AggregateFactory.create(UserAggregate.class, new Users(), command, (userId, e) -> {
                if (e != null) {
                    e.printStackTrace();
                } else {
                    logger.info("User created! id: " + userId);
                }
            }).createFiscalData(fiscalData, command);
        } catch (final NullPointerException e) {
            logger.warning("[WARN] Aggregate not found");
        }
    }

    public static void updateUserFiscalData(final FiscalData fiscalData, final CommandRecord command) {
        try {
            AggregateFactory.load(UserAggregate.class, command.entityId()).updateFiscalData(fiscalData, command);

        } catch (final NullPointerException e) {
            logger.warning("[WARN] Aggregate not found");
        }
    }

    public static void updateUserSettings(final Settings settings, final CommandRecord command) {
        try {
            AggregateFactory.load(UserAggregate.class, command.entityId()).addSettings(settings, command);

        } catch (final NullPointerException e) {
            logger.warning("[WARN] Aggregate not found");
        }
    }

    public static void deleteUser(final CommandRecord command) {
        try {
            AggregateFactory.load(UserAggregate.class, command.entityId()).delete(command);

        } catch (final NullPointerException e) {
            logger.warning("[WARN] Aggregate not found");
        }
    }

    public static void createDevice(final Devices devices, final CommandRecord command) {
        AggregateFactory.create(DeviceAggregate.class, command.entityId(), devices, command, (key, e) -> {
            if (e != null) {
                e.printStackTrace();
            } else {
                logger.info("Device data added to device " + command.entityId());
            }
        });
    }

    public static void updateDevice(final Devices devices, final CommandRecord command) {
        try {
            AggregateFactory.load(DeviceAggregate.class, command.entityId()).update(devices, command);

        } catch (final NullPointerException e) {
            logger.warning("[WARN] Aggregate not found");
        }
    }

    public static void deleteDevice(final CommandRecord command) {
        try {
            AggregateFactory.load(DeviceAggregate.class, command.entityId()).delete(command);

        } catch (final NullPointerException e) {
            logger.warning("[WARN] Aggregate not found");
        }
    }

    public static void createWallets(final Wallets wallets, final CommandRecord command) {
        AggregateFactory.create(WalletsAggregate.class, command.entityId(), wallets, command, (key, e) -> {
            if (e != null) {
                e.printStackTrace();
            } else {
                logger.info("Wallet data added to wallet " + command.entityId());
            }
        });
    }

    public static void updateWallets(final Wallets wallets, final CommandRecord command) {
        try {
            AggregateFactory.load(WalletsAggregate.class, command.entityId()).update(wallets, command);

        } catch (final NullPointerException e) {
            logger.warning("[WARN] Aggregate not found");
        }
    }

    public static void createChannels(final Channels channels, final CommandRecord command) {
        AggregateFactory.create(ChannelsAggregate.class, command.entityId(), channels, command, (key, e) -> {
            if (e != null) {
                e.printStackTrace();
            } else {
                logger.info("Channel data added to channel " + command.entityId());
            }
        });
    }

    public static void updateChannels(final Channels channels, final CommandRecord command) {
        try {
            AggregateFactory.load(ChannelsAggregate.class, command.entityId()).update(channels, command);

        } catch (final NullPointerException e) {
            logger.warning("[WARN] Aggregate not found");
        }
    }
}

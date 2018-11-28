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

    public RootAggregate() {
    }

    public void createUserFiscalData(FiscalData fiscalData, CommandRecord command) {
        try {
            AggregateFactory.create(UserAggregate.class, new Users(), command, (userId, e) -> {
                if (e != null) {
                    e.printStackTrace();
                } else {
                    logger.info("User created! id: " + userId);
                }
            }).createFiscalData(fiscalData, command);
        } catch (NullPointerException e) {
            logger.warning("[WARN] Aggregate not found");
        }
    }

    public void updateUserFiscalData(FiscalData fiscalData, CommandRecord command) {
        try {
            AggregateFactory.load(UserAggregate.class, command.entityId()).updateFiscalData(fiscalData, command);

        } catch (NullPointerException e) {
            logger.warning("[WARN] Aggregate not found");
        }
    }

    public void updateUserSettings(Settings settings, CommandRecord command) {
        try {
            AggregateFactory.load(UserAggregate.class, command.entityId()).addSettings(settings, command);

        } catch (NullPointerException e) {
            logger.warning("[WARN] Aggregate not found");
        }
    }

    public void deleteUser(CommandRecord command) {
        try {
            AggregateFactory.load(UserAggregate.class, command.entityId()).delete(command);

        } catch (NullPointerException e) {
            logger.warning("[WARN] Aggregate not found");
        }
    }

    public void createDevice(Devices devices, CommandRecord command) {
        AggregateFactory.create(DeviceAggregate.class, command.entityId(), devices, command, (key, e) -> {
            if (e != null) {
                e.printStackTrace();
            } else {
                logger.info("Device data added to device " + command.entityId());
            }
        });
    }

    public void updateDevice(Devices devices, CommandRecord command) {
        try {
            AggregateFactory.load(DeviceAggregate.class, command.entityId()).update(devices, command);

        } catch (NullPointerException e) {
            logger.warning("[WARN] Aggregate not found");
        }
    }

    public void deleteDevice(CommandRecord command) {
        try {
            AggregateFactory.load(DeviceAggregate.class, command.entityId()).delete(command);

        } catch (NullPointerException e) {
            logger.warning("[WARN] Aggregate not found");
        }
    }

    public void createWallets(Wallets wallets, CommandRecord command) {
        AggregateFactory.create(WalletsAggregate.class, command.entityId(), wallets, command, (key, e) -> {
            if (e != null) {
                e.printStackTrace();
            } else {
                logger.info("Wallet data added to wallet " + command.entityId());
            }
        });
    }

    public void updateWallets(Wallets wallets, CommandRecord command) {
        try {
            AggregateFactory.load(WalletsAggregate.class, command.entityId()).update(wallets, command);

        } catch (NullPointerException e) {
            logger.warning("[WARN] Aggregate not found");
        }
    }

    public void createChannels(Channels channels, CommandRecord command) {
        AggregateFactory.create(ChannelsAggregate.class, command.entityId(), channels, command, (key, e) -> {
            if (e != null) {
                e.printStackTrace();
            } else {
                logger.info("Channel data added to channel " + command.entityId());
            }
        });
    }

    public void updateChannels(Channels channels, CommandRecord command) {
        try {
            AggregateFactory.load(ChannelsAggregate.class, command.entityId()).update(channels, command);

        } catch (NullPointerException e) {
            logger.warning("[WARN] Aggregate not found");
        }
    }
}

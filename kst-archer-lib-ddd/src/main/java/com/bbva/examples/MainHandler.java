package com.bbva.examples;

import com.bbva.avro.Channels;
import com.bbva.avro.Devices;
import com.bbva.avro.Wallets;
import com.bbva.avro.users.FiscalData;
import com.bbva.avro.users.Settings;
import com.bbva.common.config.ApplicationConfig;
import com.bbva.ddd.domain.Handler;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import com.bbva.ddd.domain.commands.write.Command;
import com.bbva.examples.aggregates.*;
import com.bbva.examples.aggregates.user.FiscalDataAggregate;
import com.bbva.examples.aggregates.user.SettingsAggregate;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class MainHandler implements Handler {

    private static String USER_COMMANDS_TOPICS = UserAggregate.baseName()
            + ApplicationConfig.COMMANDS_RECORD_NAME_SUFFIX;
    private static String USER_FISCAL_DATA_COMMANDS_TOPICS = FiscalDataAggregate.baseName()
            + ApplicationConfig.COMMANDS_RECORD_NAME_SUFFIX;
    private static String USERS_SETTINGS_COMMANDS_TOPICS = SettingsAggregate.baseName()
            + ApplicationConfig.COMMANDS_RECORD_NAME_SUFFIX;
    private static String DEVICE_COMMANDS_TOPICS = DeviceAggregate.baseName()
            + ApplicationConfig.COMMANDS_RECORD_NAME_SUFFIX;
    private static String WALLETS_COMMANDS_TOPICS = WalletsAggregate.baseName()
            + ApplicationConfig.COMMANDS_RECORD_NAME_SUFFIX;
    private static String CHANNELS_COMMANDS_TOPICS = ChannelsAggregate.baseName()
            + ApplicationConfig.COMMANDS_RECORD_NAME_SUFFIX;

    public static final String ADD_FISCAL_DATA_ACTION = "addFiscalData";
    public static final String ADD_SETTINGS_ACTION = "addSettings";
    public static final String UPDATE_DEVICE_ACTION = "updateDevice";
    public static final String UPDATE_WALLET_ACTION = "updateWallet";
    public static final String UPDATE_CHANNEL_ACTION = "updateChannel";

    private final RootAggregate rootAggregate;
    private final Logger logger = Logger.getLogger(MainHandler.class.getName());

    public MainHandler(RootAggregate rootAggregate) {
        this.rootAggregate = rootAggregate;
    }

    @Override
    public List<String> commandsSubscribed() {
        return Arrays.asList(USER_COMMANDS_TOPICS, USER_FISCAL_DATA_COMMANDS_TOPICS, USERS_SETTINGS_COMMANDS_TOPICS,
                DEVICE_COMMANDS_TOPICS, WALLETS_COMMANDS_TOPICS, CHANNELS_COMMANDS_TOPICS);
    }

    @Override
    public void processCommand(CommandRecord command) {
        logger.info("Method: " + command.name());
        logger.info("Origin header: " + command.optionalRecordHeaders().getOrigin());

        if (command.topic().equals(USER_COMMANDS_TOPICS)) {
            switch (command.name()) {
            case Command.DELETE_ACTION:
                rootAggregate.deleteUser(command);
                break;
            }

        } else if (command.topic().equals(USER_FISCAL_DATA_COMMANDS_TOPICS)) {
            FiscalData fiscalData = command.value();

            switch (command.name()) {
            case Command.CREATE_ACTION:
                rootAggregate.createUserFiscalData(fiscalData, command);
                break;
            case ADD_FISCAL_DATA_ACTION:
                rootAggregate.updateUserFiscalData(fiscalData, command);

                break;
            }

        } else if (command.topic().equals(USERS_SETTINGS_COMMANDS_TOPICS)) {
            Settings settings = command.value();

            switch (command.name()) {
            case ADD_SETTINGS_ACTION:
                rootAggregate.updateUserSettings(settings, command);
                break;
            }

        } else if (command.topic().equals(DEVICE_COMMANDS_TOPICS)) {
            Devices devices = command.value();

            switch (command.name()) {
            case Command.CREATE_ACTION:
                rootAggregate.createDevice(devices, command);
                break;
            case UPDATE_DEVICE_ACTION:
                rootAggregate.updateDevice(devices, command);
                break;
            case Command.DELETE_ACTION:
                rootAggregate.deleteDevice(command);
                break;
            }
        } else if (command.topic().equals(WALLETS_COMMANDS_TOPICS)) {
            Wallets wallets = command.value();

            switch (command.name()) {
            case Command.CREATE_ACTION:
                rootAggregate.createWallets(wallets, command);
                break;
            case UPDATE_WALLET_ACTION:
                rootAggregate.updateWallets(wallets, command);
                break;
            }
        } else if (command.topic().equals(CHANNELS_COMMANDS_TOPICS)) {
            Channels channels = command.value();

            switch (command.name()) {
            case Command.CREATE_ACTION:
                rootAggregate.createChannels(channels, command);
                break;
            case UPDATE_CHANNEL_ACTION:
                rootAggregate.updateChannels(channels, command);
                break;
            }
        } else {
            logger.info("Event not handled");
        }
    }
}
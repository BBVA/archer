package com.bbva.examples;

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

    private static final String USER_COMMANDS_TOPICS = UserAggregate.baseName()
            + ApplicationConfig.COMMANDS_RECORD_NAME_SUFFIX;
    private static final String USER_FISCAL_DATA_COMMANDS_TOPICS = FiscalDataAggregate.baseName()
            + ApplicationConfig.COMMANDS_RECORD_NAME_SUFFIX;
    private static final String USERS_SETTINGS_COMMANDS_TOPICS = SettingsAggregate.baseName()
            + ApplicationConfig.COMMANDS_RECORD_NAME_SUFFIX;
    private static final String DEVICE_COMMANDS_TOPICS = DeviceAggregate.baseName()
            + ApplicationConfig.COMMANDS_RECORD_NAME_SUFFIX;
    private static final String WALLETS_COMMANDS_TOPICS = WalletsAggregate.baseName()
            + ApplicationConfig.COMMANDS_RECORD_NAME_SUFFIX;
    private static final String CHANNELS_COMMANDS_TOPICS = ChannelsAggregate.baseName()
            + ApplicationConfig.COMMANDS_RECORD_NAME_SUFFIX;

    public static final String ADD_FISCAL_DATA_ACTION = "addFiscalData";
    public static final String ADD_SETTINGS_ACTION = "addSettings";
    public static final String UPDATE_DEVICE_ACTION = "updateDevice";
    public static final String UPDATE_WALLET_ACTION = "updateWallet";
    public static final String UPDATE_CHANNEL_ACTION = "updateChannel";

    private final RootAggregate rootAggregate;
    private static final Logger logger = Logger.getLogger(MainHandler.class.getName());

    public MainHandler(final RootAggregate rootAggregate) {
        this.rootAggregate = rootAggregate;
    }

    @Override
    public List<String> commandsSubscribed() {
        return Arrays.asList(USER_COMMANDS_TOPICS, USER_FISCAL_DATA_COMMANDS_TOPICS, USERS_SETTINGS_COMMANDS_TOPICS,
                DEVICE_COMMANDS_TOPICS, WALLETS_COMMANDS_TOPICS, CHANNELS_COMMANDS_TOPICS);
    }

    @Override
    public void processCommand(final CommandRecord command) {
        logger.info("Method: " + command.name());
        logger.info("Origin header: " + command.optionalRecordHeaders().getOrigin());

        if (command.topic().equals(USER_COMMANDS_TOPICS)) {
            switch (command.name()) {
                case Command.DELETE_ACTION:
                    RootAggregate.deleteUser(command);
                    break;
            }

        } else if (command.topic().equals(USER_FISCAL_DATA_COMMANDS_TOPICS)) {
            switch (command.name()) {
                case Command.CREATE_ACTION:
                    RootAggregate.createUserFiscalData(command);
                    break;
                case ADD_FISCAL_DATA_ACTION:
                    RootAggregate.updateUserFiscalData(command);
                    break;
            }

        } else if (command.topic().equals(USERS_SETTINGS_COMMANDS_TOPICS)) {
            switch (command.name()) {
                case ADD_SETTINGS_ACTION:
                    RootAggregate.updateUserSettings(command);
                    break;
            }

        } else if (command.topic().equals(DEVICE_COMMANDS_TOPICS)) {
            switch (command.name()) {
                case Command.CREATE_ACTION:
                    RootAggregate.createDevice(command);
                    break;
                case UPDATE_DEVICE_ACTION:
                    RootAggregate.updateDevice(command);
                    break;
                case Command.DELETE_ACTION:
                    RootAggregate.deleteDevice(command);
                    break;
            }
        } else if (command.topic().equals(WALLETS_COMMANDS_TOPICS)) {
            switch (command.name()) {
                case Command.CREATE_ACTION:
                    RootAggregate.createWallets(command);
                    break;
                case UPDATE_WALLET_ACTION:
                    RootAggregate.updateWallets(command);
                    break;
            }
        } else if (command.topic().equals(CHANNELS_COMMANDS_TOPICS)) {
            switch (command.name()) {
                case Command.CREATE_ACTION:
                    RootAggregate.createChannels(command);
                    break;
                case UPDATE_CHANNEL_ACTION:
                    RootAggregate.updateChannels(command);
                    break;
            }
        } else {
            logger.info("Event not handled");
        }
    }
}

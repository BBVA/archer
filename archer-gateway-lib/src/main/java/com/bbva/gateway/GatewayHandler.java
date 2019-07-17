package com.bbva.gateway;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.ddd.domain.Handler;
import com.bbva.ddd.domain.commands.read.CommandRecord;
import com.bbva.ddd.domain.events.read.EventRecord;
import com.bbva.gateway.config.Configuration;
import com.bbva.gateway.config.annotations.ServiceConfig;
import com.bbva.gateway.service.IGatewayService;
import com.bbva.gateway.service.impl.GatewayService;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;

import java.util.*;

import static com.bbva.gateway.constants.ConfigConstants.*;

public class GatewayHandler implements Handler {
    private static final Logger logger = LoggerFactory.getLogger(GatewayService.class);

    private static String baseName;
    private static final Map<String, IGatewayService> commandServices = new HashMap<>();
    private static final Map<String, IGatewayService> eventServices = new HashMap<>();
    List<String> commandsSubscribed = new ArrayList<>();
    List<String> eventsSubscribed = new ArrayList<>();
    protected static Configuration config;

    public GatewayHandler(final String servicePackages, final Configuration generalConfig) {
        final List<Class> serviceClasses = Configuration.getServiceClasses(servicePackages);
        config = generalConfig;
        baseName = (String) config.getCustom().get(GATEWAY_TOPIC);

        for (final Class serviceClass : serviceClasses) {
            final ServiceConfig serviceConfig = (ServiceConfig) serviceClass.getAnnotation(ServiceConfig.class);

            final LinkedHashMap<String, Object> gatewayConfig = Configuration.getServiceConfig(serviceConfig.file());
            final String commandAction = (String) gatewayConfig.get(GATEWAY_COMMAND_ACTION);
            final String event = (String) gatewayConfig.get(GATEWAY_EVENT_NAME);
            if (commandAction != null) {
                commandsSubscribed.add(baseName + ApplicationConfig.COMMANDS_RECORD_NAME_SUFFIX);
                initActionService(serviceClass, gatewayConfig, commandAction, null);
            } else if (event != null) {
                eventsSubscribed.add(event + ApplicationConfig.EVENTS_RECORD_NAME_SUFFIX);
                initActionService(serviceClass, gatewayConfig, null, event + ApplicationConfig.EVENTS_RECORD_NAME_SUFFIX);
            }
        }
    }

    @Override
    public List<String> commandsSubscribed() {
        return commandsSubscribed;
    }

    @Override
    public List<String> eventsSubscribed() {
        return eventsSubscribed;
    }

    @Override
    public void processCommand(final CommandRecord command) {
        final String action = command.name();
        if (commandServices.containsKey(action)) {
            new Thread(() -> commandServices.get(action).processRecord(command)).start();
        }
    }

    @Override
    public void processEvent(final EventRecord eventMessage) {
        if (eventServices.containsKey(eventMessage.topic())) {
            new Thread(() -> eventServices.get(eventMessage.topic()).processRecord(eventMessage)).start();
        }
    }

    private static void initActionService(final Class serviceClass, final LinkedHashMap<String, Object> gatewayConfig, final String commandAction, final String event) {
        final Configuration newConfig;
        newConfig = new Configuration();
        newConfig.setGateway(gatewayConfig);
        newConfig.setApplicationConfig(config.getApplicationConfig());
        newConfig.setCustom((LinkedHashMap<String, Object>) config.getCustom());
        IGatewayService service = null;
        try {
            service = (IGatewayService) serviceClass.newInstance();
        } catch (final IllegalAccessException | InstantiationException e) {
            logger.error("Error initializing service", e);
        }
        service.init(newConfig, baseName);
        service.postInitActions();
        if (commandAction != null) {
            commandServices.put(commandAction, service);
        } else {
            eventServices.put(event, service);
        }
    }

    public Map<String, IGatewayService> getCommandServices() {
        return commandServices;
    }

    public Map<String, IGatewayService> getEventServices() {
        return eventServices;
    }
}

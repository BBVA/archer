package com.bbva.gateway;

import com.bbva.common.config.AppConfig;
import com.bbva.ddd.domain.Handler;
import com.bbva.ddd.domain.commands.consumers.CommandHandlerContext;
import com.bbva.ddd.domain.events.consumers.EventHandlerContext;
import com.bbva.gateway.config.ConfigBuilder;
import com.bbva.gateway.config.GatewayConfig;
import com.bbva.gateway.config.annotations.ServiceConfig;
import com.bbva.gateway.service.IGatewayService;
import com.bbva.gateway.service.impl.GatewayService;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * Main handler of gateway
 */
public class GatewayHandler implements Handler {
    private static final Logger logger = LoggerFactory.getLogger(GatewayService.class);

    private static String baseName;
    private static final Map<String, IGatewayService> commandServices = new HashMap<>();
    private static final Map<String, IGatewayService> eventServices = new HashMap<>();
    List<String> commandsSubscribed = new ArrayList<>();
    List<String> eventsSubscribed = new ArrayList<>();
    protected static GatewayConfig config;

    /**
     * Constructor
     *
     * @param servicePackages package to find services
     * @param configuration   configuration
     */
    public GatewayHandler(final String servicePackages, final GatewayConfig configuration) {
        final List<Class> serviceClasses = ConfigBuilder.getServiceClasses(servicePackages);
        config = configuration;
        baseName = (String) config.custom(GatewayConfig.CustomProperties.GATEWAY_TOPIC);

        for (final Class serviceClass : serviceClasses) {
            final ServiceConfig serviceConfig = (ServiceConfig) serviceClass.getAnnotation(ServiceConfig.class);

            final LinkedHashMap<String, Object> gatewayConfig = ConfigBuilder.getServiceConfig(serviceConfig.file());
            final String commandAction = (String) gatewayConfig.get(GatewayConfig.GATEWAY_COMMAND_ACTION);
            final String event = (String) gatewayConfig.get(GatewayConfig.GATEWAY_EVENT_NAME);
            if (commandAction != null) {
                commandsSubscribed.add(baseName + AppConfig.COMMANDS_RECORD_NAME_SUFFIX);
                initActionService(serviceClass, gatewayConfig, commandAction, null);
            } else if (event != null) {
                eventsSubscribed.add(event + AppConfig.EVENTS_RECORD_NAME_SUFFIX);
                initActionService(serviceClass, gatewayConfig, null, event + AppConfig.EVENTS_RECORD_NAME_SUFFIX);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> commandsSubscribed() {
        return commandsSubscribed;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> eventsSubscribed() {
        return eventsSubscribed;
    }

    /**
     * Call to service action for command
     *
     * @param context command received
     */
    @Override
    public void processCommand(final CommandHandlerContext context) {
        final String action = context.consumedRecord().name();
        if (commandServices.containsKey(action)) {
            new Thread(() -> commandServices.get(action).processRecord(context)).start();
        }
    }

    /**
     * Call to service action for event
     *
     * @param context Event consumed from the event store
     */
    @Override
    public void processEvent(final EventHandlerContext context) {
        if (eventServices.containsKey(context.consumedRecord().topic())) {
            new Thread(() -> eventServices.get(context.consumedRecord().topic()).processRecord(context)).start();
        }
    }

    private static void initActionService(final Class serviceClass, final LinkedHashMap<String, Object> gatewayConfig, final String commandAction, final String event) {
        //TODO clone en only one line
        GatewayConfig newConfig = new GatewayConfig();
        try {
            newConfig = (GatewayConfig) BeanUtils.cloneBean(config);
        } catch (final IllegalAccessException | InstantiationException | InvocationTargetException | NoSuchMethodException e) {
            logger.error("Error copying properties", e);
        }
        newConfig.gateway().putAll(gatewayConfig);

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

    /**
     * Get the list of command services configured
     *
     * @return command services
     */
    public Map<String, IGatewayService> getCommandServices() {
        return commandServices;
    }

    /**
     * Get the list of event services configured
     *
     * @return event services
     */
    public Map<String, IGatewayService> getEventServices() {
        return eventServices;
    }
}

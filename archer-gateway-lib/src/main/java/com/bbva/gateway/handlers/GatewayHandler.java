package com.bbva.gateway.handlers;

import com.bbva.ddd.domain.changelogs.consumers.ChangelogHandlerContext;
import com.bbva.ddd.domain.commands.consumers.CommandHandlerContext;
import com.bbva.ddd.domain.events.consumers.EventHandlerContext;
import com.bbva.ddd.domain.handlers.Handler;
import com.bbva.gateway.config.ConfigBuilder;
import com.bbva.gateway.config.GatewayConfig;
import com.bbva.gateway.config.annotations.ServiceConfig;
import com.bbva.gateway.service.GatewayService;
import com.bbva.gateway.service.base.GatewayBaseService;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Main handler of gateway
 */
public class GatewayHandler implements Handler {
    private static final Logger logger = LoggerFactory.getLogger(GatewayBaseService.class);

    private static final Map<String, GatewayService> services = new HashMap<>();
    private static final Map<String, GatewayService> eventServices = new HashMap<>();
    private final List<String> commandsSubscribed = new ArrayList<>();
    private final List<String> eventsSubscribed = new ArrayList<>();
    private final List<String> changelogsSubscribed = new ArrayList<>();
    private final GatewayConfig config;

    /**
     * Constructor
     *
     * @param servicePackages package to find services
     * @param configuration   configuration
     */
    public GatewayHandler(final String servicePackages, final GatewayConfig configuration) {
        final List<Class> serviceClasses = ConfigBuilder.getServiceClasses(servicePackages);
        config = configuration;

        for (final Class serviceClass : serviceClasses) {
            final ServiceConfig serviceConfig = (ServiceConfig) serviceClass.getAnnotation(ServiceConfig.class);

            final Map<String, Object> gatewayConfig = ConfigBuilder.getServiceConfig(serviceConfig.file());
            final Map<String, Object> source = (HashMap<String, Object>) gatewayConfig.get(GatewayConfig.GatewayProperties.GATEWAY_SOURCE);
            final String sourceName = (String) source.get(GatewayConfig.GatewayProperties.GATEWAY_SOURCE_NAME);
            final String sourceType = (String) source.get(GatewayConfig.GatewayProperties.GATEWAY_SOURCE_TYPE);
            final String serviceName;
            if (sourceType.equals(GatewayConfig.SourceTypes.COMMAND.getName())) {
                final String commandAction = (String) source.get(GatewayConfig.GatewayProperties.GATEWAY_SOURCE_ACTION);
                commandsSubscribed.add(sourceName);
                serviceName = sourceName + ":" + commandAction;
            } else if (sourceType.equals(GatewayConfig.SourceTypes.EVENT.getName())) {
                eventsSubscribed.add(sourceName);
                serviceName = sourceName;
            } else {
                changelogsSubscribed.add(sourceName);
                serviceName = sourceName;
            }
            initActionService(serviceClass, gatewayConfig, serviceName);
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
     * {@inheritDoc}
     */
    @Override
    public List<String> dataChangelogsSubscribed() {
        return changelogsSubscribed;
    }

    /**
     * Call to service when a command:action happens
     *
     * @param context command received
     */
    @Override
    public void processCommand(final CommandHandlerContext context) {
        final String serviceName = context.consumedRecord().topic() + ":" + context.consumedRecord().action();
        if (services.containsKey(serviceName)) {
            new Thread(() -> services.get(serviceName).processRecord(context)).start();
        }
    }

    /**
     * Call to service when an event arrives
     *
     * @param context Event consumed from the event store
     */
    @Override
    public void processEvent(final EventHandlerContext context) {
        final String topic = context.consumedRecord().topic();
        if (services.containsKey(topic)) {
            new Thread(() -> services.get(topic).processRecord(context)).start();
        }
    }

    /**
     * Call to service when a changelog
     *
     * @param context Event consumed from the event store
     */
    @Override
    public void processDataChangelog(final ChangelogHandlerContext context) {
        final String topic = context.consumedRecord().topic();
        if (services.containsKey(topic)) {
            new Thread(() -> services.get(topic).processRecord(context)).start();
        }
    }

    private void initActionService(final Class serviceClass, final Map<String, Object> gatewayConfig, final String serviceName) {
        GatewayConfig newConfig = new GatewayConfig();
        try {
            newConfig = (GatewayConfig) BeanUtils.cloneBean(config);
        } catch (final IllegalAccessException | InstantiationException | InvocationTargetException | NoSuchMethodException e) {
            logger.error("Error copying properties", e);
        }
        newConfig.gateway().putAll(gatewayConfig);

        try {
            final GatewayService service = (GatewayService) serviceClass.newInstance();
            service.initialize(newConfig);
            //        service.postInitActions();
            services.put(serviceName, service);
        } catch (final IllegalAccessException | InstantiationException e) {
            logger.error("Error initializing service", e);
        }
    }

    /**
     * Get the list of command services configured
     *
     * @return command services
     */
//    public Map<String, GatewayService> getCommandServices() {
//        return services;
//    }

    /**
     * Get the list of event services configured
     *
     * @return event services
     */
//    public Map<String, GatewayService> getEventServices() {
//        return eventServices;
//    }
}

package com.bbva.ddd.domain;

import com.bbva.common.config.AppConfig;
import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.managers.ManagerFactory;
import com.bbva.common.managers.RunnableManager;
import com.bbva.common.producers.Producer;
import com.bbva.common.utils.TopicManager;
import com.bbva.ddd.domain.changelogs.consumers.ChangelogHandlerContext;
import com.bbva.ddd.domain.commands.consumers.CommandHandlerContext;
import com.bbva.ddd.domain.events.consumers.EventHandlerContext;
import com.bbva.ddd.domain.handlers.Handler;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Manage communication between domain and consumers layer
 */
public final class Binder {

    private static final Logger logger = LoggerFactory.getLogger(Binder.class);

    private static AppConfig appConfig;
    private Handler handler;
    private static Binder instance;
    private List<RunnableManager> consumers;

    /**
     * Constructor
     */
    private Binder(final AppConfig config) {
        appConfig = config;
    }

    /**
     * Create new instance of binder
     *
     * @return the instance
     */
    public static Binder create(final AppConfig config) {
        instance = new Binder(config);
        return instance;
    }

    public Binder build(final Handler domainHandler) {
        handler = domainHandler;
        configureHandlers(appConfig);
        return this;
    }

    /**
     * Get instance of binder
     *
     * @return the instance
     */
    public static Binder get() {
        return instance;
    }


    /**
     * Start all consumers
     */
    public void start() {
        final ExecutorService executor = Executors.newFixedThreadPool(consumers.size());

        for (final RunnableManager consumer : consumers) {
            executor.submit(consumer);
        }

        logger.info("Consumers have been started");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            for (final RunnableManager consumer : consumers) {
                consumer.shutdown();
            }
            executor.shutdown();
            try {
                executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            } catch (final InterruptedException e) {
                logger.warn("InterruptedException starting the application", e);
            }
        }));
    }

    private List<RunnableManager> configureHandlers(final AppConfig config) {
        consumers = new ArrayList<>();

        final List<String> commandsSubscribed = handler.commandsSubscribed();
        final List<String> eventsSubscribed = handler.eventsSubscribed();
        final List<String> dataChangelogsSubscribed = handler.dataChangelogsSubscribed();

        final Map<String, String> consumerTopics =
                Stream.of(commandsSubscribed, eventsSubscribed, dataChangelogsSubscribed).flatMap(Collection::stream)
                        .collect(Collectors.toMap(Function.identity(), type -> AppConfig.COMMON_RECORD_TYPE,
                                (command1, command2) -> command1));

        TopicManager.createTopics(consumerTopics, config);

        logger.info("Necessary consumer topics created");

        final String eventStore = (String) appConfig.get(AppConfig.EVENT_STORE);
        final String deliveryType = (String) appConfig.get(AppConfig.DELIVERY_TYPE);

        if (!commandsSubscribed.isEmpty()) {
            consumers.add(
                    new RunnableManager(ManagerFactory.create(eventStore, deliveryType, handler.commandsSubscribed(),
                            this::processCommand, appConfig), appConfig));
        }

        if (!eventsSubscribed.isEmpty()) {
            consumers.add(
                    new RunnableManager(ManagerFactory.create(eventStore, deliveryType, handler.eventsSubscribed(),
                            this::processEvent, appConfig), appConfig));
        }

        if (!dataChangelogsSubscribed.isEmpty()) {
            consumers.add(
                    new RunnableManager(ManagerFactory.create(eventStore, deliveryType, handler.dataChangelogsSubscribed(),
                            this::processChangelog, appConfig), appConfig));

        }
        logger.info("Handlers initialized");

        return consumers;
    }

    public void processCommand(final Object cRecord, final Object producer, final Object isReplay) {
        handler.processCommand(new CommandHandlerContext((CRecord) cRecord, (Producer) producer, (Boolean) isReplay));
    }

    public void processEvent(final Object cRecord, final Object producer, final Object isReplay) {
        handler.processEvent(new EventHandlerContext((CRecord) cRecord, (Producer) producer, (Boolean) isReplay));
    }

    public void processChangelog(final Object cRecord, final Object producer, final Object isReplay) {
        handler.processDataChangelog(new ChangelogHandlerContext((CRecord) cRecord, (Producer) producer, (Boolean) isReplay));
    }
}

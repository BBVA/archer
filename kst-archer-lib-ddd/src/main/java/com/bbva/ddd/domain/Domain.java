package com.bbva.ddd.domain;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.exceptions.ApplicationException;
import com.bbva.common.utils.GenericClass;
import com.bbva.common.utils.TopicManager;
import com.bbva.dataprocessors.DataProcessor;
import com.bbva.dataprocessors.builders.dataflows.DataflowBuilder;
import com.bbva.dataprocessors.builders.dataflows.states.EntityStateBuilder;
import com.bbva.dataprocessors.builders.dataflows.states.UniqueFieldStateBuilder;
import com.bbva.dataprocessors.builders.sql.QueryBuilder;
import com.bbva.ddd.domain.aggregates.AggregateBase;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;
import com.bbva.ddd.domain.aggregates.exceptions.AggregateDependenciesException;
import com.bbva.ddd.domain.changelogs.Repository;
import com.bbva.ddd.domain.changelogs.exceptions.RepositoryException;
import com.bbva.ddd.domain.changelogs.read.ChangelogConsumer;
import com.bbva.ddd.domain.commands.read.CommandConsumer;
import com.bbva.ddd.domain.events.read.EventConsumer;
import com.bbva.ddd.util.AnnotationUtil;
import kst.logging.Logger;
import kst.logging.LoggerFactory;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.collections.map.HashedMap;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class Domain {
    private static final Logger logger = LoggerFactory.getLogger(Domain.class);
    private final List<RunnableConsumer> consumers = new ArrayList<>();
    private final Handler handler;
    private final ApplicationConfig applicationConfig;
    private final Map<String, Class<? extends AggregateBase>> aggregatesMap = new HashedMap();

    public Domain(final Handler handler, final ApplicationConfig applicationConfig) {
        this.mapAggregates(handler);

        this.handler = handler;

        this.applicationConfig = applicationConfig;
        DataProcessor.create(this.applicationConfig);
        initRepositories();
    }

    public Domain(final ApplicationConfig applicationConfig) {
        this(new AutoConfiguredHandler(), applicationConfig);
    }

    public Domain addDataProcessorBuilder(final String name, final DataflowBuilder builder) {
        DataProcessor.get().add(name, builder);
        return this;
    }

    public Domain addDataProcessorBuilder(final QueryBuilder queryBuilder) {
        DataProcessor.get().add(queryBuilder);

        return this;
    }

    public <K, V extends SpecificRecordBase> Domain addEntityAsLocalState(final String baseName, final GenericClass<K> keyClass) {
        final String snapshotTopicName = applicationConfig.streams().get(ApplicationConfig.StreamsProperties.APPLICATION_NAME)
                + "_" + baseName;
        DataProcessor.get().add(baseName, new EntityStateBuilder<K, V>(snapshotTopicName, keyClass));
        logger.info("Local state {} added", baseName);
        return this;
    }

    public <K, V extends SpecificRecordBase, K1> Domain indexFieldAsLocalState(
            final String targetBaseName, final String originTopicName, final String fieldPath,
            final GenericClass<K> keyClass, final GenericClass<K1> key1Class) {

        DataProcessor.get().add(targetBaseName,
                new UniqueFieldStateBuilder<K, V, K1>(originTopicName, fieldPath, keyClass, key1Class));
        logger.info("Local state for index field {} added", fieldPath);
        return this;
    }

    public synchronized HelperDomain start() {

        initHandlers();

        DataProcessor.get().start();
        logger.info("States have been started");

        final HelperDomain app = new HelperDomain(applicationConfig);

        final ExecutorService executor = Executors.newFixedThreadPool(consumers.size());

        for (final RunnableConsumer consumer : consumers) {
            executor.submit(consumer);
        }

        logger.info("Consumers have been started");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            for (final RunnableConsumer consumer : consumers) {
                consumer.shutdown();
            }
            executor.shutdown();
            try {
                executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            } catch (final InterruptedException e) {
                logger.warn("InterruptedException starting the application", e);
            }
        }));

        return app;
    }

    private void mapAggregates(final Handler handler) {
        final List<Class> classes = AnnotationUtil.findAllAnnotatedClasses(Aggregate.class, handler);

        for (final Class<?> aggregateClass : classes) {
            final Aggregate aggregateAnnotation = aggregateClass.getAnnotation(Aggregate.class);
            final String baseName = aggregateAnnotation.baseName();
            aggregatesMap.put(baseName, aggregateClass.asSubclass(AggregateBase.class));
        }
    }

    private void initRepositories() {
        final Map<String, Repository> repositories = new HashMap<>();

        aggregatesMap.forEach((baseName, aggregateClass) -> {
            if (aggregateClass == null) {
                throw new ApplicationException("Aggregate cannot be null");
            }
            try {
                repositories.put(baseName, new Repository(baseName, aggregateClass, applicationConfig));
            } catch (final AggregateDependenciesException e) {
                logger.error("Error aggregating dependencies", e);
            }

            addEntityAsLocalState(baseName, new GenericClass<>(String.class));
        });

        if (repositories.size() != aggregatesMap.size()) {
            throw new RepositoryException("Error initializing repositories");
        }
        Repositories.getInstance().setRepositories(repositories);

        logger.info("Repositories initialized with Aggregates");
    }

    private void initHandlers() {
        final int numConsumers = 1;
        final List<String> commandsSubscribed = handler.commandsSubscribed();
        final List<String> eventsSubscribed = handler.eventsSubscribed();
        final List<String> dataChangelogsSubscribed = handler.dataChangelogsSubscribed();

        final Map<String, String> consumerTopics =
                Stream.of(commandsSubscribed, eventsSubscribed, dataChangelogsSubscribed).flatMap(Collection::stream)
                        .collect(Collectors.toMap(Function.identity(), type -> ApplicationConfig.COMMON_RECORD_TYPE,
                                (command1, command2) -> command1));

        TopicManager.createTopics(consumerTopics, applicationConfig);

        logger.info("Necessary consumer topics created");

        if (!commandsSubscribed.isEmpty()) {
            for (int i = 0; i < numConsumers; i++) {
                consumers.add(new CommandConsumer<>(i, handler.commandsSubscribed(), handler::processCommand,
                        applicationConfig));
            }
        }

        if (!eventsSubscribed.isEmpty()) {
            for (int i = 0; i < numConsumers; i++) {
                consumers.add(
                        new EventConsumer<>(i, handler.eventsSubscribed(), handler::processEvent, applicationConfig));
            }
        }

        if (!dataChangelogsSubscribed.isEmpty()) {
            for (int i = 0; i < numConsumers; i++) {
                consumers.add(new ChangelogConsumer<>(i, handler.dataChangelogsSubscribed(),
                        handler::processDataChangelog, applicationConfig));
            }
        }
        logger.info("Handlers initialized");
    }

}

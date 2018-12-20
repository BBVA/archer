package com.bbva.ddd.domain;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.utils.GenericClass;
import com.bbva.common.utils.TopicManager;
import com.bbva.dataprocessors.DataProcessor;
import com.bbva.dataprocessors.builders.dataflows.DataflowBuilder;
import com.bbva.dataprocessors.builders.dataflows.states.EntityStateBuilder;
import com.bbva.dataprocessors.builders.dataflows.states.UniqueFieldStateBuilder;
import com.bbva.dataprocessors.builders.sql.QueryBuilder;
import com.bbva.ddd.ApplicationServices;
import com.bbva.ddd.domain.aggregates.AggregateBase;
import com.bbva.ddd.domain.aggregates.annotations.Aggregate;
import com.bbva.ddd.domain.aggregates.exceptions.AggregateDependenciesException;
import com.bbva.ddd.domain.changelogs.Repository;
import com.bbva.ddd.domain.changelogs.exceptions.RepositoryException;
import com.bbva.ddd.domain.changelogs.read.ChangelogConsumer;
import com.bbva.ddd.domain.commands.read.CommandConsumer;
import com.bbva.ddd.domain.events.read.EventConsumer;
import kst.logging.LoggerGen;
import kst.logging.LoggerGenesis;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.collections.map.HashedMap;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class Domain {

    private final List<RunnableConsumer> consumers = new ArrayList<>();
    private final Handler handler;
    private final ApplicationConfig applicationConfig;
    private final Map<String, Class<? extends AggregateBase>> aggregatesMap = new HashedMap();
    private static final LoggerGen logger = LoggerGenesis.getLogger(Domain.class.getName());

    /**
     * @param handler
     * @param applicationConfig
     */
    public Domain(final Handler handler, final ApplicationConfig applicationConfig) throws RepositoryException {

        this.mapAggregates(handler);

        this.handler = handler;

        this.applicationConfig = applicationConfig;

        DataProcessor.create(this.applicationConfig);

        initRepositories();
    }

    public Domain addDataProcessorBuilder(final String name, final DataflowBuilder builder) {
        DataProcessor.get().add(name, builder);
        return this;
    }

    public Domain addDataProcessorBuilder(final QueryBuilder queryBuilder) {
        DataProcessor.get().add(queryBuilder);

        return this;
    }

    /**
     * Add a local state to StreamProcessor
     *
     * @param baseName
     * @param keyClass
     * @param <K>
     * @param <V>
     * @return Domain
     */
    public <K, V extends SpecificRecordBase> Domain addEntityAsLocalState(final String baseName, final GenericClass<K> keyClass) {
        final String snapshotTopicName = applicationConfig.streams().get(ApplicationConfig.StreamsProperties.APPLICATION_NAME)
                + "_" + baseName;
        DataProcessor.get().add(baseName, new EntityStateBuilder<K, V>(snapshotTopicName, keyClass));
        logger.info("Local state " + baseName + " added");
        return this;
    }

    /**
     * @param originTopicName
     * @param fieldPath
     * @param keyClass
     * @param <K>
     * @param <V>
     * @throws IllegalArgumentException
     */
    public <K, V extends SpecificRecordBase, K1> Domain indexFieldAsLocalState(final String targetBaseName,
                                                                               final String originTopicName, final String fieldPath, final GenericClass<K> keyClass, final GenericClass<K1> key1Class) {
        DataProcessor.get().add(targetBaseName,
                new UniqueFieldStateBuilder<K, V, K1>(originTopicName, fieldPath, keyClass, key1Class));
        logger.info("Local state for index field " + fieldPath + " added");
        return this;
    }

    /**
     * @return
     * @throws NullPointerException
     */
    public synchronized ApplicationServices start() {

        initHandlers();

        DataProcessor.get().start();
        logger.info("States have been started");

        final ApplicationServices app = new ApplicationServices(applicationConfig);

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
            } catch (final Exception e) {
                logger.error("Problems starting the application", e);
            }
        }));

        return app;
    }

    private void mapAggregates(final Handler handler) {
        final String mainPackage = handler.getClass().getCanonicalName().split("\\.")[0];
        aggregatesByPackage(mainPackage);

        if (aggregatesMap.isEmpty()) {
            mapAllPackagesAggregates();
        }

        logger.info(" Aggregates mapped: " + aggregatesMap.toString());
    }

    private void mapAllPackagesAggregates() {
        final Package[] packages = Package.getPackages();
        for (final Package packageLoaded : packages) {
            // Exclude unnecesary packages
            if (!packageLoaded.getName().matches("^(org|sun|java|jdk).*")) {
                aggregatesByPackage(packageLoaded.getName());
            }

        }
    }

    private void aggregatesByPackage(final String mainPackage) {
        final Reflections ref = new Reflections(new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forPackage(mainPackage, ClasspathHelper.contextClassLoader(),
                        ClasspathHelper.staticClassLoader()))
                .filterInputsBy(new FilterBuilder().include(".+\\.class")));

        for (final Class<?> aggregateClass : ref.getTypesAnnotatedWith(Aggregate.class)) {
            final Aggregate aggregateAnnotation = aggregateClass.getAnnotation(Aggregate.class);
            final String baseName = aggregateAnnotation.baseName();
            aggregatesMap.put(baseName, aggregateClass.asSubclass(AggregateBase.class));
        }
    }

    private void initRepositories() throws RepositoryException {
        final Map<String, Repository> repositories = new HashMap<>();

        aggregatesMap.forEach((baseName, aggregateClass) -> {
            if (aggregateClass == null) {
                throw new NullPointerException("Aggregate cannot be null");
            }
            try {
                repositories.put(baseName, new Repository(baseName, aggregateClass, applicationConfig));
            } catch (final AggregateDependenciesException e) {
                logger.error(e.getMessage(), e);
            }

            addEntityAsLocalState(baseName, new GenericClass<>(String.class));
        });

        if (repositories.size() != aggregatesMap.size()) {
            throw new RepositoryException("Error initializing repositories");
        }
        Repositories.getInstance().setRepositories(repositories);

        logger.info("Repositories initialized with Aggregates");
    }

    /**
     *
     */
    private void initHandlers() {
        final int numConsumers = 1;

        final List<String> consumerTopics = Stream
                .concat(Stream.concat(handler.commandsSubscribed().stream(), handler.eventsSubscribed().stream()),
                        handler.dataChangelogsSubscribed().stream())
                .collect(Collectors.toList());

        TopicManager.createTopics(consumerTopics, applicationConfig);

        logger.info("Necessary consumer topics created");

        if (!handler.commandsSubscribed().isEmpty()) {
            for (int i = 0; i < numConsumers; i++) {
                consumers.add(new CommandConsumer<>(i, handler.commandsSubscribed(), handler::processCommand,
                        applicationConfig));
            }
        }

        if (!handler.eventsSubscribed().isEmpty()) {
            for (int i = 0; i < numConsumers; i++) {
                consumers.add(
                        new EventConsumer<>(i, handler.eventsSubscribed(), handler::processEvent, applicationConfig));
            }
        }

        if (!handler.dataChangelogsSubscribed().isEmpty()) {
            for (int i = 0; i < numConsumers; i++) {
                consumers.add(new ChangelogConsumer<>(i, handler.dataChangelogsSubscribed(),
                        handler::processDataChangelog, applicationConfig));
            }
        }
        logger.info("Handlers initialized");
    }

}

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
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.collections.map.HashedMap;
import org.apache.kafka.common.KafkaException;
import org.apache.log4j.Logger;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class Domain {

    private final List<RunnableConsumer> consumers = new ArrayList<>();
    private final Handler handler;
    private ApplicationConfig applicationConfig;
    private Map<String, Class<? extends AggregateBase>> aggregatesMap = new HashedMap();
    private final static Logger logger = Logger.getLogger(Domain.class);

    /**
     * @param handler
     * @param applicationConfig
     */
    public Domain(Handler handler, ApplicationConfig applicationConfig) throws RepositoryException, KafkaException {

        this.mapAggregates(handler);

        this.handler = handler;

        this.applicationConfig = applicationConfig;

        DataProcessor.create(this.applicationConfig);

        initRepositories();
    }

    public Domain addDataProcessorBuilder(String name, DataflowBuilder builder) {
        DataProcessor.get().add(name, builder);
        return this;
    }

    public Domain addDataProcessorBuilder(QueryBuilder queryBuilder) {
        // String sourceName = ApplicationConfig.INTERNAL_NAME_PREFIX + ApplicationConfig.KSQL_PREFIX + name +
        // ApplicationConfig.CHANGELOG_RECORD_NAME_SUFFIX;
        // GlobalTableProcessorStateBuilder tableProcessorStateBuilder = new
        // GlobalTableProcessorStateBuilder<V>(sourceName);
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
    public <K, V extends SpecificRecordBase> Domain addEntityAsLocalState(String baseName, GenericClass<K> keyClass)
            throws IllegalArgumentException {
        String snapshotTopicName = applicationConfig.streams().get(ApplicationConfig.StreamsProperties.APPLICATION_NAME)
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
    public <K, V extends SpecificRecordBase, K1> Domain indexFieldAsLocalState(String targetBaseName,
            String originTopicName, String fieldPath, GenericClass<K> keyClass, GenericClass<K1> key1Class)
            throws IllegalArgumentException {
        DataProcessor.get().add(targetBaseName,
                new UniqueFieldStateBuilder<K, V, K1>(originTopicName, fieldPath, keyClass, key1Class));
        logger.info("Local state for index field " + fieldPath + " added");
        return this;
    }

    /**
     * @return
     * @throws NullPointerException
     */
    public synchronized ApplicationServices start() throws NullPointerException {

        initHandlers();

        DataProcessor.get().start();
        logger.info("States have been started");

        ApplicationServices app = new ApplicationServices(applicationConfig);

        ExecutorService executor = Executors.newFixedThreadPool(consumers.size());

        for (RunnableConsumer consumer : consumers) {
            executor.submit(consumer);
        }

        logger.info("Consumers have been started");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            for (RunnableConsumer consumer : consumers) {
                consumer.shutdown();
            }
            executor.shutdown();
            try {
                executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));

        return app;
    }

    private void mapAggregates(Handler handler) {
        String mainPackage = handler.getClass().getCanonicalName().split("\\.")[0];
        aggregatesByPackage(mainPackage);

        if (aggregatesMap.size() == 0) {
            mapAllPackagesAggregates();
        }

        logger.info(" Aggregates mapped: " + aggregatesMap.toString());
    }

    private void mapAllPackagesAggregates() {
        Package[] packages = Package.getPackages();
        for (Package packageLoaded : packages) {
            // Exclude unnecesary packages
            if (!packageLoaded.getName().matches("^(org|sun|java|jdk).*")) {
                aggregatesByPackage(packageLoaded.getName());
            }

        }
    }

    private void aggregatesByPackage(String mainPackage) {
        Reflections ref = new Reflections(new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forPackage(mainPackage, ClasspathHelper.contextClassLoader(),
                        ClasspathHelper.staticClassLoader()))
                .filterInputsBy(new FilterBuilder().include(".+\\.class")));

        for (Class<?> aggregateClass : ref.getTypesAnnotatedWith(Aggregate.class)) {
            Aggregate aggregateAnnotation = aggregateClass.getAnnotation(Aggregate.class);
            String baseName = aggregateAnnotation.baseName();
            aggregatesMap.put(baseName, aggregateClass.asSubclass(AggregateBase.class));
        }
    }

    private void initRepositories() throws RepositoryException {
        Map<String, Repository> repositories = new HashMap<>();

        aggregatesMap.forEach((baseName, aggregateClass) -> {
            if (aggregateClass == null) {
                throw new NullPointerException("Aggregate cannot be null");
            }
            try {
                repositories.put(baseName, new Repository(baseName, aggregateClass, applicationConfig));

            } catch (AggregateDependenciesException e) {
                logger.error(e.getMessage(), e);
            } catch (InvocationTargetException e) {
                logger.error(e.getMessage(), e);
            } catch (NoSuchMethodException e) {
                logger.error(e.getMessage(), e);
            } catch (InstantiationException e) {
                logger.error(e.getMessage(), e);
            } catch (IllegalAccessException e) {
                logger.error(e.getMessage(), e);
            }

            addEntityAsLocalState(baseName, new GenericClass<>(String.class));
        });

        if (repositories.size() != aggregatesMap.size()) {
            throw new RepositoryException("Error initializing repositories");
        }
        Repositories.getInstance().setRepositories(repositories);

        // for(String baseName : aggregatesMap.keySet()) {
        // addEntityAsLocalState(baseName, new GenericClass<>(String.class));
        // }

        logger.info("Repositories initialized with Aggregates");
    }

    /**
     *
     */
    private void initHandlers() {
        int numConsumers = 1;

        List<String> consumerTopics = Stream
                .concat(Stream.concat(handler.commandsSubscribed().stream(), handler.eventsSubscribed().stream()),
                        handler.dataChangelogsSubscribed().stream())
                .collect(Collectors.toList());

        TopicManager.createTopics(consumerTopics, applicationConfig);

        logger.info("Necessary consumer topics created");

        if (handler.commandsSubscribed().size() > 0) {
            for (int i = 0; i < numConsumers; i++) {
                consumers.add(new CommandConsumer<>(i, handler.commandsSubscribed(), handler::processCommand,
                        applicationConfig));
            }
        }

        if (handler.eventsSubscribed().size() > 0) {
            for (int i = 0; i < numConsumers; i++) {
                consumers.add(
                        new EventConsumer<>(i, handler.eventsSubscribed(), handler::processEvent, applicationConfig));
            }
        }

        if (handler.dataChangelogsSubscribed().size() > 0) {
            for (int i = 0; i < numConsumers; i++) {
                consumers.add(new ChangelogConsumer<>(i, handler.dataChangelogsSubscribed(),
                        handler::processDataChangelog, applicationConfig));
            }
        }
        logger.info("Handlers initialized");
    }

}

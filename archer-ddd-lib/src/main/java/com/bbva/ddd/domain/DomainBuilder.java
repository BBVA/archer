package com.bbva.ddd.domain;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.exceptions.ApplicationException;
import com.bbva.common.utils.TopicManager;
import com.bbva.dataprocessors.DataProcessor;
import com.bbva.dataprocessors.builders.dataflows.DataflowBuilder;
import com.bbva.dataprocessors.builders.dataflows.states.EntityStateBuilder;
import com.bbva.dataprocessors.builders.dataflows.states.GroupByFieldStateBuilder;
import com.bbva.dataprocessors.builders.dataflows.states.UniqueFieldStateBuilder;
import com.bbva.dataprocessors.builders.sql.QueryBuilder;
import com.bbva.ddd.domain.aggregates.AggregateBase;
import com.bbva.ddd.domain.aggregates.exceptions.AggregateDependenciesException;
import com.bbva.ddd.domain.changelogs.Repository;
import com.bbva.ddd.domain.changelogs.exceptions.RepositoryException;
import com.bbva.ddd.domain.changelogs.read.ChangelogConsumer;
import com.bbva.ddd.domain.commands.read.CommandConsumer;
import com.bbva.ddd.domain.events.read.EventConsumer;
import com.bbva.ddd.util.AnnotationUtil;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;
import org.apache.avro.specific.SpecificRecordBase;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Initialize the domain with their data processors, repositories and configured handler.
 * For example if have class with autoconfigure annotations we could do:
 * <pre>
 * {@code
 * Domain domain = DomainBuilder.create(
 *      new AnnotatedHandler(servicesPackage, config), applicationConfig());
 *
 * DataProcessor.get()
 *      .add("processor-name", new ChangelogKeyBuilder("processor-name", "internal-store"));
 *
 * domain.start()
 */
public class DomainBuilder implements Domain {

    private static final Logger logger = LoggerFactory.getLogger(DomainBuilder.class);

    private final List<RunnableConsumer> consumers = new ArrayList<>();
    private final Handler handler;
    private final ApplicationConfig applicationConfig;
    private static Domain instance;

    /**
     * Constructor
     *
     * @param handler           handler implementation
     * @param applicationConfig configuration
     */
    protected DomainBuilder(final Handler handler, final ApplicationConfig applicationConfig) {
        this.handler = handler;

        this.applicationConfig = applicationConfig;
        DataProcessor.create(this.applicationConfig);
        initRepositories();
    }

    protected DomainBuilder(final ApplicationConfig applicationConfig) {
        this(new AutoConfiguredHandler(), applicationConfig);
    }

    /**
     * Create new instance of domain
     *
     * @param handler           handler implementation
     * @param applicationConfig configuration
     * @return domain instance
     */
    public static Domain create(final Handler handler, final ApplicationConfig applicationConfig) {
        if (handler == null) {
            instance = new DomainBuilder(applicationConfig);
        } else {
            instance = new DomainBuilder(handler, applicationConfig);
        }
        return instance;
    }

    /**
     * Create new domian without handler
     *
     * @param applicationConfig configuration
     * @return domain instance
     */
    public static Domain create(final ApplicationConfig applicationConfig) {
        return DomainBuilder.create(null, applicationConfig);
    }

    /**
     * Get&Create a domain instance
     *
     * @return instance
     */
    public static Domain get() {
        if (instance == null) {
            DomainBuilder.create(null, null);
        }
        return instance;
    }

    /**
     * {@inheritDoc}
     *
     * @param name    The name of the DataProcessor. This name will be accessible from the DataflowProcessorContext context
     * @param builder A DataflowBuilder instance. It can be a custom DataflowBuilder or one of those defined in the framework
     * @return domain instance
     */
    @Override
    public Domain addDataProcessorBuilder(final String name, final DataflowBuilder builder) {
        DataProcessor.get().add(name, builder);
        return this;
    }

    /**
     * {@inheritDoc}
     *
     * @param queryBuilder A extended QueryBuilder instance like CreateStreamQueryBuilder.
     * @return domain instance
     */
    @Override
    public Domain addDataProcessorBuilder(final QueryBuilder queryBuilder) {
        DataProcessor.get().add(queryBuilder);

        return this;
    }

    /**
     * {@inheritDoc}
     *
     * @param basename Basename to get source changelog and to name readable table.
     * @param keyClass Class type of the record key. It's used to serialize and deserialize key.
     * @param <K>      Key type
     * @param <V>      Value type
     * @return domain instance
     */
    @Override
    public <K, V extends SpecificRecordBase> Domain addEntityStateProcessor(final String basename, final Class<K> keyClass) {
        final String snapshotTopicName = applicationConfig.streams().get(ApplicationConfig.StreamsProperties.APPLICATION_NAME)
                + "_" + basename;
        DataProcessor.get().add(basename, new EntityStateBuilder<K, V>(snapshotTopicName, keyClass));
        logger.info("Local state {} added", basename);
        return this;
    }

    /**
     * {@inheritDoc}
     *
     * @param storeName        Name of the readable store where the processed data will be stored.
     * @param sourceStreamName Name of the source stream where raw data are.
     * @param fieldPath        Path to field which will be indexed. For example, the path for a field bar in object foo will be foo-&#62;bar
     * @param keyClass         Class of the source key
     * @param resultKeyClass   Class of the result key
     * @param <K>              Key type
     * @param <V>              Value type
     * @param <K1>             Field type
     * @return domain instance
     */
    @Override
    public <K, V extends SpecificRecordBase, K1> Domain indexFieldStateProcessor(
            final String storeName, final String sourceStreamName, final String fieldPath,
            final Class<K> keyClass, final Class<K1> resultKeyClass) {

        DataProcessor.get().add(storeName,
                new UniqueFieldStateBuilder<K, V, K1>(sourceStreamName, fieldPath, keyClass, resultKeyClass));
        logger.info("Local state for index field {} added", fieldPath);
        return this;
    }

    /**
     * {@inheritDoc}
     *
     * @param storeName        Name of the readable store where the processed data will be stored.
     * @param sourceStreamName Name of the source stream where raw data are.
     * @param fieldName        Field in value to group values and used as key.
     * @param keyClass         Class of both the source key and the field key
     * @param valueClass       Class of the value
     * @param <K>              Key type
     * @param <V>              Value type
     * @return domain instance
     */
    @Override
    public <K, V extends SpecificRecordBase> Domain groupByFieldStateProcessor(
            final String storeName, final String sourceStreamName, final String fieldName
            , final Class<K> keyClass, final Class<V> valueClass) {

        DataProcessor.get().add(storeName,
                new GroupByFieldStateBuilder<>(sourceStreamName, keyClass, valueClass, fieldName));
        logger.info("Local state map grouped by foreignKey field {} added", fieldName);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized HelperDomain start() {

        initHandlers();

        DataProcessor.get().start();
        logger.info("States have been started");

        final HelperDomain app = HelperDomain.create(applicationConfig);

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
            } catch (final InterruptedException e) { //NOSONAR
                logger.warn("InterruptedException starting the application", e);
            }
        }));

        return app;
    }

    protected void initRepositories() {
        final Map<String, Class<? extends AggregateBase>> aggregatesMap = AnnotationUtil.mapAggregates(handler);
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

            addEntityStateProcessor(baseName, String.class);
        });

        if (repositories.size() != aggregatesMap.size()) {
            throw new RepositoryException("Error initializing repositories");
        }
        Repositories.getInstance().setRepositories(repositories);

        logger.info("Repositories initialized with Aggregates");
    }

    protected void initHandlers() {
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

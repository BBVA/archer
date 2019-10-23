package com.bbva.dataprocessors;

import com.bbva.common.config.AppConfig;
import com.bbva.dataprocessors.builders.ProcessorBuilder;
import com.bbva.dataprocessors.builders.dataflows.DataflowBuilder;
import com.bbva.dataprocessors.builders.dataflows.DataflowProcessorBuilder;
import com.bbva.dataprocessors.builders.dataflows.states.StateDataflowBuilder;
import com.bbva.dataprocessors.builders.sql.QueryBuilder;
import com.bbva.dataprocessors.builders.sql.QueryProcessorBuilder;
import com.bbva.dataprocessors.contexts.dataflow.DataflowProcessorContextSupplier;
import com.bbva.dataprocessors.contexts.sql.SQLProcessorContext;
import com.bbva.dataprocessors.contexts.sql.SQLProcessorContextSupplier;
import com.bbva.dataprocessors.states.States;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Manage dataflow and processor
 * For example, you can create a specific entity processor
 * <pre>
 * {@code
 *  DataProcessor
 *      .get()
 *      .add(basename, new EntityStateBuilder<K, V>(snapshotTopicName, keyClass));
 *
 *  DataProcessor.get().start();
 * }
 * </pre>
 */
public final class DataProcessor {

    private final static String KSQL_STATE_NAME = "ksql_processor";

    private final Map<String, ProcessorBuilder> processors = new LinkedHashMap<>();
    private final SQLProcessorContext sqlProcessorContext;
    private final AppConfig config;
    private static DataProcessor instance;

    /**
     * Constructor
     *
     * @param config application config
     */
    private DataProcessor(final AppConfig config) {
        this.config = config;
        sqlProcessorContext = new SQLProcessorContextSupplier(KSQL_STATE_NAME, config);
    }

    /**
     * Create new instance of dataprocessor
     *
     * @param configs application configuration
     * @return the instance
     */
    public static DataProcessor create(final AppConfig configs) {
        instance = new DataProcessor(configs);
        return instance;
    }

    /**
     * Get instance of data processor
     *
     * @return the instance
     */
    public static DataProcessor get() {
        return instance;
    }

    /**
     * Add new DataFlow builder to processor
     *
     * @param name    name of processor
     * @param builder builder to add
     * @return the instance
     */
    public DataProcessor add(final String name, final DataflowBuilder builder) {
        final DataflowProcessorBuilder dataflowProcessorBuilder = new DataflowProcessorBuilder(builder);
        dataflowProcessorBuilder.init(new DataflowProcessorContextSupplier(name, config));
        dataflowProcessorBuilder.build();
        processors.put(name, dataflowProcessorBuilder);

        if (builder instanceof StateDataflowBuilder) {
            States.get().add(name, dataflowProcessorBuilder);
        }

        return this;
    }

    /**
     * Add new query builder to processor
     *
     * @param queryBuilder builder to add
     * @return the instance
     */
    public DataProcessor add(final QueryBuilder queryBuilder) {
        final QueryProcessorBuilder queryProcessorBuilder = new QueryProcessorBuilder(queryBuilder);
        queryProcessorBuilder.init(sqlProcessorContext);
        queryProcessorBuilder.build();
        processors.put(KSQL_STATE_NAME + "_" + UUID.randomUUID().toString(), queryProcessorBuilder);

        return this;
    }

    /**
     * Add a list of query builders
     *
     * @param queryBuilders list
     * @return data processor instance
     */
    public DataProcessor add(final List<QueryBuilder> queryBuilders) {
        for (final QueryBuilder builder : queryBuilders) {
            add(builder);
        }
        return this;
    }

    /**
     * Start all processors
     */
    public void start() {
        for (final ProcessorBuilder processorBuilder : processors.values()) {
            processorBuilder.start();
            if (processorBuilder instanceof DataflowBuilder) {
                Runtime.getRuntime().addShutdownHook(new Thread(processorBuilder::close));
            }
        }
        sqlProcessorContext.printDataSources();
    }

}

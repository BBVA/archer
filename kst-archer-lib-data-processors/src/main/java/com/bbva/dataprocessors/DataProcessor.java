package com.bbva.dataprocessors;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.dataprocessors.builders.ProcessorBuilder;
import com.bbva.dataprocessors.builders.dataflows.DataflowBuilder;
import com.bbva.dataprocessors.builders.dataflows.DataflowProcessorBuilder;
import com.bbva.dataprocessors.builders.dataflows.states.StateDataflowBuilder;
import com.bbva.dataprocessors.builders.sql.QueryBuilder;
import com.bbva.dataprocessors.builders.sql.QueryProcessorBuilder;
import com.bbva.dataprocessors.contexts.dataflow.DataflowProcessorContextSupplier;
import com.bbva.dataprocessors.contexts.sql.SQLProcessorContext;
import com.bbva.dataprocessors.contexts.sql.SQLProcessorContextSupplier;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;

public final class DataProcessor {

    private final Map<String, ProcessorBuilder> processors = new LinkedHashMap<>();
    private final SQLProcessorContext sqlProcessorContext;
    private final ApplicationConfig config;
    private static DataProcessor instance = null;
    private final static String KSQL_STATE_NAME = "ksql_processor";

    private DataProcessor(final ApplicationConfig config) {
        this.config = config;
        sqlProcessorContext = new SQLProcessorContextSupplier(KSQL_STATE_NAME, config);
    }

    public static DataProcessor create(final ApplicationConfig configs) {
        instance = new DataProcessor(configs);
        return instance;
    }

    public static DataProcessor get() {
        return instance;
    }

    public DataProcessor add(final String name, final DataflowBuilder builder) throws IllegalArgumentException {
        final DataflowProcessorBuilder dataflowProcessorBuilder = new DataflowProcessorBuilder(builder);
        dataflowProcessorBuilder.init(new DataflowProcessorContextSupplier(name, config));
        dataflowProcessorBuilder.build();
        processors.put(name, dataflowProcessorBuilder);

        if (builder instanceof StateDataflowBuilder) {
            States.get().add(name, dataflowProcessorBuilder);
        }

        return this;
    }

    public DataProcessor add(final QueryBuilder queryBuilder) throws IllegalArgumentException {
        final QueryProcessorBuilder queryProcessorBuilder = new QueryProcessorBuilder(queryBuilder);
        queryProcessorBuilder.init(sqlProcessorContext);
        queryProcessorBuilder.build();
        processors.put(KSQL_STATE_NAME + "_" + UUID.randomUUID().toString(), queryProcessorBuilder);

        return this;
    }

    public DataProcessor add(final LinkedList<QueryBuilder> queryBuilders) throws IllegalArgumentException {
        for (final QueryBuilder builder : queryBuilders) {
            add(builder);
        }

        return this;
    }

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

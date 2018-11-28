package com.bbva.dataprocessors;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.dataprocessors.builders.ProcessorBuilder;
import com.bbva.dataprocessors.builders.dataflows.DataflowBuilder;
import com.bbva.dataprocessors.builders.dataflows.DataflowProcessorBuilder;
import com.bbva.dataprocessors.builders.dataflows.states.StateDataflowBuilder;
import com.bbva.dataprocessors.builders.sql.QueryBuilder;
import com.bbva.dataprocessors.builders.sql.QueryProcessorBuilder;
import com.bbva.dataprocessors.contexts.dataflow.DataflowProcessorContextSupplier;
import com.bbva.dataprocessors.contexts.sql.SQLProcessorContextSupplier;

import java.util.*;

public final class DataProcessor {

    private Map<String, ProcessorBuilder> processors = new LinkedHashMap<>();
    private final SQLProcessorContextSupplier sqlProcessorContext;
    private ApplicationConfig config;
    private static DataProcessor instance = null;
    private final static String KSQL_STATE_NAME = "ksql_processor";

    private DataProcessor(ApplicationConfig config) {
        this.config = config;
        sqlProcessorContext = new SQLProcessorContextSupplier(KSQL_STATE_NAME, config);
    }

    public static DataProcessor create(ApplicationConfig configs) {
        instance = new DataProcessor(configs);
        return instance;
    }

    public static DataProcessor get() {
        return instance;
    }

    public DataProcessor add(String name, DataflowBuilder builder) throws IllegalArgumentException {
        DataflowProcessorBuilder dataflowProcessorBuilder = new DataflowProcessorBuilder(builder);
        dataflowProcessorBuilder.init(new DataflowProcessorContextSupplier(name, config));
        dataflowProcessorBuilder.build();
        processors.put(name, dataflowProcessorBuilder);

        if (builder instanceof StateDataflowBuilder) {
            States.get().add(name, dataflowProcessorBuilder);
        }

        return this;
    }

    public DataProcessor add(QueryBuilder queryBuilder) throws IllegalArgumentException {
        QueryProcessorBuilder queryProcessorBuilder = new QueryProcessorBuilder(queryBuilder);
        queryProcessorBuilder.init(sqlProcessorContext);
        queryProcessorBuilder.build();
        processors.put(KSQL_STATE_NAME + "_" + UUID.randomUUID().toString(), queryProcessorBuilder);

        return this;
    }

    public DataProcessor add(LinkedList<QueryBuilder> queryBuilders) throws IllegalArgumentException {
        for (QueryBuilder builder : queryBuilders) {
            add(builder);
        }

        return this;
    }

    public void start() {
        for (ProcessorBuilder processorBuilder : processors.values()) {
            processorBuilder.start();
            if (processorBuilder instanceof DataflowBuilder) {
                Runtime.getRuntime().addShutdownHook(new Thread(processorBuilder::close));
            }
        }
        sqlProcessorContext.printDataSources();
    }

}

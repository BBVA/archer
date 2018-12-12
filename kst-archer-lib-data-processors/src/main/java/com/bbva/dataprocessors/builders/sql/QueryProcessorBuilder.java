package com.bbva.dataprocessors.builders.sql;

import com.bbva.dataprocessors.builders.ProcessorBuilder;
import com.bbva.dataprocessors.contexts.sql.SQLProcessorContext;
import kst.logging.LoggerGen;
import kst.logging.LoggerGenesis;
import org.apache.kafka.streams.KafkaStreams;

public class QueryProcessorBuilder implements ProcessorBuilder {

    private static final LoggerGen logger = LoggerGenesis.getLogger(QueryProcessorBuilder.class.getName());
    private final QueryBuilder queryBuilder;
    // TODO never setted
    private KafkaStreams streams;
    private SQLProcessorContext context;

    public QueryProcessorBuilder(final QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    public void init(final SQLProcessorContext context) {
        this.context = context;
    }

    @Override
    public void build() {
        queryBuilder.build();
    }

    @Override
    public void start() {
        try {
            logger.info("Launching query: " + queryBuilder.query());
            context.ksqlContext().sql(queryBuilder.query());
        } catch (final Exception e) {
            logger.error(e);
        }
    }

    @Override
    public KafkaStreams streams() {
        return streams;
    }

    @Override
    public void close() {
        streams.close();
    }
}

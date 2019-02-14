package com.bbva.dataprocessors.builders.sql;

import com.bbva.dataprocessors.builders.ProcessorBuilder;
import com.bbva.dataprocessors.contexts.sql.SQLProcessorContext;
import kst.logging.Logger;
import kst.logging.LoggerFactory;
import org.apache.kafka.streams.KafkaStreams;

public class QueryProcessorBuilder implements ProcessorBuilder {

    private static final Logger logger = LoggerFactory.getLogger(QueryProcessorBuilder.class);
    private final QueryBuilder queryBuilder;
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
        logger.info("Launching query: {}", queryBuilder.query());
        context.ksqlContext().sql(queryBuilder.query());
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

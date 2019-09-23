package com.bbva.dataprocessors.builders.sql;

import com.bbva.dataprocessors.builders.ProcessorBuilder;
import com.bbva.dataprocessors.contexts.sql.SQLProcessorContext;
import com.bbva.logging.Logger;
import com.bbva.logging.LoggerFactory;
import org.apache.kafka.streams.KafkaStreams;

/**
 * Processor builder for queries
 */
public class QueryProcessorBuilder implements ProcessorBuilder {

    private static final Logger logger = LoggerFactory.getLogger(QueryProcessorBuilder.class);

    private final QueryBuilder queryBuilder;
    private KafkaStreams streams;
    private SQLProcessorContext context;

    /**
     * Constructor
     *
     * @param queryBuilder builder
     */
    public QueryProcessorBuilder(final QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    /**
     * Initialize the builder
     *
     * @param context builder context
     */
    public void init(final SQLProcessorContext context) {
        this.context = context;
    }

    /**
     * Build
     */
    @Override
    public void build() {
        queryBuilder.build();
    }

    /**
     * Start the builder
     */
    @Override
    public void start() {
        logger.info("Launching query: {}", queryBuilder.query());
        context.ksqlContext().sql(queryBuilder.query());
    }

    /**
     * Get streams
     *
     * @return streams instance
     */
    @Override
    public KafkaStreams streams() {
        return streams;
    }

    /**
     * Close the builder and process
     */
    @Override
    public void close() {
        streams.close();
    }
}

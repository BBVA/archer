package com.bbva.dataprocessors.builders.sql;

import com.bbva.dataprocessors.builders.ProcessorBuilder;
import com.bbva.dataprocessors.contexts.sql.SQLProcessorContext;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.log4j.Logger;

public class QueryProcessorBuilder implements ProcessorBuilder {

    private final Logger logger;
    private final QueryBuilder queryBuilder;
    private KafkaStreams streams;
    private SQLProcessorContext context;

    public QueryProcessorBuilder(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
        logger = Logger.getLogger(QueryProcessorBuilder.class);
    }

    public void init(SQLProcessorContext context) {
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

            // Set<QueryMetadata> queryMetadataSet = context.ksqlContext().getRunningQueries();
            //
            // Iterator<QueryMetadata> queryMetadataIterator = queryMetadataSet.iterator();
            // QueryMetadata queryMetadata = queryMetadataIterator.next();
            // streams = queryMetadata.getKafkaStreams();
        } catch (Exception e) {
            e.printStackTrace();
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

package com.bbva.dataprocessors.builders.sql.queries;

import com.bbva.dataprocessors.builders.sql.QueryBuilder;

/**
 * Drop stream builder
 */
public class DropStreamQueryBuilder extends QueryBuilder {

    private static final String DROP_STREAM_QUERY = "DROP STREAM IF EXISTS %s";
    private static final String DELETE_TOPIC = " DELETE TOPIC;";

    private final String streamName;
    private final boolean deleteTopic;

    /**
     * Constructor
     *
     * @param streamName  stream name
     * @param deleteTopic flag for delete topic
     */
    public DropStreamQueryBuilder(final String streamName, final boolean deleteTopic) {
        super();
        this.streamName = streamName;
        this.deleteTopic = deleteTopic;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String build() {
        query = new StringBuilder(String.format(DROP_STREAM_QUERY, streamName));
        query.append(deleteTopic ? DELETE_TOPIC : ";");
        return query.toString();
    }
}

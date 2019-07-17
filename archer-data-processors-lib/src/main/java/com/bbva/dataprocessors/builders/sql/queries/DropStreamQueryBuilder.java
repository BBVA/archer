package com.bbva.dataprocessors.builders.sql.queries;

import com.bbva.dataprocessors.builders.sql.QueryBuilder;

public class DropStreamQueryBuilder extends QueryBuilder {

    private String query;
    private final String streamName;
    private final boolean deleteTopic;

    public DropStreamQueryBuilder(String streamName, boolean deleteTopic) {
        this.streamName = streamName;
        this.deleteTopic = deleteTopic;
    }

    @Override
    protected String query() {
        return query;
    }

    @Override
    protected String build() {

        query = "DROP STREAM IF EXISTS " + streamName + (deleteTopic ? " DELETE TOPIC;" : ";");

        return query;
    }
}

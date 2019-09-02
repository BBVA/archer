package com.bbva.dataprocessors.builders.sql.queries;

import com.bbva.dataprocessors.builders.sql.QueryBuilder;

public class DropTableQueryBuilder extends QueryBuilder {

    private String query;
    private final String tableName;
    private final boolean deleteTopic;

    public DropTableQueryBuilder(final String tableName, final boolean deleteTopic) {
        super();
        this.tableName = tableName;
        this.deleteTopic = deleteTopic;
    }

    @Override
    protected String query() {
        return query;
    }

    @Override
    protected String build() {

        query = "DROP TABLE IF EXISTS " + tableName + (deleteTopic ? " DELETE TOPIC;" : ";");

        return query;
    }
}

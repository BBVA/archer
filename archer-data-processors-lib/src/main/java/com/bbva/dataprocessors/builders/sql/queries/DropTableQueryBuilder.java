package com.bbva.dataprocessors.builders.sql.queries;

import com.bbva.dataprocessors.builders.sql.QueryBuilder;

/**
 * Drop table builder
 */
public class DropTableQueryBuilder extends QueryBuilder {

    private static final String DROP_TABLE_QUERY = "DROP TABLE IF EXISTS %s";
    private static final String DELETE_TOPIC = " DELETE TOPIC;";

    private final String tableName;
    private final boolean deleteTopic;

    /**
     * Constructor
     *
     * @param tableName   table name
     * @param deleteTopic deletion topic flag
     */
    public DropTableQueryBuilder(final String tableName, final boolean deleteTopic) {
        super();
        this.tableName = tableName;
        this.deleteTopic = deleteTopic;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String build() {
        query = new StringBuilder(String.format(DROP_TABLE_QUERY, tableName));
        query.append(deleteTopic ? DELETE_TOPIC : ";");
        return query.toString();
    }
}

package com.bbva.dataprocessors.builders.sql;

/**
 * Query builder abstract
 */
public abstract class QueryBuilder {

    protected StringBuilder query = new StringBuilder();

    /**
     * Get query
     *
     * @return query generated
     */
    protected String query() {
        return query.toString();
    }

    /**
     * Build the query
     *
     * @return
     */
    protected abstract String build();

}

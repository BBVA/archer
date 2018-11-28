package com.bbva.dataprocessors.builders.sql.queries;

import com.bbva.dataprocessors.builders.sql.QueryBuilder;

public class AutoOffsetResetQueryBuilder extends QueryBuilder {

    public static final String EARLIEST = "earliest";
    public static final String LATEST = "latest";

    private final String resetType;
    private String query;

    public AutoOffsetResetQueryBuilder() {
        resetType = EARLIEST;
    }

    public AutoOffsetResetQueryBuilder(String resetType) {
        this.resetType = resetType;
    }

    public static AutoOffsetResetQueryBuilder create() {
        return new AutoOffsetResetQueryBuilder();
    }

    public static AutoOffsetResetQueryBuilder create(String resetType) {
        return new AutoOffsetResetQueryBuilder(resetType);
    }

    @Override
    protected String query() {
        return query;
    }

    @Override
    protected String build() {
        query = "SET 'auto.offset.reset' = '" + resetType + "';";
        return query;
    }
}

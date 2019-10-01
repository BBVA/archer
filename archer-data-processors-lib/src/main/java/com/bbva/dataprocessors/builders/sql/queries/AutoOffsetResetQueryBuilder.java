package com.bbva.dataprocessors.builders.sql.queries;

import com.bbva.dataprocessors.builders.sql.QueryBuilder;

/**
 * Create a set auto offset query
 */
public class AutoOffsetResetQueryBuilder extends QueryBuilder {

    public static final String EARLIEST = "earliest";
    public static final String LATEST = "latest";
    private static final String AUTOOFFSET_RESET_QUERY = "SET 'auto.offset.reset' = '%s';";

    private final String resetType;

    /**
     * Constructor with default earliest
     */
    public AutoOffsetResetQueryBuilder() {
        super();
        resetType = EARLIEST;
    }

    /**
     * Constructor
     *
     * @param resetType reset type
     */
    public AutoOffsetResetQueryBuilder(final String resetType) {
        super();
        this.resetType = resetType;
    }


    /**
     * Build the query
     *
     * @return query generated
     */
    @Override
    protected String build() {
        query = new StringBuilder(String.format(AUTOOFFSET_RESET_QUERY, resetType));
        return query.toString();
    }
}

package com.bbva.dataprocessors.builders.sql.queries;

import com.bbva.dataprocessors.builders.sql.QueryBuilder;

import java.util.List;

/**
 * Builder for query
 */
public class SelectQueryBuilder extends QueryBuilder {

    private String select;
    private FromClauseBuilder from;
    private String where;
    private String groupBy;
    private String having;

    /**
     * Constructor
     */
    public SelectQueryBuilder() {
        super();
    }

    /**
     * Add query fields
     *
     * @param selectExpression with the fields
     * @return builder
     */
    public SelectQueryBuilder addQueryFields(final String selectExpression) {
        select = selectExpression;
        return this;
    }

    /**
     * Add query fields with list
     *
     * @param selectExpressionList list of query fields
     * @return builder
     */
    public SelectQueryBuilder addQueryFields(final List<String> selectExpressionList) {
        select = String.join(", ", selectExpressionList);
        return this;
    }

    /**
     * Set query from
     *
     * @param streamName from name
     * @return builder
     */
    public SelectQueryBuilder from(final String streamName) {
        from = new FromClauseBuilder(streamName);
        return this;
    }

    /**
     * Set from from builder
     *
     * @param fromClause from builder
     * @return builder
     */
    public SelectQueryBuilder from(final FromClauseBuilder fromClause) {
        from = fromClause;
        return this;
    }

    /**
     * Set from clause from query builder
     *
     * @param createQueryBuilder query builder
     * @return builder
     */
    public SelectQueryBuilder from(final CreateQueryBuilder createQueryBuilder) {
        from = new FromClauseBuilder(createQueryBuilder.name());
        return this;
    }

    /**
     * Set where clause
     *
     * @param whereExpression where expression
     * @return builder
     */
    public SelectQueryBuilder where(final String whereExpression) {
        where = whereExpression;
        return this;
    }

    /**
     * Set group by expression
     *
     * @param groupByExpression group by to set
     * @return builder
     */
    public SelectQueryBuilder groupBy(final String groupByExpression) {
        groupBy = groupByExpression;
        return this;
    }

    /**
     * Set having of query
     *
     * @param havingExpression having
     * @return builder
     */
    public SelectQueryBuilder having(final String havingExpression) {
        having = havingExpression;
        return this;
    }

    /**
     * {@inheritDoc}
     *
     * @return
     */
    @Override
    protected String build() {
        query.append("SELECT ")
                .append(select)
                .append(" ")
                .append(from.build())
                .append(where != null && !where.isEmpty() ? "WHERE " + where + " " : "")
                .append(groupBy != null && !groupBy.isEmpty() ? "GROUP BY " + groupBy + " " : "")
                .append(having != null && !having.isEmpty() ? "HAVING " + having + " " : "")
                .append(";");

        return query.toString();
    }
}

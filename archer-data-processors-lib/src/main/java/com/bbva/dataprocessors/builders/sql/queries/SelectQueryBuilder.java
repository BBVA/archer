package com.bbva.dataprocessors.builders.sql.queries;

import com.bbva.dataprocessors.builders.sql.QueryBuilder;

import java.util.List;

public class SelectQueryBuilder extends QueryBuilder {

    private String select;
    private FromClauseBuilder from;
    private String where;
    private String groupBy;
    private String having;
    private String query;

    public SelectQueryBuilder() {
        super();
    }

    public SelectQueryBuilder addQueryFields(final String selectExpression) {
        select = selectExpression;
        return this;
    }

    public SelectQueryBuilder addQueryFields(final List<String> selectExpressionList) {
        select = String.join(", ", selectExpressionList);
        return this;
    }

    public SelectQueryBuilder from(final String streamName) {
        from = new FromClauseBuilder(streamName);
        return this;
    }

    public SelectQueryBuilder from(final FromClauseBuilder fromClause) {
        from = fromClause;
        return this;
    }

    public SelectQueryBuilder from(final CreateQueryBuilder createQueryBuilder) {
        from = new FromClauseBuilder(createQueryBuilder.name());
        return this;
    }

    public SelectQueryBuilder where(final String whereExpression) {
        where = whereExpression;
        return this;
    }

    public SelectQueryBuilder groupBy(final String groupByExpression) {
        groupBy = groupByExpression;
        return this;
    }

    public SelectQueryBuilder having(final String havingExpression) {
        having = havingExpression;
        return this;
    }

    @Override
    protected String query() {
        return query;
    }

    @Override
    protected String build() {
        final StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("SELECT ");
        queryBuilder.append(select);
        queryBuilder.append(" ");
        queryBuilder.append(from.build());

        queryBuilder.append(where != null && !where.isEmpty() ? "WHERE " + where + " " : "");

        queryBuilder.append(groupBy != null && !groupBy.isEmpty() ? "GROUP BY " + groupBy + " " : "");

        queryBuilder.append(having != null && !having.isEmpty() ? "HAVING " + having + " " : "");

        queryBuilder.append(";");

        query = queryBuilder.toString();
        return query;
    }
}

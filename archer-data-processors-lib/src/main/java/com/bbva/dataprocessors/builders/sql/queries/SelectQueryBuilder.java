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

    }

    public SelectQueryBuilder addQueryFields(final String selectExpression) {
        this.select = selectExpression;
        return this;
    }

    public SelectQueryBuilder addQueryFields(final List<String> selectExpressionList) {
        this.select = String.join(", ", selectExpressionList);
        return this;
    }

    public SelectQueryBuilder from(final String streamName) {
        this.from = new FromClauseBuilder(streamName);
        return this;
    }

    public SelectQueryBuilder from(final FromClauseBuilder fromClause) {
        this.from = fromClause;
        return this;
    }

    public SelectQueryBuilder from(final CreateQueryBuilder createQueryBuilder) {
        this.from = new FromClauseBuilder(createQueryBuilder.name());
        return this;
    }

    public SelectQueryBuilder where(final String whereExpression) {
        this.where = whereExpression;
        return this;
    }

    public SelectQueryBuilder groupBy(final String groupByExpression) {
        this.groupBy = groupByExpression;
        return this;
    }

    public SelectQueryBuilder having(final String havingExpression) {
        this.having = havingExpression;
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

        this.query = queryBuilder.toString();
        return query;
    }
}

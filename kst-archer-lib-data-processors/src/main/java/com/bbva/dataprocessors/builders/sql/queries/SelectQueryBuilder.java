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

    public SelectQueryBuilder addQueryFields(String selectExpression) {
        this.select = selectExpression;
        return this;
    }

    public SelectQueryBuilder addQueryFields(List<String> selectExpressionList) {
        this.select = String.join(", ", selectExpressionList);
        return this;
    }

    public SelectQueryBuilder from(String streamName) {
        this.from = new FromClauseBuilder(streamName);
        return this;
    }

    public SelectQueryBuilder from(FromClauseBuilder fromClause) {
        this.from = fromClause;
        return this;
    }

    public SelectQueryBuilder from(CreateQueryBuilder createQueryBuilder) {
        this.from = new FromClauseBuilder(createQueryBuilder.name());
        return this;
    }

    public SelectQueryBuilder where(String whereExpression) {
        this.where = whereExpression;
        return this;
    }

    public SelectQueryBuilder groupBy(String groupByExpression) {
        this.groupBy = groupByExpression;
        return this;
    }

    public SelectQueryBuilder having(String havingExpression) {
        this.having = havingExpression;
        return this;
    }

    @Override
    protected String query() {
        return query;
    }

    @Override
    protected String build() {
        query = "SELECT " + select + " " + from.build();

        query += where != null && !where.isEmpty() ? "WHERE " + where + " " : " ";

        query += groupBy != null && !groupBy.isEmpty() ? "GROUP BY " + groupBy + " " : " ";

        query += having != null && !having.isEmpty() ? "HAVING " + having + " " : " ";

        query += ";";

        return query;
    }
}

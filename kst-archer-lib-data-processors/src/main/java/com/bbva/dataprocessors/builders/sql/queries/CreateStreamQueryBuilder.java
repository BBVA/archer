package com.bbva.dataprocessors.builders.sql.queries;

import com.bbva.dataprocessors.builders.sql.QueryBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateStreamQueryBuilder extends QueryBuilder implements CreateQueryBuilder {

    private final String name;
    private Map<String, String> columnsDefinition = new HashMap<>();
    private WithPropertiesClauseBuilder withProperties;
    private SelectQueryBuilder asSelect;
    private String partitionBy;
    private StringBuilder query;

    public CreateStreamQueryBuilder(final String name) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    public CreateStreamQueryBuilder columns(final Map<String, String> columnsDefinition) {
        this.columnsDefinition = columnsDefinition;
        return this;
    }

    public CreateStreamQueryBuilder with(final WithPropertiesClauseBuilder withProperties) {
        this.withProperties = withProperties;
        return this;
    }

    public CreateStreamQueryBuilder asSelect(final SelectQueryBuilder asSelect) {
        this.asSelect = asSelect;
        return this;
    }

    public CreateStreamQueryBuilder partitionBy(final String partitionByField) {
        this.partitionBy = partitionByField;
        return this;
    }

    @Override
    protected String query() {
        return query.toString();
    }

    @Override
    protected String build() {
        String columnsClause = "";
        if (!columnsDefinition.isEmpty()) {
            final List<String> columnsList = new ArrayList<>();
            for (final String prop : columnsDefinition.keySet()) {
                columnsList.add(prop + " " + columnsDefinition.get(prop).toUpperCase());
            }
            columnsClause = " (" + String.join(", ", columnsList) + ")";
        }

        final String withClause = withProperties.build();

        final StringBuilder asSelectClause = new StringBuilder();
        if (asSelect != null) {
            asSelectClause.append(" AS ");
            asSelectClause.append(asSelect.build());
        } else {
            asSelectClause.append(";");
        }

        query.append("CREATE STREAM ");
        query.append(name);
        query.append(columnsClause);
        query.append(withClause);
        query.append(asSelectClause);

        if (partitionBy != null && !partitionBy.isEmpty()) {
            query = new StringBuilder(query.toString().substring(0, query.length() - 1));
            query.append(" PARTITION BY ");
            query.append(partitionBy);
            query.append(";");
        }

        return query.toString();
    }
}

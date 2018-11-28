package com.bbva.dataprocessors.builders.sql.queries;

import com.bbva.dataprocessors.builders.sql.QueryBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateStreamQueryBuilder extends QueryBuilder implements CreateQueryBuilder {

    private String name;
    private Map<String, String> columnsDefinition = new HashMap<>();
    private WithPropertiesClauseBuilder withProperties;
    private SelectQueryBuilder asSelect;
    private String partitionBy;
    private String query;

    public CreateStreamQueryBuilder(String name) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    public CreateStreamQueryBuilder columns(Map<String, String> columnsDefinition) {
        this.columnsDefinition = columnsDefinition;
        return this;
    }

    public CreateStreamQueryBuilder with(WithPropertiesClauseBuilder withProperties) {
        this.withProperties = withProperties;
        return this;
    }

    public CreateStreamQueryBuilder asSelect(SelectQueryBuilder asSelect) {
        this.asSelect = asSelect;
        return this;
    }

    public CreateStreamQueryBuilder partitionBy(String partitionByField) {
        this.partitionBy = partitionByField;
        return this;
    }

    @Override
    protected String query() {
        return query;
    }

    @Override
    protected String build() {
        String columnsClause = "";
        if (!columnsDefinition.isEmpty()) {
            List<String> columnsList = new ArrayList<>();
            for (String prop : columnsDefinition.keySet()) {
                columnsList.add(prop + " " + columnsDefinition.get(prop).toUpperCase());
            }
            columnsClause = " (" + String.join(", ", columnsList) + ")";
        }

        String withClause = withProperties.build();

        String asSelectClause = "";
        if (asSelect != null) {
            asSelectClause += " AS " + asSelect.build();
        } else {
            asSelectClause = ";";
        }

        query = "CREATE STREAM " + name + columnsClause + withClause + asSelectClause;

        if (partitionBy != null && !partitionBy.isEmpty()) {
            query = query.substring(0, query.length() - 1);
            query += " PARTITION BY " + partitionBy + ";";
        }

        return query;
    }
}

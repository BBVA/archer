package com.bbva.dataprocessors.builders.sql.queries;

import com.bbva.dataprocessors.builders.sql.QueryBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateTableQueryBuilder extends QueryBuilder implements CreateQueryBuilder {

    private String name;
    private Map<String, String> columnsDefinition = new HashMap<>();
    private WithPropertiesClauseBuilder withProperties;
    private SelectQueryBuilder asSelect;
    private String query;

    public CreateTableQueryBuilder(String name) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    public CreateTableQueryBuilder columns(Map<String, String> columnsDefinition) {
        this.columnsDefinition = columnsDefinition;
        return this;
    }

    public CreateTableQueryBuilder with(WithPropertiesClauseBuilder withProperties) {
        this.withProperties = withProperties;
        return this;
    }

    public CreateTableQueryBuilder asSelect(SelectQueryBuilder asSelect) {
        this.asSelect = asSelect;
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

        this.query = "CREATE TABLE " + name + columnsClause + withClause + asSelectClause;

        return query;
    }
}

package com.bbva.dataprocessors.builders.sql.queries;

import com.bbva.dataprocessors.builders.sql.QueryBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateTableQueryBuilder extends QueryBuilder implements CreateQueryBuilder {

    private final String name;
    private Map<String, String> columnsDefinition = new HashMap<>();
    private WithPropertiesClauseBuilder withProperties;
    private SelectQueryBuilder asSelect;
    private StringBuilder query = new StringBuilder();

    public CreateTableQueryBuilder(final String name) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    public CreateTableQueryBuilder columns(final Map<String, String> columnsDefinition) {
        this.columnsDefinition = columnsDefinition;
        return this;
    }

    public CreateTableQueryBuilder with(final WithPropertiesClauseBuilder withProperties) {
        this.withProperties = withProperties;
        return this;
    }

    public CreateTableQueryBuilder asSelect(final SelectQueryBuilder asSelect) {
        this.asSelect = asSelect;
        return this;
    }

    @Override
    protected String query() {
        return query.toString();
    }

    @Override
    protected String build() {

        final StringBuilder columnsClause = new StringBuilder();
        if (!columnsDefinition.isEmpty()) {
            final List<String> columnsList = new ArrayList<>();
            for (final String prop : columnsDefinition.keySet()) {
                columnsList.add(prop + " " + columnsDefinition.get(prop).toUpperCase());
            }
            columnsClause.append(" (");
            columnsClause.append(String.join(", ", columnsList));
            columnsClause.append(")");
        }

        final String withClause = withProperties.build();

        final StringBuilder asSelectClause = new StringBuilder();
        if (asSelect != null) {
            asSelectClause.append(" AS ");
            asSelectClause.append(asSelect.build());
        } else {
            asSelectClause.append(";");
        }

        query = new StringBuilder("CREATE TABLE ");
        query.append(name);
        query.append(columnsClause);
        query.append(withClause);
        query.append(asSelectClause);

        return query.toString();
    }
}

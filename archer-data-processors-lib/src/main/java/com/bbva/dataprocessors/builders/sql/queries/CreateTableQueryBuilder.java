package com.bbva.dataprocessors.builders.sql.queries;

import com.bbva.dataprocessors.builders.sql.QueryBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Create table builder
 */
public class CreateTableQueryBuilder extends QueryBuilder implements CreateQueryBuilder {

    private final String name;
    private Map<String, String> columnsDefinition = new HashMap<>();
    private WithPropertiesClauseBuilder withProperties;
    private SelectQueryBuilder asSelect;

    /**
     * Constructor
     *
     * @param name name
     */
    public CreateTableQueryBuilder(final String name) {
        super();
        this.name = name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String name() {
        return name;
    }

    /**
     * Set columns map
     *
     * @param columnsDefinition map with column definition
     * @return builder
     */
    public CreateTableQueryBuilder columns(final Map<String, String> columnsDefinition) {
        this.columnsDefinition = columnsDefinition;
        return this;
    }

    /**
     * Set with clause
     *
     * @param withProperties with builder
     * @return builder
     */
    public CreateTableQueryBuilder with(final WithPropertiesClauseBuilder withProperties) {
        this.withProperties = withProperties;
        return this;
    }

    /**
     * Se select
     *
     * @param asSelect select
     * @return builder
     */
    public CreateTableQueryBuilder asSelect(final SelectQueryBuilder asSelect) {
        this.asSelect = asSelect;
        return this;
    }

    /**
     * {@inheritDoc}
     */
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
            asSelectClause.append(" AS ")
                    .append(asSelect.build());
        } else {
            asSelectClause.append(";");
        }

        query = new StringBuilder("CREATE TABLE ")
                .append(name)
                .append(columnsClause)
                .append(withClause)
                .append(asSelectClause);

        return query.toString();
    }
}

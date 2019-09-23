package com.bbva.dataprocessors.builders.sql.queries;

import com.bbva.dataprocessors.builders.sql.QueryBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Create a stream query builder
 */
public class CreateStreamQueryBuilder extends QueryBuilder implements CreateQueryBuilder {

    private final String name;
    private Map<String, String> columnsDefinition = new HashMap<>();
    private WithPropertiesClauseBuilder withProperties;
    private SelectQueryBuilder asSelect;
    private String partitionBy;

    /**
     * Constructor
     *
     * @param name name
     */
    public CreateStreamQueryBuilder(final String name) {
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
     * Set the columns map
     *
     * @param columnsDefinition columns map
     * @return builder
     */
    public CreateStreamQueryBuilder columns(final Map<String, String> columnsDefinition) {
        this.columnsDefinition = columnsDefinition;
        return this;
    }

    /**
     * Set with clause
     *
     * @param withProperties with clause builder
     * @return builder
     */
    public CreateStreamQueryBuilder with(final WithPropertiesClauseBuilder withProperties) {
        this.withProperties = withProperties;
        return this;
    }

    /**
     * Set the select
     *
     * @param asSelect select builder
     * @return builder
     */
    public CreateStreamQueryBuilder asSelect(final SelectQueryBuilder asSelect) {
        this.asSelect = asSelect;
        return this;
    }

    /**
     * Set partitionBy field
     *
     * @param partitionByField the field
     * @return builder
     */
    public CreateStreamQueryBuilder partitionBy(final String partitionByField) {
        partitionBy = partitionByField;
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
            asSelectClause.append(" AS ");
            asSelectClause.append(asSelect.build());
        } else {
            asSelectClause.append(";");
        }

        query = new StringBuilder("CREATE STREAM ");
        query.append(name);
        query.append(columnsClause.toString());
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

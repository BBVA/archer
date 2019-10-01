package com.bbva.dataprocessors.builders.sql.queries;

import java.util.LinkedList;
import java.util.List;

/**
 * From clause builder
 */
public class FromClauseBuilder {

    private final String streamName;
    private final List<JoinClauseBuilder> joinBuilderList = new LinkedList<>();
    private String clause = "";

    /**
     * Constrructor
     *
     * @param streamName stream name
     */
    public FromClauseBuilder(final String streamName) {
        this.streamName = streamName;
    }

    /**
     * Constructor
     *
     * @param createQueryBuilder create query builder
     */
    public FromClauseBuilder(final CreateQueryBuilder createQueryBuilder) {
        streamName = createQueryBuilder.name();
    }

    /**
     * Add a join builder
     *
     * @param joinBuilder join builder
     * @return builder
     */
    public FromClauseBuilder join(final JoinClauseBuilder joinBuilder) {
        joinBuilderList.add(joinBuilder);
        return this;
    }

    /**
     * Build the clause
     *
     * @return clause
     */
    String build() {
        clause += "FROM " + streamName + " ";

        if (!joinBuilderList.isEmpty()) {
            final StringBuilder clauseBuilder = new StringBuilder(clause);
            for (final JoinClauseBuilder join : joinBuilderList) {
                clauseBuilder.append(join.build());
            }
            clause = clauseBuilder.toString();
        }

        return clause;
    }
}

package com.bbva.dataprocessors.builders.sql.queries;

import java.util.LinkedList;
import java.util.List;

public class FromClauseBuilder {

    private final String streamName;
    private final List<JoinClauseBuilder> joinBuilderList = new LinkedList<>();
    private String clause = "";

    public FromClauseBuilder(final String streamName) {
        this.streamName = streamName;
    }

    public FromClauseBuilder(final CreateQueryBuilder createQueryBuilder) {
        streamName = createQueryBuilder.name();
    }

    public FromClauseBuilder join(final JoinClauseBuilder joinBuilder) {
        joinBuilderList.add(joinBuilder);
        return this;
    }

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

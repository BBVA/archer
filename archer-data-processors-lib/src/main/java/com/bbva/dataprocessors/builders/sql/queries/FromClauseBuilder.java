package com.bbva.dataprocessors.builders.sql.queries;

import java.util.LinkedList;
import java.util.List;

public class FromClauseBuilder {

    private String streamName;
    private List<JoinClauseBuilder> joinBuilderList = new LinkedList<>();
    private String clause = "";

    public FromClauseBuilder(String streamName) {
        this.streamName = streamName;
    }

    public FromClauseBuilder(CreateQueryBuilder createQueryBuilder) {
        this.streamName = createQueryBuilder.name();
    }

    public FromClauseBuilder join(JoinClauseBuilder joinBuilder) {
        this.joinBuilderList.add(joinBuilder);
        return this;
    }

    String build() {
        clause += "FROM " + streamName + " ";

        if (!joinBuilderList.isEmpty()) {
            StringBuilder clauseBuilder = new StringBuilder(clause);
            for (JoinClauseBuilder join : joinBuilderList) {
                clauseBuilder.append(join.build());
            }
            clause = clauseBuilder.toString();
        }

        return clause;
    }
}
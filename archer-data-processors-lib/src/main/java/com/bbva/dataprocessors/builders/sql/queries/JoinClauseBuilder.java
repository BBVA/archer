package com.bbva.dataprocessors.builders.sql.queries;

public class JoinClauseBuilder {

    public static final String FULL = "FULL";
    public static final String LEFT = "LEFT";
    public static final String INNER = "INNER";

    private final String type;
    private final String criteria;
    private final String name;
    private WithinClauseBuilder within;

    public JoinClauseBuilder(final String type, final String name, final String criteria) {
        this.type = type;
        this.name = name;
        this.criteria = criteria;
    }

    public JoinClauseBuilder(final String type, final String name, final String criteria, final WithinClauseBuilder within) {
        this.type = type;
        this.name = name;
        this.criteria = criteria;
        this.within = within;
    }

    public JoinClauseBuilder(final String type, final CreateQueryBuilder createQueryBuilder, final String criteria) {
        this.type = type;
        name = createQueryBuilder.name();
        this.criteria = criteria;
    }

    public JoinClauseBuilder(final String type, final CreateQueryBuilder createQueryBuilder, final String criteria,
                             final WithinClauseBuilder within) {
        this.type = type;
        name = createQueryBuilder.name();
        this.criteria = criteria;
        this.within = within;
    }

    public static WithinClauseBuilder createWithinClause(final String before, final String after) {
        return new WithinClauseBuilder(before, after);
    }

    public static WithinClauseBuilder createWithinClause(final String nTime) {
        return new WithinClauseBuilder(nTime);
    }

    String build() {
        return type + " JOIN " + name + " " + (within != null ? within.build() : "") + " ON " + criteria + " ";
    }

    public static class WithinClauseBuilder {

        private String before;
        private String after;
        private String nTime;

        WithinClauseBuilder(final String before, final String after) {
            this.before = before;
            this.after = after;
        }

        public WithinClauseBuilder(final String nTime) {
            this.nTime = nTime;
        }

        String build() {
            return "WITHIN " + (nTime != null ? nTime : "before " + before + " after " + after);
        }
    }
}

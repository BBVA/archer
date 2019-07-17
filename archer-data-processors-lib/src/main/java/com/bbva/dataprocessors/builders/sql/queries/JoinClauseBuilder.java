package com.bbva.dataprocessors.builders.sql.queries;

public class JoinClauseBuilder {

    public static final String FULL = "FULL";
    public static final String LEFT = "LEFT";
    public static final String INNER = "INNER";

    private String type;
    private String criteria;
    private String name;
    private WithinClauseBuilder within;

    public JoinClauseBuilder(String type, String name, String criteria) {
        this.type = type;
        this.name = name;
        this.criteria = criteria;
    }

    public JoinClauseBuilder(String type, String name, String criteria, WithinClauseBuilder within) {
        this.type = type;
        this.name = name;
        this.criteria = criteria;
        this.within = within;
    }

    public JoinClauseBuilder(String type, CreateQueryBuilder createQueryBuilder, String criteria) {
        this.type = type;
        this.name = createQueryBuilder.name();
        this.criteria = criteria;
    }

    public JoinClauseBuilder(String type, CreateQueryBuilder createQueryBuilder, String criteria,
            WithinClauseBuilder within) {
        this.type = type;
        this.name = createQueryBuilder.name();
        this.criteria = criteria;
        this.within = within;
    }

    public static WithinClauseBuilder createWithinClause(String before, String after) {
        return new WithinClauseBuilder(before, after);
    }

    public static WithinClauseBuilder createWithinClause(String nTime) {
        return new WithinClauseBuilder(nTime);
    }

    String build() {
        return type + " JOIN " + name + " " + (within != null ? within.build() : "") + " ON " + criteria + " ";
    }

    public static class WithinClauseBuilder {

        private String before;
        private String after;
        private String nTime;

        WithinClauseBuilder(String before, String after) {
            this.before = before;
            this.after = after;
        }

        public WithinClauseBuilder(String nTime) {
            this.nTime = nTime;
        }

        String build() {
            return "WITHIN " + (nTime != null ? nTime : "before " + before + " after " + after);
        }
    }
}

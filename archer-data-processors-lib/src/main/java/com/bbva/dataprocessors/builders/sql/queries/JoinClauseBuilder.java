package com.bbva.dataprocessors.builders.sql.queries;

/**
 * Joins builder
 */
public class JoinClauseBuilder {

    public enum TYPE {FULL, LEFT, INNER}

    private final String type;
    private final String criteria;
    private final String name;
    private WithinClauseBuilder within;

    /**
     * Constructor
     *
     * @param type     join type
     * @param name     join name
     * @param criteria criteria
     */
    public JoinClauseBuilder(final String type, final String name, final String criteria) {
        this.type = type;
        this.name = name;
        this.criteria = criteria;
    }

    /**
     * Constructor
     *
     * @param type     joint type
     * @param name     name
     * @param criteria join criteria
     * @param within   within
     */
    public JoinClauseBuilder(final String type, final String name, final String criteria, final WithinClauseBuilder within) {
        this.type = type;
        this.name = name;
        this.criteria = criteria;
        this.within = within;
    }

    /**
     * Constructor
     *
     * @param type               join type
     * @param createQueryBuilder create builder
     * @param criteria           criteria
     */
    public JoinClauseBuilder(final String type, final CreateQueryBuilder createQueryBuilder, final String criteria) {
        this.type = type;
        name = createQueryBuilder.name();
        this.criteria = criteria;
    }

    /**
     * Constructor
     *
     * @param type               join type
     * @param createQueryBuilder create builder
     * @param criteria           criteria
     * @param within             within
     */
    public JoinClauseBuilder(final String type, final CreateQueryBuilder createQueryBuilder, final String criteria,
                             final WithinClauseBuilder within) {
        this.type = type;
        name = createQueryBuilder.name();
        this.criteria = criteria;
        this.within = within;
    }

    /**
     * Create within
     *
     * @param before before
     * @param after  after
     * @return within builder
     */
    public static WithinClauseBuilder createWithinClause(final String before, final String after) {
        return new WithinClauseBuilder(before, after);
    }

    /**
     * Create within
     *
     * @param nTime time
     * @return within builder
     */
    public static WithinClauseBuilder createWithinClause(final String nTime) {
        return new WithinClauseBuilder(nTime);
    }

    /**
     * Build the join
     *
     * @return join clause
     */
    String build() {
        final StringBuilder joinQuery = new StringBuilder()
                .append(type)
                .append(" JOIN ")
                .append(name)
                .append(within != null ? within.build() : "")
                .append(" ON ")
                .append(criteria)
                .append(" ");

        return joinQuery.toString();
    }

    /**
     * Within builder
     */
    public static class WithinClauseBuilder {

        private String before;
        private String after;
        private String nTime;

        /**
         * Constructor
         *
         * @param before before
         * @param after  after
         */
        WithinClauseBuilder(final String before, final String after) {
            this.before = before;
            this.after = after;
        }

        /**
         * Constructor
         *
         * @param nTime time
         */
        public WithinClauseBuilder(final String nTime) {
            this.nTime = nTime;
        }

        /**
         * Generate a within clause builder
         *
         * @return within clause
         */
        String build() {
            final StringBuilder withinQuery = new StringBuilder()
                    .append("WITHIN ")
                    .append(nTime != null ? nTime : "before ")
                    .append(before)
                    .append(" after ")
                    .append(after);

            return withinQuery.toString();
        }
    }
}

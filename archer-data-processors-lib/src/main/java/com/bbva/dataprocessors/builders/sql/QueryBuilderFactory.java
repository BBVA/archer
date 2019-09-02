package com.bbva.dataprocessors.builders.sql;

import com.bbva.dataprocessors.builders.sql.queries.*;

import java.util.LinkedList;

public class QueryBuilderFactory {

    public static LinkedList<DropStreamQueryBuilder> dropStreams(final LinkedList<CreateStreamQueryBuilder> streamsBuilder,
                                                                 final boolean deleteTopic) {
        final LinkedList<DropStreamQueryBuilder> dropStreamBuilderList = new LinkedList<>();
        for (final CreateStreamQueryBuilder builder : streamsBuilder) {
            dropStreamBuilderList.add(new DropStreamQueryBuilder(builder.name(), deleteTopic));
        }
        return dropStreamBuilderList;
    }

    public static DropStreamQueryBuilder dropStream(final String streamName, final boolean deleteTopic) {
        return new DropStreamQueryBuilder(streamName, deleteTopic);
    }

    public static DropStreamQueryBuilder dropStream(final CreateStreamQueryBuilder streamBuilder, final boolean deleteTopic) {
        return new DropStreamQueryBuilder(streamBuilder.name(), deleteTopic);
    }

    public static LinkedList<DropTableQueryBuilder> dropTables(final LinkedList<CreateTableQueryBuilder> tableBuilders,
                                                               final boolean deleteTopic) {
        final LinkedList<DropTableQueryBuilder> dropStreamBuilderList = new LinkedList<>();
        for (final CreateTableQueryBuilder builder : tableBuilders) {
            dropStreamBuilderList.add(new DropTableQueryBuilder(builder.name(), deleteTopic));
        }
        return dropStreamBuilderList;
    }

    public static DropTableQueryBuilder dropTable(final String streamName, final boolean deleteTopic) {
        return new DropTableQueryBuilder(streamName, deleteTopic);
    }

    public static DropTableQueryBuilder dropTable(final CreateStreamQueryBuilder tableBuilder, final boolean deleteTopic) {
        return new DropTableQueryBuilder(tableBuilder.name(), deleteTopic);
    }

    public static CreateStreamQueryBuilder createStream(final String streamName) {
        return new CreateStreamQueryBuilder(streamName);
    }

    public static CreateTableQueryBuilder createTable(final String tableName) {
        return new CreateTableQueryBuilder(tableName);
    }

    public static SelectQueryBuilder selectQuery() {
        return new SelectQueryBuilder();
    }

    public static WithPropertiesClauseBuilder withProperties() {
        return new WithPropertiesClauseBuilder();
    }

    public static FromClauseBuilder createFrom(final String streamName) {
        return new FromClauseBuilder(streamName);
    }

    public static FromClauseBuilder createFrom(final CreateQueryBuilder createQueryBuilder) {
        return new FromClauseBuilder(createQueryBuilder);
    }

    public static JoinClauseBuilder createJoin(final String type, final String name, final String criteria) {
        return new JoinClauseBuilder(type, name, criteria);
    }

    public static JoinClauseBuilder createJoin(final String type, final String name, final String criteria,
                                               final JoinClauseBuilder.WithinClauseBuilder within) {
        return new JoinClauseBuilder(type, name, criteria, within);
    }

    public static JoinClauseBuilder createJoin(final String type, final CreateQueryBuilder createQueryBuilder, final String criteria) {
        return new JoinClauseBuilder(type, createQueryBuilder, criteria);
    }

    public static JoinClauseBuilder createJoin(final String type, final CreateQueryBuilder createQueryBuilder, final String criteria,
                                               final JoinClauseBuilder.WithinClauseBuilder within) {
        return new JoinClauseBuilder(type, createQueryBuilder, criteria, within);
    }

    public static JoinClauseBuilder.WithinClauseBuilder createWithin(final String before, final String after) {
        return JoinClauseBuilder.createWithinClause(before, after);
    }

    public static JoinClauseBuilder.WithinClauseBuilder createWithin(final String nTime) {
        return JoinClauseBuilder.createWithinClause(nTime);
    }
}

package com.bbva.dataprocessors.builders.sql;

import com.bbva.dataprocessors.builders.sql.queries.*;

import java.util.LinkedList;

public class QueryBuilderFactory {

    public static LinkedList<DropStreamQueryBuilder> dropStreams(LinkedList<CreateStreamQueryBuilder> streamsBuilder,
            boolean deleteTopic) {
        LinkedList<DropStreamQueryBuilder> dropStreamBuilderList = new LinkedList<>();
        for (CreateStreamQueryBuilder builder : streamsBuilder) {
            dropStreamBuilderList.add(new DropStreamQueryBuilder(builder.name(), deleteTopic));
        }
        return dropStreamBuilderList;
    }

    public static DropStreamQueryBuilder dropStream(String streamName, boolean deleteTopic) {
        return new DropStreamQueryBuilder(streamName, deleteTopic);
    }

    public static DropStreamQueryBuilder dropStream(CreateStreamQueryBuilder streamBuilder, boolean deleteTopic) {
        return new DropStreamQueryBuilder(streamBuilder.name(), deleteTopic);
    }

    public static LinkedList<DropTableQueryBuilder> dropTables(LinkedList<CreateTableQueryBuilder> tableBuilders,
            boolean deleteTopic) {
        LinkedList<DropTableQueryBuilder> dropStreamBuilderList = new LinkedList<>();
        for (CreateTableQueryBuilder builder : tableBuilders) {
            dropStreamBuilderList.add(new DropTableQueryBuilder(builder.name(), deleteTopic));
        }
        return dropStreamBuilderList;
    }

    public static DropTableQueryBuilder dropTable(String streamName, boolean deleteTopic) {
        return new DropTableQueryBuilder(streamName, deleteTopic);
    }

    public static DropTableQueryBuilder dropTable(CreateStreamQueryBuilder tableBuilder, boolean deleteTopic) {
        return new DropTableQueryBuilder(tableBuilder.name(), deleteTopic);
    }

    public static CreateStreamQueryBuilder createStream(String streamName) {
        return new CreateStreamQueryBuilder(streamName);
    }

    public static CreateTableQueryBuilder createTable(String tableName) {
        return new CreateTableQueryBuilder(tableName);
    }

    public static SelectQueryBuilder selectQuery() {
        return new SelectQueryBuilder();
    }

    public static WithPropertiesClauseBuilder withProperties() {
        return new WithPropertiesClauseBuilder();
    }

    public static FromClauseBuilder createFrom(String streamName) {
        return new FromClauseBuilder(streamName);
    }

    public static FromClauseBuilder createFrom(CreateQueryBuilder createQueryBuilder) {
        return new FromClauseBuilder(createQueryBuilder);
    }

    public static JoinClauseBuilder createJoin(String type, String name, String criteria) {
        return new JoinClauseBuilder(type, name, criteria);
    }

    public static JoinClauseBuilder createJoin(String type, String name, String criteria,
            JoinClauseBuilder.WithinClauseBuilder within) {
        return new JoinClauseBuilder(type, name, criteria, within);
    }

    public static JoinClauseBuilder createJoin(String type, CreateQueryBuilder createQueryBuilder, String criteria) {
        return new JoinClauseBuilder(type, createQueryBuilder, criteria);
    }

    public static JoinClauseBuilder createJoin(String type, CreateQueryBuilder createQueryBuilder, String criteria,
            JoinClauseBuilder.WithinClauseBuilder within) {
        return new JoinClauseBuilder(type, createQueryBuilder, criteria, within);
    }

    public static JoinClauseBuilder.WithinClauseBuilder createWithin(String before, String after) {
        return JoinClauseBuilder.createWithinClause(before, after);
    }

    public static JoinClauseBuilder.WithinClauseBuilder createWithin(String nTime) {
        return JoinClauseBuilder.createWithinClause(nTime);
    }
}

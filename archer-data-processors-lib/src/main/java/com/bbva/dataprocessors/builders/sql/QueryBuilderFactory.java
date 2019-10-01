package com.bbva.dataprocessors.builders.sql;

import com.bbva.dataprocessors.builders.sql.queries.*;

import java.util.LinkedList;
import java.util.List;

/**
 * Factory class to create query builders
 */
public class QueryBuilderFactory {

    /**
     * Builder to drop streams
     *
     * @param streamBuilders list of stream builders
     * @param deleteTopic    topic deletion flag
     * @return list of drop stream builders
     */
    public static List<DropStreamQueryBuilder> dropStreams(final List<CreateStreamQueryBuilder> streamBuilders,
                                                           final boolean deleteTopic) {
        final LinkedList<DropStreamQueryBuilder> dropStreamBuilderList = new LinkedList<>();
        for (final CreateStreamQueryBuilder builder : streamBuilders) {
            dropStreamBuilderList.add(new DropStreamQueryBuilder(builder.name(), deleteTopic));
        }
        return dropStreamBuilderList;
    }

    /**
     * Builder to drop stream from stream name
     *
     * @param streamName  the name of the stream
     * @param deleteTopic topic deletion flag
     * @return drop stream builder
     */
    public static DropStreamQueryBuilder dropStream(final String streamName, final boolean deleteTopic) {
        return new DropStreamQueryBuilder(streamName, deleteTopic);
    }

    /**
     * Builder to drop stream from create stream builder
     *
     * @param streamBuilder create stream builder
     * @param deleteTopic   topic deletion flag
     * @return drop stream builder
     */
    public static DropStreamQueryBuilder dropStream(final CreateStreamQueryBuilder streamBuilder, final boolean deleteTopic) {
        return new DropStreamQueryBuilder(streamBuilder.name(), deleteTopic);
    }

    /**
     * Builder to drop tables
     *
     * @param tableBuilders list of table builders
     * @param deleteTopic   topic deletion flag
     * @return list of drop table builders
     */
    public static List<DropTableQueryBuilder> dropTables(final List<CreateTableQueryBuilder> tableBuilders,
                                                         final boolean deleteTopic) {
        final LinkedList<DropTableQueryBuilder> dropStreamBuilderList = new LinkedList<>();
        for (final CreateTableQueryBuilder builder : tableBuilders) {
            dropStreamBuilderList.add(new DropTableQueryBuilder(builder.name(), deleteTopic));
        }
        return dropStreamBuilderList;
    }

    /**
     * Builder to drop table from stream name
     *
     * @param streamName  stream name
     * @param deleteTopic topic deletion flag
     * @return drop table builder
     */
    public static DropTableQueryBuilder dropTable(final String streamName, final boolean deleteTopic) {
        return new DropTableQueryBuilder(streamName, deleteTopic);
    }

    /**
     * Builder to drop table from CreateStream builder
     *
     * @param tableBuilder create stream builder
     * @param deleteTopic  topic deletion flag
     * @return drop table builder
     */
    public static DropTableQueryBuilder dropTable(final CreateStreamQueryBuilder tableBuilder, final boolean deleteTopic) {
        return new DropTableQueryBuilder(tableBuilder.name(), deleteTopic);
    }

    /**
     * Create stream builder
     *
     * @param streamName stream name
     * @return builder
     */
    public static CreateStreamQueryBuilder createStream(final String streamName) {
        return new CreateStreamQueryBuilder(streamName);
    }

    /**
     * Create table builder
     *
     * @param tableName table name
     * @return builder
     */
    public static CreateTableQueryBuilder createTable(final String tableName) {
        return new CreateTableQueryBuilder(tableName);
    }

    /**
     * Select builder
     *
     * @return builder
     */
    public static SelectQueryBuilder selectQuery() {
        return new SelectQueryBuilder();
    }

    /**
     * Builder with properties clause
     *
     * @return builder
     */
    public static WithPropertiesClauseBuilder withProperties() {
        return new WithPropertiesClauseBuilder();
    }

    /**
     * Builder with from clause
     *
     * @param streamName stream name
     * @return builder
     */
    public static FromClauseBuilder createFrom(final String streamName) {
        return new FromClauseBuilder(streamName);
    }

    /**
     * Builder with from clause with create builder
     *
     * @param createQueryBuilder create query builder
     * @return from builder
     */
    public static FromClauseBuilder createFrom(final CreateQueryBuilder createQueryBuilder) {
        return new FromClauseBuilder(createQueryBuilder);
    }

    /**
     * Create join builder
     *
     * @param type     join type
     * @param name     name of the table
     * @param criteria criteria
     * @return join builder
     */
    public static JoinClauseBuilder createJoin(final String type, final String name, final String criteria) {
        return new JoinClauseBuilder(type, name, criteria);
    }

    /**
     * Create join builder
     *
     * @param type     join type
     * @param name     name of the table
     * @param criteria criteria
     * @param within   within clause
     * @return join builder
     */
    public static JoinClauseBuilder createJoin(final String type, final String name, final String criteria,
                                               final JoinClauseBuilder.WithinClauseBuilder within) {
        return new JoinClauseBuilder(type, name, criteria, within);
    }

    /**
     * Create join builder from query builder
     *
     * @param type               join type
     * @param createQueryBuilder query builder
     * @param criteria           criteria
     * @return join builder
     */
    public static JoinClauseBuilder createJoin(final String type, final CreateQueryBuilder createQueryBuilder, final String criteria) {
        return new JoinClauseBuilder(type, createQueryBuilder, criteria);
    }

    /**
     * Create join builder
     *
     * @param type               join type
     * @param createQueryBuilder query builder
     * @param criteria           criteria
     * @param within             within
     * @return join builder
     */
    public static JoinClauseBuilder createJoin(final String type, final CreateQueryBuilder createQueryBuilder, final String criteria,
                                               final JoinClauseBuilder.WithinClauseBuilder within) {
        return new JoinClauseBuilder(type, createQueryBuilder, criteria, within);
    }

    /**
     * Create within clause builder
     *
     * @param before before
     * @param after  after
     * @return within builder
     */
    public static JoinClauseBuilder.WithinClauseBuilder createWithin(final String before, final String after) {
        return JoinClauseBuilder.createWithinClause(before, after);
    }

    /**
     * Create within clause builder
     *
     * @param nTime time
     * @return within builder
     */
    public static JoinClauseBuilder.WithinClauseBuilder createWithin(final String nTime) {
        return JoinClauseBuilder.createWithinClause(nTime);
    }
}

package com.bbva.dataprocessors.builders.sql;

import com.bbva.common.util.PowermockExtension;
import com.bbva.dataprocessors.builders.sql.queries.*;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;

import java.util.LinkedList;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
public class QueryBuilderFactoryTest {

    @DisplayName("Create query builder factory")
    @Test
    public void createueryBuilderFactoty() {

        final LinkedList<CreateTableQueryBuilder> tableBuilders = new LinkedList<>();
        tableBuilders.add(new CreateTableQueryBuilder("table"));
        final LinkedList<CreateStreamQueryBuilder> dropStreams = new LinkedList<>();
        dropStreams.add(new CreateStreamQueryBuilder("stream"));

        Assertions.assertAll("QueryBuilderFactory",
                () -> Assertions.assertTrue(QueryBuilderFactory.createFrom("stream") instanceof FromClauseBuilder),
                () -> Assertions.assertTrue(QueryBuilderFactory.createFrom(() -> null) instanceof FromClauseBuilder),
                () -> Assertions.assertTrue(QueryBuilderFactory.createJoin("type", "name", "criteria") instanceof JoinClauseBuilder),
                () -> Assertions.assertTrue(QueryBuilderFactory.createJoin("type", () -> null, "criteria") instanceof JoinClauseBuilder),
                () -> Assertions.assertTrue(QueryBuilderFactory.createJoin("type", () -> null, "criteria", new JoinClauseBuilder.WithinClauseBuilder("ntime")) instanceof JoinClauseBuilder),
                () -> Assertions.assertTrue(QueryBuilderFactory.createStream("stream") instanceof CreateStreamQueryBuilder),
                () -> Assertions.assertTrue(QueryBuilderFactory.createTable("table") instanceof CreateTableQueryBuilder),
                () -> Assertions.assertTrue(QueryBuilderFactory.createWithin("nTime") instanceof JoinClauseBuilder.WithinClauseBuilder),
                () -> Assertions.assertTrue(QueryBuilderFactory.createWithin("before", "after") instanceof JoinClauseBuilder.WithinClauseBuilder),
                () -> Assertions.assertTrue(QueryBuilderFactory.dropStream("stream", false) instanceof DropStreamQueryBuilder),
                () -> Assertions.assertTrue(QueryBuilderFactory.dropStream(new CreateStreamQueryBuilder("stream"), false) instanceof DropStreamQueryBuilder),
                () -> Assertions.assertTrue(QueryBuilderFactory.dropTable("table", false) instanceof DropTableQueryBuilder),
                () -> Assertions.assertTrue(QueryBuilderFactory.dropTable(new CreateStreamQueryBuilder("table"), false) instanceof DropTableQueryBuilder),
                () -> Assertions.assertTrue(QueryBuilderFactory.dropTables(tableBuilders, false) instanceof LinkedList),
                () -> Assertions.assertTrue(QueryBuilderFactory.dropStreams(dropStreams, false) instanceof LinkedList)
        );

    }

}

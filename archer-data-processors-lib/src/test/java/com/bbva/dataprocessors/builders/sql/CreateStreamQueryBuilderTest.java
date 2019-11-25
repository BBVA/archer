package com.bbva.dataprocessors.builders.sql;

import com.bbva.common.util.PowermockExtension;
import com.bbva.dataprocessors.builders.sql.queries.*;
import com.bbva.dataprocessors.contexts.sql.SQLProcessorContext;
import io.confluent.ksql.KsqlContext;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
public class CreateStreamQueryBuilderTest {

    @DisplayName("Create query stream build")
    @Test
    public void createQueryBuilder() throws Exception {
        final CreateStreamQueryBuilder createStreamQueryBuilder = new CreateStreamQueryBuilder("table");
        createStreamQueryBuilder.with(new WithPropertiesClauseBuilder());

        final Map<String, String> columns = new HashMap<>();
        columns.put("column1", "column2");
        createStreamQueryBuilder.columns(columns);

        final SelectQueryBuilder selectQueryBuilder = new SelectQueryBuilder();
        selectQueryBuilder.from("streamName");
        selectQueryBuilder.groupBy("groupBy");
        selectQueryBuilder.having("having");
        selectQueryBuilder.where("where");
        selectQueryBuilder.addQueryFields("column");

        createStreamQueryBuilder.asSelect(selectQueryBuilder);
        createStreamQueryBuilder.partitionBy("partition");
        final QueryProcessorBuilder queryProcessorBuilder = new QueryProcessorBuilder(createStreamQueryBuilder);

        final SQLProcessorContext context = PowerMockito.mock(SQLProcessorContext.class);
        PowerMockito.when(context, "ksqlContext").thenReturn(PowerMockito.mock(KsqlContext.class));

        queryProcessorBuilder.init(context);
        queryProcessorBuilder.build();
        queryProcessorBuilder.start();

        Assertions.assertAll("createStreamQueryBuilder",
                () -> Assertions.assertNotNull(queryProcessorBuilder)
        );
    }

    @DisplayName("Create query stream build ok")
    @Test
    public void createQueryStreamBuilder() throws Exception {
        final CreateStreamQueryBuilder createStreamQueryBuilder = new CreateStreamQueryBuilder("table");
        final WithPropertiesClauseBuilder withPropertiesClauseBuilder = new WithPropertiesClauseBuilder();
        withPropertiesClauseBuilder.replicas(3);
        withPropertiesClauseBuilder.kafkaTopic("topic");
        withPropertiesClauseBuilder.key("key");
        withPropertiesClauseBuilder.partitions(3);
        withPropertiesClauseBuilder.timestamp(new Date().getTime());
        withPropertiesClauseBuilder.timestampFormat("dd/MM/yyyy");
        withPropertiesClauseBuilder.valueFormat("format");
        createStreamQueryBuilder.with(withPropertiesClauseBuilder);

        final Map<String, String> columns = new HashMap<>();
        columns.put("column1", "column2");
        createStreamQueryBuilder.columns(columns);

        final SelectQueryBuilder selectQueryBuilder = new SelectQueryBuilder();
        selectQueryBuilder.groupBy("groupBy");
        selectQueryBuilder.having("having");
        selectQueryBuilder.where("where");
        selectQueryBuilder.addQueryFields(Arrays.asList("column,column2".split(",")));
        selectQueryBuilder.from(new FromClauseBuilder("from").join(new JoinClauseBuilder("type", "name", "criteria")));

        createStreamQueryBuilder.asSelect(selectQueryBuilder);
        createStreamQueryBuilder.partitionBy("partition");
        final QueryProcessorBuilder queryProcessorBuilder = new QueryProcessorBuilder(createStreamQueryBuilder);

        final SQLProcessorContext context = PowerMockito.mock(SQLProcessorContext.class);
        PowerMockito.when(context, "ksqlContext").thenReturn(PowerMockito.mock(KsqlContext.class));

        queryProcessorBuilder.init(context);
        queryProcessorBuilder.build();
        queryProcessorBuilder.start();

        Assertions.assertAll("createStreamQueryBuilder",
                () -> Assertions.assertNotNull(queryProcessorBuilder)
        );
    }

    @DisplayName("Create query stream build with properties")
    @Test
    public void createQueryStreamBuilderWithProperties() throws Exception {
        final CreateStreamQueryBuilder createStreamQueryBuilder = new CreateStreamQueryBuilder("table");
        final WithPropertiesClauseBuilder withPropertiesClauseBuilder = new WithPropertiesClauseBuilder();
        withPropertiesClauseBuilder.replicas(3);
        withPropertiesClauseBuilder.kafkaTopic("topic");
        withPropertiesClauseBuilder.key("key");
        withPropertiesClauseBuilder.partitions(3);
        withPropertiesClauseBuilder.timestamp(new Date().getTime());
        withPropertiesClauseBuilder.timestampFormat("dd/MM/yyyy");
        withPropertiesClauseBuilder.valueFormat("format");
        createStreamQueryBuilder.with(withPropertiesClauseBuilder);

        final Map<String, String> columns = new HashMap<>();
        columns.put("column1", "column2");
        createStreamQueryBuilder.columns(columns);

        final SelectQueryBuilder selectQueryBuilder = new SelectQueryBuilder();
        selectQueryBuilder.groupBy("groupBy");
        selectQueryBuilder.having("having");
        selectQueryBuilder.where("where");
        selectQueryBuilder.addQueryFields(Arrays.asList("column,column2".split(",")));

        final CreateStreamQueryBuilder createStreamQueryBuilderSelect = new CreateStreamQueryBuilder("name");
        createStreamQueryBuilderSelect.partitionBy("partitionField");
        createStreamQueryBuilderSelect.columns(columns);
        createStreamQueryBuilderSelect.with(withPropertiesClauseBuilder);
        createStreamQueryBuilderSelect.asSelect(selectQueryBuilder);
        selectQueryBuilder.from(createStreamQueryBuilderSelect);

        createStreamQueryBuilder.partitionBy("partition");
        final QueryProcessorBuilder queryProcessorBuilder = new QueryProcessorBuilder(createStreamQueryBuilder);

        final SQLProcessorContext context = PowerMockito.mock(SQLProcessorContext.class);
        PowerMockito.when(context, "ksqlContext").thenReturn(PowerMockito.mock(KsqlContext.class));

        queryProcessorBuilder.init(context);
        queryProcessorBuilder.build();
        queryProcessorBuilder.start();

        Assertions.assertAll("createStreamQueryBuilder",
                () -> Assertions.assertNotNull(queryProcessorBuilder),
                () -> Assertions.assertEquals("topic", withPropertiesClauseBuilder.kafkaTopic()),
                () -> Assertions.assertEquals(3, withPropertiesClauseBuilder.replicas()),
                () -> Assertions.assertEquals("key", withPropertiesClauseBuilder.key()),
                () -> Assertions.assertEquals(3, withPropertiesClauseBuilder.partitions()),
                () -> Assertions.assertEquals("dd/MM/yyyy", withPropertiesClauseBuilder.timestampFormat()),
                () -> Assertions.assertEquals("format", withPropertiesClauseBuilder.valueFormat())

        );

    }

    @DisplayName("Create query stream build with joins")
    @Test
    public void createQueryStreamBuilderWithJoins() throws Exception {
        final CreateStreamQueryBuilder createStreamQueryBuilder = new CreateStreamQueryBuilder("table");
        final WithPropertiesClauseBuilder withPropertiesClauseBuilder = new WithPropertiesClauseBuilder();
        withPropertiesClauseBuilder.replicas(3);
        withPropertiesClauseBuilder.kafkaTopic("topic");
        withPropertiesClauseBuilder.key("key");
        withPropertiesClauseBuilder.partitions(3);
        withPropertiesClauseBuilder.timestamp(new Date().getTime());
        withPropertiesClauseBuilder.timestampFormat("dd/MM/yyyy");
        withPropertiesClauseBuilder.valueFormat("format");
        createStreamQueryBuilder.with(withPropertiesClauseBuilder);

        final Map<String, String> columns = new HashMap<>();
        columns.put("column1", "column2");
        createStreamQueryBuilder.columns(columns);

        final SelectQueryBuilder selectQueryBuilder = new SelectQueryBuilder();
        selectQueryBuilder.groupBy("groupBy");
        selectQueryBuilder.having("having");
        selectQueryBuilder.where("where");
        selectQueryBuilder.addQueryFields(Arrays.asList("column,column2".split(",")));

        final CreateStreamQueryBuilder createStreamQueryBuilderSelect = new CreateStreamQueryBuilder("name");
        createStreamQueryBuilderSelect.partitionBy("partitionField");
        createStreamQueryBuilderSelect.columns(columns);
        createStreamQueryBuilderSelect.with(withPropertiesClauseBuilder);
        createStreamQueryBuilderSelect.asSelect(selectQueryBuilder);

        final JoinClauseBuilder joinClauseBuilder = new JoinClauseBuilder("type", "name", "criteria", new JoinClauseBuilder.WithinClauseBuilder("nTime"));

        selectQueryBuilder.from(new FromClauseBuilder("from").join(joinClauseBuilder));

        createStreamQueryBuilder.partitionBy("partition");
        final QueryProcessorBuilder queryProcessorBuilder = new QueryProcessorBuilder(createStreamQueryBuilder);

        final SQLProcessorContext context = PowerMockito.mock(SQLProcessorContext.class);
        PowerMockito.when(context, "ksqlContext").thenReturn(PowerMockito.mock(KsqlContext.class));

        queryProcessorBuilder.init(context);
        queryProcessorBuilder.build();
        queryProcessorBuilder.start();

        Assertions.assertAll("createStreamQueryBuilder",
                () -> Assertions.assertNotNull(JoinClauseBuilder.createWithinClause("time")),
                () -> Assertions.assertNotNull(JoinClauseBuilder.createWithinClause("before", "after"))

        );

    }

}

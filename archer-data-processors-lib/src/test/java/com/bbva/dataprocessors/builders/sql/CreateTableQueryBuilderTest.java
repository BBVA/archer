package com.bbva.dataprocessors.builders.sql;

import com.bbva.dataprocessors.builders.sql.queries.CreateTableQueryBuilder;
import com.bbva.dataprocessors.builders.sql.queries.SelectQueryBuilder;
import com.bbva.dataprocessors.builders.sql.queries.WithPropertiesClauseBuilder;
import com.bbva.dataprocessors.contexts.sql.SQLProcessorContext;
import com.bbva.dataprocessors.util.PowermockExtension;
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
public class CreateTableQueryBuilderTest {

    @DisplayName("Create table build")
    @Test
    public void createTableQueryBuilder() throws Exception {
        final CreateTableQueryBuilder createTableQueryBuilder = new CreateTableQueryBuilder("table");
        final WithPropertiesClauseBuilder withPropertiesClauseBuilder = new WithPropertiesClauseBuilder();
        withPropertiesClauseBuilder.replicas(3);
        withPropertiesClauseBuilder.kafkaTopic("topic");
        withPropertiesClauseBuilder.key("key");
        withPropertiesClauseBuilder.partitions(3);
        withPropertiesClauseBuilder.timestamp(new Date().getTime());
        withPropertiesClauseBuilder.timestampFormat("dd/MM/yyyy");
        withPropertiesClauseBuilder.valueFormat("format");
        createTableQueryBuilder.with(withPropertiesClauseBuilder);

        final SelectQueryBuilder selectQueryBuilder = new SelectQueryBuilder();
        selectQueryBuilder.from("table");
        selectQueryBuilder.groupBy("grupBy");
        selectQueryBuilder.having("having");
        selectQueryBuilder.where("where");
        selectQueryBuilder.addQueryFields(Arrays.asList("column,column2".split(",")));
        createTableQueryBuilder.asSelect(selectQueryBuilder);

        final Map<String, String> columns = new HashMap<>();
        columns.put("column1", "column2");
        createTableQueryBuilder.columns(columns);
        final QueryProcessorBuilder queryProcessorBuilder = new QueryProcessorBuilder(createTableQueryBuilder);

        final SQLProcessorContext context = PowerMockito.mock(SQLProcessorContext.class);
        PowerMockito.when(context, "ksqlContext").thenReturn(PowerMockito.mock(KsqlContext.class));

        queryProcessorBuilder.init(context);
        queryProcessorBuilder.build();
        queryProcessorBuilder.start();

        Assertions.assertAll("createStreamQueryBuilder",
                () -> Assertions.assertNotNull(queryProcessorBuilder)
        );
    }

}

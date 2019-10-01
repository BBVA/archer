package com.bbva.dataprocessors.builders.sql;

import com.bbva.common.util.PowermockExtension;
import com.bbva.dataprocessors.builders.sql.queries.FromClauseBuilder;
import com.bbva.dataprocessors.builders.sql.queries.JoinClauseBuilder;
import com.bbva.dataprocessors.builders.sql.queries.SelectQueryBuilder;
import com.bbva.dataprocessors.contexts.sql.SQLProcessorContext;
import io.confluent.ksql.KsqlContext;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
public class SelectQueryBuilderTest {

    @DisplayName("Create table build")
    @Test
    public void createTableQueryBuilder() throws Exception {
        final SelectQueryBuilder selectQueryBuilder = new SelectQueryBuilder();
        final JoinClauseBuilder joinClauseBuilder = new JoinClauseBuilder("type", "name", "criteria", new JoinClauseBuilder.WithinClauseBuilder("nTime"));
        selectQueryBuilder.from(new FromClauseBuilder("from").join(joinClauseBuilder));
        selectQueryBuilder.where("where");
        selectQueryBuilder.where("groupBy");
        selectQueryBuilder.where("having");
        final QueryProcessorBuilder queryProcessorBuilder = new QueryProcessorBuilder(selectQueryBuilder);

        final SQLProcessorContext context = PowerMockito.mock(SQLProcessorContext.class);
        PowerMockito.when(context, "ksqlContext").thenReturn(PowerMockito.mock(KsqlContext.class));

        queryProcessorBuilder.init(context);
        queryProcessorBuilder.build();
        queryProcessorBuilder.start();

        Assertions.assertAll("createStreamQueryBuilder",
                () -> Assertions.assertNotNull(queryProcessorBuilder)
        );
    }

    @DisplayName("Create table build without clauses")
    @Test
    public void createTableQueryBuilderWithoutClauses() throws Exception {
        final SelectQueryBuilder selectQueryBuilder = new SelectQueryBuilder();
        selectQueryBuilder.from("stream");
        selectQueryBuilder.where("");
        selectQueryBuilder.where("");
        selectQueryBuilder.where("");
        final QueryProcessorBuilder queryProcessorBuilder = new QueryProcessorBuilder(selectQueryBuilder);

        final SQLProcessorContext context = PowerMockito.mock(SQLProcessorContext.class);
        PowerMockito.when(context, "ksqlContext").thenReturn(PowerMockito.mock(KsqlContext.class));

        queryProcessorBuilder.init(context);
        queryProcessorBuilder.build();
        queryProcessorBuilder.start();

        Assertions.assertAll("createStreamQueryBuilder",
                () -> Assertions.assertNotNull(queryProcessorBuilder)
        );
    }

    @DisplayName("Create table build without null clauses")
    @Test
    public void createTableQueryBuilderWithoutNullClauses() throws Exception {
        final SelectQueryBuilder selectQueryBuilder = new SelectQueryBuilder();
        selectQueryBuilder.from("stream");
        selectQueryBuilder.where(null);
        selectQueryBuilder.where(null);
        selectQueryBuilder.where(null);
        final QueryProcessorBuilder queryProcessorBuilder = new QueryProcessorBuilder(selectQueryBuilder);

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

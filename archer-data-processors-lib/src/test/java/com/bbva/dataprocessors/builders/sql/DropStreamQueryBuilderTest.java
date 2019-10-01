package com.bbva.dataprocessors.builders.sql;

import com.bbva.common.util.PowermockExtension;
import com.bbva.dataprocessors.builders.sql.queries.DropStreamQueryBuilder;
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
public class DropStreamQueryBuilderTest {

    @DisplayName("Create table build")
    @Test
    public void createTableQueryBuilder() throws Exception {
        final DropStreamQueryBuilder dropStreamQueryBuilder = new DropStreamQueryBuilder("stream", true);
        final QueryProcessorBuilder queryProcessorBuilder = new QueryProcessorBuilder(dropStreamQueryBuilder);

        final SQLProcessorContext context = PowerMockito.mock(SQLProcessorContext.class);
        PowerMockito.when(context, "ksqlContext").thenReturn(PowerMockito.mock(KsqlContext.class));

        queryProcessorBuilder.init(context);
        queryProcessorBuilder.build();
        queryProcessorBuilder.start();

        Assertions.assertAll("createStreamQueryBuilder",
                () -> Assertions.assertNotNull(queryProcessorBuilder)
        );
    }

    @DisplayName("Create table build with no delete")
    @Test
    public void createTableQueryBuildernoDelete() throws Exception {
        final DropStreamQueryBuilder dropStreamQueryBuilder = new DropStreamQueryBuilder("stream", false);
        final QueryProcessorBuilder queryProcessorBuilder = new QueryProcessorBuilder(dropStreamQueryBuilder);

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

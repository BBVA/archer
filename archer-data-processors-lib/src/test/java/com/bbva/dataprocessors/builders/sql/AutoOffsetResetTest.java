package com.bbva.dataprocessors.builders.sql;

import com.bbva.common.util.PowermockExtension;
import com.bbva.dataprocessors.builders.sql.queries.AutoOffsetResetQueryBuilder;
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
public class AutoOffsetResetTest {

    @DisplayName("Create SQLProcessor with auto offset reset build")
    @Test
    public void createDataProcessorAndGetOk() throws Exception {
        final AutoOffsetResetQueryBuilder autoOffsetResetQueryBuilder = new AutoOffsetResetQueryBuilder();
        final AutoOffsetResetQueryBuilder autoOffsetResetQueryBuilderMod = new AutoOffsetResetQueryBuilder(AutoOffsetResetQueryBuilder.LATEST);
        final QueryProcessorBuilder queryProcessorBuilder = new QueryProcessorBuilder(autoOffsetResetQueryBuilderMod);

        final SQLProcessorContext context = PowerMockito.mock(SQLProcessorContext.class);
        PowerMockito.when(context, "ksqlContext").thenReturn(PowerMockito.mock(KsqlContext.class));

        queryProcessorBuilder.init(context);
        queryProcessorBuilder.build();
        queryProcessorBuilder.start();

        Assertions.assertAll("dataProcessor",
                () -> Assertions.assertNotNull(queryProcessorBuilder),
                () -> Assertions.assertNotNull(autoOffsetResetQueryBuilder)
        );
    }

}

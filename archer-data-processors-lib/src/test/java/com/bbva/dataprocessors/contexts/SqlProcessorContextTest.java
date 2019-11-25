package com.bbva.dataprocessors.contexts;

import com.bbva.common.config.AppConfig;
import com.bbva.common.config.ConfigBuilder;
import com.bbva.common.util.PowermockExtension;
import com.bbva.dataprocessors.contexts.sql.SQLProcessorContextSupplier;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.ksql.KsqlContext;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({SQLProcessorContextSupplier.class, KsqlContext.class})
public class SqlProcessorContextTest {

    @DisplayName("Create SQL context ok")
    @Test
    public void initProcessorOk() throws Exception {
        PowerMockito.whenNew(CachedSchemaRegistryClient.class).withAnyArguments().thenReturn(PowerMockito.mock(CachedSchemaRegistryClient.class));
        PowerMockito.mockStatic(KsqlContext.class);

        final AppConfig configuration = ConfigBuilder.create();
        final SQLProcessorContextSupplier sQLProcessorContextSupplier =
                new SQLProcessorContextSupplier("processor-name", configuration);

        Assertions.assertAll("sql-processor",
                () -> Assertions.assertNotNull(sQLProcessorContextSupplier),
                () -> Assertions.assertNotNull(sQLProcessorContextSupplier.configs()),
                () -> Assertions.assertNotNull(sQLProcessorContextSupplier.schemaRegistryClient()),
                () -> Assertions.assertNull(sQLProcessorContextSupplier.ksqlContext()),
                () -> Assertions.assertEquals("processor-name", sQLProcessorContextSupplier.name())
        );
    }


}

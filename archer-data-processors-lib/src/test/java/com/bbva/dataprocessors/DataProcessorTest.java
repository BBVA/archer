package com.bbva.dataprocessors;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.dataprocessors.builders.dataflows.DataflowBuilder;
import com.bbva.dataprocessors.builders.dataflows.DataflowProcessorBuilder;
import com.bbva.dataprocessors.builders.sql.QueryBuilder;
import com.bbva.dataprocessors.contexts.dataflow.DataflowProcessorContextSupplier;
import com.bbva.dataprocessors.contexts.sql.SQLProcessorContextSupplier;
import com.bbva.dataprocessors.util.PowermockExtension;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.util.LinkedList;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest(DataProcessor.class)
public class DataProcessorTest {

    @DisplayName("Create DataProcessor and get it ok")
    @Test
    public void createDataProcessorAndGetOk() throws Exception {
        final SQLProcessorContextSupplier sQLProcessorContextSupplier = Mockito.mock(SQLProcessorContextSupplier.class);
        PowerMockito.whenNew(SQLProcessorContextSupplier.class).withAnyArguments().thenReturn(sQLProcessorContextSupplier);

        final DataProcessor dataProcessor = DataProcessor.create(new ApplicationConfig());
        final DataProcessor dataProcessorGet = DataProcessor.get();

        Assertions.assertAll("dataProcessor",
                () -> Assertions.assertEquals(dataProcessor, dataProcessorGet)
        );
    }

    @DisplayName("Create DataProcessor and add processors ok")
    @Test
    public void addProcessorsOk() throws Exception {
        final SQLProcessorContextSupplier sQLProcessorContextSupplier = Mockito.mock(SQLProcessorContextSupplier.class);
        final DataflowProcessorBuilder dataflowProcessorBuilder = Mockito.mock(DataflowProcessorBuilder.class);
        final DataflowProcessorContextSupplier dataflowProcessorContextSupplier = Mockito.mock(DataflowProcessorContextSupplier.class);
        final QueryBuilder queryBuilder = PowerMockito.mock(QueryBuilder.class);

        PowerMockito.whenNew(SQLProcessorContextSupplier.class).withAnyArguments().thenReturn(sQLProcessorContextSupplier);
        PowerMockito.whenNew(DataflowProcessorBuilder.class).withAnyArguments().thenReturn(dataflowProcessorBuilder);
        PowerMockito.whenNew(DataflowProcessorContextSupplier.class).withAnyArguments().thenReturn(dataflowProcessorContextSupplier);

        final LinkedList<QueryBuilder> listQueryBuilders = new LinkedList<>();
        listQueryBuilders.add(queryBuilder);
        final DataProcessor dataProcessor = DataProcessor.create(new ApplicationConfig());
        dataProcessor.add("dataflow", PowerMockito.mock(DataflowBuilder.class));
        dataProcessor.add(queryBuilder);
        dataProcessor.add(listQueryBuilders);

        Assertions.assertNotNull(dataProcessor);
    }


    @DisplayName("Create DataProcessor with processors and start it ok")
    @Test
    public void addProcessorsAndStart() throws Exception {
        final SQLProcessorContextSupplier sQLProcessorContextSupplier = Mockito.mock(SQLProcessorContextSupplier.class);
        final DataflowProcessorBuilder dataflowProcessorBuilder = Mockito.mock(DataflowProcessorBuilder.class);
        final DataflowProcessorContextSupplier dataflowProcessorContextSupplier = Mockito.mock(DataflowProcessorContextSupplier.class);

        PowerMockito.whenNew(SQLProcessorContextSupplier.class).withAnyArguments().thenReturn(sQLProcessorContextSupplier);
        PowerMockito.whenNew(DataflowProcessorBuilder.class).withAnyArguments().thenReturn(dataflowProcessorBuilder);
        PowerMockito.whenNew(DataflowProcessorContextSupplier.class).withAnyArguments().thenReturn(dataflowProcessorContextSupplier);

        final DataProcessor dataProcessor = DataProcessor.create(new ApplicationConfig());
        dataProcessor.add("dataflow", PowerMockito.mock(DataflowBuilder.class));

        Exception ex = null;
        try {
            dataProcessor.start();
        } catch (final Exception e) {
            ex = e;
        }

        Assertions.assertNull(ex);
    }
}

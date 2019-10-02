package com.bbva.dataprocessors.transformers;

import com.bbva.common.util.PowermockExtension;
import com.bbva.dataprocessors.transformers.records.SpecificRecordImpl;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
public class SelectForeignKeyTransformerTest {

    @DisplayName("Create and init EntityTransformer ok")
    @Test
    public void initPorcessorOk() {
        final ProcessorContext processorContext = Mockito.mock(ProcessorContext.class);

        final SelectForeignKeyTransformer selectForeignKeyTransformer = new SelectForeignKeyTransformer("transformer", "foereignKey", String.class);
        selectForeignKeyTransformer.init(processorContext);
        selectForeignKeyTransformer.close();

        Assertions.assertNotNull(selectForeignKeyTransformer);
    }


    @DisplayName("Transform object without old value ok")
    @Test
    public void transformWithoutOldValueOk() throws Exception {
        final ProcessorContext processorContext = Mockito.mock(ProcessorContext.class);
        final KeyValueStore keyValueStore = PowerMockito.mock(KeyValueStore.class);

        PowerMockito.when(processorContext, "getStateStore", Mockito.any(String.class)).thenReturn(keyValueStore);
        PowerMockito.when(keyValueStore, "get", Mockito.any(String.class)).thenReturn(null);

        final SelectForeignKeyTransformer selectForeignKeyTransformer = new SelectForeignKeyTransformer("transformer", "name", SpecificRecordImpl.class);
        selectForeignKeyTransformer.init(processorContext);
        final KeyValue transformed = selectForeignKeyTransformer.transform("key", createRecord("new-name"));

        Assertions.assertAll("transformed",
                () -> Assertions.assertNotNull(transformed),
                () -> Assertions.assertEquals("new-name", transformed.key)
        );
    }

    @DisplayName("Transform object without old value ok")
    @Test
    public void transformWithoutNewValueOk() throws Exception {
        final ProcessorContext processorContext = Mockito.mock(ProcessorContext.class);
        final KeyValueStore keyValueStore = PowerMockito.mock(KeyValueStore.class);

        PowerMockito.when(processorContext, "getStateStore", Mockito.any(String.class)).thenReturn(keyValueStore);
        PowerMockito.when(keyValueStore, "get", Mockito.any(String.class)).thenReturn(createRecord("old-name"));

        final SelectForeignKeyTransformer selectForeignKeyTransformer = new SelectForeignKeyTransformer("transformer", "name", SpecificRecordImpl.class);
        selectForeignKeyTransformer.init(processorContext);
        final KeyValue transformed = selectForeignKeyTransformer.transform("key", null);

        Assertions.assertAll("transformed",
                () -> Assertions.assertNotNull(transformed),
                () -> Assertions.assertEquals("old-name", transformed.key)
        );
    }

    @DisplayName("Transform object withnot existing foreign keyok")
    @Test
    public void transformWithNotExistsFk() throws Exception {
        final ProcessorContext processorContext = Mockito.mock(ProcessorContext.class);
        final KeyValueStore keyValueStore = PowerMockito.mock(KeyValueStore.class);

        PowerMockito.when(processorContext, "getStateStore", Mockito.any(String.class)).thenReturn(keyValueStore);
        PowerMockito.when(keyValueStore, "get", Mockito.any(String.class)).thenReturn(null);

        final SelectForeignKeyTransformer selectForeignKeyTransformer = new SelectForeignKeyTransformer("transformer", "invalid", SpecificRecordImpl.class);
        selectForeignKeyTransformer.init(processorContext);
        final KeyValue transformed = selectForeignKeyTransformer.transform("key", createRecord("new-name"));

        Assertions.assertAll("transformed",
                () -> Assertions.assertNull(transformed)
        );
    }

    private SpecificRecordImpl createRecord(final String name) {
        final SpecificRecordImpl record = new SpecificRecordImpl();
        record.put("name", name);
        return record;
    }
}

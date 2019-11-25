package com.bbva.dataprocessors.transformers;

import com.bbva.common.util.PowermockExtension;
import com.bbva.dataprocessors.transformers.records.SpecificRecordImpl;
import com.bbva.dataprocessors.util.beans.Person;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
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
public class UniqueFieldTransformerTest {

    @DisplayName("Create and init EntityTransformer ok")
    @Test
    public void initProcessorOk() {
        final ProcessorContext processorContext = Mockito.mock(ProcessorContext.class);

        final UniqueFieldTransformer uniqueFieldTransformer = new UniqueFieldTransformer("transformer", "field");
        uniqueFieldTransformer.init(processorContext);
        uniqueFieldTransformer.close();

        Assertions.assertNotNull(uniqueFieldTransformer);
    }

    @DisplayName("Transform object without old value ok")
    @Test
    public void transformWithoutOldValueOk() throws Exception {
        final ProcessorContext processorContext = Mockito.mock(ProcessorContext.class);
        final KeyValueStore keyValueStore = PowerMockito.mock(KeyValueStore.class);

        PowerMockito.when(processorContext, "getStateStore", Mockito.any(String.class)).thenReturn(keyValueStore);
        PowerMockito.when(keyValueStore, "get", Mockito.any(String.class)).thenReturn(null);

        final UniqueFieldTransformer uniqueFieldTransformer = new UniqueFieldTransformer("stateStoreName", "name");
        uniqueFieldTransformer.init(processorContext);
        final KeyValue transformed = uniqueFieldTransformer.transform("key", createRecord("new-name"));

        Assertions.assertAll("transformed",
                () -> Assertions.assertNotNull(transformed),
                () -> Assertions.assertEquals("new-name", transformed.key)
        );
    }

    @DisplayName("Transform object with old value ok")
    @Test
    public void processWithOldValueOk() throws Exception {
        final ProcessorContext processorContext = Mockito.mock(ProcessorContext.class);
        final KeyValueStore keyValueStore = PowerMockito.mock(KeyValueStore.class);

        PowerMockito.when(processorContext, "getStateStore", Mockito.any(String.class)).thenReturn(keyValueStore);
        PowerMockito.when(keyValueStore, "get", Mockito.any(String.class)).thenReturn(createRecord("old-name"));

        final UniqueFieldTransformer uniqueFieldTransformer = new UniqueFieldTransformer("stateStoreName", "name");
        uniqueFieldTransformer.init(processorContext);
        final Person newPerson = new Person();
        newPerson.setPhone("123456789");
        final KeyValue transformed = uniqueFieldTransformer.transform("key", createRecord("new-name"));

        Assertions.assertAll("transformed",
                () -> Assertions.assertNotNull(transformed),
                () -> Assertions.assertEquals("new-name", transformed.key)
        );
    }

    @DisplayName("Transform object with old value and null value ok")
    @Test
    public void processWithOldValueAndNullValueOk() throws Exception {
        final ProcessorContext processorContext = Mockito.mock(ProcessorContext.class);
        final KeyValueStore keyValueStore = PowerMockito.mock(KeyValueStore.class);

        PowerMockito.when(processorContext, "getStateStore", Mockito.any(String.class)).thenReturn(keyValueStore);
        final KeyValueIterator keyValueIterator = new com.bbva.dataprocessors.transformers.records.KeyValueIterator();
        PowerMockito.when(keyValueStore, "all").thenReturn(keyValueIterator);

        final UniqueFieldTransformer uniqueFieldTransformer = new UniqueFieldTransformer("stateStoreName", "name");
        uniqueFieldTransformer.init(processorContext);
        final Person newPerson = new Person();
        newPerson.setPhone("123456789");
        final KeyValue transformed = uniqueFieldTransformer.transform("key", null);

        Assertions.assertAll("transformed",
                () -> Assertions.assertNotNull(transformed),
                () -> Assertions.assertEquals("key", transformed.key)
        );
    }

    @DisplayName("Transform object with old value and null value ok")
    @Test
    public void processWithOldValueAndNullValueNotExistsOk() throws Exception {
        final ProcessorContext processorContext = Mockito.mock(ProcessorContext.class);
        final KeyValueStore keyValueStore = PowerMockito.mock(KeyValueStore.class);

        PowerMockito.when(processorContext, "getStateStore", Mockito.any(String.class)).thenReturn(keyValueStore);
        final KeyValueIterator keyValueIterator = new com.bbva.dataprocessors.transformers.records.KeyValueIterator();
        PowerMockito.when(keyValueStore, "all").thenReturn(keyValueIterator);

        final UniqueFieldTransformer uniqueFieldTransformer = new UniqueFieldTransformer("stateStoreName", "name");
        uniqueFieldTransformer.init(processorContext);
        final Person newPerson = new Person();
        newPerson.setPhone("123456789");
        final KeyValue transformed = uniqueFieldTransformer.transform("no-key", null);

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

package com.bbva.dataprocessors.transformers;

import com.bbva.dataprocessors.util.PowermockExtension;
import com.bbva.dataprocessors.util.beans.Person;
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
public class EntityTransformerTest {

    @DisplayName("Create and init EntityTransformer ok")
    @Test
    public void initTransformerOk() {
        final ProcessorContext processorContext = Mockito.mock(ProcessorContext.class);

        final EntityTransformer entityTransformer = new EntityTransformer("transformer");
        entityTransformer.init(processorContext);
        entityTransformer.close();

        Assertions.assertNotNull(entityTransformer);
    }

    @DisplayName("Transform object without old value ok")
    @Test
    public void transformWithoutOldValueOk() throws Exception {
        final ProcessorContext processorContext = Mockito.mock(ProcessorContext.class);
        final KeyValueStore keyValueStore = PowerMockito.mock(KeyValueStore.class);

        PowerMockito.when(processorContext, "getStateStore", Mockito.any(String.class)).thenReturn(keyValueStore);
        PowerMockito.when(keyValueStore, "get", Mockito.any(String.class)).thenReturn(null);

        final EntityTransformer entityTransformer = new EntityTransformer("stateStoreName");
        entityTransformer.init(processorContext);
        final KeyValue transformed = entityTransformer.transform("key", new Person("name", "lastName"));

        Assertions.assertAll("all metadata",
                () -> Assertions.assertNotNull(transformed),
                () -> Assertions.assertEquals("key", transformed.key),
                () -> Assertions.assertTrue(transformed.value instanceof Person),
                () -> Assertions.assertEquals("name", Person.class.cast(transformed.value).getName())
        );
    }

    @DisplayName("Transform object with old value ok")
    @Test
    public void processWithOldValueOk() throws Exception {
        final ProcessorContext processorContext = Mockito.mock(ProcessorContext.class);
        final KeyValueStore keyValueStore = PowerMockito.mock(KeyValueStore.class);

        PowerMockito.when(processorContext, "getStateStore", Mockito.any(String.class)).thenReturn(keyValueStore);
        PowerMockito.when(keyValueStore, "get", Mockito.any(String.class)).thenReturn(new Person("name", "lastName"));

        final EntityTransformer entityTransformer = new EntityTransformer("stateStoreName");
        entityTransformer.init(processorContext);
        Person newPerson = new Person();
        newPerson.setPhone("123456789");
        final KeyValue transformed = entityTransformer.transform("key", newPerson);

        Assertions.assertAll("all metadata",
                () -> Assertions.assertNotNull(transformed),
                () -> Assertions.assertEquals("key", transformed.key),
                () -> Assertions.assertTrue(transformed.value instanceof Person),
                () -> Assertions.assertEquals("name", Person.class.cast(transformed.value).getName()),
                () -> Assertions.assertEquals("123456789", Person.class.cast(transformed.value).getPhone())
        );
    }

}

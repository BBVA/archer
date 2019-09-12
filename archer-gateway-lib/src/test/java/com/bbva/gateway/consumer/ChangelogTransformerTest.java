package com.bbva.gateway.consumer;

import com.bbva.gateway.consumer.headers.ChangelogHeaders;
import com.bbva.gateway.consumer.transformer.ChangelogTransformer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

@RunWith(JUnit5.class)
public class ChangelogTransformerTest {

    @DisplayName("Create changelog transformer and merge keys ok")
    @Test
    public void createTransformerAndMergeKeysOk() {

        final ChangelogTransformer changelogTransformer = new ChangelogTransformer("base");

        //changelogTransformer.transform("key", "value");

        Assertions.assertAll("ChangelogTransformer",
                () -> Assertions.assertNotNull(changelogTransformer)
        );
    }

    @DisplayName("Transform object without old value ok")
    @Test
    public void transformWithoutOldValueOk() throws Exception {
        final ProcessorContext processorContext = Mockito.mock(ProcessorContext.class);
        final KeyValueStore keyValueStore = PowerMockito.mock(KeyValueStore.class);

        final Headers headers = new ChangelogHeaders();

        PowerMockito.when(processorContext, "getStateStore", Mockito.any(String.class)).thenReturn(keyValueStore);
        PowerMockito.when(processorContext, "headers").thenReturn(headers);
        PowerMockito.when(keyValueStore, "get", Mockito.any(String.class)).thenReturn(null);

        final ChangelogTransformer uniqueFieldTransformer = new ChangelogTransformer("base");
        uniqueFieldTransformer.init(processorContext);
        final KeyValue transformed = uniqueFieldTransformer.transform("key", "new-value");

        Assertions.assertAll("transformed",
                () -> Assertions.assertNotNull(transformed),
                () -> Assertions.assertEquals("key", transformed.key)
        );
    }
}

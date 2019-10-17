package com.bbva.gateway.transformers;

import com.bbva.gateway.transformers.headers.ChangelogHeaders;
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
public class HeaderAsKeyStateTransformerTest {

    @DisplayName("Create changelog transformer and merge keys ok")
    @Test
    public void createTransformerAndMergeKeysOk() {

        final HeaderAsKeyStateTransformer changelogTransformer = new HeaderAsKeyStateTransformer("base");

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

        final HeaderAsKeyStateTransformer uniqueFieldTransformer = new HeaderAsKeyStateTransformer("base");
        uniqueFieldTransformer.init(processorContext);
        final KeyValue transformed = uniqueFieldTransformer.transform("key", "new-value");

        Assertions.assertAll("transformed",
                () -> Assertions.assertNotNull(transformed),
                () -> Assertions.assertEquals("key", transformed.key)
        );
    }

}

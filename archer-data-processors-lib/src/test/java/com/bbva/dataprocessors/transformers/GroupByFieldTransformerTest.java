package com.bbva.dataprocessors.transformers;

import com.bbva.common.util.PowermockExtension;
import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.ChangelogHeaderType;
import com.bbva.dataprocessors.records.GenericRecordList;
import com.bbva.dataprocessors.transformers.records.PersonalData;
import com.bbva.dataprocessors.transformers.records.SpecificRecordImpl;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.common.header.Headers;
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
import org.powermock.core.classloader.annotations.PrepareForTest;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest(GroupByFieldTransformer.class)
public class GroupByFieldTransformerTest {

    @DisplayName("Create and init EntityTransformer ok")
    @Test
    public void initPorcessorOk() {
        final ProcessorContext processorContext = Mockito.mock(ProcessorContext.class);

        final GroupByFieldTransformer groupByFieldTransformer = new GroupByFieldTransformer("transformer", SpecificRecordImpl.class);
        groupByFieldTransformer.init(processorContext);
        groupByFieldTransformer.close();

        Assertions.assertNotNull(groupByFieldTransformer);
    }

    @DisplayName("Transform object without old value ok")
    @Test
    public void transformWithoutOldValueOk() throws Exception {
        final ProcessorContext processorContext = Mockito.mock(ProcessorContext.class);
        final KeyValueStore keyValueStore = PowerMockito.mock(KeyValueStore.class);

        PowerMockito.when(processorContext, "getStateStore", Mockito.any(String.class)).thenReturn(keyValueStore);
        PowerMockito.when(keyValueStore, "get", Mockito.any(String.class)).thenReturn(null);

        final GroupByFieldTransformer groupByFieldTransformer = new GroupByFieldTransformer("transformer", PersonalData.class);
        groupByFieldTransformer.init(processorContext);
        final KeyValue transformed = groupByFieldTransformer.transform("key", createRecord("new-name"));

        Assertions.assertAll("transformed",
                () -> Assertions.assertNotNull(transformed),
                () -> Assertions.assertEquals("key", transformed.key),
                () -> Assertions.assertTrue(transformed.value instanceof GenericData.Record)

        );
    }

    @DisplayName("Transform object without old value ok")
    @Test
    public void transformWithNullValueOk() throws Exception {
        final ProcessorContext processorContext = Mockito.mock(ProcessorContext.class);
        final KeyValueStore keyValueStore = PowerMockito.mock(KeyValueStore.class);

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(ChangelogHeaderType.UUID_KEY, new ByteArrayValue("uuid"));
        PowerMockito.whenNew(RecordHeaders.class).withAnyArguments().thenReturn(recordHeaders);

        PowerMockito.whenNew(GenericRecordList.class).withAnyArguments().thenReturn(PowerMockito.mock(GenericRecordList.class));

        final Headers headers = PowerMockito.mock(Headers.class);
        PowerMockito.when(processorContext, "headers").thenReturn(headers);
        PowerMockito.when(processorContext, "getStateStore", Mockito.any(String.class)).thenReturn(keyValueStore);
        final KeyValueIterator keyValueIterator = new com.bbva.dataprocessors.transformers.records.KeyValueIListIterator();
        PowerMockito.when(keyValueStore, "all").thenReturn(keyValueIterator);

        final GroupByFieldTransformer groupByFieldTransformer = new GroupByFieldTransformer("transformer", PersonalData.class);
        groupByFieldTransformer.init(processorContext);
        final KeyValue transformed = groupByFieldTransformer.transform("key", null);

        Assertions.assertAll("transformed",
                () -> Assertions.assertNull(transformed)

        );
    }

    private PersonalData createRecord(final String name) {
        final PersonalData record = new PersonalData();
        record.setName(name);
        return record;
    }

}

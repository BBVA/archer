package com.bbva.common.producers;

import com.bbva.common.config.AppConfiguration;
import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.producers.records.GenericRecordImpl;
import com.bbva.common.producers.records.SpecificRecordImpl;
import com.bbva.common.util.PowermockExtension;
import com.bbva.common.utils.headers.RecordHeaders;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.utils.Bytes;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.nio.ByteBuffer;
import java.util.concurrent.Future;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({DefaultProducer.class, CachedProducer.class})
@PowerMockIgnore("javax.management.*")
public class CachedProducerTest {

    @DisplayName("Create cached producer")
    @Test
    public void createCahedProducerOk() throws Exception {

        final DefaultProducer defaultProducer = Mockito.mock(DefaultProducer.class);
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(defaultProducer);

        final ApplicationConfig configuration = new AppConfiguration().init();
        final CachedProducer producer = new CachedProducer(configuration);

        Assertions.assertAll("producer",
                () -> Assertions.assertNotNull(producer)
        );
    }

    @DisplayName("Add message to producer")
    @Test
    public void addRecordOk() throws Exception {

        final DefaultProducer defaultProducer = Mockito.mock(DefaultProducer.class);
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(defaultProducer);

        final ApplicationConfig configuration = new AppConfiguration().init();
        final CachedProducer producer = new CachedProducer(configuration);

        final Future mockedFuture = producer.add(new PRecord<>("test", "key", "value", new RecordHeaders()), null);
        Assertions.assertAll("producer",
                () -> Assertions.assertNotNull(producer),
                () -> Assertions.assertNull(mockedFuture)
        );
    }

    @DisplayName("Add message to producer")
    @Test
    public void addRecordCompletionOk() throws Exception {

        final DefaultProducer defaultProducer = Mockito.mock(DefaultProducer.class);
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(defaultProducer);

        final ApplicationConfig configuration = new AppConfiguration().init();
        final CachedProducer producer = new CachedProducer(configuration);

        final Future mockedFuture = producer.add(new PRecord<>("test", "key", "value", new RecordHeaders()), null);
        Assertions.assertAll("producer",
                () -> Assertions.assertNotNull(producer),
                () -> Assertions.assertNull(mockedFuture)
        );
    }


    @DisplayName("Add and remove message to producer")
    @Test
    public void addAndRemoveRecordOk() throws Exception {

        final DefaultProducer defaultProducer = Mockito.mock(DefaultProducer.class);
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(defaultProducer);

        final ApplicationConfig configuration = new AppConfiguration().init();
        final CachedProducer producer = new CachedProducer(configuration);

        producer.add(new PRecord<>("test", "key", "value", new RecordHeaders()), null);
        final Future mockedFuture = producer.remove(new PRecord<>("test", "key", "value", new RecordHeaders()), String.class, null);

        Assertions.assertAll("producer",
                () -> Assertions.assertNotNull(producer),
                () -> Assertions.assertNull(mockedFuture)
        );
    }


    @DisplayName("Add message to producer with different serializers")
    @Test
    public void addRecordSerializersOk() throws Exception {

        final DefaultProducer defaultProducer = Mockito.mock(DefaultProducer.class);
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(defaultProducer);

        final ApplicationConfig configuration = new AppConfiguration().init();
        final CachedProducer producer = new CachedProducer(configuration);

        producer.add(new PRecord<>("test", Integer.valueOf(1), Long.valueOf(1), new RecordHeaders()), null);
        producer.add(new PRecord<>("test1", new Bytes("bytes".getBytes()), ByteBuffer.allocate(1), new RecordHeaders()), null);
        producer.add(new PRecord<>("test2", ByteBuffer.allocate(1), new Bytes("bytes".getBytes()), new RecordHeaders()), null);
        producer.add(new PRecord<>("test4", new Double(0), "bytes".getBytes(), new RecordHeaders()), null);

        producer.remove((new PRecord<>("test5", "bytes", new Double(0), new RecordHeaders())), Double.class, null);
        producer.remove((new PRecord<>("test6", "bytes", ByteBuffer.allocate(1), new RecordHeaders())), ByteBuffer.class, null);
        producer.remove((new PRecord<>("test7", "bytes", new Bytes("bytes".getBytes()), new RecordHeaders())), Bytes.class, null);
        producer.remove((new PRecord<>("test8", "bytes", new SpecificRecordImpl(), new RecordHeaders())), SpecificRecord.class, null);
        producer.remove((new PRecord<>("test9", "bytes", new GenericRecordImpl(), new RecordHeaders())), GenericRecord.class, null);

        final Future mockedFuture = producer.add(new PRecord<>("test", new GenericRecordImpl(), new SpecificRecordImpl(), new RecordHeaders()), null);

        Assertions.assertAll("producer",
                () -> Assertions.assertNotNull(producer),
                () -> Assertions.assertNull(mockedFuture)
        );
    }
}

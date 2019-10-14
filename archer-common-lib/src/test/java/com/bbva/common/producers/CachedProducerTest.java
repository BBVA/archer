package com.bbva.common.producers;

import com.bbva.common.config.AppConfig;
import com.bbva.common.config.ConfigBuilder;
import com.bbva.common.producers.record.PRecord;
import com.bbva.common.producers.records.SpecificRecordBaseImpl;
import com.bbva.common.util.PowermockExtension;
import com.bbva.common.utils.headers.RecordHeaders;
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

        final AppConfig configuration = ConfigBuilder.create();
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

        final AppConfig configuration = ConfigBuilder.create();
        final CachedProducer producer = new CachedProducer(configuration);

        final Future mockedFuture = producer.add(new PRecord("test", "key", new SpecificRecordBaseImpl(), new RecordHeaders()), null);
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

        final AppConfig configuration = ConfigBuilder.create();
        final CachedProducer producer = new CachedProducer(configuration);

        final Future mockedFuture = producer.add(new PRecord("test", "key", new SpecificRecordBaseImpl(), new RecordHeaders()), null);
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

        final AppConfig configuration = ConfigBuilder.create();
        final CachedProducer producer = new CachedProducer(configuration);

        final Future mockedFuture = producer.add(new PRecord("test", "key", new SpecificRecordBaseImpl(), new RecordHeaders()), null);

        Assertions.assertAll("producer",
                () -> Assertions.assertNotNull(producer),
                () -> Assertions.assertNull(mockedFuture)
        );
    }

}

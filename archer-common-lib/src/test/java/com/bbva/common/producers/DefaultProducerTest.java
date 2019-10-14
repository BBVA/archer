package com.bbva.common.producers;

import com.bbva.common.config.AppConfig;
import com.bbva.common.config.ConfigBuilder;
import com.bbva.common.producers.callback.DefaultProducerCallback;
import com.bbva.common.producers.record.PRecord;
import com.bbva.common.producers.records.SpecificRecordBaseImpl;
import com.bbva.common.util.PowermockExtension;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.serdes.SpecificAvroSerializer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.Serdes;
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
@PrepareForTest(DefaultProducer.class)
@PowerMockIgnore("javax.management.*")
public class DefaultProducerTest {

    @DisplayName("Create producer")
    @Test
    public void createCahedProducerOk() throws Exception {

        final KafkaProducer kafkaProducer = Mockito.mock(KafkaProducer.class);
        PowerMockito.whenNew(KafkaProducer.class).withAnyArguments().thenReturn(kafkaProducer);

        final AppConfig configuration = ConfigBuilder.create();
        final DefaultProducer producer = new DefaultProducer(configuration, Serdes.String().serializer(), PowerMockito.mock(SpecificAvroSerializer.class));

        Assertions.assertAll("producer",
                () -> Assertions.assertNotNull(producer)
        );
    }

    @DisplayName("Create producer without serializers")
    @Test
    public void createProducerOk() throws Exception {

        final KafkaProducer kafkaProducer = Mockito.mock(KafkaProducer.class);
        PowerMockito.whenNew(KafkaProducer.class).withAnyArguments().thenReturn(kafkaProducer);

        final AppConfig configuration = ConfigBuilder.create();
        final DefaultProducer producer = new DefaultProducer(configuration);

        Assertions.assertAll("producer",
                () -> Assertions.assertNotNull(producer)
        );
    }

    @DisplayName("Add record with exactly once flag")
    @Test
    public void saveRecordExactlyOnceOk() throws Exception {

        final KafkaProducer kafkaProducer = Mockito.mock(KafkaProducer.class);
        PowerMockito.whenNew(KafkaProducer.class).withAnyArguments().thenReturn(kafkaProducer);

        final AppConfig configuration = ConfigBuilder.create();
        final DefaultProducer producer = new DefaultProducer(configuration, Serdes.String().serializer(), PowerMockito.mock(SpecificAvroSerializer.class));
        final Future result = producer.send(new PRecord("test", "key", new SpecificRecordBaseImpl(), new RecordHeaders()), new DefaultProducerCallback());
        Assertions.assertAll("producer",
                () -> Assertions.assertNull(result)
        );
    }

    @DisplayName("Add record with exactly once flag that produce exception")
    @Test
    public void saveRecordExactlyOnceException() {

        Assertions.assertThrows(ProducerFencedException.class, () -> {
            final KafkaProducer kafkaProducer = Mockito.mock(KafkaProducer.class);
            PowerMockito.whenNew(KafkaProducer.class).withAnyArguments().thenReturn(kafkaProducer);

            PowerMockito.when(kafkaProducer, "send", Mockito.any(ProducerRecord.class), Mockito.any(Callback.class)).thenThrow(new ProducerFencedException("ProducerFencedException"));

            final AppConfig configuration = ConfigBuilder.create();
            final DefaultProducer producer = new DefaultProducer(configuration, Serdes.String().serializer(), PowerMockito.mock(SpecificAvroSerializer.class));
            producer.send(new PRecord("test", "key", new SpecificRecordBaseImpl(), new RecordHeaders()), new DefaultProducerCallback());
        });
    }


    @DisplayName("Add record")
    @Test
    public void saveRecordOk() throws Exception {

        final KafkaProducer kafkaProducer = Mockito.mock(KafkaProducer.class);
        PowerMockito.whenNew(KafkaProducer.class).withAnyArguments().thenReturn(kafkaProducer);

        final AppConfig configuration = ConfigBuilder.create();
        final DefaultProducer producer = new DefaultProducer(configuration, Serdes.String().serializer(), PowerMockito.mock(SpecificAvroSerializer.class));
        final Future result = producer.send(new PRecord("test", "key", new SpecificRecordBaseImpl(), new RecordHeaders()), new DefaultProducerCallback());
        Assertions.assertAll("producer",
                () -> Assertions.assertNull(result)
        );
    }
}

package com.bbva.ddd.domain;

import com.bbva.common.config.AppConfiguration;
import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.consumers.CRecord;
import com.bbva.common.consumers.DefaultConsumer;
import com.bbva.common.util.PowermockExtension;
import com.bbva.common.utils.headers.RecordHeaders;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({RunnableConsumer.class, DefaultConsumer.class})
public class RunnableConsumerTest {

    @DisplayName("Create Consumer ok")
    @Test
    public void createConsumerOk() {
        final ApplicationConfig configuration = new AppConfiguration().init();
        final RunnableConsumer consumer = new RunnableConsumer(0, new ArrayList<>(), null, configuration) {
            @Override
            public CRecord message(final String topic, final int partition, final long offset, final long timestamp, final TimestampType timestampType, final String key, final SpecificRecordBase value, final RecordHeaders headers) {
                return null;
            }
        };

        Assertions.assertAll("RunnableConsumer",
                () -> Assertions.assertNotNull(consumer)
        );
    }

    @DisplayName("Create Consumer ok")
    @Test
    public void runConsumerOk() throws Exception {
        final KafkaConsumer kafkaConsumer = Mockito.mock(KafkaConsumer.class);
        PowerMockito.whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(kafkaConsumer);
        final Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();
        final List<ConsumerRecord<String, String>> consumerRecords = new ArrayList<>();
        records.put(new TopicPartition("topic", 0), consumerRecords);
        PowerMockito.when(kafkaConsumer, "poll", Mockito.any()).thenReturn(new ConsumerRecords<>(records));

        final ApplicationConfig configuration = new AppConfiguration().init();
        configuration.put(ApplicationConfig.REPLAY_TOPICS, "topic1,topic2");
        final RunnableConsumer consumer = new RunnableConsumer(0, new ArrayList<>(), null, configuration) {
            @Override
            public CRecord message(final String topic, final int partition, final long offset, final long timestamp, final TimestampType timestampType, final String key, final SpecificRecordBase value, final RecordHeaders headers) {
                return null;
            }
        };
        HelperDomain.create(configuration);
        new Thread(() -> consumer.run()).start();
        Thread.sleep(500);
        consumer.shutdown();

        Assertions.assertAll("RunnableConsumer",
                () -> Assertions.assertNotNull(consumer)
        );
    }
}

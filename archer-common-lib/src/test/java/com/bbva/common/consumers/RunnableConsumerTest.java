package com.bbva.common.consumers;

import com.bbva.common.config.AppConfig;
import com.bbva.common.config.ConfigBuilder;
import com.bbva.common.consumers.contexts.ConsumerContext;
import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.Producer;
import com.bbva.common.util.PowermockExtension;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
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
        final AppConfig configuration = ConfigBuilder.create();
        final RunnableConsumer consumer = new RunnableConsumer(0, new ArrayList<>(), null, configuration) {
            @Override
            public ConsumerContext context(final CRecord c, final Producer producer, final Boolean isReplay) {
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

        final AppConfig configuration = ConfigBuilder.create();
        final RunnableConsumer consumer = new RunnableConsumer(0, new ArrayList<>(), null, configuration) {
            @Override
            public ConsumerContext context(final CRecord c, final Producer producer, final Boolean isReplay) {
                return null;
            }
        };

        new Thread(consumer::run).start();
        Thread.sleep(500);
        consumer.shutdown();

        Assertions.assertAll("RunnableConsumer",
                () -> Assertions.assertNotNull(consumer)
        );
    }

    @DisplayName("Create Consumer with replay ok")
    @Test
    public void runConsumerWitReplayOk() throws Exception {
        final KafkaConsumer kafkaConsumer = Mockito.mock(KafkaConsumer.class);
        PowerMockito.whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(kafkaConsumer);
        final Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();
        final List<ConsumerRecord<String, String>> consumerRecords = new ArrayList<>();
        records.put(new TopicPartition("topic", 0), consumerRecords);
        PowerMockito.when(kafkaConsumer, "poll", Mockito.any()).thenReturn(new ConsumerRecords<>(records));

        final AppConfig configuration = ConfigBuilder.create();
        configuration.put(AppConfig.REPLAY_TOPICS, "topic1,topic2");
        final RunnableConsumer consumer = new RunnableConsumer(0, new ArrayList<>(), null, configuration) {
            @Override
            public ConsumerContext context(final CRecord c, final Producer producer, final Boolean isReplay) {
                return null;
            }
        };

        new Thread(consumer::run).start();
        Thread.sleep(500);
        consumer.shutdown();

        Assertions.assertAll("RunnableConsumer",
                () -> Assertions.assertNotNull(consumer)
        );
    }
}

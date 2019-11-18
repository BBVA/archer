package com.bbva.common.managers;

import com.bbva.common.config.AppConfig;
import com.bbva.common.config.ConfigBuilder;
import com.bbva.common.managers.kafka.KafkaAtLeastOnceManager;
import com.bbva.common.producers.DefaultProducer;
import com.bbva.common.producers.records.SpecificRecordBaseImpl;
import com.bbva.common.util.PowermockExtension;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.util.*;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({KafkaAtLeastOnceManager.class})
public class KafkaAlLeastOnceManagerTest {

    @DisplayName("Create KafkaExactlyOnceManager ok")
    @Test
    public void createManagerOk() {
        final AppConfig configuration = ConfigBuilder.create();
        final KafkaAtLeastOnceManager consumer = new KafkaAtLeastOnceManager(new ArrayList<>(), null, configuration);

        Assertions.assertAll("KafkaExactlyOnceManager",
                () -> Assertions.assertNotNull(consumer)
        );
    }

    @DisplayName("Create Manager ok")
    @Test
    public void runManagerOk() throws Exception {
        final KafkaConsumer kafkaConsumer = Mockito.mock(KafkaConsumer.class);
        PowerMockito.whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(kafkaConsumer);
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));

        final Set<TopicPartition> topicPartitions = new HashSet<>();
        final Map<TopicPartition, List<ConsumerRecord<String, SpecificRecordBase>>> records = new HashMap<>();
        final List<ConsumerRecord<String, SpecificRecordBase>> consumerRecords = new ArrayList<>();
        consumerRecords.add(new ConsumerRecord<>("topic", 0, 1, "key", new SpecificRecordBaseImpl()));

        records.put(new TopicPartition("topic", 0), consumerRecords);
        topicPartitions.add(new TopicPartition("topic", 0));
        PowerMockito.when(kafkaConsumer, "assignment").thenReturn(topicPartitions);

        PowerMockito.when(kafkaConsumer, "poll", Mockito.any()).thenReturn(new ConsumerRecords<>(records));
        PowerMockito.when(kafkaConsumer, "position", Mockito.any()).thenReturn(3L);

        final AppConfig configuration = ConfigBuilder.create();
        final KafkaAtLeastOnceManager consumer = new KafkaAtLeastOnceManager(new ArrayList<>(), this::callback, configuration);

        final RunnableManager runnableManager = new RunnableManager(consumer, configuration);
        new Thread(runnableManager::run).start();

        Thread.sleep(500);
        consumer.shutdown();

        Assertions.assertAll("KafkaExactlyOnceManager",
                () -> Assertions.assertNotNull(consumer)
        );
    }

    @DisplayName("Create Manager ok")
    @Test
    public void runManagerWakeupExceptionOk() throws Exception {
        final KafkaConsumer kafkaConsumer = Mockito.mock(KafkaConsumer.class);
        PowerMockito.whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(kafkaConsumer);
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));

        final Set<TopicPartition> topicPartitions = new HashSet<>();
        final Map<TopicPartition, List<ConsumerRecord<String, SpecificRecordBase>>> records = new HashMap<>();
        final List<ConsumerRecord<String, SpecificRecordBase>> consumerRecords = new ArrayList<>();
        consumerRecords.add(new ConsumerRecord<>("topic", 0, 1, "key", new SpecificRecordBaseImpl()));

        records.put(new TopicPartition("topic", 0), consumerRecords);
        topicPartitions.add(new TopicPartition("topic", 0));
        PowerMockito.when(kafkaConsumer, "assignment").thenReturn(topicPartitions);

        PowerMockito.when(kafkaConsumer, "poll", Mockito.any()).thenThrow(new WakeupException());
        PowerMockito.when(kafkaConsumer, "position", Mockito.any()).thenReturn(3L);

        final AppConfig configuration = ConfigBuilder.create();
        final KafkaAtLeastOnceManager consumer = new KafkaAtLeastOnceManager(new ArrayList<>(), this::callback, configuration);

        final RunnableManager runnableManager = new RunnableManager(consumer, configuration);
        new Thread(runnableManager::run).start();

        Thread.sleep(500);
        consumer.shutdown();

        Assertions.assertAll("KafkaExactlyOnceManager",
                () -> Assertions.assertNotNull(consumer)
        );
    }

    @DisplayName("Create Manager with replay ok")
    @Test
    public void runManagerWithReplayOk() throws Exception {
        final KafkaConsumer kafkaConsumer = Mockito.mock(KafkaConsumer.class);
        PowerMockito.whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(kafkaConsumer);
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));

        final Set<TopicPartition> topicPartitions = new HashSet<>();

        final Map<TopicPartition, List<ConsumerRecord<String, SpecificRecordBase>>> records = new HashMap<>();
        final List<ConsumerRecord<String, SpecificRecordBase>> consumerRecordsList = new ArrayList<>();
        records.put(new TopicPartition("topic", 0), consumerRecordsList);
        topicPartitions.add(new TopicPartition("topic", 0));
        PowerMockito.when(kafkaConsumer, "assignment").thenReturn(topicPartitions);

        consumerRecordsList.add(new ConsumerRecord<>("topic", 0, 1, "key", new SpecificRecordBaseImpl()));
        final ConsumerRecords consumerRecords = new ConsumerRecords<>(records);

        PowerMockito.when(kafkaConsumer, "poll", Mockito.any()).thenReturn(consumerRecords);
        PowerMockito.when(kafkaConsumer, "position", Mockito.any()).thenReturn(2L);

        final AppConfig configuration = ConfigBuilder.create();
        configuration.put(AppConfig.REPLAY_TOPICS, "topic1,topic2");
        final KafkaAtLeastOnceManager consumer = new KafkaAtLeastOnceManager(new ArrayList<>(), this::callback, configuration);

        final RunnableManager runnableManager = new RunnableManager(consumer, configuration);
        new Thread(runnableManager::run).start();

        Assertions.assertAll("KafkaExactlyOnceManager",
                () -> Assertions.assertNotNull(consumer)
        );
    }

    @DisplayName("Create Manager with replay and last offset zero ok")
    @Test
    public void runManagerWithReplayLastOffsetZeroOk() throws Exception {
        final KafkaConsumer kafkaConsumer = Mockito.mock(KafkaConsumer.class);
        PowerMockito.whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(kafkaConsumer);
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));

        final Set<TopicPartition> topicPartitions = new HashSet<>();

        final Map<TopicPartition, List<ConsumerRecord<String, SpecificRecordBase>>> records = new HashMap<>();
        final List<ConsumerRecord<String, SpecificRecordBase>> consumerRecordsList = new ArrayList<>();
        records.put(new TopicPartition("topic", 0), consumerRecordsList);
        topicPartitions.add(new TopicPartition("topic", 0));
        PowerMockito.when(kafkaConsumer, "assignment").thenReturn(topicPartitions);

        consumerRecordsList.add(new ConsumerRecord<>("topic", 0, 1, "key", new SpecificRecordBaseImpl()));
        final ConsumerRecords consumerRecords = new ConsumerRecords<>(records);

        PowerMockito.when(kafkaConsumer, "poll", Mockito.any()).thenReturn(consumerRecords);
        PowerMockito.when(kafkaConsumer, "position", Mockito.any()).thenReturn(0L);

        final AppConfig configuration = ConfigBuilder.create();
        configuration.put(AppConfig.REPLAY_TOPICS, "topic1,topic2");
        final KafkaAtLeastOnceManager consumer = new KafkaAtLeastOnceManager(new ArrayList<>(), this::callback, configuration);

        final RunnableManager runnableManager = new RunnableManager(consumer, configuration);
        new Thread(runnableManager::run).start();

        Assertions.assertAll("KafkaExactlyOnceManager",
                () -> Assertions.assertNotNull(consumer)
        );
    }

    @DisplayName("Create Manager with replay without partitions ok")
    @Test
    public void runManagerWitReplayWithoutPartitions() throws Exception {
        final KafkaConsumer kafkaConsumer = Mockito.mock(KafkaConsumer.class);
        PowerMockito.whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(kafkaConsumer);
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));

        final Set<TopicPartition> topicPartitions = new HashSet<>();

        final Map<TopicPartition, List<ConsumerRecord<String, String>>> records = new HashMap<>();
        final List<ConsumerRecord<String, String>> consumerRecords = new ArrayList<>();
        records.put(new TopicPartition("topic", 0), consumerRecords);
        PowerMockito.when(kafkaConsumer, "assignment").thenReturn(topicPartitions);

        PowerMockito.when(kafkaConsumer, "poll", Mockito.any()).thenReturn(new ConsumerRecords<>(records));
        PowerMockito.when(kafkaConsumer, "position", Mockito.any()).thenReturn(3L);

        final AppConfig configuration = ConfigBuilder.create();
        configuration.put(AppConfig.REPLAY_TOPICS, "topic1,topic2");
        final KafkaAtLeastOnceManager consumer = new KafkaAtLeastOnceManager(new ArrayList<>(), this::callback, configuration);

        final RunnableManager runnableManager = new RunnableManager(consumer, configuration);
        new Thread(runnableManager::run).start();

        Thread.sleep(500);
        consumer.shutdown();

        Assertions.assertAll("KafkaExactlyOnceManager",
                () -> Assertions.assertNotNull(consumer)
        );
    }

    private void callback(final Object record, final Object producer, final Object isReplay) {

    }
}

package com.bbva.common.managers;

import com.bbva.common.config.AppConfig;
import com.bbva.common.config.ConfigBuilder;
import com.bbva.common.managers.kafka.KafkaExactlyOnceManager;
import com.bbva.common.producers.TransactionalProducer;
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
@PrepareForTest({KafkaExactlyOnceManager.class})
public class KafkaExactlyOnceManagerTest {

    @DisplayName("Create KafkaExactlyOnceManager ok")
    @Test
    public void createKafkaExactlyOnceManagerOk() {
        final AppConfig configuration = ConfigBuilder.create();
        final KafkaExactlyOnceManager consumer = new KafkaExactlyOnceManager(new ArrayList<>(), this::callback, configuration);

        Assertions.assertAll("KafkaExactlyOnceManager",
                () -> Assertions.assertNotNull(consumer)
        );
    }

    @DisplayName("Create  and run a KafkaExactlyOnceManager ok")
    @Test
    public void runKafkaExactlyOnceManagerOk() throws Exception {
        final KafkaConsumer kafkaConsumer = Mockito.mock(KafkaConsumer.class);
        PowerMockito.whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(kafkaConsumer);
        PowerMockito.whenNew(TransactionalProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(TransactionalProducer.class));

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
        final KafkaExactlyOnceManager consumer = new KafkaExactlyOnceManager(new ArrayList<>(), this::callback, configuration);

        final RunnableManager runnableManager = new RunnableManager(consumer, configuration);
        new Thread(runnableManager::run).start();

        Thread.sleep(500);
        consumer.shutdown();

        Assertions.assertAll("KafkaExactlyOnceManager",
                () -> Assertions.assertNotNull(consumer)
        );
    }


    @DisplayName("Create and run a KafkaExactlyOnceManager produce WakeupException")
    @Test
    public void runKafkaExactlyOnceManagerWakeupExceptionOk() throws Exception {
        final KafkaConsumer kafkaConsumer = Mockito.mock(KafkaConsumer.class);
        PowerMockito.whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(kafkaConsumer);
        PowerMockito.whenNew(TransactionalProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(TransactionalProducer.class));

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
        final KafkaExactlyOnceManager consumer = new KafkaExactlyOnceManager(new ArrayList<>(), this::callback, configuration);

        final RunnableManager runnableManager = new RunnableManager(consumer, configuration);
        new Thread(runnableManager::run).start();

        Thread.sleep(500);
        consumer.shutdown();

        Assertions.assertAll("KafkaExactlyOnceManager",
                () -> Assertions.assertNotNull(consumer)
        );
    }

    @DisplayName("Create KafkaExactlyOnceManager with replay ok")
    @Test
    public void runKafkaExactlyOnceManagerWithReplayOk() throws Exception {
        final KafkaConsumer kafkaConsumer = Mockito.mock(KafkaConsumer.class);
        PowerMockito.whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(kafkaConsumer);
        PowerMockito.whenNew(TransactionalProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(TransactionalProducer.class));

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
        configuration.put(AppConfig.REPLAY_TOPICS, "topic1,topic2");
        final KafkaExactlyOnceManager consumer = new KafkaExactlyOnceManager(new ArrayList<>(), this::callback, configuration);

        final RunnableManager runnableManager = new RunnableManager(consumer, configuration);
        new Thread(runnableManager::run).start();

        Thread.sleep(500);
        consumer.shutdown();

        Assertions.assertAll("KafkaExactlyOnceManager",
                () -> Assertions.assertNotNull(consumer)
        );
    }

    @DisplayName("Create KafkaExactlyOnceManager with replay and last offset zero ok")
    @Test
    public void runKafkaExactlyOnceManagerWithReplayLastOffsetZeroOk() throws Exception {
        final KafkaConsumer kafkaConsumer = Mockito.mock(KafkaConsumer.class);
        PowerMockito.whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(kafkaConsumer);
        PowerMockito.whenNew(TransactionalProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(TransactionalProducer.class));

        final Set<TopicPartition> topicPartitions = new HashSet<>();
        final Map<TopicPartition, List<ConsumerRecord<String, SpecificRecordBase>>> records = new HashMap<>();
        final List<ConsumerRecord<String, SpecificRecordBase>> consumerRecords = new ArrayList<>();
        consumerRecords.add(new ConsumerRecord<>("topic", 0, 1, "key", new SpecificRecordBaseImpl()));

        records.put(new TopicPartition("topic", 0), consumerRecords);
        topicPartitions.add(new TopicPartition("topic", 0));
        PowerMockito.when(kafkaConsumer, "assignment").thenReturn(topicPartitions);

        PowerMockito.when(kafkaConsumer, "poll", Mockito.any()).thenReturn(new ConsumerRecords<>(records));
        PowerMockito.when(kafkaConsumer, "position", Mockito.any()).thenReturn(0L);

        final AppConfig configuration = ConfigBuilder.create();
        configuration.put(AppConfig.REPLAY_TOPICS, "topic1,topic2");
        final KafkaExactlyOnceManager consumer = new KafkaExactlyOnceManager(new ArrayList<>(), this::callback, configuration);

        final RunnableManager runnableManager = new RunnableManager(consumer, configuration);
        new Thread(runnableManager::run).start();

        Thread.sleep(500);
        consumer.shutdown();

        Assertions.assertAll("KafkaExactlyOnceManager",
                () -> Assertions.assertNotNull(consumer)
        );
    }

    @DisplayName("Create KafkaExactlyOnceManager with no records replay ok")
    @Test
    public void runKafkaExactlyOnceManagerWithNoRecordsReplayOk() throws Exception {
        final KafkaConsumer kafkaConsumer = Mockito.mock(KafkaConsumer.class);
        PowerMockito.whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(kafkaConsumer);
        PowerMockito.whenNew(TransactionalProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(TransactionalProducer.class));

        final Set<TopicPartition> topicPartitions = new HashSet<>();
        final Map<TopicPartition, List<ConsumerRecord<String, SpecificRecordBase>>> records = new HashMap<>();
        final List<ConsumerRecord<String, SpecificRecordBase>> consumerRecords = new ArrayList<>();

        records.put(new TopicPartition("topic", 0), consumerRecords);
        topicPartitions.add(new TopicPartition("topic", 0));
        PowerMockito.when(kafkaConsumer, "assignment").thenReturn(topicPartitions);

        PowerMockito.when(kafkaConsumer, "poll", Mockito.any()).thenReturn(new ConsumerRecords<>(records));
        PowerMockito.when(kafkaConsumer, "position", Mockito.any()).thenReturn(3L);

        final AppConfig configuration = ConfigBuilder.create();
        configuration.put(AppConfig.REPLAY_TOPICS, "topic1,topic2");
        final KafkaExactlyOnceManager consumer = new KafkaExactlyOnceManager(new ArrayList<>(), this::callback, configuration);

        final RunnableManager runnableManager = new RunnableManager(consumer, configuration);
        new Thread(runnableManager::run).start();

        Thread.sleep(500);
        consumer.shutdown();

        Assertions.assertAll("KafkaExactlyOnceManager",
                () -> Assertions.assertNotNull(consumer)
        );
    }

    @DisplayName("Create KafkaExactlyOnceManager with replay ok")
    @Test
    public void runKafkaExactlyOnceManagerWithHighOffsetReplayOk() throws Exception {
        final KafkaConsumer kafkaConsumer = Mockito.mock(KafkaConsumer.class);
        PowerMockito.whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(kafkaConsumer);
        PowerMockito.whenNew(TransactionalProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(TransactionalProducer.class));

        final Set<TopicPartition> topicPartitions = new HashSet<>();
        final Map<TopicPartition, List<ConsumerRecord<String, SpecificRecordBase>>> records = new HashMap<>();
        final List<ConsumerRecord<String, SpecificRecordBase>> consumerRecords = new ArrayList<>();
        consumerRecords.add(new ConsumerRecord<>("topic", 0, 10, "key", new SpecificRecordBaseImpl()));

        records.put(new TopicPartition("topic", 0), consumerRecords);
        topicPartitions.add(new TopicPartition("topic", 0));
        PowerMockito.when(kafkaConsumer, "assignment").thenReturn(topicPartitions);

        PowerMockito.when(kafkaConsumer, "poll", Mockito.any()).thenReturn(new ConsumerRecords<>(records));
        PowerMockito.when(kafkaConsumer, "position", Mockito.any()).thenReturn(3L);

        final AppConfig configuration = ConfigBuilder.create();
        configuration.put(AppConfig.REPLAY_TOPICS, "topic1,topic2");
        final KafkaExactlyOnceManager consumer = new KafkaExactlyOnceManager(new ArrayList<>(), this::callback, configuration);

        final RunnableManager runnableManager = new RunnableManager(consumer, configuration);
        new Thread(runnableManager::run).start();

        Thread.sleep(500);
        consumer.shutdown();

        Assertions.assertAll("KafkaExactlyOnceManager",
                () -> Assertions.assertNotNull(consumer)
        );
    }

    @DisplayName("Create KafkaExactlyOnceManager with replay without partitions ok")
    @Test
    public void runKafkaExactlyOnceManagerWitReplayWithoutPartitions() throws Exception {
        final KafkaConsumer kafkaConsumer = Mockito.mock(KafkaConsumer.class);
        PowerMockito.whenNew(KafkaConsumer.class).withAnyArguments().thenReturn(kafkaConsumer);
        PowerMockito.whenNew(TransactionalProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(TransactionalProducer.class));

        final Set<TopicPartition> topicPartitions = new HashSet<>();
        PowerMockito.when(kafkaConsumer, "assignment").thenReturn(topicPartitions);


        PowerMockito.when(kafkaConsumer, "poll", Mockito.any()).thenReturn(PowerMockito.mock(ConsumerRecords.class));
        PowerMockito.when(kafkaConsumer, "position", Mockito.any()).thenReturn(3L);

        final AppConfig configuration = ConfigBuilder.create();
        configuration.put(AppConfig.REPLAY_TOPICS, "topic1,topic2");
        final KafkaExactlyOnceManager consumer = new KafkaExactlyOnceManager(new ArrayList<>(), this::callback, configuration);

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

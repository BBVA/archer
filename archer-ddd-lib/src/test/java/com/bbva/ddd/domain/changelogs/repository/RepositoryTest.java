package com.bbva.ddd.domain.changelogs.repository;

import com.bbva.common.producers.CachedProducer;
import com.bbva.common.util.PowermockExtension;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({CachedProducer.class, Repository.class})
public class RepositoryTest {

   /* @DisplayName("Create repository ok")
    @Test
    public void createRepository() throws AggregateDependenciesException, Exception {
        final DefaultProducer producer = PowerMockito.mock(DefaultProducer.class);
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(producer);

        final AppConfig configuration = ConfigBuilder.create();
        final Repository repository = new RepositoryImpl<PersonalDataAggregate>("topic", PersonalDataAggregate.class, configuration);

        Assertions.assertAll("ChangelogConsumer",
                () -> Assertions.assertNotNull(repository),
                () -> Assertions.assertEquals("topic", repository.getBaseName())
        );
    }

    @DisplayName("Repository create ok")
    @Test
    public void repositoryCreate() throws AggregateDependenciesException, Exception {

        final DefaultProducer producer = PowerMockito.mock(DefaultProducer.class);
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(producer);
        PowerMockito.when(producer, "send", Mockito.any(), Mockito.any()).thenReturn(PowerMockito.mock(Future.class));

        final AppConfig configuration = ConfigBuilder.create();
        final HelperDomain helper = HelperDomain.create(configuration);
        helper.setReplayMode(false);

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(ChangelogHeaderType.UUID_KEY, new ByteArrayValue("key"));
        recordHeaders.add(CommandHeaderType.ENTITY_UUID_KEY, new ByteArrayValue("entityUUid"));
        recordHeaders.add(ChangelogHeaderType.AGGREGATE_UUID_KEY, new ByteArrayValue("euuid"));
        recordHeaders.add(ChangelogHeaderType.AGGREGATE_NAME_KEY, new ByteArrayValue("aggName"));
        recordHeaders.add(ChangelogHeaderType.AGGREGATE_METHOD_KEY, new ByteArrayValue("aggMethod"));
        recordHeaders.add(CommonHeaderType.FLAG_REPLAY_KEY, new ByteArrayValue(false));
        recordHeaders.add(CommonHeaderType.TYPE_KEY, new ByteArrayValue("type-key"));

        final Repository repository = new Repository("topic", PersonalDataAggregate.class, configuration);
        final AggregateBase aggregateBase = repository.create("key", new PersonalData(), new CommandRecord("topic", 1, 1,
                new Date().getTime(), TimestampType.CREATE_TIME, "key",
                new PersonalData(), recordHeaders), new DefaultProducerCallback());

        Assertions.assertAll("ChangelogConsumer",
                () -> Assertions.assertNotNull(repository),
                () -> Assertions.assertNotNull(aggregateBase.getData()),
                () -> Assertions.assertNotNull(aggregateBase.getId()),
                () -> Assertions.assertNotNull(aggregateBase.getValueClass()),
                () -> Assertions.assertEquals("topic", repository.getBaseName())
        );
    }

    @DisplayName("Repository create without key ok")
    @Test
    public void repositoryCreateWithoutKey() throws AggregateDependenciesException, Exception {

        final DefaultProducer producer = PowerMockito.mock(DefaultProducer.class);
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(producer);
        PowerMockito.when(producer, "send", Mockito.any(), Mockito.any()).thenReturn(PowerMockito.mock(Future.class));

        final AppConfig configuration = ConfigBuilder.create();
        final HelperDomain helper = HelperDomain.create(configuration);
        helper.setReplayMode(false);

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(ChangelogHeaderType.UUID_KEY, new ByteArrayValue("key"));
        recordHeaders.add(CommandHeaderType.ENTITY_UUID_KEY, new ByteArrayValue("entityUUid"));
        recordHeaders.add(ChangelogHeaderType.AGGREGATE_UUID_KEY, new ByteArrayValue("euuid"));
        recordHeaders.add(ChangelogHeaderType.AGGREGATE_NAME_KEY, new ByteArrayValue("aggName"));
        recordHeaders.add(ChangelogHeaderType.AGGREGATE_METHOD_KEY, new ByteArrayValue("aggMethod"));
        recordHeaders.add(CommonHeaderType.FLAG_REPLAY_KEY, new ByteArrayValue(false));
        recordHeaders.add(CommonHeaderType.TYPE_KEY, new ByteArrayValue("type-key"));

        final Repository repository = new Repository("topic", PersonalDataAggregate.class, configuration);
        final AggregateBase aggregateBase = repository.create(null, new PersonalData(), new CommandRecord("topic", 1, 1,
                new Date().getTime(), TimestampType.CREATE_TIME, "key",
                new PersonalData(), recordHeaders), new DefaultProducerCallback());

        Assertions.assertAll("ChangelogConsumer",
                () -> Assertions.assertNotNull(repository),
                () -> Assertions.assertNotNull(aggregateBase.getData()),
                () -> Assertions.assertNotNull(aggregateBase.getId()),
                () -> Assertions.assertNotNull(aggregateBase.getValueClass()),
                () -> Assertions.assertEquals("topic", repository.getBaseName())
        );
    }

    @DisplayName("Repository create ok")
    @Test
    public void repositoryCreateWithoutReference() throws AggregateDependenciesException, Exception {

        final DefaultProducer producer = PowerMockito.mock(DefaultProducer.class);
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(producer);
        PowerMockito.when(producer, "send", Mockito.any(), Mockito.any()).thenReturn(PowerMockito.mock(Future.class));

        final AppConfig configuration = ConfigBuilder.create();
        final HelperDomain helper = HelperDomain.create(configuration);
        helper.setReplayMode(false);

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(ChangelogHeaderType.UUID_KEY, new ByteArrayValue("key"));
        recordHeaders.add(CommandHeaderType.ENTITY_UUID_KEY, new ByteArrayValue("entityUUid"));
        recordHeaders.add(ChangelogHeaderType.AGGREGATE_UUID_KEY, new ByteArrayValue("euuid"));
        recordHeaders.add(ChangelogHeaderType.AGGREGATE_NAME_KEY, new ByteArrayValue("aggName"));
        recordHeaders.add(ChangelogHeaderType.AGGREGATE_METHOD_KEY, new ByteArrayValue("aggMethod"));
        recordHeaders.add(CommonHeaderType.FLAG_REPLAY_KEY, new ByteArrayValue(false));
        recordHeaders.add(CommonHeaderType.TYPE_KEY, new ByteArrayValue("type-key"));

        final Repository repository = new Repository("topic", PersonalDataAggregate.class, configuration);
        final AggregateBase aggregateBase = repository.create("key", new PersonalData(), null, new DefaultProducerCallback());

        Assertions.assertAll("ChangelogConsumer",
                () -> Assertions.assertNotNull(repository),
                () -> Assertions.assertNotNull(aggregateBase.getData()),
                () -> Assertions.assertNotNull(aggregateBase.getId()),
                () -> Assertions.assertNotNull(aggregateBase.getValueClass()),
                () -> Assertions.assertEquals("topic", repository.getBaseName())
        );
    }


    @DisplayName("Repository save ok")
    @Test
    public void repositorySave() throws AggregateDependenciesException, Exception {

        final DefaultProducer producer = PowerMockito.mock(DefaultProducer.class);
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(producer);
        PowerMockito.when(producer, "send", Mockito.any(), Mockito.any()).thenReturn(PowerMockito.mock(Future.class));

        final AppConfig configuration = ConfigBuilder.create();
        final HelperDomain helper = HelperDomain.create(configuration);
        helper.setReplayMode(false);

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(ChangelogHeaderType.UUID_KEY, new ByteArrayValue("key"));
        recordHeaders.add(CommandHeaderType.ENTITY_UUID_KEY, new ByteArrayValue("entityUUid"));
        recordHeaders.add(ChangelogHeaderType.AGGREGATE_UUID_KEY, new ByteArrayValue("euuid"));
        recordHeaders.add(ChangelogHeaderType.AGGREGATE_NAME_KEY, new ByteArrayValue("aggName"));
        recordHeaders.add(ChangelogHeaderType.AGGREGATE_METHOD_KEY, new ByteArrayValue("aggMethod"));
        recordHeaders.add(CommonHeaderType.FLAG_REPLAY_KEY, new ByteArrayValue(false));

        final Repository repository = new Repository("topic", PersonalDataAggregate.class, configuration);
        final AggregateBase aggregateBase = repository.loadFromStore("key");

        Assertions.assertAll("ChangelogConsumer",
                () -> Assertions.assertNotNull(repository),
                () -> Assertions.assertNull(aggregateBase)
        );
    }*/

}

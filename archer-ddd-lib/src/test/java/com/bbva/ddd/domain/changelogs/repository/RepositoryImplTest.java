package com.bbva.ddd.domain.changelogs.repository;

import com.bbva.common.config.ConfigBuilder;
import com.bbva.common.exceptions.ApplicationException;
import com.bbva.common.producers.DefaultProducer;
import com.bbva.common.producers.records.SpecificRecordBaseImpl;
import com.bbva.common.util.PowermockExtension;
import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.CommandHeaderType;
import com.bbva.common.utils.headers.types.CommonHeaderType;
import com.bbva.ddd.domain.changelogs.aggregate.PersonalAggregate;
import com.bbva.ddd.domain.changelogs.aggregate.PersonalDataAggregate;
import com.bbva.ddd.domain.changelogs.repository.aggregates.AggregateBase;
import com.bbva.ddd.domain.commands.consumers.CommandRecord;
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

import java.util.Date;
import java.util.concurrent.Future;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({RepositoryImpl.class})
public class RepositoryImplTest {

    @DisplayName("Create repository ok")
    @Test
    public void createRepository() throws Exception {
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));

        ConfigBuilder.create();
        final Repository repository = new RepositoryImpl<>(null, false);

        Assertions.assertNotNull(repository);
    }

    @DisplayName("Load from repository ok")
    @Test
    public void loadRepository() throws Exception {
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));

        ConfigBuilder.create();
        final Repository repository = new RepositoryImpl<>(null, false);
        repository.load(PersonalDataAggregate.class, "key");
        Assertions.assertNotNull(repository);
    }

    @DisplayName("Create repository ok")
    @Test
    public void createRepositoryData() throws Exception {

        final DefaultProducer producer = PowerMockito.mock(DefaultProducer.class);
        PowerMockito.when(producer, "send", Mockito.any(), Mockito.any()).thenReturn(PowerMockito.mock(Future.class));

        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(producer);


        ConfigBuilder.create();

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(CommandHeaderType.NAME_KEY, new ByteArrayValue("create"));
        recordHeaders.add(CommandHeaderType.UUID_KEY, new ByteArrayValue("key"));
        recordHeaders.add(CommandHeaderType.ENTITY_UUID_KEY, new ByteArrayValue("euid"));
        recordHeaders.add(CommonHeaderType.TYPE_KEY, new ByteArrayValue("key"));
        recordHeaders.add(CommonHeaderType.FLAG_REPLAY_KEY, new ByteArrayValue(true));

        final CommandRecord referenceRecord = new CommandRecord("topic", 1, 1, new Date().getTime(),
                TimestampType.CREATE_TIME, "key", null, recordHeaders);
        final Repository repository = new RepositoryImpl<>(referenceRecord, false);
        repository.create(PersonalDataAggregate.class, new SpecificRecordBaseImpl(), null);
        Assertions.assertNotNull(repository);
    }

    @DisplayName("Create repository ok")
    @Test
    public void createRepositoryreplayData() throws Exception {

        final DefaultProducer producer = PowerMockito.mock(DefaultProducer.class);
        PowerMockito.when(producer, "send", Mockito.any(), Mockito.any()).thenReturn(PowerMockito.mock(Future.class));

        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(producer);


        ConfigBuilder.create();

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(CommandHeaderType.NAME_KEY, new ByteArrayValue("create"));
        recordHeaders.add(CommandHeaderType.UUID_KEY, new ByteArrayValue("key"));
        recordHeaders.add(CommandHeaderType.ENTITY_UUID_KEY, new ByteArrayValue("euid"));
        recordHeaders.add(CommonHeaderType.TYPE_KEY, new ByteArrayValue("key"));
        recordHeaders.add(CommonHeaderType.FLAG_REPLAY_KEY, new ByteArrayValue(true));

        final CommandRecord referenceRecord = new CommandRecord("topic", 1, 1, new Date().getTime(),
                TimestampType.CREATE_TIME, "key", null, recordHeaders);
        final Repository repository = new RepositoryImpl<>(referenceRecord, true);
        repository.create(PersonalDataAggregate.class, new SpecificRecordBaseImpl(), null);
        Assertions.assertNotNull(repository);
    }

    @DisplayName("Create and load repository ok")
    @Test
    public void createAndLoadRepository() throws Exception {

        final DefaultProducer producer = PowerMockito.mock(DefaultProducer.class);
        PowerMockito.when(producer, "send", Mockito.any(), Mockito.any()).thenReturn(PowerMockito.mock(Future.class));

        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(producer);


        ConfigBuilder.create();

        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(CommandHeaderType.NAME_KEY, new ByteArrayValue("create"));
        recordHeaders.add(CommandHeaderType.UUID_KEY, new ByteArrayValue("key"));
        recordHeaders.add(CommandHeaderType.ENTITY_UUID_KEY, new ByteArrayValue("euid"));
        recordHeaders.add(CommonHeaderType.TYPE_KEY, new ByteArrayValue("key"));
        recordHeaders.add(CommonHeaderType.FLAG_REPLAY_KEY, new ByteArrayValue(true));

        final CommandRecord referenceRecord = new CommandRecord("topic", 1, 1, new Date().getTime(),
                TimestampType.CREATE_TIME, "key", new SpecificRecordBaseImpl(), recordHeaders);
        final Repository repository = new RepositoryImpl<>(referenceRecord, false);
        repository.create(PersonalDataAggregate.class, "key", new SpecificRecordBaseImpl(), null);
        repository.create(PersonalDataAggregate.class, "key", new SpecificRecordBaseImpl(), null);

        final AggregateBase aggregateBase = repository.load(PersonalDataAggregate.class, "key");

        Assertions.assertNotNull(aggregateBase);
    }

    @DisplayName("Create repository without annotationok")
    @Test
    public void createRepositoryko() throws Exception {

        final DefaultProducer producer = PowerMockito.mock(DefaultProducer.class);
        PowerMockito.when(producer, "send", Mockito.any(), Mockito.any()).thenReturn(PowerMockito.mock(Future.class));

        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(producer);

        ConfigBuilder.create();
        final RecordHeaders recordHeaders = new RecordHeaders();
        recordHeaders.add(CommandHeaderType.NAME_KEY, new ByteArrayValue("create"));
        recordHeaders.add(CommandHeaderType.UUID_KEY, new ByteArrayValue("key"));
        recordHeaders.add(CommandHeaderType.ENTITY_UUID_KEY, new ByteArrayValue("euid"));
        recordHeaders.add(CommonHeaderType.TYPE_KEY, new ByteArrayValue("key"));
        recordHeaders.add(CommonHeaderType.FLAG_REPLAY_KEY, new ByteArrayValue(true));

        final CommandRecord referenceRecord = new CommandRecord("topic", 1, 1, new Date().getTime(),
                TimestampType.CREATE_TIME, "key", null, recordHeaders);
        final Repository repository = new RepositoryImpl<>(referenceRecord, false);

        Assertions.assertThrows(ApplicationException.class, () ->
                repository.create(PersonalAggregate.class, new SpecificRecordBaseImpl(), null)
        );
    }

    @DisplayName("Load from invalid repository ok")
    @Test
    public void loadRepositoryKo() throws Exception {
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));

        ConfigBuilder.create();
        final Repository repository = new RepositoryImpl<>(null, false);

        Assertions.assertThrows(ApplicationException.class, () ->
                repository.load(PersonalAggregate.class, "key")
        );
    }
}

package com.bbva.ddd.domain.commands.read;

import com.bbva.common.config.AppConfiguration;
import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.producers.CachedProducer;
import com.bbva.common.util.PowermockExtension;
import com.bbva.common.utils.ByteArrayValue;
import com.bbva.common.utils.headers.RecordHeaders;
import com.bbva.common.utils.headers.types.ChangelogHeaderType;
import com.bbva.common.utils.headers.types.CommandHeaderType;
import com.bbva.common.utils.headers.types.CommonHeaderType;
import com.bbva.ddd.domain.HelperDomain;
import com.bbva.ddd.domain.aggregates.AggregateBase;
import com.bbva.ddd.domain.aggregates.exceptions.AggregateDependenciesException;
import com.bbva.ddd.domain.callback.DefaultProducerCallback;
import com.bbva.ddd.domain.changelogs.Repository;
import com.bbva.ddd.domain.changelogs.aggregate.PersonalDataAggregate;
import com.bbva.ddd.domain.commands.write.records.PersonalData;
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
@PrepareForTest({CachedProducer.class, Repository.class})
public class RepositoryTest {

    @DisplayName("Create repository ok")
    @Test
    public void createRepository() throws AggregateDependenciesException {
        final ApplicationConfig configuration = new AppConfiguration().init();
        final Repository repository = new Repository("topic", PersonalDataAggregate.class, configuration);

        Assertions.assertAll("ChangelogConsumer",
                () -> Assertions.assertNotNull(repository),
                () -> Assertions.assertEquals("topic", repository.getBaseName())
        );
    }

    @DisplayName("Repository create ok")
    @Test
    public void repositoryCreate() throws AggregateDependenciesException, Exception {

        final CachedProducer producer = PowerMockito.mock(CachedProducer.class);
        PowerMockito.whenNew(CachedProducer.class).withAnyArguments().thenReturn(producer);
        PowerMockito.when(producer, "add", Mockito.any(), Mockito.any()).thenReturn(PowerMockito.mock(Future.class));

        final ApplicationConfig configuration = new AppConfiguration().init();
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
        final AggregateBase aggregateBase = repository.create("key", new PersonalData(), new CommandRecord("topic", 1, 1,
                new Date().getTime(), TimestampType.CREATE_TIME, null,
                new PersonalData(), recordHeaders), new DefaultProducerCallback());

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

        final CachedProducer producer = PowerMockito.mock(CachedProducer.class);
        PowerMockito.whenNew(CachedProducer.class).withAnyArguments().thenReturn(producer);
        PowerMockito.when(producer, "add", Mockito.any(), Mockito.any()).thenReturn(PowerMockito.mock(Future.class));

        final ApplicationConfig configuration = new AppConfiguration().init();
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
    }

}

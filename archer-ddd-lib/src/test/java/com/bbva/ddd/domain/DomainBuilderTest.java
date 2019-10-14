package com.bbva.ddd.domain;

import com.bbva.common.config.AppConfig;
import com.bbva.common.producers.CachedProducer;
import com.bbva.common.util.PowermockExtension;
import com.bbva.common.utils.TopicManager;
import com.bbva.dataprocessors.DataProcessor;
import com.bbva.dataprocessors.builders.dataflows.DataflowBuilder;
import com.bbva.dataprocessors.builders.sql.queries.CreateStreamQueryBuilder;
import com.bbva.ddd.domain.changelogs.consumers.ChangelogConsumer;
import com.bbva.ddd.domain.changelogs.repository.Repository;
import com.bbva.ddd.domain.commands.consumers.CommandConsumer;
import com.bbva.ddd.domain.events.consumers.EventConsumer;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.util.concurrent.Executors;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({TopicManager.class, DataProcessor.class, Executors.class, DomainBuilder.class, CachedProducer.class, Repository.class})
public class DomainBuilderTest {

    @DisplayName("Create domain ok")
    @Test
    public void createDomain() throws Exception {
        PowerMockito.whenNew(DataProcessor.class).withAnyArguments().thenReturn(PowerMockito.mock(DataProcessor.class));
        PowerMockito.whenNew(CachedProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(CachedProducer.class));

        final Domain domain = DomainBuilder.create(new AppConfig());
        final Domain domainCached = DomainBuilder.get();
        Assertions.assertAll("DomainBuilder",
                () -> Assertions.assertEquals(domain, domainCached)
        );
    }

    @DisplayName("Create domain with get method ok")
    @Test
    public void getCreateDomain() throws Exception {
        PowerMockito.whenNew(DataProcessor.class).withAnyArguments().thenReturn(PowerMockito.mock(DataProcessor.class));
        PowerMockito.whenNew(CachedProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(CachedProducer.class));

        Assertions.assertThrows(NullPointerException.class, () -> DomainBuilder.get());
    }

    @DisplayName("Create domain ko")
    @Test
    public void getCreateDomainWithoutConfig() throws Exception {
        PowerMockito.whenNew(DataProcessor.class).withAnyArguments().thenReturn(PowerMockito.mock(DataProcessor.class));
        PowerMockito.whenNew(CachedProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(CachedProducer.class));

        Assertions.assertThrows(NullPointerException.class, () -> DomainBuilder.get());
    }

    @DisplayName("Add processors to domain ok")
    @Test
    public void addProcessors() throws Exception {

        PowerMockito.whenNew(DataProcessor.class).withAnyArguments().thenReturn(PowerMockito.mock(DataProcessor.class));
        PowerMockito.whenNew(CachedProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(CachedProducer.class));

        Domain domain = DomainBuilder.create(new AppConfig());
        domain = domain.addDataProcessorBuilder(new CreateStreamQueryBuilder("query-builder"));
        domain = domain.addDataProcessorBuilder("dataflow", PowerMockito.mock(DataflowBuilder.class));
        domain = domain.addEntityStateProcessor("entity", String.class);

        Assertions.assertNotNull(domain);

    }

    @DisplayName("Index field and group by ok")
    @Test
    public void indexFieldAndGroupBy() throws Exception {

        PowerMockito.whenNew(DataProcessor.class).withAnyArguments().thenReturn(PowerMockito.mock(DataProcessor.class));
        PowerMockito.whenNew(CachedProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(CachedProducer.class));

        Domain domain = DomainBuilder.create(new AutoConfiguredHandler(), new AppConfig());
        domain = domain.indexFieldStateProcessor("storeName", "sourceStreamName", "fieldPath",
                String.class, String.class);
        domain = domain.groupByFieldStateProcessor("storeName", "sourceStreamName", "fieldPath",
                null, null);

        Assertions.assertNotNull(domain);

    }

    @DisplayName("Start domain ok")
    @Test
    public void startDomian() throws Exception {
        PowerMockito.whenNew(DataProcessor.class).withAnyArguments().thenReturn(PowerMockito.mock(DataProcessor.class));
        PowerMockito.whenNew(CachedProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(CachedProducer.class));
        PowerMockito.whenNew(CommandConsumer.class).withAnyArguments().thenReturn(PowerMockito.mock(CommandConsumer.class));
        PowerMockito.whenNew(EventConsumer.class).withAnyArguments().thenReturn(PowerMockito.mock(EventConsumer.class));
        PowerMockito.whenNew(ChangelogConsumer.class).withAnyArguments().thenReturn(PowerMockito.mock(ChangelogConsumer.class));

        PowerMockito.mockStatic(TopicManager.class);

        final Domain domain = DomainBuilder.create(new AutoConfiguredHandler(), new AppConfig());
        final HelperDomain helperDomain = domain.start();
        Assertions.assertAll("DomainBuilder",
                () -> Assertions.assertNotNull(helperDomain)
        );
    }
}

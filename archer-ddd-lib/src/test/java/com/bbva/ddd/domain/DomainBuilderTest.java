package com.bbva.ddd.domain;

import com.bbva.common.config.AppConfig;
import com.bbva.common.config.ConfigBuilder;
import com.bbva.common.consumers.adapters.RunnableConsumerAdapter;
import com.bbva.common.producers.DefaultProducer;
import com.bbva.common.util.PowermockExtension;
import com.bbva.common.utils.TopicManager;
import com.bbva.dataprocessors.DataProcessor;
import com.bbva.dataprocessors.builders.dataflows.DataflowBuilder;
import com.bbva.dataprocessors.builders.sql.queries.CreateStreamQueryBuilder;
import com.bbva.ddd.application.ApplicationHelper;
import com.bbva.ddd.domain.changelogs.repository.Repository;
import com.bbva.ddd.domain.handlers.AutoConfiguredHandler;
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
@PrepareForTest({TopicManager.class, DataProcessor.class, Executors.class, Repository.class, Domain.class, Binder.class, ApplicationHelper.class})
public class DomainBuilderTest {

    @DisplayName("Create domain ok")
    @Test
    public void createDomain() throws Exception {
        PowerMockito.whenNew(DataProcessor.class).withAnyArguments().thenReturn(PowerMockito.mock(DataProcessor.class));
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));
        PowerMockito.whenNew(RunnableConsumerAdapter.class).withAnyArguments().thenReturn(PowerMockito.mock(RunnableConsumerAdapter.class));
        PowerMockito.mockStatic(TopicManager.class);

        final Domain domain = Domain.Builder.create(new AppConfig()).build();
        Assertions.assertAll("DomainBuilder",
                () -> Assertions.assertNotNull(domain)
        );
    }

    @DisplayName("Create domain ok")
    @Test
    public void createDomainWithoutConfig() throws Exception {
        PowerMockito.whenNew(DataProcessor.class).withAnyArguments().thenReturn(PowerMockito.mock(DataProcessor.class));
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));
        PowerMockito.whenNew(RunnableConsumerAdapter.class).withAnyArguments().thenReturn(PowerMockito.mock(RunnableConsumerAdapter.class));
        PowerMockito.mockStatic(TopicManager.class);

        ConfigBuilder.create();
        final Domain domain = Domain.Builder.create().build();
        Assertions.assertAll("DomainBuilder",
                () -> Assertions.assertNotNull(domain)
        );
    }

    @DisplayName("Add processors to domain ok")
    @Test
    public void addProcessors() throws Exception {

        PowerMockito.whenNew(DataProcessor.class).withAnyArguments().thenReturn(PowerMockito.mock(DataProcessor.class));
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));
        PowerMockito.whenNew(RunnableConsumerAdapter.class).withAnyArguments().thenReturn(PowerMockito.mock(RunnableConsumerAdapter.class));
        PowerMockito.mockStatic(TopicManager.class);

        Domain.Builder domain = Domain.Builder.create(new AppConfig());
        domain = domain.addDataProcessorBuilder(new CreateStreamQueryBuilder("query-builder"));
        domain = domain.addDataProcessorBuilder("dataflow", PowerMockito.mock(DataflowBuilder.class));
        domain = domain.addEntityStateProcessor("entity", String.class);

        Assertions.assertNotNull(domain.build());

    }

    @DisplayName("Index field and group by ok")
    @Test
    public void indexFieldAndGroupBy() throws Exception {

        PowerMockito.whenNew(DataProcessor.class).withAnyArguments().thenReturn(PowerMockito.mock(DataProcessor.class));
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));
        PowerMockito.whenNew(RunnableConsumerAdapter.class).withAnyArguments().thenReturn(PowerMockito.mock(RunnableConsumerAdapter.class));
        PowerMockito.mockStatic(TopicManager.class);

        Domain.Builder domain = Domain.Builder.create(new AppConfig())
                .handler(new AutoConfiguredHandler());

        domain = domain.indexFieldStateProcessor("storeName", "sourceStreamName", "fieldPath",
                String.class, String.class);
        domain = domain.groupByFieldStateProcessor("storeName", "sourceStreamName", "fieldPath",
                null, null);

        Assertions.assertNotNull(domain.build());

    }

    @DisplayName("Start domain ok")
    @Test
    public void startDomian() throws Exception {
        PowerMockito.whenNew(DataProcessor.class).withAnyArguments().thenReturn(PowerMockito.mock(DataProcessor.class));
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));
        PowerMockito.whenNew(RunnableConsumerAdapter.class).withAnyArguments().thenReturn(PowerMockito.mock(RunnableConsumerAdapter.class));
        PowerMockito.mockStatic(TopicManager.class);

        final Domain domain = Domain.Builder.create(new AppConfig())
                .handler(new AutoConfiguredHandler())
                .build();

        domain.start();

        Assertions.assertAll("DomainBuilder",
                () -> Assertions.assertNotNull(domain)
        );
    }
}

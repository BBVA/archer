package com.bbva.ddd.domain;

import com.bbva.common.config.AppConfig;
import com.bbva.common.consumers.adapters.RunnableConsumerAdapter;
import com.bbva.common.consumers.record.CRecord;
import com.bbva.common.producers.DefaultProducer;
import com.bbva.common.producers.Producer;
import com.bbva.common.util.PowermockExtension;
import com.bbva.common.utils.TopicManager;
import com.bbva.dataprocessors.DataProcessor;
import com.bbva.ddd.application.ApplicationHelper;
import com.bbva.ddd.domain.changelogs.repository.Repository;
import com.bbva.ddd.domain.handlers.AutoConfiguredHandler;
import com.bbva.ddd.domain.handlers.TestHandler;
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
public class BinderTest {

    @DisplayName("Create binder ok")
    @Test
    public void createBinder() {
        final Binder binder = Binder.create(new AppConfig());
        Assertions.assertAll("Binder",
                () -> Assertions.assertNotNull(binder)
        );
    }

    @DisplayName("Create binder ok")
    @Test
    public void createBinderWithoutConfig() throws Exception {
        PowerMockito.whenNew(RunnableConsumerAdapter.class).withAnyArguments().thenReturn(PowerMockito.mock(RunnableConsumerAdapter.class));
        PowerMockito.mockStatic(TopicManager.class);


        final Binder binder = Binder.create(new AppConfig());
        Binder.get().build(new TestHandler());
        Assertions.assertAll("Binder",
                () -> Assertions.assertNotNull(binder)
        );
    }

    @DisplayName("Start binder ok")
    @Test
    public void startBinder() throws Exception {
        PowerMockito.whenNew(DataProcessor.class).withAnyArguments().thenReturn(PowerMockito.mock(DataProcessor.class));
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));
        PowerMockito.whenNew(RunnableConsumerAdapter.class).withAnyArguments().thenReturn(PowerMockito.mock(RunnableConsumerAdapter.class));
        PowerMockito.mockStatic(TopicManager.class);

        final Binder binder = Binder.create(new AppConfig())
                .build(new AutoConfiguredHandler());

        binder.start();

        Assertions.assertAll("Binder",
                () -> Assertions.assertNotNull(binder)
        );
    }

    @DisplayName("Process binder events ok")
    @Test
    public void processEvents() throws Exception {
        PowerMockito.whenNew(DataProcessor.class).withAnyArguments().thenReturn(PowerMockito.mock(DataProcessor.class));
        PowerMockito.whenNew(DefaultProducer.class).withAnyArguments().thenReturn(PowerMockito.mock(DefaultProducer.class));
        PowerMockito.whenNew(RunnableConsumerAdapter.class).withAnyArguments().thenReturn(PowerMockito.mock(RunnableConsumerAdapter.class));
        PowerMockito.mockStatic(TopicManager.class);

        final Binder binder = Binder.create(new AppConfig())
                .build(PowerMockito.mock(AutoConfiguredHandler.class));

        binder.processCommand(PowerMockito.mock(CRecord.class), PowerMockito.mock(Producer.class), true);
        binder.processEvent(PowerMockito.mock(CRecord.class), PowerMockito.mock(Producer.class), true);
        binder.processChangelog(PowerMockito.mock(CRecord.class), PowerMockito.mock(Producer.class), true);

        Assertions.assertAll("Binder",
                () -> Assertions.assertNotNull(binder)
        );
    }
}

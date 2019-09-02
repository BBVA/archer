package com.bbva.dataprocessors.builders.dataflows;

import com.bbva.common.config.ApplicationConfig;
import com.bbva.common.exceptions.ApplicationException;
import com.bbva.common.util.PowermockExtension;
import com.bbva.common.utils.TopicManager;
import com.bbva.common.utils.serdes.SpecificAvroSerde;
import com.bbva.dataprocessors.builders.dataflows.states.EntityStateBuilder;
import com.bbva.dataprocessors.contexts.dataflow.DataflowProcessorContext;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({EntityStateBuilder.class, DataflowProcessorBuilder.class, TopicManager.class, StreamsBuilder.class})
public class EntityStateBuilderTest {

    @DisplayName("Create Dataflow processor and build")
    @Test
    public void createDataProcessorBuild() {

        Assertions.assertThrows(ApplicationException.class, () -> {
            final EntityStateBuilder entityStateBuilder = new EntityStateBuilder("snapshot", String.class);
            final DataflowProcessorBuilder dataflowProcessorBuilder = new DataflowProcessorBuilder(entityStateBuilder);

            final DataflowProcessorContext context = PowerMockito.mock(DataflowProcessorContext.class);
            final StreamsBuilder streamsBuilder = PowerMockito.mock(StreamsBuilder.class);

            PowerMockito.when(context, "schemaRegistryClient").thenReturn(PowerMockito.mock(CachedSchemaRegistryClient.class));
            PowerMockito.when(context, "streamsBuilder").thenReturn(streamsBuilder);
            PowerMockito.doThrow(new ApplicationException()).when(streamsBuilder, "addStateStore", Mockito.any());

            PowerMockito.whenNew(SpecificAvroSerde.class).withAnyArguments().thenReturn(PowerMockito.mock(SpecificAvroSerde.class));
            PowerMockito.mockStatic(TopicManager.class);

            dataflowProcessorBuilder.init(context);
            dataflowProcessorBuilder.build();
            dataflowProcessorBuilder.start();
        });

    }

    @DisplayName("Create Dataflow processor and start")
    @Test
    public void createDataProcessorStart() throws Exception {

        final EntityStateBuilder entityStateBuilder = new EntityStateBuilder("snapshot", String.class);
        final DataflowProcessorBuilder dataflowProcessorBuilder = new DataflowProcessorBuilder(entityStateBuilder);

        final DataflowProcessorContext context = PowerMockito.mock(DataflowProcessorContext.class);
        final StreamsBuilder streamsBuilder = PowerMockito.mock(StreamsBuilder.class);

        PowerMockito.when(context, "schemaRegistryClient").thenReturn(PowerMockito.mock(CachedSchemaRegistryClient.class));
        PowerMockito.when(context, "streamsBuilder").thenReturn(streamsBuilder);
        PowerMockito.when(context, "configs").thenReturn(new ApplicationConfig());
        PowerMockito.when(streamsBuilder, "build").thenReturn(new Topology());

        PowerMockito.whenNew(KafkaStreams.class).withAnyArguments().thenReturn(PowerMockito.mock(KafkaStreams.class));
        PowerMockito.mockStatic(TopicManager.class);

        dataflowProcessorBuilder.init(context);
        dataflowProcessorBuilder.start();

        Assertions.assertAll("dataProcessor",
                () -> Assertions.assertNotNull(dataflowProcessorBuilder)
        );
    }

}

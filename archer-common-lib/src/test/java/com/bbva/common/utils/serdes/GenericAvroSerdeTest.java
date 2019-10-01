package com.bbva.common.utils.serdes;

import com.bbva.common.util.PowermockExtension;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.api.extension.ExtendWith;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.util.Collections;

@RunWith(JUnit5.class)
@ExtendWith(PowermockExtension.class)
@PrepareForTest({GenericAvroSerializer.class, GenericAvroDeserializer.class})
public class GenericAvroSerdeTest {

    @DisplayName("Check specific avro serve")
    @Test
    public void createSerdeAndConfigureOk() throws Exception {
        PowerMockito.whenNew(KafkaAvroSerializer.class).withAnyArguments().thenReturn(PowerMockito.mock(KafkaAvroSerializer.class));
        PowerMockito.whenNew(KafkaAvroDeserializer.class).withAnyArguments().thenReturn(PowerMockito.mock(KafkaAvroDeserializer.class));

        final GenericAvroSerde genericAvroSerde = new GenericAvroSerde();
        final GenericAvroSerde genericAvroSerdeWithSchema = new GenericAvroSerde(PowerMockito.mock(SchemaRegistryClient.class));

        genericAvroSerde.configure(Collections.emptyMap(), true);
        genericAvroSerde.close();

        Assertions.assertNotEquals(genericAvroSerde, genericAvroSerdeWithSchema);
    }


    @DisplayName("Serialize and deserialize")
    @Test
    public void serializeAndDeserializeOk() throws Exception {
        PowerMockito.whenNew(KafkaAvroSerializer.class).withAnyArguments().thenReturn(PowerMockito.mock(KafkaAvroSerializer.class));
        PowerMockito.whenNew(KafkaAvroDeserializer.class).withAnyArguments().thenReturn(PowerMockito.mock(KafkaAvroDeserializer.class));

        final GenericAvroSerde genericAvroSerde = new GenericAvroSerde();

        genericAvroSerde.serializer();
        genericAvroSerde.deserializer();

        Assertions.assertNotNull(genericAvroSerde);
    }

    @DisplayName("Serializer and deserializer constructor")
    @Test
    public void serializerAndDeserializerOk() {

        final GenericAvroSerializer serializer = new GenericAvroSerializer(PowerMockito.mock(SchemaRegistryClient.class));

        final GenericAvroDeserializer deserializer = new GenericAvroDeserializer(PowerMockito.mock(SchemaRegistryClient.class));

        Assertions.assertNotNull(serializer);
        Assertions.assertNotNull(deserializer);
    }
}

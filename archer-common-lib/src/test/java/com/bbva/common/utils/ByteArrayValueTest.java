package com.bbva.common.utils;

import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;

@RunWith(JUnit5.class)
public class ByteArrayValueTest {

    @DisplayName("Check byte array value string")
    @Test
    public void checkByteArrayValueString() {
        final ByteArrayValue value = new ByteArrayValue("string");
        Assertions.assertEquals("string", value.asString());
    }

    @DisplayName("Check byte array value boolean")
    @Test
    public void checkByteArrayValueBoolean() {
        final ByteArrayValue value = new ByteArrayValue(true);
        Assertions.assertEquals(true, value.asBoolean());
    }

    @DisplayName("Check byte array value float")
    @Test
    public void checkByteArrayValueFloat() {
        final ByteArrayValue value = new ByteArrayValue(1F);
        Assertions.assertEquals(1F, value.asFloat());
    }

    @DisplayName("Check byte array value integer")
    @Test
    public void checkByteArrayValueInteger() {
        final ByteArrayValue value = new ByteArrayValue(1);
        Assertions.assertEquals(1, value.asInteger());
    }

    @DisplayName("Check byte array value long")
    @Test
    public void checkByteArrayValueLong() {
        final ByteArrayValue value = new ByteArrayValue(1L);
        Assertions.assertEquals(1L, value.asLong());
    }

}

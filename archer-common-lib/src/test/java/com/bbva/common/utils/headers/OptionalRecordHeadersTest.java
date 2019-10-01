package com.bbva.common.utils.headers;

import org.junit.gen5.api.Assertions;
import org.junit.gen5.api.DisplayName;
import org.junit.gen5.api.Test;
import org.junit.gen5.junit4.runner.JUnit5;
import org.junit.runner.RunWith;

import java.util.ArrayList;

@RunWith(JUnit5.class)
public class OptionalRecordHeadersTest {

    @DisplayName("Check optional record headers")
    @Test
    public void checkOptionalRecordHeaders() {
        final OptionalRecordHeaders recordHeaders = new OptionalRecordHeaders(new ArrayList<>());
        recordHeaders.addAck("all");
        recordHeaders.addOrigin("localhost");

        Assertions.assertAll("headertypes",
                () -> Assertions.assertEquals("all", recordHeaders.getAck()),
                () -> Assertions.assertEquals("localhost", recordHeaders.getOrigin()),
                () -> Assertions.assertNull(recordHeaders.getHeader("not-exists"))
        );
    }

}

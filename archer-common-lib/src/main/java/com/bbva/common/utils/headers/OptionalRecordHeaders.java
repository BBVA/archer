package com.bbva.common.utils.headers;

import com.bbva.common.utils.ByteArrayValue;
import org.apache.kafka.common.header.Header;

import java.util.List;

public class OptionalRecordHeaders extends RecordHeaders {

    private static final String CUSTOM_ORIGIN_KEY = "custom.origin";
    private static final String CUSTOM_ACK_KEY = "custom.ack";

    public OptionalRecordHeaders() {
        super();
    }

    public OptionalRecordHeaders(final List<Header> headers) {
        super(headers);
    }

    public OptionalRecordHeaders addOrigin(final String value) {
        headers.add(new RecordHeader(OptionalRecordHeaders.CUSTOM_ORIGIN_KEY, ByteArrayValue.Serde.serialize(value)));
        return this;
    }

    public OptionalRecordHeaders addAck(final String value) {
        headers.add(new RecordHeader(OptionalRecordHeaders.CUSTOM_ACK_KEY, ByteArrayValue.Serde.serialize(value)));
        return this;
    }

    public String getOrigin() {
        final ByteArrayValue value = find(OptionalRecordHeaders.CUSTOM_ORIGIN_KEY);
        return value != null ? value.asString() : null;
    }

    public String getAck() {
        final ByteArrayValue value = find(OptionalRecordHeaders.CUSTOM_ACK_KEY);
        return value != null ? value.asString() : null;
    }

}

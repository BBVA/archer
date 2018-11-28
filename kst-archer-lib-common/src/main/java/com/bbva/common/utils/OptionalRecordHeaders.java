package com.bbva.common.utils;

import org.apache.kafka.common.header.Header;

import java.util.List;

public class OptionalRecordHeaders extends RecordHeaders {

    private static String CUSTOM_ORIGIN_KEY = "custom.origin";
    private static String CUSTOM_ACK_KEY = "custom.ack";

    public OptionalRecordHeaders() {
    }

    public OptionalRecordHeaders(List<Header> headers) {
        super(headers);
    }

    public OptionalRecordHeaders addOrigin(String value) {
        headers.add(new RecordHeader(OptionalRecordHeaders.CUSTOM_ORIGIN_KEY, new GenericValue(value)));
        return this;
    }

    public OptionalRecordHeaders addAck(String value) {
        headers.add(new RecordHeader(OptionalRecordHeaders.CUSTOM_ACK_KEY, new GenericValue(value)));
        return this;
    }

    public String getOrigin() {
        GenericValue value = find(OptionalRecordHeaders.CUSTOM_ORIGIN_KEY);
        return value != null ? value.asString() : null;
    }

    public String getAck() {
        GenericValue value = find(OptionalRecordHeaders.CUSTOM_ACK_KEY);
        return value != null ? value.asString() : null;
    }

}

package com.bbva.common.utils.headers;

import com.bbva.common.utils.ByteArrayValue;
import org.apache.kafka.common.header.Header;

import java.util.List;

/**
 * Optional record headerds
 */
public class OptionalRecordHeaders extends RecordHeaders {

    private static final String CUSTOM_ORIGIN_KEY = "custom.origin";
    private static final String CUSTOM_ACK_KEY = "custom.ack";

    /**
     * Constructor
     *
     * @param headers list of optional headers
     */
    public OptionalRecordHeaders(final List<Header> headers) {
        super(headers);
    }

    /**
     * Add origin headers
     *
     * @param value value of the header
     * @return optional headers
     */
    public OptionalRecordHeaders addOrigin(final String value) {
        return addHeader(value, OptionalRecordHeaders.CUSTOM_ORIGIN_KEY);
    }

    /**
     * Add ack headers
     *
     * @param value value of the header
     * @return optional headers
     */
    public OptionalRecordHeaders addAck(final String value) {
        return addHeader(value, OptionalRecordHeaders.CUSTOM_ACK_KEY);
    }

    /**
     * Get origin header
     *
     * @return value of headers
     */
    public String getOrigin() {
        return getHeader(OptionalRecordHeaders.CUSTOM_ORIGIN_KEY);
    }

    /**
     * Get ack header
     *
     * @return value of headers
     */
    public String getAck() {
        return getHeader(OptionalRecordHeaders.CUSTOM_ACK_KEY);
    }

    protected String getHeader(final String headerKey) {
        final ByteArrayValue value = find(headerKey);
        return value != null ? value.asString() : null;
    }

    protected OptionalRecordHeaders addHeader(final String value, final String headerKey) {
        headers.add(new RecordHeader(headerKey, ByteArrayValue.Serde.serialize(value)));
        return this;
    }

}

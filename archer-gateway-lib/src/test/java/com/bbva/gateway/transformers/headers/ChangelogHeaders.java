package com.bbva.gateway.transformers.headers;


import com.bbva.common.utils.headers.RecordHeader;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.util.Iterator;

public class ChangelogHeaders implements Headers {

    private RecordHeader lastHeader;

    public void add(final RecordHeader header) {
        lastHeader = header;
    }

    @Override
    public Headers add(final Header header) throws IllegalStateException {
        return null;
    }

    @Override
    public Headers add(final String s, final byte[] bytes) throws IllegalStateException {
        return null;
    }

    @Override
    public Headers remove(final String s) throws IllegalStateException {
        return null;
    }

    @Override
    public Header lastHeader(final String s) {
        return lastHeader;
    }

    @Override
    public Iterable<Header> headers(final String s) {
        return null;
    }

    @Override
    public Header[] toArray() {
        return new Header[0];
    }

    @Override
    public Iterator<Header> iterator() {
        return null;
    }
}

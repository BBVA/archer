package com.bbva.gateway.consumer.headers;


import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.util.Iterator;

public class ChangelogHeaders implements Headers {


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
        return null;
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
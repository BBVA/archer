package com.bbva.dataprocessors.exceptions;

public class StoreNotFoundException extends RuntimeException {

    private static final long serialVersionUID = 4091407403590125246L;

    public StoreNotFoundException() {
        super();
    }

    public StoreNotFoundException(final String message) {
        super(message);
    }

}

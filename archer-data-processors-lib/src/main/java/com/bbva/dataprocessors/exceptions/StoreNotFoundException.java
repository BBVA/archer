package com.bbva.dataprocessors.exceptions;

/**
 * Custom store not found exception
 */
public class StoreNotFoundException extends RuntimeException {

    private static final long serialVersionUID = 4091407403590125246L;

    /**
     * Constructor
     */
    public StoreNotFoundException() {
        super();
    }

    /**
     * Constructor
     *
     * @param message exception message
     */
    public StoreNotFoundException(final String message) {
        super(message);
    }

}

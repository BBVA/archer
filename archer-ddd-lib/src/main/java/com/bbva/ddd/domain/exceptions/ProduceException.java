package com.bbva.ddd.domain.exceptions;

/**
 * Custom producer exception to manage errors in production
 */
public class ProduceException extends RuntimeException {

    private static final long serialVersionUID = -1872568827813815386L;

    /**
     * Constructor
     */
    public ProduceException() {
        super();
    }

    /**
     * Constructor
     *
     * @param message error message
     * @param cause   exception cause
     */
    public ProduceException(final String message, final Throwable cause) {
        super(message, cause);
    }

}

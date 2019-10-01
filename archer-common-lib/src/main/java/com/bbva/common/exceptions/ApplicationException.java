package com.bbva.common.exceptions;

/**
 * Custom application exception
 */
public class ApplicationException extends RuntimeException {

    private static final long serialVersionUID = -8250375143559017596L;

    /**
     * Constructor
     */
    public ApplicationException() {
        super();
    }

    /**
     * Constructor
     *
     * @param message message
     */
    public ApplicationException(final String message) {
        super(message);
    }

    /**
     * Constructor
     *
     * @param message message
     * @param cause   cause
     */
    public ApplicationException(final String message, final Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructor
     *
     * @param cause cause
     */
    public ApplicationException(final Throwable cause) {
        super(cause);
    }

}

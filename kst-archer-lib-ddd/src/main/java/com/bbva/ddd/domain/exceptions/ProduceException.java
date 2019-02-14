package com.bbva.ddd.domain.exceptions;

public class ProduceException extends RuntimeException {

    private static final long serialVersionUID = -1872568827813815386L;

    public ProduceException() {
        super();
    }

    public ProduceException(final String message) {
        super(message);
    }

    public ProduceException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public ProduceException(final Throwable cause) {
        super(cause);
    }
}
